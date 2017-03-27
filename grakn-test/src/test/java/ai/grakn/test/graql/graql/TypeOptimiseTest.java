package ai.grakn.test.graql.graql;

import ai.grakn.GraknGraph;
import ai.grakn.concept.Concept;
import ai.grakn.concept.TypeName;
import ai.grakn.graphs.MovieGraph;
import ai.grakn.graql.Graql;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.Var;
import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.Disjunction;
import ai.grakn.graql.admin.MatchQueryAdmin;
import ai.grakn.graql.admin.PatternAdmin;
import ai.grakn.graql.admin.RelationPlayer;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.pattern.property.IsaProperty;
import ai.grakn.graql.internal.pattern.property.RelationProperty;
import ai.grakn.test.GraphContext;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static ai.grakn.graql.Graql.and;
import static ai.grakn.graql.Graql.match;
import static ai.grakn.graql.Graql.var;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 *
 */
public class TypeOptimiseTest {

    private GraknGraph graph = rule.graph();

    @ClassRule
    public static final GraphContext rule = GraphContext.preLoad(MovieGraph.get());

    @Before
    public void setUp() {
    }

    @Test
    public void testABunchOfQueriesToEnsureNotWorse() {

        MatchQuery initialQuery = match(var("y").isa("person"), var("r").rel("actor","y").rel(var("a"),"x"));
        testExpandedQueryIsEquivalent(initialQuery);

        initialQuery = match(var("x").isa("movie"), var("r").rel("actor","y").rel(var("a"),"x"));
        testExpandedQueryIsEquivalent(initialQuery);

        initialQuery = match(var("x").isa("movie").has("title","Godfather"), var("r").rel("actor","y").rel(var("a"),"x"));
        testExpandedQueryIsEquivalent(initialQuery);

        initialQuery = match(var("x").isa(var("y")), var().rel("production-with-cast","x"));
        testExpandedQueryIsEquivalent(initialQuery);
    }

    public void testExpandedQueryIsEquivalent(MatchQuery initialQuery) {
        System.out.println("The initial query: "+initialQuery.toString());
        MatchQuery expandedQuery = generateOntologyQueryAndMap(initialQuery);
        System.out.println("The expanded query: "+expandedQuery.toString());
        List<Map<String, Concept>> results = initialQuery.withGraph(graph).execute();
        List<Map<String, Concept>> expandedResults = expandedQuery.withGraph(graph).execute();

        System.out.println("initial results:");
        printResults(results);
        System.out.println("expanded results:");
        printResults(expandedResults);

        assertEquals(results.size(), expandedResults.size());
        results.forEach(result-> assertTrue(expandedResults.stream()
                .map(expandedResult -> isUnionOfVarsEqual(result, expandedResult))
                .reduce(false, (a, b) -> a || b)));
    }

    private boolean isUnionOfVarsEqual(Map<String,Concept> result1, Map<String,Concept> result2) {
        return result1.entrySet().stream()
                .map((x)-> result2.get(x.getKey())==x.getValue())
                .reduce(true,(a,b)-> a & b);
    }

    private void printResults(List<Map<String,Concept>> results) {
        results.forEach(result -> {
            result.forEach((k,v) -> System.out.print(" "+k+" "+v.toString()));
            System.out.println();
        });
    }

    private MatchQuery generateOntologyQueryAndMap(MatchQuery initialQuery) {
        Set<Var> ontologyQuery = new HashSet<>();
        Set<VarName> knownTypes = new HashSet<>();
        Set<VarName> unknownRoles = new HashSet<>();
        // take the query and break into parts that can be executed independently
        MatchQueryAdmin initialQueryAdmin = initialQuery.admin();
        Conjunction<PatternAdmin> initialPatterns = initialQueryAdmin.getPattern();
        Disjunction<Conjunction<VarAdmin>> initialNormalForm = initialPatterns.getDisjunctiveNormalForm();
        // go through independent queries looking for types
        initialNormalForm.getPatterns().iterator().forEachRemaining(aVarAdmin -> {
            // for each query obtain ALL the vars in a list and consider in turn
            // TODO: absorb this call into the disjunctive normal form method
            aVarAdmin.getVars().iterator().forEachRemaining(aVar -> {
                // get type if it has been specified
                getTypeVar(aVar, knownTypes).ifPresent(ontologyQuery::add);
                // if there is a relation property attached to the var it is a relation - current starting point
                Optional<RelationProperty> relation = aVar.getProperty(RelationProperty.class);
                if (relation.isPresent()) {
                    // get a var representing the relation type
                    VarAdmin relationTypeVar = getTypeVarOrDummy(aVar, knownTypes);
                    // cycle through role players to construct ontology query
                    relation.get().getRelationPlayers().forEach(
                        relationPlayer -> {
                            // get role type
                            VarAdmin roleTypeVar = getRoleTypeVar(relationPlayer, knownTypes, unknownRoles);
                            // get roleplayer type
                            VarAdmin rolePlayerType = getTypeVarOrDummy(relationPlayer.getRolePlayer(), knownTypes);
                            // put together an ontology query
                            rolePlayerType.playsRole(roleTypeVar);
                            relationTypeVar.hasRole(roleTypeVar);
                            ontologyQuery.add(rolePlayerType);
                        }
                    );
                    ontologyQuery.add(relationTypeVar);
                }
            });
        });

        System.out.println("known types:");
        knownTypes.stream().map(VarName::toString).collect(Collectors.toSet()).forEach(System.out::println);

        // execute the ontology query
        List<Map<String, Concept>> results = match(ontologyQuery).withGraph(graph).execute();

        // bundle results into sets to deduplicate
        Map<VarName, Set<TypeName>> deduplicatedResults = new HashMap<>();
        results.forEach(result -> {
            result.forEach((varNameString, concept) -> {
                VarName varName = VarName.of(varNameString);
                if (!knownTypes.contains(varName)) {
                    deduplicatedResults.computeIfAbsent(varName, (x)-> new HashSet<>()).add(concept.asType().getName());
                }
            });
        });

        System.out.println("found "+String.valueOf(deduplicatedResults.size())+" vars without types.");
        System.out.println("options for these types are:");
        deduplicatedResults.forEach((k,v)->System.out.println(k.toString()+' '+v.toString()));

        // append types to the initial query
        Set<Var> queryExtension = deduplicatedResults.keySet().stream()
                .filter((x) -> deduplicatedResults.get(x).size() == 1)
                .filter((x) -> !knownTypes.contains(x))
                .map((x) -> {
                    if (unknownRoles.contains(x)) {
//                        return var(x).name(deduplicatedResults.get(x).iterator().next().toString());
                        return var().name(deduplicatedResults.get(x).iterator().next()).sub(var(x));
                    } else {
                        return var(x).isa(deduplicatedResults.get(x).iterator().next().toString());
                    }
                })
                .collect(Collectors.toSet());
        return match(and(and(initialPatterns), and(queryExtension)));
    }

    private Optional<VarAdmin> getTypeVar(VarAdmin aVar, Set<VarName> knownTypes) {
        Optional<IsaProperty> property = aVar.getProperty(IsaProperty.class);
        if (property.isPresent()) {
            VarName varName = aVar.getVarName();
            knownTypes.add(varName);
            return Optional.of(var(varName).name(property.get().getType().getTypeName().get()).admin());
        } else {
            return Optional.empty();
        }
    }

    private VarAdmin getTypeVarOrDummy(VarAdmin aVar, Set<VarName> knownTypes) {
        Optional<VarAdmin> mightBeVar = getTypeVar(aVar, knownTypes);
        if (mightBeVar.isPresent()) {
            return mightBeVar.get();
        } else {
            return var(aVar.getVarName()).admin();
        }
    }

    private VarAdmin getRoleTypeVar(RelationPlayer relationPlayer, Set<VarName> knownTypes, Set<VarName> unknownRoles) {
        Optional<VarAdmin> aRole = relationPlayer.getRoleType();
        if (aRole.isPresent()) {
            VarAdmin roleTypeVar = aRole.get();
            Optional<TypeName> mightBeVar = roleTypeVar.getTypeName();
            if (mightBeVar.isPresent()) {
                knownTypes.add(roleTypeVar.getVarName());
            } else {
                unknownRoles.add(roleTypeVar.getVarName());
            }
            return roleTypeVar;
        } else {
            throw new RuntimeException("role types must ALL be specified");
        }
    }

}
