package ai.grakn.test.graql.graql;

import ai.grakn.GraknGraph;
import ai.grakn.concept.TypeLabel;
import ai.grakn.graphs.MovieGraph;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.Var;
import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.Answer;
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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

        initialQuery = match(var("x").isa("movie").has("title", "Godfather"), var("r").rel("actor", "y").rel(var("a"),"x"));
        testExpandedQueryIsEquivalent(initialQuery);

        initialQuery = match(var("x").isa(var("z")).has("title", "Godfather"), var("r").rel("actor","y").rel(var("a"),"x"));
        testExpandedQueryIsEquivalent(initialQuery);

        initialQuery = match(var("x").isa(var("y")), var().rel("production-with-cast", "x"));
        testExpandedQueryIsEquivalent(initialQuery);

        initialQuery = match(var("x").isa(var("y")));
        testExpandedQueryIsEquivalent(initialQuery);

        initialQuery = match(var("x").isa(var("y")),var("y"));
        testExpandedQueryIsEquivalent(initialQuery);

        initialQuery = match(var("a").isa("movie").has("title", "Godfather"), var("b").isa("movie").has("title", "Heat"));
        testExpandedQueryIsEquivalent(initialQuery);

        initialQuery = match(var("x").isa("movie").has("title", "Godfather"),
                var("r").rel("actor","y").rel(var("a"),"x"),
                var("c").isa("genre").has("name", "drama"),
                var("f").rel(var("d"),"b").rel(var("e"),"c"));
        testExpandedQueryIsEquivalent(initialQuery);
    }

    public void testExpandedQueryIsEquivalent(MatchQuery initialQuery) {
        // need to see implicit types in query
        graph.showImplicitConcepts(true);
        System.out.println("The initial query: "+initialQuery.toString());
        MatchQuery expandedQuery = generateOntologyQueryAndMap(initialQuery);
        System.out.println("The expanded query: "+expandedQuery.toString());
        List<Answer> results = initialQuery.withGraph(graph).execute();
        List<Answer> expandedResults = expandedQuery.withGraph(graph).execute();

//        System.out.println("initial results:");
//        printResults(results);
//        System.out.println("expanded results:");
//        printResults(expandedResults);

        results.forEach(result -> {
            boolean isEqual = expandedResults.stream()
                    .map(expandedResult -> isUnionOfVarsEqual(result, expandedResult))
                    .reduce(false, (a, b) -> a || b);
            if (!isEqual) {
                System.out.println("assertion failed on this result: ");
                printResult(result);
            }
            assertTrue(isEqual);
        });
        assertEquals(results.size(), expandedResults.size());
    }

    private boolean isUnionOfVarsEqual(Answer result1, Answer result2) {
        return result1.entrySet().stream()
                .map((x)-> result2.get(x.getKey())==x.getValue())
                .reduce(true,(a,b)-> a & b);
    }

    private void printResults(List<Answer> results) {
        results.forEach(result -> {
            printResult(result);
        });
    }

    private void printResult(Answer result) {
        result.forEach((k,v) -> System.out.print(" "+k+" "+v.toString()));
        System.out.println();
    }

    private <R> MatchQuery generateOntologyQueryAndMap(MatchQuery initialQuery) {
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
                // get type if it has been specified and is not a var itself
                getTypeVar(aVar, knownTypes).ifPresent((typeVar) -> {
                    if (typeVar.getTypeLabel().isPresent()) {
                        ontologyQuery.add(typeVar);
                    }
                });
                // if there is a relation property attached to the var it is a relation - current starting point
                Optional<RelationProperty> relation = aVar.getProperty(RelationProperty.class);
                if (relation.isPresent()) {
                    // get a var representing the relation type
                    VarAdmin relationTypeVar = getTypeVarOrDummy(aVar, knownTypes);
                    // cycle through role players to construct ontology query
                    List<RelationPlayer> relationPlayers = relation.get().getRelationPlayers().collect(Collectors.toCollection(ArrayList::new));
                    for (RelationPlayer relationPlayer : relationPlayers) {
                            // get role type
                            VarAdmin roleTypeVar = getRoleTypeVar(relationPlayer, knownTypes, unknownRoles);
                            // get roleplayer type
                            VarAdmin rolePlayerType = getTypeVarOrDummy(relationPlayer.getRolePlayer(), knownTypes);
                            // put together an ontology query
                            rolePlayerType = (VarAdmin) rolePlayerType.plays(roleTypeVar);
                            relationTypeVar = (VarAdmin) relationTypeVar.relates(roleTypeVar);
                            ontologyQuery.add(rolePlayerType);
                    }
                    ontologyQuery.add(relationTypeVar);
                }
            });
        });

        System.out.println("known types:");
        knownTypes.stream().map(VarName::toString).collect(Collectors.toSet()).forEach(System.out::println);

        // execute the ontology query if there is anything to infer
        //TODO: move this inside the disjunction loop
        if (!ontologyQuery.isEmpty()) {
            System.out.println("the ontology query is: "+match(ontologyQuery).toString());
            List<Answer> results = match(ontologyQuery).withGraph(graph).execute();

            // bundle results into sets to deduplicate
            Map<VarName, Set<TypeLabel>> deduplicatedResults = new HashMap<>();
            results.forEach(result -> {
                result.forEach((varName, concept) -> {
                    if (!knownTypes.contains(varName)) {
                        deduplicatedResults.computeIfAbsent(varName, (x) -> new HashSet<>()).add(concept.asType().getLabel());
                    }
                });
            });

            System.out.println("found " + String.valueOf(deduplicatedResults.size()) + " vars without types.");
            System.out.println("options for these types are:");
            deduplicatedResults.forEach((k, v) -> System.out.println(k.toString() + ' ' + v.toString()));

            // append types to the initial query
            Set<Var> queryExtension = deduplicatedResults.keySet().stream()
                    .filter((x) -> deduplicatedResults.get(x).size() == 1)
                    .filter((x) -> !knownTypes.contains(x))
                    .map((x) -> {
                        if (unknownRoles.contains(x)) {
                            return var().label(deduplicatedResults.get(x).iterator().next()).sub(var(x));
                        } else {
                            return var(x).isa(deduplicatedResults.get(x).iterator().next().toString());
                        }
                    })
                    .collect(Collectors.toSet());
            return match(and(and(initialPatterns), and(queryExtension)));
        } else {
            System.out.println("No ontology query could be constructed.");
            return initialQuery;
        }
    }

    private Optional<VarAdmin> getTypeVar(VarAdmin aVar, Set<VarName> knownTypes) {
        Optional<IsaProperty> property = aVar.getProperty(IsaProperty.class);
        if (property.isPresent()) {
            VarName varName = aVar.getVarName();
            Optional<TypeLabel> mightBeATypeLabel = property.get().getType().getTypeLabel();
            // check if the type is known - if not just return
            if (mightBeATypeLabel.isPresent()) {
                knownTypes.add(varName);
                return Optional.of(var(varName).label(mightBeATypeLabel.get()).admin());
            } else {
                return Optional.of(var(varName).admin());
            }
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
            Optional<TypeLabel> mightBeVar = roleTypeVar.getTypeLabel();
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
