package ai.grakn.test.graql.graql;

import ai.grakn.GraknGraph;
import ai.grakn.concept.Concept;
import ai.grakn.graphs.MovieGraph;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.Var;
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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static ai.grakn.graql.Graql.match;
import static ai.grakn.graql.Graql.var;
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
    public void testOptimisedIsFaster() {
        MatchQuery initialQuery = match(var("x").isa("movie"), var().rel("actor","y").rel(var("a"),"x"));
        System.out.println(initialQuery.toString());
        MatchQuery expandedQuery = generateOntologyQueryAndMap(initialQuery);
        System.out.println(expandedQuery.toString());
        List<Map<String, Concept>> results = expandedQuery.withGraph(graph).execute();
        results.forEach(result -> {
            System.out.println("result:");
            result.forEach((k,v) -> System.out.println(k+v.toString()));
        });
        assertTrue(true);
    }

    private MatchQuery generateOntologyQueryAndMap(MatchQuery initialQuery) {
        Set<Var> ontologyQuery = new HashSet<>();
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
                getTypeVar(aVar).ifPresent(ontologyQuery::add);
                // if there is a relation property attached to the var it is a relation - current starting point
                Optional<RelationProperty> relation = aVar.getProperty(RelationProperty.class);
                if (relation.isPresent()) {
                    // get a var representing the relation type
                    VarAdmin relationTypeVar = getTypeVarOrDummy(aVar);
                    // cycle through role players to construct ontology query
                    relation.get().getRelationPlayers().forEach(
                        relationPlayer -> {
                            // get role type
                            VarAdmin roleTypeVar = getRoleTypeVar(relationPlayer);
                            // get roleplayer type
                            VarAdmin rolePlayerType = getTypeVarOrDummy(relationPlayer.getRolePlayer());
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

        return match(ontologyQuery);
    }

    private Optional<VarAdmin> getTypeVar(VarAdmin aVar) {
        // check if the var has a type defined
        Optional<IsaProperty> property = aVar.getProperty(IsaProperty.class);
        // if not create a var for the type
        if (property.isPresent()) {
            return Optional.of(var(aVar.getVarName()).name(property.get().getType().getTypeName().get()).admin());
        } else {
            return Optional.empty();
        }
    }

    private VarAdmin getTypeVarOrDummy(VarAdmin aVar) {
        Optional<VarAdmin> mightBeVar = getTypeVar(aVar);
        if (mightBeVar.isPresent()) {
            return mightBeVar.get();
        } else {
            return var(aVar.getVarName()).admin();
        }
    }

    private VarAdmin getRoleTypeVar(RelationPlayer relationPlayer) {
        Optional<VarAdmin> aRole = relationPlayer.getRoleType();
        if (aRole.isPresent()) {
            return aRole.get();
        } else {
            return var(UUID.randomUUID().toString().toLowerCase()).admin();
        }
    }

}
