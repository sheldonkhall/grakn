package ai.grakn.test.graql.analytics;

import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.ResourceType;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.graql.Graql;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.Var;
import com.google.common.collect.Sets;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static ai.grakn.graql.Graql.insert;
import static ai.grakn.graql.Graql.match;
import static ai.grakn.graql.Graql.var;

/**
 *
 */
public class PersistScriptTest {

    final GraknGraph graknGraph = Grakn.factory(Grakn.DEFAULT_URI, "grakn").getGraph();

    @Test
    public void testPersistCluster() {
        Map<String, Set<String>> result = Graql.compute().withGraph(graknGraph).cluster().in("product", "also-viewed").members().execute();

        String clusterResourceType = "cluster-also-viewed";
        insertResourceOntology(Sets.newHashSet("product", "also-viewed"), clusterResourceType);

        result.forEach((clusterId, memberIds) -> {
            Set<InsertQuery> clusterInsert = new HashSet<>();
            if (memberIds.size() > 2) {
                memberIds.forEach(memberId -> {
                    String thisConcept = "thisConcept";
                    clusterInsert.add(match(var(thisConcept).id(ConceptId.of(memberId))).insert(var(thisConcept).has(clusterResourceType, clusterId)));
                });
                clusterInsert.forEach(insertQuery -> {
                    insertQuery.withGraph(graknGraph).execute();
                    try {
                        graknGraph.commit();
                    } catch (GraknValidationException e) {
                        e.printStackTrace();
                    }
                });
            }
        });
        System.out.println(result);
    }

    private void insertResourceOntology(Set<String> entitiesWithResource, String clusterResourceType) {
        graknGraph.rollback();

        // find root cluster resource and add if necessary
        String rootClusterResource = "cluster";
        Var rootClusterResourceVar = var().sub("resource").name(rootClusterResource);
        List<Map<String, Concept>> result = match(rootClusterResourceVar).withGraph(graknGraph).execute();
        if (result.isEmpty()) {
            insert(rootClusterResourceVar.datatype(ResourceType.DataType.STRING)).withGraph(graknGraph).execute();
            try {
                graknGraph.commit();
            } catch (GraknValidationException e) {
                e.printStackTrace();
            }
        }

        // add new resource type as sub cluster
        Set<InsertQuery> ontologyInsert = new HashSet<>();
        String thisEntityType = "thisEntityType";
        insert(var().sub("cluster").name(clusterResourceType).datatype(ResourceType.DataType.STRING))
                .withGraph(graknGraph).execute();
        try {
            graknGraph.commit();
        } catch (GraknValidationException e) {
            e.printStackTrace();
        }
        entitiesWithResource.forEach(entity -> {
            ontologyInsert.add(match(var(thisEntityType).sub("concept").name(entity))
                    .insert(var(thisEntityType).hasResource(clusterResourceType)));
        });
        ontologyInsert.forEach(insert -> {
            System.out.println(insert.toString());
            insert.withGraph(graknGraph).execute();
            try {
                graknGraph.commit();
            } catch (GraknValidationException e) {
                e.printStackTrace();
            }
        });
    }

}
