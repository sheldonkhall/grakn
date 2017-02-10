package ai.grakn.test.graql.analytics;

import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.TypeName;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.graql.Graql;
import ai.grakn.graql.InsertQuery;
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

    final GraknGraph graknGraph = Grakn.factory(Grakn.DEFAULT_URI, "amazon").getGraph();

    private void insertResourceOntology(Set<String> entitiesWithResource, String clusterResourceType, ResourceType.DataType dataType) {
        graknGraph.rollback();

        // add new resource type as sub cluster
        Set<InsertQuery> ontologyInsert = new HashSet<>();
        String thisEntityType = "thisEntityType";
        insert(var().sub("resource").name(clusterResourceType).datatype(dataType))
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

    /**
     * Create the ontology to relate an entity with a set of other entities and identify the entity with an id. The
     * relation type and roles are automatically generated using the name of the entity type. A resource type is also
     * created using the entity name that can hold a string ID.
     *
     * @param entityType the entity that will have other entities related to it.
     * @param entitiesConnectedToEntity the other entities to relate to the given entity.
     */
    private void insertEntityOntology(String entityType, Set<String> entitiesConnectedToEntity) {
        graknGraph.rollback();

        // put the generic ID resource in the graph
        String idID = "ID";
        putVar(var().sub("resource").name(idID));

        // start making the ontology changes for the relation
        Set<Var> mutation = new HashSet<>();

        Var entityVar = var(entityType).name(entityType);

        String ownerRole = getOwnerRoleTypeFromEntityType(entityType);
        String valueRole = getValueRoleTypeFromEntityType(entityType);

        // put relation and roles
        mutation.add(var().sub("role").name(ownerRole));
        mutation.add(var().sub("role").name(valueRole));
        mutation.add(var().sub("relation").name(getRelationTypeFromEntityType(entityType)).hasRole(ownerRole).hasRole(valueRole));

        // put entity and resource
        mutation.add(var().sub(idID).name(getResourceTypeFromEntityType(entityType)).datatype(ResourceType.DataType.STRING));
        mutation.add(var(entityType).playsRole(valueRole).hasResource(getResourceTypeFromEntityType(entityType)));

        // assert connected entities play role
        entitiesConnectedToEntity.forEach(connectedEntity -> {
            mutation.add(var().name(connectedEntity).playsRole(ownerRole));
        });

        // persist
        match(entityVar).insert(mutation).withGraph(graknGraph).execute();
        try {
            graknGraph.commit();
        } catch (GraknValidationException e) {
            e.printStackTrace();
        }
    }

    private String getResourceTypeFromEntityType(String entityType) {return entityType+"-id";}

    private String getRelationTypeFromEntityType(String entityType) {return "has-"+entityType;}

    private String getOwnerRoleTypeFromEntityType(String entityType) {return entityType+"-owner";}

    private String getValueRoleTypeFromEntityType(String entityType) {return entityType+"-value";}

    @Test
    public void testPersistBoughtTogether() {
        persistClusterAndDegrees("together",Sets.newHashSet("product","bought-together"));
    }

//    @Test
//    public void testPersistAlsoBought() {
//        persistClusterAndDegrees("cluster-also-bought",Sets.newHashSet("product","also-bought"));
//    }
//
//    @Test
//    public void testPersistAlsoViewed() {
//        persistClusterAndDegrees("cluster-also-viewed",Sets.newHashSet("product","also-viewed"));
//    }

    @Test
    public void testPersistAll() {
        persistClusterAndDegrees("all",Sets.newHashSet("product","also-viewed","also-bought","bought-together"));
    }

    @Test
    public void testPersistCategory() {
        persistClusterAndDegrees("category-groups",Sets.newHashSet("category","hierarchy"));
    }

    @Test
    public void testPersistRecommendation() {
        persistDegreesEntity("co-category",Sets.newHashSet("co-category","implied"));
    }

    @Test
    public void testPersistPurchase() {
        persistDegreesEntity("product",Sets.newHashSet("product","purchase"));
    }

    // not very interesting
//    @Test
//    public void testPersistUserProduct() {
//        persistClusterAndDegrees("cluster-user-product",Sets.newHashSet("user","product","product-review"));
//    }

    private void persistClusterAndDegrees(String clusterName, Set<String> subGraph) {
        persistCluster(clusterName, subGraph);
//        persistDegrees(clusterName);
        persistDegreesEntity(clusterName, Sets.newHashSet(clusterName, getRelationTypeFromEntityType(clusterName)));
    }

    private void persistCluster(String clusterName, Set<String> subGraph) {
        Map<String, Set<String>> result = Graql.compute().withGraph(graknGraph).cluster().in(subGraph.toArray(new String[subGraph.size()])).members().execute();

        // put the cluster supertype
        Var clusterEntityType = var().sub("entity").name("cluster");
        putVar(clusterEntityType);

        // put the cluster
        Var clusterSubEntityType = var().sub("cluster").name(clusterName);
        putVar(clusterSubEntityType);

        insertEntityOntology(clusterName, subGraph);
        int minClusterSize = 3;

        // insert clusters without members
        result.keySet().forEach(clusterID -> {
            if (result.get(clusterID).size()>=minClusterSize) {
                insert(var().isa(clusterName)
                        .has(getResourceTypeFromEntityType(clusterName), clusterID))
                        .withGraph(graknGraph).execute();
            }
        });
        try {
            graknGraph.commit();
        } catch (GraknValidationException e) {
            e.printStackTrace();
        }

        // relate members of clusters
        result.forEach((clusterId, memberIds) -> {
            Set<InsertQuery> clusterInsert = new HashSet<>();
            if (memberIds.size() >= minClusterSize) {
                memberIds.forEach(memberId -> {
                    String thisConcept = "thisConcept";
                    String thisCluster = "thisCluster";
                    clusterInsert.add(
                            match(
                                    var(thisConcept).id(ConceptId.of(memberId)).isa("entity"),
                                    var(thisCluster).isa(clusterName).has(getResourceTypeFromEntityType(clusterName), clusterId))
                                    .insert(
                                            var().isa(getRelationTypeFromEntityType(clusterName))
                                                    .rel(getOwnerRoleTypeFromEntityType(clusterName),thisConcept)
                                                    .rel(getValueRoleTypeFromEntityType(clusterName),thisCluster)));
                });
                clusterInsert.forEach(insertQuery -> {
                    insertQuery.withGraph(graknGraph).execute();
                });
                try {
                    graknGraph.commit();
                } catch (GraknValidationException e) {
                    e.printStackTrace();
                }
            }
        });
        System.out.println(result);

    }

    private void persistDegreesEntity(String entityName, Set<String> subGraph) {
        Map<Long, Set<String>> result = Graql.compute().withGraph(graknGraph).degree().of(entityName).in(subGraph.toArray(new String[subGraph.size()])).execute();

        String degreeResourceType = "degree";
        insertResourceOntology(Sets.newHashSet(entityName), degreeResourceType, ResourceType.DataType.LONG);

        result.forEach((degree, memberIds) -> {
            Set<InsertQuery> degreeInsert = new HashSet<>();
            memberIds.forEach(memberId -> {
                String thisConcept = "thisConcept";
                degreeInsert.add(match(var(thisConcept).id(ConceptId.of(memberId))).insert(var(thisConcept).has(degreeResourceType, degree)));
            });
            degreeInsert.forEach(insertQuery -> {
                insertQuery.withGraph(graknGraph).execute();
                try {
                    graknGraph.commit();
                } catch (GraknValidationException e) {
                    e.printStackTrace();
                }
            });
        });
        System.out.println(result);
    }

    private void putVar(Var var) {
        List<Map<String, Concept>> clusterSubEntityResult = match(var).withGraph(graknGraph).execute();
        if (clusterSubEntityResult.isEmpty()) {
            insert(var).withGraph(graknGraph).execute();
            try {
                graknGraph.commit();
            } catch (GraknValidationException e) {
                e.printStackTrace();
            }
        }
    }
}
