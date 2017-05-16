package ai.grakn.test.graql.analytics;

import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.GraknSession;
import ai.grakn.GraknTxType;
import ai.grakn.client.BatchMutatorClient;
import ai.grakn.concept.EntityType;
import ai.grakn.graql.Graql;
import ai.grakn.test.DistributionContext;
import org.junit.ClassRule;
import org.junit.Test;

import static ai.grakn.graql.Graql.insert;
import static ai.grakn.graql.Graql.var;
import static ai.grakn.graql.internal.query.aggregate.Aggregates.count;
import static org.junit.Assert.assertTrue;

/**
 *
 */
public class AnalyticsJVMTest {

    @ClassRule
    public static final DistributionContext context = DistributionContext.startSingleQueueEngineProcess();

    private static final String thing = "thing";

    @Test
    public void countUpdatesAfterNewInformationAdded() {

        // add the ontology
        String keyspace;
        try (GraknSession session = Grakn.session(Grakn.DEFAULT_URI, "test")) {
            try (GraknGraph graph = session.open(GraknTxType.WRITE)) {
                keyspace = graph.getKeyspace();
                EntityType entityType1 = graph.putEntityType(thing);
                graph.commit();
            }
        }

        // add some entities
        BatchMutatorClient bulkMutatorClient = new BatchMutatorClient(keyspace, Grakn.DEFAULT_URI);
        bulkMutatorClient.add(insert(var().isa(thing)));
        bulkMutatorClient.add(insert(var().isa(thing)));
        bulkMutatorClient.waitToFinish();

        // count them using graql and analytics and check they are right
        try (GraknSession session = Grakn.session(Grakn.DEFAULT_URI, "test")) {
            try (GraknGraph graph = session.open(GraknTxType.READ)) {
                Long initialGraqlCount = graph.graql().match(var().isa(thing)).aggregate(count()).execute();
                assertTrue(initialGraqlCount.equals(2L));
            }
            try (GraknGraph graph = session.open(GraknTxType.READ)) {
                Long initialAnalyticsCount = Graql.compute().withGraph(graph).count().execute();
                assertTrue(initialAnalyticsCount.equals(2L));
            }
        }

        // add some more
        bulkMutatorClient = new BatchMutatorClient(keyspace, Grakn.DEFAULT_URI);
        bulkMutatorClient.add(insert(var().isa(thing)));
        bulkMutatorClient.add(insert(var().isa(thing)));
        bulkMutatorClient.waitToFinish();

        // count them using graql and analytics and check they are right
        try (GraknSession session = Grakn.session(Grakn.DEFAULT_URI, "test")) {
            try (GraknGraph graph = session.open(GraknTxType.READ)) {
                Long secondGraqlCount = graph.graql().match(var().isa(thing)).aggregate(count()).execute();
                assertTrue(secondGraqlCount.equals(4L));
            }
            try (GraknGraph graph = session.open(GraknTxType.READ)) {
                Long secondAnalyticsCount = Graql.compute().withGraph(graph).count().execute();
                assertTrue(secondAnalyticsCount.equals(4L));
            }
        }
    }
}
