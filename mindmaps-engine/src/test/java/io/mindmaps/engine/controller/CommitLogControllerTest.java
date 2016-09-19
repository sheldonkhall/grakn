/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.engine.controller;

import com.jayway.restassured.http.ContentType;
import io.mindmaps.MindmapsGraph;
import io.mindmaps.concept.*;
import io.mindmaps.engine.Util;
import io.mindmaps.engine.postprocessing.Cache;
import io.mindmaps.engine.util.ConfigProperties;
import io.mindmaps.exception.MindmapsValidationException;
import io.mindmaps.factory.MindmapsClient;
import io.mindmaps.graph.internal.AbstractMindmapsGraph;
import io.mindmaps.util.REST;
import io.mindmaps.util.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import spark.Spark;

import java.util.UUID;

import static com.jayway.restassured.RestAssured.delete;
import static com.jayway.restassured.RestAssured.given;
import static org.junit.Assert.*;

public class CommitLogControllerTest {
    private Cache cache;

    @BeforeClass
    public static void startController() {
        System.setProperty(ConfigProperties.CONFIG_FILE_SYSTEM_PROPERTY, ConfigProperties.TEST_CONFIG_FILE);
    }

    @Before
    public void setUp() throws Exception {
        Spark.stop();
        Thread.sleep(2000);
        new CommitLogController();
        new GraphFactoryController();
        Util.setRestAssuredBaseURI(ConfigProperties.getInstance().getProperties());

        cache = Cache.getInstance();

        String commitLog = "{\n" +
                "    \"concepts\":[\n" +
                "        {\"id\":\"1\", \"type\":\"" + Schema.BaseType.CASTING + "\"}, \n" +
                "        {\"id\":\"2\", \"type\":\"" + Schema.BaseType.CASTING + "\"}, \n" +
                "        {\"id\":\"3\", \"type\":\"" + Schema.BaseType.CASTING + "\"}, \n" +
                "        {\"id\":\"4\", \"type\":\"" + Schema.BaseType.CASTING + "\"}, \n" +
                "        {\"id\":\"5\", \"type\":\"" + Schema.BaseType.RELATION + "\"},\n" +
                "        {\"id\":\"6\", \"type\":\"" + Schema.BaseType.RESOURCE + "\"},\n" +
                "        {\"id\":\"7\", \"type\":\"" + Schema.BaseType.RESOURCE + "\"},\n" +
                "        {\"id\":\"8\", \"type\":\"" + Schema.BaseType.RELATION + "\"},\n" +
                "        {\"id\":\"9\", \"type\":\"" + Schema.BaseType.RELATION + "\"},\n" +
                "        {\"id\":\"10\", \"type\":\"" + Schema.BaseType.RELATION + "\"}\n" +
                "    ]\n" +
                "}";

        given().contentType(ContentType.JSON).body(commitLog).when().
                post(REST.WebPath.COMMIT_LOG_URI + "?" + REST.Request.GRAPH_NAME_PARAM + "=" + "test").
                then().statusCode(200).extract().response().andReturn();
        Thread.sleep(2000);
    }

    @After
    public void takeDown() throws InterruptedException {
        cache.getCastingJobs().clear();
        cache.getResourceJobs().clear();
    }

    @Test
    public void testControllerWorking() throws InterruptedException {
        waitForCache(true, "test", 1);
        assertEquals(4, cache.getCastingJobs().values().iterator().next().size());
        waitForCache(false, "test", 1);
        assertEquals(2, cache.getResourceJobs().values().iterator().next().size());
    }

    private void waitForCache(boolean isCasting, String keyspace, int value) throws InterruptedException {
        boolean flag = true;
        while(flag){
            if(isCasting){
                if(cache.getCastingJobs().get(keyspace).size() < value){
                    Thread.sleep(1000);
                } else{
                    flag = false;
                }
            } else {
                if(cache.getResourceJobs().get(keyspace).size() < value){
                    Thread.sleep(1000);
                } else {
                    flag = false;
                }
            }
        }
    }

    @Test
    public void testCommitLogSubmission() throws MindmapsValidationException {
        final String BOB = "bob";
        final String TIM = "tim";

        MindmapsGraph bob = MindmapsClient.getGraph(BOB);
        MindmapsGraph tim = MindmapsClient.getGraph(TIM);

        addSomeData(bob);

        assertEquals(2, cache.getCastingJobs().get(BOB).size());
        assertEquals(1, cache.getResourceJobs().get(BOB).size());

        assertNull(cache.getCastingJobs().get(TIM));
        assertNull(cache.getResourceJobs().get(TIM));

        addSomeData(tim);

        assertEquals(2, cache.getCastingJobs().get(TIM).size());
        assertEquals(1, cache.getResourceJobs().get(TIM).size());

        MindmapsClient.getGraph(BOB).clear();
        MindmapsClient.getGraph(TIM).clear();

        assertEquals(0, cache.getCastingJobs().get(BOB).size());
        assertEquals(0, cache.getCastingJobs().get(TIM).size());
        assertEquals(0, cache.getResourceJobs().get(BOB).size());
        assertEquals(0, cache.getResourceJobs().get(TIM).size());

        cache.getResourceJobs().get(BOB).forEach(resourceId -> {
            Concept concept = ((AbstractMindmapsGraph) bob).getConceptByBaseIdentifier(resourceId);
            assertTrue(concept.isResource());
        });

    }

    private void addSomeData(MindmapsGraph graph) throws MindmapsValidationException {
        RoleType role1 = graph.putRoleType("Role 1");
        RoleType role2 = graph.putRoleType("Role 2");
        RelationType relationType = graph.putRelationType("A Relation Type").hasRole(role1).hasRole(role2);
        EntityType type = graph.putEntityType("A Thing").playsRole(role1).playsRole(role2);
        ResourceType<String> resourceType = graph.putResourceType("A Resource Type Thing", ResourceType.DataType.STRING).playsRole(role1).playsRole(role2);
        Entity entity = graph.addEntity(type);
        Resource resource = graph.putResource(UUID.randomUUID().toString(), resourceType);

        graph.addRelation(relationType).putRolePlayer(role1, entity).putRolePlayer(role2, resource);

        graph.commit();
    }

    @Test
    public void testDeleteController() throws InterruptedException {
        assertEquals(4, cache.getCastingJobs().values().iterator().next().size());

        delete(REST.WebPath.COMMIT_LOG_URI + "?" + REST.Request.GRAPH_NAME_PARAM + "=" + "test").
                then().statusCode(200).extract().response().andReturn();

        assertEquals(0, cache.getCastingJobs().values().iterator().next().size());
    }
}