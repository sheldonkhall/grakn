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

package io.mindmaps.graql.internal.analytics;

import io.mindmaps.MindmapsGraph;
import io.mindmaps.MindmapsTinkerTestBase;
import io.mindmaps.concept.EntityType;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;

import static org.junit.Assert.assertEquals;

/**
 *
 */
public class AnalyticsLogicTest extends MindmapsTinkerTestBase {

    String keyspace;
    MindmapsGraph graph;

    @Before
    public void setUp() throws InterruptedException {
        graph = batchGraphWithNewKeyspace();
        keyspace = graph.getKeyspace();
    }

//    @After
//    public void cleanGraph() {
//        graph.clear();
//        graph.close();
//    }

    @Test
    public void testTinkerGraphComputerActuallyWorks() {
        String thingId = "thing";
        int numberOfEntities = 10;
        EntityType thing = graph.putEntityType(thingId);
        for (int i=0; i<numberOfEntities; i++) graph.addEntity(thing);

        Analytics analytics = new Analytics(keyspace, new HashSet<>(), new HashSet<>());
        assertEquals(numberOfEntities, analytics.count());
    }

}
