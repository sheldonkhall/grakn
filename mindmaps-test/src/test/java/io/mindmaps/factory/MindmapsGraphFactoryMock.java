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

package io.mindmaps.factory;
import io.mindmaps.MindmapsComputer;
import io.mindmaps.util.REST;
import org.apache.tinkerpop.gremlin.structure.Graph;

/**
 *
 */
public class MindmapsGraphFactoryMock extends MindmapsGraphFactoryImpl {
    private final String keyspace;
    private final String uri;

    public MindmapsGraphFactoryMock(String keyspace, String uri) {
        super(keyspace.toLowerCase(), uri);
        this.keyspace = keyspace;
        this.uri = uri;
    }

    public MindmapsComputer getGraphComputer(int numberOfWorkers) {
        ConfigureFactory configuredFactory = configureGraphFactory(keyspace, uri, REST.GraphConfig.COMPUTER);
        Graph graph = configuredFactory.factory.getTinkerPopGraph(false);
        return new MindmapsComputerMock(graph, configuredFactory.graphComputer, numberOfWorkers);
    }
}
