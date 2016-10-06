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

package io.mindmaps;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import io.mindmaps.engine.util.ConfigProperties;
import org.junit.BeforeClass;

public class MindmapsTinkerTestBase extends AbstractMindmapsEngineTest {

    private static void hideLogs() {
        Logger logger = (Logger) org.slf4j.LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        logger.setLevel(Level.OFF);
    }

    @BeforeClass
    public static void startEmbeddedCassandra() throws Exception {
        startTestEngine(ConfigProperties.TEST_TINKER_COMPUTER);
        hideLogs();
    }

    @Override
    public void buildGraph() {
        graph = batchGraphWithNewKeyspace();
    }

    @Override
    public void clearGraph() {
        graph.clear();
    }
}