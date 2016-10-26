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

package io.mindmaps.test.graql.shell;

import com.google.common.base.Strings;
import io.mindmaps.graql.GraqlClientImpl;
import io.mindmaps.graql.GraqlShell;
import io.mindmaps.test.AbstractRollbackGraphTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Random;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class GraqlShellTest extends AbstractRollbackGraphTest {
    private static InputStream trueIn;
    private static PrintStream trueOut;
    private static PrintStream trueErr;
    private static String expectedVersion = "graql-9.9.9";
    private static final String historyFile = "/graql-test-history";

    @BeforeClass
    public static void setUpClass() throws Exception {
        trueIn = System.in;
        trueOut = System.out;
        trueErr = System.err;
    }

    @AfterClass
    public static void resetIO() {
        System.setIn(trueIn);
        System.setOut(trueOut);
        System.setErr(trueErr);
    }

    @Test
    public void testStartAndExitShell() throws Exception {
        // Assert simply that the shell starts and terminates without errors
        assertTrue(testShell("exit\n").matches("[\\s\\S]*>>> exit(\r\n?|\n)"));
    }

    @Test
    public void testHelpOption() throws Exception {
        String result = testShell("", "--help");

        // Check for a few expected usage messages
        assertThat(
                result,
                allOf(
                        containsString("usage"), containsString("graql.sh"), containsString("-e"),
                        containsString("--execute <arg>"), containsString("query to execute")
                )
        );
    }

    @Test
    public void testVersionOption() throws Exception {
        String result = testShell("", "--version");
        assertThat(result, containsString(expectedVersion));
    }

    @Test
    public void testExecuteOption() throws Exception {
        String result = testShell("", "-e", "match $x isa role-type; ask;");

        // When using '-e', only results should be printed, no prompt or query
        assertThat(result, allOf(containsString("False"), not(containsString(">>>")), not(containsString("match"))));
    }

    // TODO: Fix this test
    @Ignore
    @Test
    public void testFileOption() throws Exception {
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        testShell("", err, "-f", "src/test/graql/shell-test.gql");
        assertEquals(err.toString(), "");
    }

    @Test
    public void testMatchQuery() throws Exception {
        String[] result = testShell("match $x isa type\nexit").split("\r\n?|\n");

        // Make sure we find a few results (don't be too fussy about the output here)
        assertEquals(">>> match $x isa type", result[4]);
        assertTrue(result.length > 5);
    }

    @Test
    public void testAskQuery() throws Exception {
        String result = testShell("match $x isa relation-type; ask;\n");
        assertThat(result, containsString("False"));
    }

    @Test
    public void testInsertQuery() throws Exception {
        String result = testShell(
                "match $x isa entity-type; ask;\ninsert my-type isa entity-type;\nmatch $x isa entity-type; ask;\n"
        );
        assertThat(result, allOf(containsString("False"), containsString("True")));
    }

    @Test
    public void testInsertOutput() throws Exception {
        String[] result = testShell("insert a-type isa entity-type; thingy isa a-type\n").split("\r\n?|\n");

        // Expect six lines output - four for the license, one for the query, no results and a new prompt
        assertEquals(6, result.length);
        assertEquals(">>> insert a-type isa entity-type; thingy isa a-type", result[4]);
        assertEquals(">>> ", result[5]);
    }

    @Test
    public void testAutocomplete() throws Exception {
        String result = testShell("match $x isa \t");

        // Make sure all the autocompleters are working (except shell commands because we are writing a query)
        assertThat(
                result,
                allOf(
                        containsString("type"), containsString("match"),
                        not(containsString("exit")), containsString("$x")
                )
        );
    }

    @Test
    public void testAutocompleteShellCommand() throws Exception {
        String result = testShell("\t");

        // Make sure all the autocompleters are working (including shell commands because we are not writing a query)
        assertThat(result, allOf(containsString("type"), containsString("match"), containsString("exit")));
    }

    @Test
    public void testAutocompleteFill() throws Exception {
        String result = testShell("match $x sub typ\t;\n");
        assertThat(result, containsString("\"relation-type\""));
    }

    @Test
    public void testReasoner() throws Exception {
        String result = testShell(
                "insert man isa entity-type; person isa entity-type;\n" +
                "insert 'felix' isa man;\n" +
                "match $x isa person;\n" +
                "insert my-rule isa inference-rule lhs {$x isa man;} rhs {$x isa person;};\n" +
                "match $x isa person;\n"
        );

        // Make sure first 'match' query has no results and second has exactly one result
        String[] results = result.split("\n");
        int matchCount = 0;
        for (int i = 0; i < results.length; i ++) {
            if (results[i].contains(">>> match $x isa person")) {

                if (matchCount == 0) {
                    // First 'match' result is before rule is added, so should have no results
                    assertFalse(results[i + 1].contains("felix"));
                } else {
                    // Second 'match' result is after rule is added, so should have a result
                    assertTrue(results[i + 1].contains("felix"));
                }

                matchCount ++;
            }
        }

        assertEquals(2, matchCount);
    }

    @Test
    public void testInvalidQuery() throws Exception {
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        testShell("insert movie isa entity-type; moon isa movie; europa isa moon;\n", err);

        assertThat(err.toString(), allOf(containsString("moon"), containsString("not"), containsString("type")));
    }

    @Test
    public void testComputeCount() throws Exception {
        String result = testShell("insert X isa entity-type; a isa X; b isa X; c isa X;\ncommit\ncompute count;\n");
        assertThat(result, containsString("\n3\n"));
    }

    @Test
    public void testRollback() throws Exception {
        String[] result = testShell("insert E isa entity-type;\nrollback\nmatch $x isa entity-type\n").split("\n");

        // Make sure there are no results for match query
        assertEquals(">>> match $x isa entity-type", result[result.length-2]);
        assertEquals(">>> ", result[result.length-1]);
    }

    @Test
    public void testErrorWhenEngineNotRunning() throws Exception {
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        testShell("", err, "-u", "localhost:7654");

        assertFalse(err.toString().isEmpty());
    }

    @Test
    public void fuzzTest() throws Exception {
        int repeats = 100;
        for (int i = 0; i < repeats; i ++) {
            System.out.println(i);
            testShell(randomString(i));
        }
    }

    @Test
    public void testLargeQuery() throws Exception {
        String id = Strings.repeat("really-", 100000) + "long-id";
        String[] result = testShell("insert X isa entity-type; '" + id + "' isa X;\nmatch $x isa X;\n").split("\n");
        assertThat(result[result.length-2], allOf(containsString("$x"), containsString(id)));
    }

    private static String randomString(int length) {
        Random random = new Random();
        StringBuilder sb = new StringBuilder();

        random.ints().limit(length).forEach(i -> sb.append((char) i));

        return sb.toString();
    }

    private String testShell(String input, String... args) throws Exception {
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        return testShell(input, err, args);
    }

    private String testShell(String input, ByteArrayOutputStream berr, String... args) throws Exception {
        String[] newArgs = Arrays.copyOf(args, args.length + 2);
        newArgs[newArgs.length-2] = "-n";
        newArgs[newArgs.length-1] = graph.getKeyspace();

        InputStream in = new ByteArrayInputStream(input.getBytes());

        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(bout);
        PrintStream err = new PrintStream(berr);

        try {
            System.setIn(in);
            System.setOut(out);
            System.setErr(err);
            
            GraqlShell.runShell(newArgs, expectedVersion, historyFile, new GraqlClientImpl());
        } catch (Exception e) {
            System.setErr(trueErr);
            e.printStackTrace();
            err.flush();
            fail(berr.toString());
        } finally {
            resetIO();
        }

        out.flush();
        err.flush();


        return bout.toString();
    }
}

