/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.beeline.src.test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.beeline.BeeLine;
import org.apache.hive.jdbc.TestJdbcDriver2;
import org.apache.hive.service.server.HiveServer2;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * TestBeeLineWithArgs - executes tests of the command-line arguments to BeeLine
 *
 */
//public class TestBeeLineWithArgs extends TestCase {
public class TestBeeLineWithArgs {

  // Default location of HiveServer2
  final static String JDBC_URL = BeeLine.BEELINE_DEFAULT_JDBC_URL + "localhost:10000";
  private static final String tableName = "TestBeelineTable1";
  private static final String tableComment = "Test table comment";
  private static HiveServer2 hiveServer2;

  /**
   * Start up a local Hive Server 2 for these tests
   */
  @BeforeClass
  public static void preTests() throws Exception {
    HiveConf hiveConf = new HiveConf();
    //  hiveConf.logVars(System.err);
    // System.err.flush();

    hiveServer2 = new HiveServer2();
    hiveServer2.init(hiveConf);
    System.err.println("Starting HiveServer2...");
    hiveServer2.start();
    Thread.sleep(1000);
    createTable();

  }

  /**
   * Create table for use by tests
   * @throws ClassNotFoundException
   * @throws SQLException
   */
  private static void createTable() throws ClassNotFoundException, SQLException {
    Class.forName(BeeLine.BEELINE_DEFAULT_JDBC_DRIVER);
    Connection con = DriverManager.getConnection(JDBC_URL,"", "");

    assertNotNull("Connection is null", con);
    assertFalse("Connection should not be closed", con.isClosed());
    Statement stmt = con.createStatement();
    assertNotNull("Statement is null", stmt);

    stmt.execute("set hive.support.concurrency = false");

    HiveConf conf = new HiveConf(TestJdbcDriver2.class);
    String dataFileDir = conf.get("test.data.files").replace('\\', '/')
        .replace("c:", "");
    Path dataFilePath = new Path(dataFileDir, "kv1.txt");
    // drop table. ignore error.
    try {
      stmt.execute("drop table " + tableName);
    } catch (Exception ex) {
      fail(ex.toString());
    }

    // create table
    stmt.execute("create table " + tableName
        + " (under_col int comment 'the under column', value string) comment '"
        + tableComment + "'");

    // load data
    stmt.execute("load data local inpath '"
        + dataFilePath.toString() + "' into table " + tableName);
  }

  /**
   * Shut down a local Hive Server 2 for these tests
   */
  @AfterClass
  public static void postTests() {
    try {
      if (hiveServer2 != null) {
        System.err.println("Stopping HiveServer2...");
        hiveServer2.stop();
      }
    } catch (Throwable t) {
      t.printStackTrace();
    }
  }

  /**
   * Execute a script with "beeline -f"
   * @param scriptFileName The name of the script to execute
   * @throws Any exception while executing
   * @return The stderr and stdout from running the script
   */
  private String testCommandLineScript(String scriptFileName) throws Throwable {
    String[] args = {"-d", BeeLine.BEELINE_DEFAULT_JDBC_DRIVER, "-u", JDBC_URL, "-f", scriptFileName};
    BeeLine beeLine = new BeeLine();
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    PrintStream beelineOutputStream = new PrintStream(os);
    beeLine.setOutputStream(beelineOutputStream);
    beeLine.setErrorStream(beelineOutputStream);
    beeLine.begin(args, null);
    String output = os.toString("UTF8");

    return output;
  }

  /**
   * Attempt to execute a simple script file with the -f option to BeeLine
   * Test for presence of an expected pattern
   * in the output (stdout or stderr), fail if not found
   * Print PASSED or FAILED
   * @paramm testName Name of test to print
   * @param expecttedPattern Text to look for in command output
   * @param shouldMatch true if the pattern should be found, false if it should not
   * @throws Exception on command execution error
   */
  private void testScriptFile(String testName, String scriptText, String expectedPattern, boolean shouldMatch) throws Throwable {

    long startTime = System.currentTimeMillis();
    System.out.println(">>> STARTED " + testName);

    // Put the script content in a temp file
    File scriptFile = File.createTempFile(testName, "temp");
    scriptFile.deleteOnExit();
    PrintStream os = new PrintStream(new FileOutputStream(scriptFile));
    os.print(scriptText);
    os.close();

    if(shouldMatch) {
      try {
        String output = testCommandLineScript(scriptFile.getAbsolutePath());
        long elapsedTime = (System.currentTimeMillis() - startTime)/1000;
        String time = "(" + elapsedTime + "s)";
        if (output.contains(expectedPattern)) {
          System.out.println(">>> PASSED " + testName + " " + time);
        } else {
          System.err.println("Output: " + output);
          System.err.println(">>> FAILED " + testName + " (ERROR) " + time);
          fail(testName);
        }
      } catch (Throwable e) {
        e.printStackTrace();
        throw e;
      }
    } else {
      try {
        String output = testCommandLineScript(scriptFile.getAbsolutePath());
        long elapsedTime = (System.currentTimeMillis() - startTime)/1000;
        String time = "(" + elapsedTime + "s)";
        if (output.contains(expectedPattern)) {
          System.err.println("Output: " + output);
          System.err.println(">>> FAILED " + testName + " (ERROR) " + time);
          fail(testName);
        } else {
          System.out.println(">>> PASSED " + testName + " " + time);
        }
      } catch (Throwable e) {
        System.err.println("Exception: " + e.toString());
        e.printStackTrace();
        throw e;
      }
    }
    scriptFile.delete();
  }

  /**
   * Attempt to execute a simple script file with the -f option to BeeLine
   * Test for presence of an expected pattern
   * in the output (stdout or stderr), fail if not found
   * Print PASSED or FAILED
   */
  @Test
  public void testPositiveScriptFile() throws Throwable {
    final String TEST_NAME = "testPositiveScriptFile";
    final String SCRIPT_TEXT = "show databases;\n";
    final String EXPECTED_PATTERN = " default ";
    testScriptFile(TEST_NAME, SCRIPT_TEXT, EXPECTED_PATTERN, true);
  }

  /**
   * Attempt to execute a simple script file with the -f option to BeeLine
   * The first command should fail and the second command should not execute
   * Print PASSED or FAILED
   */
  @Test
  public void testBreakOnErrorScriptFile() throws Throwable {
    final String TEST_NAME = "testBreakOnErrorScriptFile";
    final String SCRIPT_TEXT = "select * from abcdefg01;\nshow databases;\n";
    final String EXPECTED_PATTERN = " default ";
    testScriptFile(TEST_NAME, SCRIPT_TEXT, EXPECTED_PATTERN, false);
  }


  /**
   * Select null from table , check how null is printed
   * Print PASSED or FAILED
   */
  @Test
  public void testNullDefault() throws Throwable {
    final String TEST_NAME = "testNullDefault";
    final String SCRIPT_TEXT = "set hive.support.concurrency = false;\n" +
    		"select under_col+null from " + tableName + " limit 1 ;\n";
    final String EXPECTED_PATTERN = "NULL";
    testScriptFile(TEST_NAME, SCRIPT_TEXT, EXPECTED_PATTERN, true);
  }

  /**
   * Select null from table , check if default null is printed differently
   * Print PASSED or FAILED
   */
  @Test
  public void testNullNonDefault() throws Throwable {
    final String TEST_NAME = "testNullNonDefault";
    final String SCRIPT_TEXT = "set hive.support.concurrency = false;\n" +
    		"!set nullstring nolls;\n select under_col+null from " + tableName + " limit 1 ;\n";
    final String EXPECTED_PATTERN = "nolls";
    testScriptFile(TEST_NAME, SCRIPT_TEXT, EXPECTED_PATTERN, true);
  }

  /**
   * Attempt to execute a missing script file with the -f option to BeeLine
   * Print PASSED or FAILED
   */
  @Test
  public void testNegativeScriptFile() throws Throwable {
    final String TEST_NAME = "testNegativeScriptFile";
    final String EXPECTED_PATTERN = " default ";

    long startTime = System.currentTimeMillis();
    System.out.println(">>> STARTED " + TEST_NAME);

    // Create and delete a temp file
    File scriptFile = File.createTempFile("beelinenegative", "temp");
    scriptFile.delete();

    try {
        String output = testCommandLineScript(scriptFile.getAbsolutePath());
      long elapsedTime = (System.currentTimeMillis() - startTime)/1000;
      String time = "(" + elapsedTime + "s)";
      if (output.contains(EXPECTED_PATTERN)) {
        System.err.println("Output: " + output);
        System.err.println(">>> FAILED " + TEST_NAME + " (ERROR) " + time);
        fail(TEST_NAME);
      } else {
        System.out.println(">>> PASSED " + TEST_NAME + " " + time);
      }
    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }
}
