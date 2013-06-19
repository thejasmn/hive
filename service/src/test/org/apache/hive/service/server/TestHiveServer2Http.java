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

package org.apache.hive.service.server;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test hive server2 using thrift over http transport.
 *
 */
public class TestHiveServer2Http {

  private static HiveServer2 hiveServer2;
  private static int portNum;
  private static final String HTTP_PATH = "hs2path";

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    HiveConf hiveConf = new HiveConf();
    hiveConf.setVar(ConfVars.HIVE_SERVER2_SERVERMODE, "http");
    portNum = MetaStoreUtils.findFreePort();
    hiveConf.setIntVar(ConfVars.HIVE_SERVER2_HTTP_PORT, portNum);
    hiveConf.setVar(ConfVars.HIVE_SERVER2_HTTP_PATH, HTTP_PATH);
    hiveConf.setBoolVar(ConfVars.HIVE_SERVER2_ENABLE_DOAS, false);
    hiveServer2 = new HiveServer2();
    hiveServer2.init(hiveConf);
    hiveServer2.start();
    Thread.sleep(1000);

    Class.forName(org.apache.hive.jdbc.HiveDriver.class.getName());
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    if (hiveServer2 != null) {
      hiveServer2.stop();
    }
  }

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
  }

  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testPositive() throws SQLException {
    test("http", HTTP_PATH);

  }

  private void test(String serverMode, String httpPath) throws SQLException {
    String url = "jdbc:hive2://localhost:" + portNum
        + "/default?"
        + ConfVars.HIVE_SERVER2_SERVERMODE + "=" + serverMode
        + ";"
        + ConfVars.HIVE_SERVER2_HTTP_PATH + "=" + httpPath
        ;

    Connection con1 = DriverManager.getConnection(url, "", "");

    assertNotNull("Connection is null", con1);
    assertFalse("Connection should not be closed", con1.isClosed());

    Statement stmt = con1.createStatement();
    assertNotNull("Statement is null", stmt);

    stmt.execute("show databases");

    ResultSet res = stmt.getResultSet();
    assertTrue("has at least one database", res.next());

    stmt.close();
  }

  @Test
  public void testInvalidPath() throws SQLException {
    //test that invalid http path results in exception
    boolean caughtEx = false;
    try{
      test("http", "invalidPath");
    }catch(SQLException e){
      caughtEx = true;
    }
    assertTrue("exception expected", caughtEx);

  }

  //Disabled test - resulted in OOM ex
  //TODO investigate why the OOM happened
  public void testIncorrectMode() throws SQLException {
    //test that trying to connect using thrift results in exception
    boolean caughtEx = false;
    try{
      test("thrift", "invalidPath");
    }catch(SQLException e){
      caughtEx = true;
    }
    assertTrue("exception expected", caughtEx);

  }

}
