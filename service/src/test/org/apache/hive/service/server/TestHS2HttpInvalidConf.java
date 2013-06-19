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

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hive.service.auth.HiveAuthFactory.AuthTypes;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test hive server2 using thrift over http transport.
 *
 */
public class TestHS2HttpInvalidConf {
  private static int portNum;

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    portNum = MetaStoreUtils.findFreePort();
    Class.forName(org.apache.hive.jdbc.HiveDriver.class.getName());
  }

  private void startHS2WithConf(HiveConf hiveConf)
      throws SQLException, IOException {
    hiveConf.setVar(ConfVars.HIVE_SERVER2_SERVERMODE, "http");
    portNum = MetaStoreUtils.findFreePort();
    hiveConf.setIntVar(ConfVars.HIVE_SERVER2_HTTP_PORT, portNum);

    HiveServer2 hiveServer2 = new HiveServer2();
    hiveServer2.init(hiveConf);
    hiveServer2.start();

  }


  public void testWithAuthMode(AuthTypes authType) {
    //test that invalid http path results in exception
    boolean caughtEx = false;
    try{
      HiveConf hconf = new HiveConf();
      hconf.setVar(ConfVars.HIVE_SERVER2_AUTHENTICATION, authType.toString());

      //unfortunately, the startup can't throw an exception
      // because of the way the service interfaces are
      startHS2WithConf(hconf);

      String url = "jdbc:hive2://localhost:" + portNum;

      //this should throw an exception as the cluster will be down
      DriverManager.getConnection(url, "", "");
    }catch(SQLException e){
      caughtEx = true;
    } catch (IOException e) {
      //this exception is not expected
      e.printStackTrace();
    }
    assertTrue("exception expected", caughtEx);
  }

  @Test
  public void testKerberosMode() {
    testWithAuthMode(AuthTypes.KERBEROS);
  }

  @Test
  public void testLDAPMode() {
    testWithAuthMode(AuthTypes.LDAP);
  }

  @Test
  public void testCustomMode() {
    testWithAuthMode(AuthTypes.CUSTOM);
  }
}
