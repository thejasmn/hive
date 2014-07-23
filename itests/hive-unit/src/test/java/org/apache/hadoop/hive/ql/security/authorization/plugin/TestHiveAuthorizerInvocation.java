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

package org.apache.hadoop.hive.ql.security.authorization.plugin;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;
import org.apache.hadoop.hive.ql.security.SessionStateUserAuthenticator;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject.HivePrivilegeObjectType;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;
import org.mockito.Mockito;

/**
 * Test HiveAuthorizer api invocation
 */
public class TestHiveAuthorizerInvocation  {
  protected static HiveConf conf;
  protected static Driver driver;

  static HiveAuthorizer mockedAuthorizer;

  /**
   * This factory creates a mocked HiveAuthorizer class.
   * Use the mocked class to capture the argument passed to it in the test case.
   */
  static class MockedHiveAuthorizerFactory implements HiveAuthorizerFactory {
    @Override
    public HiveAuthorizer createHiveAuthorizer(HiveMetastoreClientFactory metastoreClientFactory,
        HiveConf conf, HiveAuthenticationProvider authenticator) {
      TestHiveAuthorizerInvocation.mockedAuthorizer = Mockito.mock(HiveAuthorizer.class);
      return TestHiveAuthorizerInvocation.mockedAuthorizer;
    }

  }

  @BeforeClass
  public static void beforeTest() throws Exception {
    conf = new HiveConf();

    // Turn on sql standard authorization
    conf.setVar(ConfVars.HIVE_AUTHORIZATION_MANAGER, MockedHiveAuthorizerFactory.class.getName());
    conf.setVar(ConfVars.HIVE_AUTHENTICATOR_MANAGER, SessionStateUserAuthenticator.class.getName());
    conf.setBoolVar(ConfVars.HIVE_AUTHORIZATION_ENABLED, true);
    conf.setBoolVar(ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    conf.setBoolVar(ConfVars.HIVE_SERVER2_ENABLE_DOAS, false);

    SessionState.start(conf);
    driver = new Driver(conf);
  }

  @AfterClass
  public static void afterTets() throws Exception {
    driver.close();
  }

  @Test
  public void testInputColumnsUsed() throws HiveAuthzPluginException, HiveAccessControlException, CommandNeedRetryException{
    String tableName = this.getClass().getSimpleName();
    CommandProcessorResponse resp = driver.run("create table " + tableName + " (i int, j int, k string)");
    assertEquals(0, resp.getResponseCode());

    int status = driver.compile("select i, k from " + tableName);
    assertEquals(0, status);

    // Create argument capturer
    // a class variable cast to this generic of generic class
    Class<List<HivePrivilegeObject>> class_listPrivObjects =
        (Class) List.class;
    ArgumentCaptor<List<HivePrivilegeObject>> inputsCapturer = ArgumentCaptor
        .forClass(class_listPrivObjects);

    verify(mockedAuthorizer, times(2)).checkPrivileges(any(HiveOperationType.class),
        inputsCapturer.capture(),
        Matchers.anyListOf(HivePrivilegeObject.class), any(HiveAuthzContext.class));

    assertEquals("number of input lists captured", 2, inputsCapturer.getAllValues().size());
    List<HivePrivilegeObject> inputs = inputsCapturer.getAllValues().get(1);
    assertEquals("number of inputs in second call to mocked api", 1, inputs.size());

    HivePrivilegeObject tableObj = inputs.get(0);
    assertEquals("input type", HivePrivilegeObjectType.TABLE_OR_VIEW, tableObj.getType());
    assertTrue("table name", tableName.equalsIgnoreCase(tableObj.getTableViewURI()));
    assertEquals("no of columns used", 2 , tableObj.getColumns().size());

    Collections.sort(tableObj.getColumns());
    assertEquals("Columns used", Arrays.asList("i", "k"), tableObj.getColumns());
  }


}
