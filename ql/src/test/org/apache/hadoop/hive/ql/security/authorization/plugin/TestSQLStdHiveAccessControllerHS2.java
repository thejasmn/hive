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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.security.HadoopDefaultAuthenticator;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzSessionContext.Builder;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzSessionContext.CLIENT_TYPE;
import org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAccessController;
import org.junit.Test;

import com.google.common.base.Joiner;

/**
 * Test SQLStdHiveAccessController
 */
public class TestSQLStdHiveAccessControllerHS2 {

  /**
   * Test if SQLStdHiveAccessController is applying configuration security
   * policy on hiveconf correctly
   *
   * @throws HiveAuthzPluginException
   */
  @Test
  public void testConfigProcessing() throws HiveAuthzPluginException {
    HiveConf processedConf = newAuthEnabledConf();
    SQLStdHiveAccessController accessController = new SQLStdHiveAccessController(null,
        processedConf, new HadoopDefaultAuthenticator(), getHS2SessionCtx()
        );
    accessController.applyAuthorizationConfigPolicy(processedConf);

    // check that hook to disable transforms has been added
    assertTrue("Check for transform query disabling hook",
        processedConf.getVar(ConfVars.PREEXECHOOKS).contains(DisallowTransformHook.class.getName()));

    String[] settableParams = getSettableParams();
    verifyParamSettability(settableParams, processedConf);

  }

  private HiveConf newAuthEnabledConf() {
    HiveConf conf = new HiveConf();
    conf.setBoolVar(ConfVars.HIVE_AUTHORIZATION_ENABLED, true);
    return conf;
  }

  /**
   * @return list of parameters that should be possible to set
   */
  private String[] getSettableParams() {
    List<String> settableParams = new ArrayList<String>(
        Arrays.asList(SettableConfigUpdater.defaultConfVars));
    for (String regex : SettableConfigUpdater.defaultPatterns) {
      // create dummy param that matches regex
      String confParam = regex.replace(".*", ".dummy");
      settableParams.add(confParam);
    }
    return settableParams.toArray(new String[0]);
  }

  private HiveAuthzSessionContext getHS2SessionCtx() {
    Builder ctxBuilder = new HiveAuthzSessionContext.Builder();
    ctxBuilder.setClientType(CLIENT_TYPE.HIVESERVER2);
    return ctxBuilder.build();
  }

  /**
   * Verify that params in settableParams can be modified, and other random ones can't be modified
   * @param settableParams
   * @param processedConf
   */
  private void verifyParamSettability(String [] settableParams, HiveConf processedConf) {
    // verify that the whitlelist params can be set
    for (String param : settableParams) {
      try {
        processedConf.verifyAndSet(param, "dummy");
      } catch (IllegalArgumentException e) {
        fail("Unable to set value for parameter in whitelist " + param + " " + e);
      }
    }

    // verify that non whitelist params can't be set
    assertConfModificationException(processedConf, "dummy.param");
    // does not make sense to have any of the metastore config variables to be
    // modifiable
    for (ConfVars metaVar : HiveConf.metaVars) {
      assertConfModificationException(processedConf, metaVar.varname);
    }
  }

  /**
   * Test that setting HIVE_AUTHORIZATION_SQL_STD_AUTH_CONFIG_WHITELIST_APPEND config works
   * @throws HiveAuthzPluginException
   */
  @Test
  public void testConfigProcessingCustomSetWhitelistAppend() throws HiveAuthzPluginException {
    // append new config params to whitelist
    String[] paramRegexes = { "hive.ctest.param", "hive.abc..*" };
    String[] settableParams = { "hive.ctest.param", "hive.abc.def" };
    verifySettability(paramRegexes, settableParams,
        ConfVars.HIVE_AUTHORIZATION_SQL_STD_AUTH_CONFIG_WHITELIST_APPEND);
  }

  /**
   * Test that setting HIVE_AUTHORIZATION_SQL_STD_AUTH_CONFIG_WHITELIST config works
   * @throws HiveAuthzPluginException
   */
  @Test
  public void testConfigProcessingCustomSetWhitelist() throws HiveAuthzPluginException {
    // append new config params to whitelist
    String[] paramRegexes = { "hive.ctest.param", "hive.abc..*" };
    String[] settableParams = { "hive.ctest.param", "hive.abc.def" };
    verifySettability(paramRegexes, settableParams,
        ConfVars.HIVE_AUTHORIZATION_SQL_STD_AUTH_CONFIG_WHITELIST);
  }

  private void verifySettability(String[] paramRegexes, String[] settableParams,
      ConfVars hiveAuthorizationSqlStdAuthConfigWhitelist) throws HiveAuthzPluginException {
    HiveConf processedConf = newAuthEnabledConf();
    processedConf.setVar(hiveAuthorizationSqlStdAuthConfigWhitelist,
        Joiner.on(",").join(paramRegexes));

    SQLStdHiveAccessController accessController = new SQLStdHiveAccessController(null,
        processedConf, new HadoopDefaultAuthenticator(), getHS2SessionCtx());
    accessController.applyAuthorizationConfigPolicy(processedConf);

    verifyParamSettability(settableParams, processedConf);
  }

  private void assertConfModificationException(HiveConf processedConf, String param) {
    boolean caughtEx = false;
    try {
      processedConf.verifyAndSet(param, "dummy");
    } catch (IllegalArgumentException e) {
      caughtEx = true;
    }
    assertTrue("Exception should be thrown while modifying the param " + param, caughtEx);
  }


}
