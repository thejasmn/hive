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
package org.apache.hadoop.hive.metastore;

import java.io.IOException;

import junit.framework.TestCase;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.server.HiveServer2;

public class TestServerSpecificConfig extends TestCase {

  /**
   * Verify if appropriate server configuration (metastore, hiveserver2)
   * get loaded when the embedded clients are loaded
   *
   * @throws IOException
   * @throws Throwable
   */
  public void testHiveMetastoreSiteConfigs() throws IOException, Throwable {
    HiveConf conf = new HiveConf();
    // check the properties expected in hive client without metastore
    assertFalse(conf.isLoadMetastoreConfig());
    assertEquals("from.hive-site.xml",
        conf.get("hive.dummyparam.test.server.specific.config.override"));
    assertNull(conf.get("hive.dummyparam.test.server.specific.config.metastoresite"));


    // check config properties expected with embedded metastore client
    HiveMetaStoreClient msClient = new HiveMetaStoreClient(conf);
    assertTrue(conf.isLoadMetastoreConfig());
    assertEquals("from.hivemetastore-site.xml",
        conf.get("hive.dummyparam.test.server.specific.config.override"));

    assertEquals("from.hivemetastore-site.xml",
        conf.get("hive.dummyparam.test.server.specific.config.metastoresite"));

    assertEquals("from.hive-site.xml",
        conf.get("hive.dummyparam.test.server.specific.config.hivesite"));

    //verify that hiveserver2 config is not loaded
    assertFalse(conf.isLoadHiveServer2Config());
    assertNull(conf.get("hive.dummyparam.test.server.specific.config.hiveserver2site"));

    // check if hiveserver2 config gets loaded when HS2 is started

    HiveServer2 hs2 = new HiveServer2();
    conf = new HiveConf();
    assertTrue(conf.isLoadHiveServer2Config());
    assertEquals("from.hiveserver2-site.xml",
        conf.get("hive.dummyparam.test.server.specific.config.override"));

    assertEquals("from.hiveserver2-site.xml",
        conf.get("hive.dummyparam.test.server.specific.config.hiveserver2site"));

    assertEquals("from.hivemetastore-site.xml",
        conf.get("hive.dummyparam.test.server.specific.config.metastoresite"));

    assertEquals("from.hive-site.xml",
        conf.get("hive.dummyparam.test.server.specific.config.hivesite"));


  }
}