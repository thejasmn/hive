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

package org.apache.hadoop.hive.ql.security.authorization;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.HiveMetaStore.HMSHandler;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.metadata.AuthorizationException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;

/**
 *
 */
public class AuthorizationAPIAuthorizationProvider extends HiveAuthorizationProviderBase
    implements HiveMetastoreAuthorizationProvider, HiveMetastoreAuthzAPIAuthorizer {

  @Override
  public void init(Configuration conf) throws HiveException {
    setConf(conf);
  }

  @Override
  public void authorize(Privilege[] readRequiredPriv, Privilege[] writeRequiredPriv)
      throws HiveException, AuthorizationException {
    // not authorized by this implementation, ie operation is allowed by it
  }

  @Override
  public void authorize(Database db, Privilege[] readRequiredPriv, Privilege[] writeRequiredPriv)
      throws HiveException, AuthorizationException {
    // not authorized by this implementation, ie operation is allowed by it
  }

  @Override
  public void authorize(Table table, Privilege[] readRequiredPriv, Privilege[] writeRequiredPriv)
      throws HiveException, AuthorizationException {
    // not authorized by this implementation, ie operation is allowed by it
  }

  @Override
  public void authorize(Partition part, Privilege[] readRequiredPriv, Privilege[] writeRequiredPriv)
      throws HiveException, AuthorizationException {
    // not authorized by this implementation, ie operation is allowed by it
  }

  @Override
  public void authorize(Table table, Partition part, List<String> columns,
      Privilege[] readRequiredPriv, Privilege[] writeRequiredPriv) throws HiveException,
      AuthorizationException {
    // not authorized by this implementation, ie operation is allowed by it
  }

  @Override
  public void setMetaStoreHandler(HMSHandler handler) {
    // no-op - HMSHander not needed by this impl
  }

  @Override
  public void authorizeAuthorizationApiInvocation() {
    if (HiveMetaStore.isMetaStoreRemote()) {
      throw new AuthorizationException("Metastore Authorization api invocation for "
          + "remote metastore is disabled in this configuration.");
    }
  }


}
