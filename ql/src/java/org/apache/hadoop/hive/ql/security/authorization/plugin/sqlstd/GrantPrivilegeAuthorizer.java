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
package org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.security.authorization.AuthorizationUtils;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizationPluginException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrincipal;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilege;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.thrift.TException;

/**
 * Utility class to authorize grant/revoke privileges
 */
public class GrantPrivilegeAuthorizer {

  static void authorize(List<HivePrincipal> hivePrincipals,
      List<HivePrivilege> hivePrivileges, HivePrivilegeObject hivePrivObject, boolean grantOption,
      IMetaStoreClient metastoreClient, String userName) throws HiveAuthorizationPluginException {

    // check if this user has grant privileges for this privileges on this
    // object

    // map priv being granted to required privileges
    RequiredPrivileges reqPrivs = getGrantRequiredPrivileges(hivePrivileges);

    // api for checking required privileges for a user
    checkRequiredPrivileges(hivePrincipals, reqPrivs, hivePrivObject, metastoreClient, userName);
  }

  private static void checkRequiredPrivileges(List<HivePrincipal> hivePrincipals,
      RequiredPrivileges reqPrivs, HivePrivilegeObject hivePrivObject,
      IMetaStoreClient metastoreClient, String userName) throws HiveAuthorizationPluginException {
    for (HivePrincipal hivePrincipal : hivePrincipals) {
      checkRequiredPrivileges(hivePrincipal, reqPrivs, hivePrivObject, metastoreClient, userName);
    }
  }

  private static void checkRequiredPrivileges(HivePrincipal hivePrincipal,
      RequiredPrivileges reqPrivileges, HivePrivilegeObject hivePrivObject,
      IMetaStoreClient metastoreClient, String userName) throws HiveAuthorizationPluginException {
    // keep track of the principals on which privileges have been checked for
    // this object

    // get privileges for this user and its role on this object
    PrincipalPrivilegeSet thrifPrivs = null;
    try {
      thrifPrivs = metastoreClient.get_privilege_set(
          AuthorizationUtils.getThriftHiveObjectRef(hivePrivObject), userName, null);
    } catch (MetaException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (TException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (HiveException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    // convert to RequiredPrivileges
    RequiredPrivileges availPrivs = getRequiredPrivsFromThrift(thrifPrivs);

    // check if required privileges is subset of available privileges
    Collection<SQLPrivilegeWithGrantTypes> missingPrivs = reqPrivileges.findMissingPrivs(availPrivs);
    if (missingPrivs.size() != 0) {
      // there are some required privileges missing, create error message
      StringBuilder errMsg = new StringBuilder("Permission denied. User " + userName
          + " does not have following privileges: ");
      for (SQLPrivilegeWithGrantTypes reqPriv : missingPrivs) {
        errMsg.append(reqPriv.toInfoString()).append(", ");
      }
      throw new HiveAuthorizationPluginException(errMsg.toString());
    }

  }

  private static RequiredPrivileges getRequiredPrivsFromThrift(PrincipalPrivilegeSet thrifPrivs)
      throws HiveAuthorizationPluginException {

    RequiredPrivileges reqPrivs = new RequiredPrivileges();
    // add user privileges
    Map<String, List<PrivilegeGrantInfo>> userPrivs = thrifPrivs.getUserPrivileges();
    if (userPrivs != null && userPrivs.size() != 1) {
      throw new HiveAuthorizationPluginException("Invalid number of user privilege objects: "
          + userPrivs.size());
    }
    addRequiredPrivs(reqPrivs, userPrivs);

    // add role privileges
    Map<String, List<PrivilegeGrantInfo>> rolePrivs = thrifPrivs.getRolePrivileges();
    addRequiredPrivs(reqPrivs, rolePrivs);
    return reqPrivs;
  }

  /**
   * Add privileges to RequiredPrivileges object reqPrivs from thrift availPrivs
   * object
   * @param reqPrivs
   * @param availPrivs
   * @throws HiveAuthorizationPluginException
   */
  private static void addRequiredPrivs(RequiredPrivileges reqPrivs,
      Map<String, List<PrivilegeGrantInfo>> availPrivs) throws HiveAuthorizationPluginException {
    if(availPrivs == null){
      return;
    }
    for (Map.Entry<String, List<PrivilegeGrantInfo>> userPriv : availPrivs.entrySet()) {
      List<PrivilegeGrantInfo> userPrivGInfos = userPriv.getValue();
      for (PrivilegeGrantInfo userPrivGInfo : userPrivGInfos) {
        reqPrivs.addPrivilege(userPrivGInfo.getPrivilege(), userPrivGInfo.isGrantOption());
      }
    }
  }

  private static RequiredPrivileges getGrantRequiredPrivileges(List<HivePrivilege> hivePrivileges)
      throws HiveAuthorizationPluginException {
    RequiredPrivileges reqPrivs = new RequiredPrivileges();
    for (HivePrivilege hivePriv : hivePrivileges) {
      reqPrivs.addPrivilege(hivePriv.getName(), true /* grant priv required */);
    }
    return reqPrivs;
  }

}
