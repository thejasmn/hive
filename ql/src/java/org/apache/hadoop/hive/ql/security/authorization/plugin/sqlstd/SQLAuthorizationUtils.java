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

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.HiveObjectType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.security.authorization.AuthorizationUtils;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizationPluginException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrincipal;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilege;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject.HivePrivilegeObjectType;
import org.apache.thrift.TException;

public class SQLAuthorizationUtils {

  private static final String[] SUPPORTED_PRIVS = { "INSERT", "UPDATE", "DELETE", "SELECT" };
  private static final Set<String> SUPPORTED_PRIVS_SET = new HashSet<String>(
      Arrays.asList(SUPPORTED_PRIVS));

  /**
   * Create thrift privileges bag
   *
   * @param hivePrincipals
   * @param hivePrivileges
   * @param hivePrivObject
   * @param grantorPrincipal
   * @param grantOption
   * @return
   * @throws HiveAuthorizationPluginException
   */
  static PrivilegeBag getThriftPrivilegesBag(List<HivePrincipal> hivePrincipals,
      List<HivePrivilege> hivePrivileges, HivePrivilegeObject hivePrivObject,
      HivePrincipal grantorPrincipal, boolean grantOption) throws HiveAuthorizationPluginException {
    HiveObjectRef privObj = getThriftHiveObjectRef(hivePrivObject);
    PrivilegeBag privBag = new PrivilegeBag();
    for (HivePrivilege privilege : hivePrivileges) {
      if (privilege.getColumns() != null && privilege.getColumns().size() > 0) {
        throw new HiveAuthorizationPluginException("Privileges on columns not supported currently"
            + " in sql standard authorization mode");
      }
      if (!SUPPORTED_PRIVS_SET.contains(privilege.getName().toUpperCase(Locale.US))) {
        throw new HiveAuthorizationPluginException("Privilege: " + privilege.getName()
            + " is not supported in sql standard authorization mode");
      }
      PrivilegeGrantInfo grantInfo = getThriftPrivilegeGrantInfo(privilege, grantorPrincipal,
          grantOption);
      for (HivePrincipal principal : hivePrincipals) {
        HiveObjectPrivilege objPriv = new HiveObjectPrivilege(privObj, principal.getName(),
            AuthorizationUtils.getThriftPrincipalType(principal.getType()), grantInfo);
        privBag.addToPrivileges(objPriv);
      }
    }
    return privBag;
  }

  static PrivilegeGrantInfo getThriftPrivilegeGrantInfo(HivePrivilege privilege,
      HivePrincipal grantorPrincipal, boolean grantOption) throws HiveAuthorizationPluginException {
    try {
      return AuthorizationUtils.getThriftPrivilegeGrantInfo(privilege, grantorPrincipal,
          grantOption);
    } catch (HiveException e) {
      throw new HiveAuthorizationPluginException(e);
    }
  }

  /**
   * Create a thrift privilege object from the plugin interface privilege object
   *
   * @param privObj
   * @return
   * @throws HiveAuthorizationPluginException
   */
  static HiveObjectRef getThriftHiveObjectRef(HivePrivilegeObject privObj)
      throws HiveAuthorizationPluginException {
    try {
      return AuthorizationUtils.getThriftHiveObjectRef(privObj);
    } catch (HiveException e) {
      throw new HiveAuthorizationPluginException(e);
    }
  }

  static HivePrivilegeObjectType getPluginObjType(HiveObjectType objectType)
      throws HiveAuthorizationPluginException {
    switch (objectType) {
    case DATABASE:
      return HivePrivilegeObjectType.DATABASE;
    case TABLE:
      return HivePrivilegeObjectType.TABLE;
    case COLUMN:
    case GLOBAL:
    case PARTITION:
      throw new HiveAuthorizationPluginException("Unsupported object type " + objectType);
    default:
      throw new AssertionError("Unexpected object type " + objectType);
    }
  }

  public static void validatePrivileges(List<HivePrivilege> hivePrivileges) throws HiveAuthorizationPluginException {
    for (HivePrivilege hivePrivilege : hivePrivileges) {
      if (hivePrivilege.getColumns() != null && hivePrivilege.getColumns().size() != 0) {
        throw new HiveAuthorizationPluginException(
            "Privilege with columns are not currently supported with sql standard authorization:"
                + hivePrivilege);
      }
    }
  }

  static RequiredPrivileges getReqPrivilegesFromMetaStore(IMetaStoreClient metastoreClient,
      String userName, HivePrivilegeObject hivePrivObject) throws HiveAuthorizationPluginException {

    // get privileges for this user and its role on this object
    PrincipalPrivilegeSet thrifPrivs = null;
    try {
      thrifPrivs = metastoreClient.get_privilege_set(
          AuthorizationUtils.getThriftHiveObjectRef(hivePrivObject), userName, null);
    } catch (MetaException e) {
      throwGetPrivErr(e, hivePrivObject, userName);
    } catch (TException e) {
      throwGetPrivErr(e, hivePrivObject, userName);
    } catch (HiveException e) {
      throwGetPrivErr(e, hivePrivObject, userName);
    }

    // convert to RequiredPrivileges
    return getRequiredPrivsFromThrift(thrifPrivs);
  }

  private static void throwGetPrivErr(Exception e, HivePrivilegeObject hivePrivObject,
      String userName) throws HiveAuthorizationPluginException {
    String msg = "Error getting privileges on " + hivePrivObject + " for " + userName;
    throw new HiveAuthorizationPluginException(msg, e);
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

  public static void assertNoMissingPrivilege(Collection<SQLPrivTypeGrant> missingPrivs,
      HivePrincipal hivePrincipal, HivePrivilegeObject hivePrivObject)
          throws HiveAuthorizationPluginException {
    if (missingPrivs.size() != 0) {
      // there are some required privileges missing, create error message
      StringBuilder errMsg = new StringBuilder("Permission denied. " + hivePrincipal
          + " does not have following privileges on " + hivePrivObject + " :");
      for (SQLPrivTypeGrant reqPriv : missingPrivs) {
        errMsg.append(reqPriv.toInfoString()).append(", ");
      }
      throw new HiveAuthorizationPluginException(errMsg.toString());
    }
  }


}
