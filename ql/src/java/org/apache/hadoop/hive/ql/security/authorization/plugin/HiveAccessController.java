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

import java.util.List;

import org.apache.hadoop.hive.common.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.hive.common.classification.InterfaceStability.Evolving;

/**
 * Interface that is invoked by access control commands, including grant/revoke role/privileges,
 * create/drop roles, and commands to read the state of authorization rules.
 * Methods here have corresponding methods in HiveAuthorizer, check method documentation there.
 */
@LimitedPrivate(value = { "" })
@Evolving
public interface HiveAccessController {

  void grantPrivileges(List<HivePrincipal> hivePrincipals, List<HivePrivilege> hivePrivileges,
      HivePrivilegeObject hivePrivObject, HivePrincipal grantorPrincipal, boolean grantOption)
          throws HiveAuthzPluginException, HiveAccessControlException;

  void revokePrivileges(List<HivePrincipal> hivePrincipals, List<HivePrivilege> hivePrivileges,
      HivePrivilegeObject hivePrivObject, HivePrincipal grantorPrincipal, boolean grantOption)
          throws HiveAuthzPluginException, HiveAccessControlException;

  void createRole(String roleName, HivePrincipal adminGrantor)
      throws HiveAuthzPluginException, HiveAccessControlException;

  void dropRole(String roleName)
      throws HiveAuthzPluginException, HiveAccessControlException;

  List<HiveRole> getRoles(HivePrincipal hivePrincipal)
      throws HiveAuthzPluginException, HiveAccessControlException;

  void grantRole(List<HivePrincipal> hivePrincipals, List<String> roles, boolean grantOption,
      HivePrincipal grantorPrinc)
          throws HiveAuthzPluginException, HiveAccessControlException;

  void revokeRole(List<HivePrincipal> hivePrincipals, List<String> roles, boolean grantOption,
      HivePrincipal grantorPrinc)
          throws HiveAuthzPluginException, HiveAccessControlException;

  List<String> getAllRoles()
      throws HiveAuthzPluginException, HiveAccessControlException;

  List<HivePrivilegeInfo> showPrivileges(HivePrincipal principal, HivePrivilegeObject privObj)
      throws HiveAuthzPluginException, HiveAccessControlException;

  void setCurrentRole(String roleName) throws HiveAuthzPluginException, HiveAccessControlException;

  List<HiveRole> getCurrentRoles() throws HiveAuthzPluginException;

  List<HiveRoleGrant> getPrincipalsInRoleInfo(String roleName);
}
