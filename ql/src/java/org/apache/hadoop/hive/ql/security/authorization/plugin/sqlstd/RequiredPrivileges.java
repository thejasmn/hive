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

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizationPluginException;

public class RequiredPrivileges {
  enum SQL_PRIVILEGE_TYPES{SELECT, INSERT, UPDATE, DELETE, ADMIN};

  static class PrivilegeWithGrant{
    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((privilege == null) ? 0 : privilege.hashCode());
      result = prime * result + (withGrant ? 1231 : 1237);
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      PrivilegeWithGrant other = (PrivilegeWithGrant) obj;
      if (privilege != other.privilege)
        return false;
      if (withGrant != other.withGrant)
        return false;
      return true;
    }

    final SQL_PRIVILEGE_TYPES privilege;
    final boolean withGrant;
    public PrivilegeWithGrant(SQL_PRIVILEGE_TYPES priv, boolean withGrant) {
      this.privilege = priv;
      this.withGrant = withGrant;
    }

    public PrivilegeWithGrant(String priv, boolean withGrant) throws HiveAuthorizationPluginException {
      this.privilege = getRequirePrivilege(priv);
      this.withGrant = withGrant;
    }

    private SQL_PRIVILEGE_TYPES getRequirePrivilege(String priv)
        throws HiveAuthorizationPluginException {

      SQL_PRIVILEGE_TYPES reqPriv;
      try {
        reqPriv = SQL_PRIVILEGE_TYPES.valueOf(priv);
      } catch(IllegalArgumentException e) {
        throw new HiveAuthorizationPluginException("Invalid privilege " + priv, e);
      } catch(NullPointerException e) {
        throw new HiveAuthorizationPluginException("Null privilege obtained", e);
      }
      return reqPriv;
    }
  }
  private final Set<PrivilegeWithGrant> privilegeGrantSet = new HashSet<PrivilegeWithGrant>();

  public void addPrivilege(String priv, boolean withGrant) throws HiveAuthorizationPluginException {
    PrivilegeWithGrant privWGrant = new PrivilegeWithGrant(priv, withGrant);
    privilegeGrantSet.add(privWGrant);
  }

  public Set<PrivilegeWithGrant> getRequiredPrivilegeSet() {
    return privilegeGrantSet;
  }

  /**
   * Find the missing privileges in availPrivs
   * @param availPrivs - available privileges
   * @return missing privileges as RequiredPrivileges object
   */
  public RequiredPrivileges findMissingPrivs(RequiredPrivileges availPrivs) {
    RequiredPrivileges missingPrivileges = new RequiredPrivileges();
    for (PrivilegeWithGrant requiredPriv : privilegeGrantSet) {
      if (!availPrivs.privilegeGrantSet.contains(requiredPriv)) {
        missingPrivileges.addPrivilege(requiredPriv);
      }
    }
    return missingPrivileges;
  }

  private void addPrivilege(PrivilegeWithGrant requiredPriv) {
    privilegeGrantSet.add(requiredPriv);
  }

  Set<PrivilegeWithGrant> getPrivilegeWithGrants(){
    return privilegeGrantSet;
  }

}
