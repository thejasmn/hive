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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizationPluginException;

public class RequiredPrivileges {

  private final Set<SQLPrivTypeGrant> privilegeGrantSet = new HashSet<SQLPrivTypeGrant>();

  public void addPrivilege(String priv, boolean withGrant) throws HiveAuthorizationPluginException {
    SQLPrivTypeGrant privType = SQLPrivTypeGrant.getSQLPrivilegeWithGrantTypes(priv, withGrant);
    addPrivilege(privType);
    privilegeGrantSet.add(privType);
    if(withGrant){
      //as with grant also implies without grant privilege, add without privilege as well
      addPrivilege(priv, false);
    }
  }

  public Set<SQLPrivTypeGrant> getRequiredPrivilegeSet() {
    return privilegeGrantSet;
  }

  /**
   * Find the missing privileges in availPrivs
   *
   * @param availPrivs
   *          - available privileges
   * @return missing privileges as RequiredPrivileges object
   */
  public Collection<SQLPrivTypeGrant> findMissingPrivs(RequiredPrivileges availPrivs) {
    MissingPrivilegeCapturer missingPrivCapturer = new MissingPrivilegeCapturer();
    for (SQLPrivTypeGrant requiredPriv : privilegeGrantSet) {
      if (!availPrivs.privilegeGrantSet.contains(requiredPriv)) {
        missingPrivCapturer.addMissingPrivilege(requiredPriv);
      }
    }
    return missingPrivCapturer.getMissingPrivileges();
  }

  private void addPrivilege(SQLPrivTypeGrant requiredPriv) {
    privilegeGrantSet.add(requiredPriv);
  }

  Set<SQLPrivTypeGrant> getPrivilegeWithGrants() {
    return privilegeGrantSet;
  }

  /**
   * Capture privileges that are missing. If privilege "X with grant" and "X without grant"
   * are reported missing, capture only "X with grant". This is useful for better error messages.
   */
  class MissingPrivilegeCapturer {

    private Map<SQLPrivilegeType, SQLPrivTypeGrant> priv2privWithGrant = new HashMap<SQLPrivilegeType, SQLPrivTypeGrant>();

    void addMissingPrivilege(SQLPrivTypeGrant newPrivWGrant) {
      SQLPrivTypeGrant matchingPrivWGrant = priv2privWithGrant.get(newPrivWGrant.privType);
      if (matchingPrivWGrant != null) {
        if (matchingPrivWGrant.withGrant || !newPrivWGrant.withGrant) {
          // the existing entry already has grant, or new priv does not have
          // grant
          // no update needs to be done.
          return;
        }
      }
      // add the new entry
      priv2privWithGrant.put(newPrivWGrant.privType, newPrivWGrant);
    }

    Collection<SQLPrivTypeGrant> getMissingPrivileges() {
      return priv2privWithGrant.values();
    }

  }

  public void addAll(SQLPrivTypeGrant[] inputPrivs) {
    if (inputPrivs == null) {
      return;
    }
    for (SQLPrivTypeGrant privType : inputPrivs) {
      addPrivilege(privType);
    }
  }

}
