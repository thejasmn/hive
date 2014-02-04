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

import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizationPluginException;


public enum SQLPrivilegeTypeWithGrant {
  SELECT_NOGRANT(SQLPrivilegeType.SELECT, false),
  SELECT_WGRANT(SQLPrivilegeType.SELECT, true),
  INSERT_NOGRANT(SQLPrivilegeType.INSERT, false),
  INSERT_WGRANT(SQLPrivilegeType.INSERT, true),
  UPDATE_NOGRANT(SQLPrivilegeType.UPDATE, false),
  UPDATE_WGRANT(SQLPrivilegeType.UPDATE, true),
  DELETE_NOGRANT(SQLPrivilegeType.DELETE, false),
  DELETE_WGRANT(SQLPrivilegeType.DELETE, true);

  final SQLPrivilegeType privType;
  final boolean withGrant;
  SQLPrivilegeTypeWithGrant(SQLPrivilegeType privType, boolean isGrant){
    this.privType = privType;
    this.withGrant = isGrant;
  }

  public static SQLPrivilegeTypeWithGrant getSQLPrivilegeWithGrantTypes(
      SQLPrivilegeType privType, boolean isGrant) {
    String typeName = privType.name() + (isGrant ? "_WGRANT" : "_NOGRANT");
    return SQLPrivilegeTypeWithGrant.valueOf(typeName);
  }

  public static SQLPrivilegeTypeWithGrant getSQLPrivilegeWithGrantTypes(
      String privTypeStr, boolean isGrant) throws HiveAuthorizationPluginException {
    SQLPrivilegeType ptype = SQLPrivilegeType.getRequirePrivilege(privTypeStr);
    return getSQLPrivilegeWithGrantTypes(ptype, isGrant);
  }

  /**
   * @return String representation for use in error messages
   */
  public String toInfoString(){
    return privType.toString() + (withGrant ? " with grant" : "");
  }

};
