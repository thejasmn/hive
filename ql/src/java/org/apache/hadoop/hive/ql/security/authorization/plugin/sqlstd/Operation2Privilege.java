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

import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;

public enum Operation2Privilege {

  EXPLAIN (HiveOperationType.EXPLAIN, null, null),
  LOAD (HiveOperationType.LOAD, null, null),
  EXPORT (HiveOperationType.EXPORT, null, null),
  IMPORT (HiveOperationType.IMPORT, null, null),
  CREATEDATABASE (HiveOperationType.CREATEDATABASE, null, null),
  DROPDATABASE (HiveOperationType.DROPDATABASE, null, null),
  SWITCHDATABASE (HiveOperationType.SWITCHDATABASE, null, null),
  LOCKDB (HiveOperationType.LOCKDB, null, null),
  UNLOCKDB (HiveOperationType.UNLOCKDB, null, null),
  DROPTABLE  (HiveOperationType.DROPTABLE , null, null),
  DESCTABLE (HiveOperationType.DESCTABLE, null, null),
  DESCFUNCTION (HiveOperationType.DESCFUNCTION, null, null),
  MSCK (HiveOperationType.MSCK, null, null),
  ALTERTABLE_ADDCOLS (HiveOperationType.ALTERTABLE_ADDCOLS, null, null),
  ALTERTABLE_REPLACECOLS (HiveOperationType.ALTERTABLE_REPLACECOLS, null, null),
  ALTERTABLE_RENAMECOL (HiveOperationType.ALTERTABLE_RENAMECOL, null, null),
  ALTERTABLE_RENAMEPART (HiveOperationType.ALTERTABLE_RENAMEPART, null, null),
  ALTERTABLE_RENAME (HiveOperationType.ALTERTABLE_RENAME, null, null),
  ALTERTABLE_DROPPARTS (HiveOperationType.ALTERTABLE_DROPPARTS, null, null),
  ALTERTABLE_ADDPARTS (HiveOperationType.ALTERTABLE_ADDPARTS, null, null),
  ALTERTABLE_TOUCH (HiveOperationType.ALTERTABLE_TOUCH, null, null),
  ALTERTABLE_ARCHIVE (HiveOperationType.ALTERTABLE_ARCHIVE, null, null),
  ALTERTABLE_UNARCHIVE (HiveOperationType.ALTERTABLE_UNARCHIVE, null, null),
  ALTERTABLE_PROPERTIES (HiveOperationType.ALTERTABLE_PROPERTIES, null, null),
  ALTERTABLE_SERIALIZER (HiveOperationType.ALTERTABLE_SERIALIZER, null, null),
  ALTERTABLE_PARTCOLTYPE (HiveOperationType.ALTERTABLE_PARTCOLTYPE, null, null),
  ALTERPARTITION_SERIALIZER (HiveOperationType.ALTERPARTITION_SERIALIZER, null, null),
  ALTERTABLE_SERDEPROPERTIES (HiveOperationType.ALTERTABLE_SERDEPROPERTIES, null, null),
  ALTERPARTITION_SERDEPROPERTIES (HiveOperationType.ALTERPARTITION_SERDEPROPERTIES, null, null),
  ALTERTABLE_CLUSTER_SORT (HiveOperationType.ALTERTABLE_CLUSTER_SORT, null, null),
  ANALYZE_TABLE (HiveOperationType.ANALYZE_TABLE, null, null),
  ALTERTABLE_BUCKETNUM (HiveOperationType.ALTERTABLE_BUCKETNUM, null, null),
  ALTERPARTITION_BUCKETNUM (HiveOperationType.ALTERPARTITION_BUCKETNUM, null, null),
  SHOWDATABASES (HiveOperationType.SHOWDATABASES, null, null),
  SHOWTABLES (HiveOperationType.SHOWTABLES, null, null),
  SHOWCOLUMNS (HiveOperationType.SHOWCOLUMNS, null, null),
  SHOW_TABLESTATUS (HiveOperationType.SHOW_TABLESTATUS, null, null),
  SHOW_TBLPROPERTIES (HiveOperationType.SHOW_TBLPROPERTIES, null, null),
  SHOW_CREATETABLE (HiveOperationType.SHOW_CREATETABLE, null, null),
  SHOWFUNCTIONS (HiveOperationType.SHOWFUNCTIONS, null, null),
  SHOWINDEXES (HiveOperationType.SHOWINDEXES, null, null),
  SHOWPARTITIONS (HiveOperationType.SHOWPARTITIONS, null, null),
  SHOWLOCKS (HiveOperationType.SHOWLOCKS, null, null),
  CREATEFUNCTION (HiveOperationType.CREATEFUNCTION, null, null),
  DROPFUNCTION (HiveOperationType.DROPFUNCTION, null, null),
  CREATEMACRO (HiveOperationType.CREATEMACRO, null, null),
  DROPMACRO (HiveOperationType.DROPMACRO, null, null),
  CREATEVIEW (HiveOperationType.CREATEVIEW, null, null),
  DROPVIEW (HiveOperationType.DROPVIEW, null, null),
  CREATEINDEX (HiveOperationType.CREATEINDEX, null, null),
  DROPINDEX (HiveOperationType.DROPINDEX, null, null),
  ALTERINDEX_REBUILD (HiveOperationType.ALTERINDEX_REBUILD, null, null),
  ALTERVIEW_PROPERTIES (HiveOperationType.ALTERVIEW_PROPERTIES, null, null),
  DROPVIEW_PROPERTIES (HiveOperationType.DROPVIEW_PROPERTIES, null, null),
  LOCKTABLE (HiveOperationType.LOCKTABLE, null, null),
  UNLOCKTABLE (HiveOperationType.UNLOCKTABLE, null, null),
  ALTERTABLE_PROTECTMODE (HiveOperationType.ALTERTABLE_PROTECTMODE, null, null),
  ALTERPARTITION_PROTECTMODE (HiveOperationType.ALTERPARTITION_PROTECTMODE, null, null),
  ALTERTABLE_FILEFORMAT (HiveOperationType.ALTERTABLE_FILEFORMAT, null, null),
  ALTERPARTITION_FILEFORMAT (HiveOperationType.ALTERPARTITION_FILEFORMAT, null, null),
  ALTERTABLE_LOCATION (HiveOperationType.ALTERTABLE_LOCATION, null, null),
  ALTERPARTITION_LOCATION (HiveOperationType.ALTERPARTITION_LOCATION, null, null),
  CREATETABLE (HiveOperationType.CREATETABLE, null, null),
  TRUNCATETABLE (HiveOperationType.TRUNCATETABLE, null, null),
  CREATETABLE_AS_SELECT (HiveOperationType.CREATETABLE_AS_SELECT, null, null),
  QUERY (HiveOperationType.QUERY, asArray(SQLPrivilegeTypeWithGrant.SELECT_NOGRANT), null),
  ALTERINDEX_PROPS (HiveOperationType.ALTERINDEX_PROPS, null, null),
  ALTERDATABASE (HiveOperationType.ALTERDATABASE, null, null),
  DESCDATABASE (HiveOperationType.DESCDATABASE, null, null),
  ALTERTABLE_MERGEFILES (HiveOperationType.ALTERTABLE_MERGEFILES, null, null),
  ALTERPARTITION_MERGEFILES (HiveOperationType.ALTERPARTITION_MERGEFILES, null, null),
  ALTERTABLE_SKEWED (HiveOperationType.ALTERTABLE_SKEWED, null, null),
  ALTERTBLPART_SKEWED_LOCATION (HiveOperationType.ALTERTBLPART_SKEWED_LOCATION, null, null),
  ALTERVIEW_RENAME (HiveOperationType.ALTERVIEW_RENAME, null, null),


  // The following actions are authorized through SQLStdHiveAccessController, and it is not
  // using this privilege mapping
  CREATEROLE (HiveOperationType.CREATEROLE, null, null),
  DROPROLE (HiveOperationType.DROPROLE, null, null),
  GRANT_PRIVILEGE (HiveOperationType.GRANT_PRIVILEGE, null, null),
  REVOKE_PRIVILEGE (HiveOperationType.REVOKE_PRIVILEGE, null, null),
  SHOW_GRANT (HiveOperationType.SHOW_GRANT, null, null),
  GRANT_ROLE (HiveOperationType.GRANT_ROLE, null, null),
  REVOKE_ROLE (HiveOperationType.REVOKE_ROLE, null, null),
  SHOW_ROLES (HiveOperationType.SHOW_ROLES, null, null),
  SHOW_ROLE_GRANT (HiveOperationType.SHOW_ROLE_GRANT, null, null);



  Operation2Privilege(HiveOperationType opType, SQLPrivilegeTypeWithGrant[] inputPrivs,
      SQLPrivilegeTypeWithGrant[] outputPrivs){

  }

  /**
   * Function to help write the enum constructor in a concise way.
   * @param privs
   * @return
   */
  private static SQLPrivilegeTypeWithGrant[] asArray(SQLPrivilegeTypeWithGrant... privs) {
    return privs;
  }

};
