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

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;

public class Operation2Privilege {


  private static class InOutPrivs {
    private final SQLPrivilegeTypeWithGrant[] inputPrivs;
    private final SQLPrivilegeTypeWithGrant[] outputPrivs;

    InOutPrivs(SQLPrivilegeTypeWithGrant[] inputPrivs, SQLPrivilegeTypeWithGrant[] outputPrivs) {
      this.inputPrivs = inputPrivs;
      this.outputPrivs = outputPrivs;
    }

    private SQLPrivilegeTypeWithGrant[] getInputPrivs() {
      return inputPrivs;
    }

    private SQLPrivilegeTypeWithGrant[] getOutputPrivs() {
      return outputPrivs;
    }
  }

  private static Map<HiveOperationType, InOutPrivs> op2Priv;

  static {
    op2Priv = new HashMap<HiveOperationType, InOutPrivs>();

    op2Priv.put(HiveOperationType.EXPLAIN, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.LOAD, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.EXPORT, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.IMPORT, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.CREATEDATABASE, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.DROPDATABASE, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.SWITCHDATABASE, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.LOCKDB, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.UNLOCKDB, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.DROPTABLE, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.DESCTABLE, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.DESCFUNCTION, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.MSCK, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.ALTERTABLE_ADDCOLS, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.ALTERTABLE_REPLACECOLS, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.ALTERTABLE_RENAMECOL, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.ALTERTABLE_RENAMEPART, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.ALTERTABLE_RENAME, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.ALTERTABLE_DROPPARTS, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.ALTERTABLE_ADDPARTS, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.ALTERTABLE_TOUCH, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.ALTERTABLE_ARCHIVE, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.ALTERTABLE_UNARCHIVE, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.ALTERTABLE_PROPERTIES, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.ALTERTABLE_SERIALIZER, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.ALTERTABLE_PARTCOLTYPE, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.ALTERPARTITION_SERIALIZER, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.ALTERTABLE_SERDEPROPERTIES, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.ALTERPARTITION_SERDEPROPERTIES, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.ALTERTABLE_CLUSTER_SORT, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.ANALYZE_TABLE, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.ALTERTABLE_BUCKETNUM, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.ALTERPARTITION_BUCKETNUM, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.SHOWDATABASES, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.SHOWTABLES, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.SHOWCOLUMNS, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.SHOW_TABLESTATUS, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.SHOW_TBLPROPERTIES, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.SHOW_CREATETABLE, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.SHOWFUNCTIONS, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.SHOWINDEXES, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.SHOWPARTITIONS, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.SHOWLOCKS, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.CREATEFUNCTION, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.DROPFUNCTION, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.CREATEMACRO, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.DROPMACRO, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.CREATEVIEW, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.DROPVIEW, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.CREATEINDEX, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.DROPINDEX, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.ALTERINDEX_REBUILD, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.ALTERVIEW_PROPERTIES, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.DROPVIEW_PROPERTIES, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.LOCKTABLE, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.UNLOCKTABLE, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.ALTERTABLE_PROTECTMODE, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.ALTERPARTITION_PROTECTMODE, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.ALTERTABLE_FILEFORMAT, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.ALTERPARTITION_FILEFORMAT, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.ALTERTABLE_LOCATION, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.ALTERPARTITION_LOCATION, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.CREATETABLE, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.TRUNCATETABLE, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.CREATETABLE_AS_SELECT, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.QUERY, new InOutPrivs(arr(SQLPrivilegeTypeWithGrant.SELECT_NOGRANT), null));
    op2Priv.put(HiveOperationType.ALTERINDEX_PROPS, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.ALTERDATABASE, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.DESCDATABASE, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.ALTERTABLE_MERGEFILES, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.ALTERPARTITION_MERGEFILES, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.ALTERTABLE_SKEWED, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.ALTERTBLPART_SKEWED_LOCATION, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.ALTERVIEW_RENAME, new InOutPrivs(null, null));


    //  The following actions are authorized through SQLStdHiveAccessController,
    // and it is not using this privilege mapping
    // op2Priv.put(HiveOperationType.CREATEROLE, new InOutPrivs(null, null));
    // op2Priv.put(HiveOperationType.DROPROLE, new InOutPrivs(null, null));
    // op2Priv.put(HiveOperationType.GRANT_PRIVILEGE, new InOutPrivs(null,
    // null));
    // op2Priv.put(HiveOperationType.REVOKE_PRIVILEGE, new InOutPrivs(null,
    // null));
    // op2Priv.put(HiveOperationType.SHOW_GRANT, new InOutPrivs(null, null));
    // op2Priv.put(HiveOperationType.GRANT_ROLE, new InOutPrivs(null, null));
    // op2Priv.put(HiveOperationType.REVOKE_ROLE, new InOutPrivs(null, null));
    // op2Priv.put(HiveOperationType.SHOW_ROLES, new InOutPrivs(null, null));
    // op2Priv.put(HiveOperationType.SHOW_ROLE_GRANT, new InOutPrivs(null,
    // null));

  }

  /**
   * Convenience method so that creation of this array in InOutPrivs constructor is not too verbose
   * @param grantList
   * @return grantList
   */
  private static SQLPrivilegeTypeWithGrant[] arr(SQLPrivilegeTypeWithGrant... grantList) {
    return grantList;
  }

  public static SQLPrivilegeTypeWithGrant[] getInputPrivs(HiveOperationType opType){
    return op2Priv.get(opType).getInputPrivs();
  }

  public static SQLPrivilegeTypeWithGrant[] getOutputPrivs(HiveOperationType opType){
    return op2Priv.get(opType).getOutputPrivs();
  }


}
