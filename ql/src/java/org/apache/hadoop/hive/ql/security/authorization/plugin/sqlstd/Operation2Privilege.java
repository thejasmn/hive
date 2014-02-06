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
    private final SQLPrivTypeGrant[] inputPrivs;
    private final SQLPrivTypeGrant[] outputPrivs;

    InOutPrivs(SQLPrivTypeGrant[] inputPrivs, SQLPrivTypeGrant[] outputPrivs) {
      this.inputPrivs = inputPrivs;
      this.outputPrivs = outputPrivs;
    }

    private SQLPrivTypeGrant[] getInputPrivs() {
      return inputPrivs;
    }

    private SQLPrivTypeGrant[] getOutputPrivs() {
      return outputPrivs;
    }
  }

  private static Map<HiveOperationType, InOutPrivs> op2Priv;

  static {
    op2Priv = new HashMap<HiveOperationType, InOutPrivs>();

    op2Priv.put(HiveOperationType.EXPLAIN, new InOutPrivs(arr(SQLPrivTypeGrant.SELECT_NOGRANT),
        arr(SQLPrivTypeGrant.SELECT_NOGRANT))); //??
    op2Priv.put(HiveOperationType.LOAD, new InOutPrivs(arr(SQLPrivTypeGrant.ADMIN_PRIV), null));
    op2Priv.put(HiveOperationType.EXPORT, new InOutPrivs(arr(SQLPrivTypeGrant.SELECT_NOGRANT), null));
    op2Priv.put(HiveOperationType.IMPORT, new InOutPrivs(arr(SQLPrivTypeGrant.ADMIN_PRIV), null));
    op2Priv.put(HiveOperationType.CREATEDATABASE, new InOutPrivs(arr(SQLPrivTypeGrant.ADMIN_PRIV), null));
    op2Priv.put(HiveOperationType.DROPDATABASE, new InOutPrivs(arr(SQLPrivTypeGrant.ADMIN_PRIV), null));
    op2Priv.put(HiveOperationType.SWITCHDATABASE, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.LOCKDB, new InOutPrivs(arr(SQLPrivTypeGrant.ADMIN_PRIV), null));
    op2Priv.put(HiveOperationType.UNLOCKDB, new InOutPrivs(arr(SQLPrivTypeGrant.ADMIN_PRIV), null));
    op2Priv.put(HiveOperationType.DROPTABLE, new InOutPrivs(arr(SQLPrivTypeGrant.OWNER_PRIV), null));
    op2Priv.put(HiveOperationType.DESCTABLE, new InOutPrivs(arr(SQLPrivTypeGrant.SELECT_NOGRANT), null));
    op2Priv.put(HiveOperationType.DESCFUNCTION, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.MSCK, new InOutPrivs(arr(SQLPrivTypeGrant.ADMIN_PRIV), null));
    op2Priv.put(HiveOperationType.ALTERTABLE_ADDCOLS, new InOutPrivs(arr(SQLPrivTypeGrant.OWNER_PRIV), null));
    op2Priv.put(HiveOperationType.ALTERTABLE_REPLACECOLS, new InOutPrivs(arr(SQLPrivTypeGrant.OWNER_PRIV), null));
    op2Priv.put(HiveOperationType.ALTERTABLE_RENAMECOL, new InOutPrivs(arr(SQLPrivTypeGrant.OWNER_PRIV), null));
    op2Priv.put(HiveOperationType.ALTERTABLE_RENAMEPART, new InOutPrivs(arr(SQLPrivTypeGrant.OWNER_PRIV), null));
    op2Priv.put(HiveOperationType.ALTERTABLE_RENAME, new InOutPrivs(arr(SQLPrivTypeGrant.OWNER_PRIV), null));
    op2Priv.put(HiveOperationType.ALTERTABLE_DROPPARTS, new InOutPrivs(arr(SQLPrivTypeGrant.OWNER_PRIV), null));
    op2Priv.put(HiveOperationType.ALTERTABLE_ADDPARTS, new InOutPrivs(arr(SQLPrivTypeGrant.OWNER_PRIV), null));
    op2Priv.put(HiveOperationType.ALTERTABLE_TOUCH, new InOutPrivs(arr(SQLPrivTypeGrant.OWNER_PRIV), null));
    op2Priv.put(HiveOperationType.ALTERTABLE_ARCHIVE, new InOutPrivs(arr(SQLPrivTypeGrant.OWNER_PRIV), null));
    op2Priv.put(HiveOperationType.ALTERTABLE_UNARCHIVE, new InOutPrivs(arr(SQLPrivTypeGrant.OWNER_PRIV), null));
    op2Priv.put(HiveOperationType.ALTERTABLE_PROPERTIES, new InOutPrivs(arr(SQLPrivTypeGrant.OWNER_PRIV), null));
    op2Priv.put(HiveOperationType.ALTERTABLE_SERIALIZER, new InOutPrivs(arr(SQLPrivTypeGrant.OWNER_PRIV), null));
    op2Priv.put(HiveOperationType.ALTERTABLE_PARTCOLTYPE, new InOutPrivs(arr(SQLPrivTypeGrant.OWNER_PRIV), null));
    op2Priv.put(HiveOperationType.ALTERPARTITION_SERIALIZER, new InOutPrivs(arr(SQLPrivTypeGrant.OWNER_PRIV), null));
    op2Priv.put(HiveOperationType.ALTERTABLE_SERDEPROPERTIES, new InOutPrivs(arr(SQLPrivTypeGrant.OWNER_PRIV), null));
    op2Priv.put(HiveOperationType.ALTERPARTITION_SERDEPROPERTIES, new InOutPrivs(arr(SQLPrivTypeGrant.OWNER_PRIV), null));
    op2Priv.put(HiveOperationType.ALTERTABLE_CLUSTER_SORT, new InOutPrivs(arr(SQLPrivTypeGrant.OWNER_PRIV), null));
    op2Priv.put(HiveOperationType.ANALYZE_TABLE, new InOutPrivs(arr(SQLPrivTypeGrant.SELECT_NOGRANT, SQLPrivTypeGrant.INSERT_NOGRANT), null));
    op2Priv.put(HiveOperationType.ALTERTABLE_BUCKETNUM, new InOutPrivs(arr(SQLPrivTypeGrant.OWNER_PRIV), null));
    op2Priv.put(HiveOperationType.ALTERPARTITION_BUCKETNUM, new InOutPrivs(arr(SQLPrivTypeGrant.OWNER_PRIV), null));
    op2Priv.put(HiveOperationType.SHOWDATABASES, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.SHOWTABLES, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.SHOWCOLUMNS, new InOutPrivs(arr(SQLPrivTypeGrant.SELECT_NOGRANT), null));
    op2Priv.put(HiveOperationType.SHOW_TABLESTATUS, new InOutPrivs(arr(SQLPrivTypeGrant.SELECT_NOGRANT), null));
    op2Priv.put(HiveOperationType.SHOW_TBLPROPERTIES, new InOutPrivs(arr(SQLPrivTypeGrant.SELECT_NOGRANT), null));

    //show create table is more sensitive information, includes table properties etc
    // for now require select WITH GRANT
    op2Priv.put(HiveOperationType.SHOW_CREATETABLE, new InOutPrivs(arr(SQLPrivTypeGrant.SELECT_WGRANT), null));
    op2Priv.put(HiveOperationType.SHOWFUNCTIONS, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.SHOWINDEXES, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.SHOWPARTITIONS, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.SHOWLOCKS, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.CREATEFUNCTION, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.DROPFUNCTION, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.CREATEMACRO, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.DROPMACRO, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.CREATEVIEW, new InOutPrivs(arr(SQLPrivTypeGrant.SELECT_WGRANT), null));

    // require view ownership
    op2Priv.put(HiveOperationType.DROPVIEW, new InOutPrivs(arr(SQLPrivTypeGrant.OWNER_PRIV), null));

    op2Priv.put(HiveOperationType.CREATEINDEX, new InOutPrivs(arr(SQLPrivTypeGrant.ADMIN_PRIV), null));
    op2Priv.put(HiveOperationType.DROPINDEX, new InOutPrivs(arr(SQLPrivTypeGrant.ADMIN_PRIV), null));
    op2Priv.put(HiveOperationType.ALTERINDEX_REBUILD, new InOutPrivs(arr(SQLPrivTypeGrant.ADMIN_PRIV), null));

    // require view ownership
    op2Priv.put(HiveOperationType.ALTERVIEW_PROPERTIES, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.DROPVIEW_PROPERTIES, new InOutPrivs(null, null));

    // require table ownership
    op2Priv.put(HiveOperationType.LOCKTABLE, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.UNLOCKTABLE, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.ALTERTABLE_PROTECTMODE, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.ALTERPARTITION_PROTECTMODE, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.ALTERTABLE_FILEFORMAT, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.ALTERPARTITION_FILEFORMAT, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.ALTERTABLE_LOCATION, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.ALTERPARTITION_LOCATION, new InOutPrivs(null, null));

    op2Priv.put(HiveOperationType.CREATETABLE, new InOutPrivs(null, null));

    // require table ownership
    op2Priv.put(HiveOperationType.TRUNCATETABLE, new InOutPrivs(arr(SQLPrivTypeGrant.OWNER_PRIV), null));

    op2Priv.put(HiveOperationType.CREATETABLE_AS_SELECT, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.QUERY, new InOutPrivs(arr(SQLPrivTypeGrant.SELECT_NOGRANT), null));
    op2Priv.put(HiveOperationType.ALTERINDEX_PROPS, new InOutPrivs(arr(SQLPrivTypeGrant.ADMIN_PRIV), null));
    op2Priv.put(HiveOperationType.ALTERDATABASE, new InOutPrivs(arr(SQLPrivTypeGrant.ADMIN_PRIV), null));
    op2Priv.put(HiveOperationType.DESCDATABASE, new InOutPrivs(null, null));

    // require table ownership
    op2Priv.put(HiveOperationType.ALTERTABLE_MERGEFILES, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.ALTERPARTITION_MERGEFILES, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.ALTERTABLE_SKEWED, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.ALTERTBLPART_SKEWED_LOCATION, new InOutPrivs(null, null));

    //require view ownership
    op2Priv.put(HiveOperationType.ALTERVIEW_RENAME, new InOutPrivs(null, null));


    // The following actions are authorized through SQLStdHiveAccessController,
    // and it is not using this privilege mapping, but it might make sense to move it here
    op2Priv.put(HiveOperationType.CREATEROLE, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.DROPROLE, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.GRANT_PRIVILEGE, new InOutPrivs(null,
        null));
    op2Priv.put(HiveOperationType.REVOKE_PRIVILEGE, new InOutPrivs(null,
        null));
    op2Priv.put(HiveOperationType.SHOW_GRANT, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.GRANT_ROLE, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.REVOKE_ROLE, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.SHOW_ROLES, new InOutPrivs(null, null));
    op2Priv.put(HiveOperationType.SHOW_ROLE_GRANT, new InOutPrivs(null,
        null));

  }

  /**
   * Convenience method so that creation of this array in InOutPrivs constructor is not too verbose
   * @param grantList
   * @return grantList
   */
  private static SQLPrivTypeGrant[] arr(SQLPrivTypeGrant... grantList) {
    return grantList;
  }

  public static SQLPrivTypeGrant[] getInputPrivs(HiveOperationType opType){
    return op2Priv.get(opType).getInputPrivs();
  }

  public static SQLPrivTypeGrant[] getOutputPrivs(HiveOperationType opType){
    return op2Priv.get(opType).getOutputPrivs();
  }


}
