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

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.parse.HiveParser;

/**
 * Privilege defines a privilege in Hive. Each privilege has a name and scope associated with it.
 * This class contains all of the predefined privileges in Hive.
 */
public class Privilege {
  private static Map<Integer, PrivilegeType> token2Type = new HashMap<Integer, PrivilegeType>();
  private static Map<String, PrivilegeType> name2Type = new HashMap<String, PrivilegeType>();

  public enum PrivilegeType {
    ALL(HiveParser.TOK_PRIV_ALL, "All"),
    ALTER_DATA(HiveParser.TOK_PRIV_ALTER_DATA, "Update"),
    ALTER_METADATA(HiveParser.TOK_PRIV_ALTER_METADATA, "Alter"),
    CREATE(HiveParser.TOK_PRIV_CREATE, "Create"),
    DROP(HiveParser.TOK_PRIV_DROP, "Drop"),
    INDEX(HiveParser.TOK_PRIV_INDEX, "Index"),
    LOCK(HiveParser.TOK_PRIV_LOCK, "Lock"),
    SELECT(HiveParser.TOK_PRIV_SELECT, "Select"),
    SHOW_DATABASE(HiveParser.TOK_PRIV_SHOW_DATABASE, "Show_Database"),
    INSERT(HiveParser.TOK_PRIV_INSERT, "Insert"),
    DELETE(HiveParser.TOK_PRIV_DELETE, "Delete"),
    UNKNOWN(null, null);

    String name;
    PrivilegeType(Integer token, String name){
      this.name = name;
      token2Type.put(token, this);
      name2Type.put(name == null ? null : name.toLowerCase(), this);
    }
  }


  public static PrivilegeType getPrivTypeByToken(int token) {
    PrivilegeType privType = token2Type.get(token);
    if(privType != null){
      return privType;
    }
    return PrivilegeType.UNKNOWN;
  }

  public static PrivilegeType getPrivTypeByName(String privilegeName) {
    String canonicalizedName = privilegeName.toLowerCase();
    PrivilegeType privType = name2Type.get(canonicalizedName);
    if(privType != null){
      return privType;
    }
    return PrivilegeType.UNKNOWN;
  }

  private PrivilegeType priv;

  private EnumSet<PrivilegeScope> supportedScopeSet;

  private Privilege(PrivilegeType priv, EnumSet<PrivilegeScope> scopeSet) {
    super();
    this.priv = priv;
    this.supportedScopeSet = scopeSet;
  }

  public Privilege(PrivilegeType priv) {
    super();
    this.priv = priv;

  }

  public PrivilegeType getPriv() {
    return priv;
  }

  public void setPriv(PrivilegeType priv) {
    this.priv = priv;
  }

  public boolean supportColumnLevel() {
    return supportedScopeSet != null
        && supportedScopeSet.contains(PrivilegeScope.COLUMN_LEVEL_SCOPE);
  }

  public boolean supportDBLevel() {
    return supportedScopeSet != null
        && supportedScopeSet.contains(PrivilegeScope.DB_LEVEL_SCOPE);
  }

  public boolean supportTableLevel() {
    return supportedScopeSet != null
        && supportedScopeSet.contains(PrivilegeScope.TABLE_LEVEL_SCOPE);
  }

  @Override
  public String toString() {
    return this.getPriv().toString();
  }

  public Privilege() {
  }

  public static Privilege ALL = new Privilege(PrivilegeType.ALL,
      PrivilegeScope.ALLSCOPE_EXCEPT_COLUMN);

  public static Privilege ALTER_METADATA = new Privilege(PrivilegeType.ALTER_METADATA,
      PrivilegeScope.ALLSCOPE_EXCEPT_COLUMN);

  public static Privilege ALTER_DATA = new Privilege(PrivilegeType.ALTER_DATA,
      PrivilegeScope.ALLSCOPE_EXCEPT_COLUMN);

  public static Privilege CREATE = new Privilege(PrivilegeType.CREATE,
      PrivilegeScope.ALLSCOPE_EXCEPT_COLUMN);

  public static Privilege DROP = new Privilege(PrivilegeType.DROP,
      PrivilegeScope.ALLSCOPE_EXCEPT_COLUMN);

  public static Privilege INDEX = new Privilege(PrivilegeType.INDEX,
      PrivilegeScope.ALLSCOPE);

  public static Privilege LOCK = new Privilege(PrivilegeType.LOCK,
      PrivilegeScope.ALLSCOPE_EXCEPT_COLUMN);

  public static Privilege SELECT = new Privilege(PrivilegeType.SELECT,
      PrivilegeScope.ALLSCOPE);

  public static Privilege INSERT = new Privilege(PrivilegeType.INSERT,
      PrivilegeScope.ALLSCOPE_EXCEPT_COLUMN);

  public static Privilege DELETE = new Privilege(PrivilegeType.DELETE,
      PrivilegeScope.ALLSCOPE_EXCEPT_COLUMN);

  public static Privilege SHOW_DATABASE = new Privilege(PrivilegeType.SHOW_DATABASE,
      EnumSet.of(PrivilegeScope.USER_LEVEL_SCOPE));

}
