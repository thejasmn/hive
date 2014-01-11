package org.apache.hadoop.hive.ql.security.authorization.plugin;

/**
 * Represents the object on which privilege is being granted/revoked 
 */
public class HivePrivilegeObject {

  public enum HivePrivilegeObjectType { DATABASE, TABLE, VIEW};
  private final HivePrivilegeObjectType type;
  private final String dbname;
  private final String tableviewname;
  
  public HivePrivilegeObject(HivePrivilegeObjectType type, String dbname, String tableviewname){
    this.type = type;
    this.dbname = dbname;
    this.tableviewname = tableviewname;
  }

  public HivePrivilegeObjectType getType() {
    return type;
  }

  public String getDbname() {
    return dbname;
  }

  public String getTableviewname() {
    return tableviewname;
  } 
}
