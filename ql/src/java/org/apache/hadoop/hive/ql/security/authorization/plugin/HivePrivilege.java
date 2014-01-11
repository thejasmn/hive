package org.apache.hadoop.hive.ql.security.authorization.plugin;

import java.util.List;

/**
 * Represents the hive privilege being granted/revoked
 */
public class HivePrivilege {
  private final String name;
  private final List<String> columns;
 
  public HivePrivilege(String name, List<String> columns){
    this.name = name;
    this.columns = columns;
  }

  public String getName() {
    return name;
  }

  public List<String> getColumns() {
    return columns;
  }
  
}
