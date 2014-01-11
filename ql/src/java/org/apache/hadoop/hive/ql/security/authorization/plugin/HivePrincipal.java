package org.apache.hadoop.hive.ql.security.authorization.plugin;

/**
 * Represents the user or role in grant/revoke statements
 */
public class HivePrincipal {
  
  public enum HivePrincipalType{
    USER, ROLE, UNKNOWN
  }
  
  private final String name;
  private final HivePrincipalType type;
  
  public HivePrincipal(String name, HivePrincipalType type){
    this.name = name;
    this.type = type;
  }
  
  public String getName() {
    return name;
  }
  public HivePrincipalType getType() {
    return type;
  }
  
}
