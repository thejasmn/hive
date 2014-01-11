package org.apache.hadoop.hive.ql.security.authorization.plugin;

public class HiveCurrentAuthorizationID {
  private final HivePrincipal currentUser;
  private final HivePrincipal currentRole;
  
  public HiveCurrentAuthorizationID(HivePrincipal currentUser, HivePrincipal currentRole){
    this.currentUser = currentUser;
    this.currentRole = currentRole;
  }

  public HivePrincipal getCurrentUser() {
    return currentUser;
  }

  public HivePrincipal getCurrentRole() {
    return currentRole;
  }
  
}
