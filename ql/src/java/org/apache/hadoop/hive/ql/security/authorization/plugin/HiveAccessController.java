package org.apache.hadoop.hive.ql.security.authorization.plugin;

import java.util.List;

public interface HiveAccessController {

//  grantPrivileges();
//  revokePrivileges(); 
//  grantRole()
//  revokeRole()
//  showAllRoles()
//// other access control functions

  void grantPrivileges(List<HivePrincipal> hivePrincipals, List<HivePrivilege> hivePrivileges,
      HivePrivilegeObject hivePrivObject, HivePrincipal grantorPrincipal, boolean grantOption);
  
  void revokePrivileges(List<HivePrincipal> hivePrincipals, List<HivePrivilege> hivePrivileges,
      HivePrivilegeObject hivePrivObject, HivePrincipal grantorPrincipal, boolean grantOption);

  void createRole(String roleName, HivePrincipal adminGrantor);

  void dropRole(String roleName);

  List<String> getRoles(HivePrincipal hivePrincipal);
  
}
