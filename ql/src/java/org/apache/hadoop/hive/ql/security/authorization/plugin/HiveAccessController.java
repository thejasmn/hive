package org.apache.hadoop.hive.ql.security.authorization.plugin;

import java.util.List;

public interface HiveAccessController {

  void grantPrivileges(List<HivePrincipal> hivePrincipals, List<HivePrivilege> hivePrivileges,
      HivePrivilegeObject hivePrivObject, HivePrincipal grantorPrincipal, boolean grantOption);
//  grantPrivileges();
//  revokePrivileges(); 
//  grantRole()
//  revokeRole()
//  showAllRoles()
//// other access control functions

  void revokePrivileges(List<HivePrincipal> hivePrincipals, List<HivePrivilege> hivePrivileges,
      HivePrivilegeObject hivePrivObject, HivePrincipal grantorPrincipal, boolean grantOption);
}
