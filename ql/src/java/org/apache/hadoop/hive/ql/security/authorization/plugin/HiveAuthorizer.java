/**
 * 
 */
package org.apache.hadoop.hive.ql.security.authorization.plugin;

import java.util.List;

import org.apache.hadoop.hive.common.classification.InterfaceAudience.Public;
import org.apache.hadoop.hive.common.classification.InterfaceStability.Evolving;

/**
 * Interface for hive authorization plugins.
 * Used by the DDLTasks for access control statement,
 * and for checking authorization from Driver.doAuthorization()
 */
@Public
@Evolving
public interface HiveAuthorizer {

  /**
   * Grant privileges for principals on the object
   * @param hivePrincipals
   * @param hivePrivileges
   * @param hivePrivObject
   * @param grantorPrincipal
   * @param grantOption
   */
  void grantPrivileges(List<HivePrincipal> hivePrincipals, List<HivePrivilege> hivePrivileges,
      HivePrivilegeObject hivePrivObject, HivePrincipal grantorPrincipal, boolean grantOption);

  /**
   * Revoke privileges for principals on the object
   * @param hivePrincipals
   * @param hivePrivileges
   * @param hivePrivObject
   * @param grantorPrincipal
   * @param grantOption
   */
  void revokePrivileges(List<HivePrincipal> hivePrincipals, List<HivePrivilege> hivePrivileges,
      HivePrivilegeObject hivePrivObject, HivePrincipal grantorPrincipal, boolean grantOption);

  /**
   * Create role
   * @param roleName
   * @param adminGrantor - The user in "[ WITH ADMIN <user> ]" clause of "create role"
   */
  void createRole(String roleName, HivePrincipal adminGrantor);

  /**
   * Drop role
   * @param roleName
   */
  void dropRole(String roleName);

  /**
   * Get roles that this user/role belongs to
   * @param hivePrincipal - user or role
   * @return list of roles
   */
  List<String> getRoles(HivePrincipal hivePrincipal);


  
  
//grantPrivileges(..)
//revokePrivileges(..)
//grantRole(..)
//revokeRole(..)
//showAllRoles(..)
//showRolesForUser(..)
//// other access control functions
//
//validateAuthority(HiveAction, inputs, outputs)

}

