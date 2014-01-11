package org.apache.hadoop.hive.ql.security.authorization.plugin;

import java.util.List;

import org.apache.hadoop.hive.common.classification.InterfaceAudience.Public;
import org.apache.hadoop.hive.common.classification.InterfaceStability.Evolving;

/**
 * Convenience implementation of HiveAuthorizer.
 * You can customize the behavior by passing different implementations of 
 * {@link HiveAccessController} and {@link HiveAuthorizationValidator} to constructor.
 *
 */
@Public
@Evolving
public class HiveAuthorizerImpl implements HiveAuthorizer {
  HiveAccessController accessController;
  HiveAuthorizationValidator authValidator;

   HiveAuthorizerImpl(HiveAccessController accessController, HiveAuthorizationValidator authValidator){
     this.accessController = accessController;
     this.authValidator = authValidator;
   }

  @Override
  public void grantPrivileges(List<HivePrincipal> hivePrincipals,
      List<HivePrivilege> hivePrivileges, HivePrivilegeObject hivePrivObject,
      HivePrincipal grantorPrincipal, boolean grantOption) {
    accessController.grantPrivileges(hivePrincipals, hivePrivileges, hivePrivObject, 
        grantorPrincipal, grantOption);
    
  }

  @Override
  public void revokePrivileges(List<HivePrincipal> hivePrincipals,
      List<HivePrivilege> hivePrivileges, HivePrivilegeObject hivePrivObject,
      HivePrincipal grantorPrincipal, boolean grantOption) {
    accessController.revokePrivileges(hivePrincipals, hivePrivileges, hivePrivObject, 
        grantorPrincipal, grantOption);
    
  }

  @Override
  public void createRole(String roleName, HivePrincipal adminGrantor) {
    accessController.createRole(roleName, adminGrantor);
  }

  @Override
  public void dropRole(String roleName) {
    accessController.dropRole(roleName);
  }

  @Override
  public List<String> getRoles(HivePrincipal hivePrincipal) {
    return accessController.getRoles(hivePrincipal);
  }


 // other access control functions

//   void validateAuthority(HiveAction, inputs, outputs){
//     authValidator.validateAuthority(HiveAction, inputs, outputs);
//   }
}
