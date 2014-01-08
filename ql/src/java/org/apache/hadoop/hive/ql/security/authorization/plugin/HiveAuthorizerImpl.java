package org.apache.hadoop.hive.ql.security.authorization.plugin;

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
   void grantPrivileges(){
      //  HiveAccessController.grantPrivileges();
   }

 // other access control functions

//   void validateAuthority(HiveAction, inputs, outputs){
//     authValidator.validateAuthority(HiveAction, inputs, outputs);
//   }
}
