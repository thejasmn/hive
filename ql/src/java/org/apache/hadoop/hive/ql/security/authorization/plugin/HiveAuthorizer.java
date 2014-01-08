/**
 * 
 */
package org.apache.hadoop.hive.ql.security.authorization.plugin;

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
//  grantPrivileges(..)
//  revokePrivileges(..)
//  grantRole(..)
//  revokeRole(..)
//  showAllRoles(..)
//  showRolesForUser(..)
//  // other access control functions
//
//  validateAuthority(HiveAction, inputs, outputs)
}

