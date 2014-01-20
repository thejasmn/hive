package org.apache.hadoop.hive.ql.security.authorization;

import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.hooks.Entity.Type;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrincipal.HivePrincipalType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject.HivePrivilegeObjectType;

/**
 * Utility code shared by hive internal code and sql standard authorization plugin implementation
 */
@LimitedPrivate(value = { "Sql standard authorization plugin" })
public class AuthorizationUtils {

  public static HivePrincipalType getHivePrincipalType(PrincipalType type) throws HiveException {
    switch(type){
    case USER:
      return HivePrincipalType.USER;
    case ROLE:
      return HivePrincipalType.ROLE;
    case GROUP:
      throw new HiveException(ErrorMsg.UNNSUPPORTED_AUTHORIZATION_PRINCIPAL_TYPE_GROUP);
    default:
      //should not happen as we take care of all existing types
      throw new HiveException("Unsupported authorization type specified");
    }
  }
  
  
  public static HivePrivilegeObjectType getHivePrivilegeObjectType(Type type) {
    switch(type){
    case DATABASE:
      return HivePrivilegeObjectType.DATABASE;
    case TABLE:
      return HivePrivilegeObjectType.TABLE;
    case LOCAL_DIR:
    case DFS_DIR:
      return HivePrivilegeObjectType.URI;
    case PARTITION:
    case DUMMYPARTITION: //need to determine if a different type is needed for dummy partitions
      return HivePrivilegeObjectType.PARTITION;
    default:
      return null;
    }
  }
  
  
}
