package org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd;

import java.util.List;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrincipal;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;

public class GrantPrivilegeAuthorizer {

   static void authorizeGrantPrivilege(List<HivePrincipal> hivePrincipals,
      HivePrivilegeObject hivePrivObject, HivePrincipal grantorPrincipal, boolean grantOption,
      IMetaStoreClient metastoreClient, String userName) {

     //check if this user has grant privileges for this privileges on this object
     metastoreClient.get

   }

}
