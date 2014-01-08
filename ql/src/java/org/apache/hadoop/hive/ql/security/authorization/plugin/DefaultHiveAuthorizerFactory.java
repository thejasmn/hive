package org.apache.hadoop.hive.ql.security.authorization.plugin;

import org.apache.hadoop.hive.common.classification.InterfaceAudience.Private;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.Hive;

@Private
public class DefaultHiveAuthorizerFactory implements HiveAuthorizerFactory{
  public HiveAuthorizer createHiveAuthorizer(Hive db, HiveConf conf){
   // return new HiveAuthorizerImpl(new DefaultHiveAccessController(db, conf), new Default HiveAuthValidator(db, conf));
   return null;
  }
}
