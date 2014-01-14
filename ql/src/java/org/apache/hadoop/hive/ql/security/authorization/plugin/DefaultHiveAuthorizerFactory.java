package org.apache.hadoop.hive.ql.security.authorization.plugin;

import org.apache.hadoop.hive.common.classification.InterfaceAudience.Private;
import org.apache.hadoop.hive.conf.HiveConf;

@Private
public class DefaultHiveAuthorizerFactory implements HiveAuthorizerFactory{
  @Override
  public HiveAuthorizer createHiveAuthorizer(HiveMetastoreClientFactory metastoreClientFactory,
      HiveConf conf, String hiveCurrentUser) {
    // return new HiveAuthorizerImpl(new DefaultHiveAccessController(db, conf), new Default HiveAuthValidator(db, conf));
    return null;
  }
}
