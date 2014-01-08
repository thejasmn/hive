package org.apache.hadoop.hive.ql.security.authorization.plugin;

import org.apache.hadoop.hive.common.classification.InterfaceAudience.Public;
import org.apache.hadoop.hive.common.classification.InterfaceStability.Evolving;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.Hive;

/**
 * Implementation of this interface specified through hive configuration will be used to 
 * create  {@link HiveAuthorizer} instance used for hive authorization.
 *
 */
@Public
@Evolving
public interface HiveAuthorizerFactory {
  HiveAuthorizer createHiveAuthorizer(Hive db, HiveConf conf);
}
