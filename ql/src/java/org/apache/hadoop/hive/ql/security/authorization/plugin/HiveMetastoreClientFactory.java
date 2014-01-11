package org.apache.hadoop.hive.ql.security.authorization.plugin;

import java.io.IOException;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;

public interface HiveMetastoreClientFactory {
  IMetaStoreClient getHiveMetastoreClient() throws IOException;
}
