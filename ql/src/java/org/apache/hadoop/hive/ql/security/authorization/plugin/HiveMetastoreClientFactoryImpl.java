package org.apache.hadoop.hive.ql.security.authorization.plugin;


import java.io.IOException;

import org.apache.hadoop.hive.common.classification.InterfaceAudience.Private;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.ql.metadata.Hive;
@Private
public class HiveMetastoreClientFactoryImpl implements HiveMetastoreClientFactory{

  @Override
  public IMetaStoreClient getHiveMetastoreClient() throws IOException {
    try {
      return Hive.get().getMSC();
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

}
