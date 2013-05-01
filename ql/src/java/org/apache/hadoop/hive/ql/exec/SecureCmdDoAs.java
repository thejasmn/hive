package org.apache.hadoop.hive.ql.exec;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.shims.HadoopShims;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.security.UserGroupInformation;

class SecureCmdDoAs {
  private final Path tokenPath;
  private final HiveConf conf;
  private final String METASTORE_SERVICE = "hivemetastore";

  SecureCmdDoAs(HiveConf conf) throws HiveException, IOException{
    this.conf = conf;
    //    String mStoreTokenStr = buildHcatDelegationToken();

    tokenPath = ShimLoader.getHadoopShims().createDelegationTokenFile(
								      conf,
								      null,
								      null
	//        ,mStoreTokenStr,
	//        METASTORE_SERVICE
        );

  }

  String addArg(String cmdline) throws HiveException {
//     HadoopShims shim = ShimLoader.getHadoopShims();
//     String endUserName;
//     try {
//       endUserName = shim.getShortUserName(shim.getUGIForConf(conf));
//     } catch (Exception e) {
//       throw new HiveException("Failure getting username", e);
//     }

    StringBuilder sb = new StringBuilder();
    sb.append(cmdline);
    //    sb.append("-D");
    //    sb.append("hive.metastore.token.signature=");
    //    sb.append(METASTORE_SERVICE);
    //    sb.append("-D");
    // sb.append("proxy.user.name=");
    // sb.append(endUserName);
    sb.append("-hadooptoken ");
    sb.append(tokenPath.toUri().getPath());
    return sb.toString();
  }

  void addEnv(Map<String, String>env){

    env.put(UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION,
        tokenPath.toUri().getPath());
  }

  private String buildHcatDelegationToken() throws HiveException {
    HiveMetaStoreClient client;
    try {
      client = new HiveMetaStoreClient(conf);
      HadoopShims shim = ShimLoader.getHadoopShims();
      String endUserName = shim.getShortUserName(shim.getUGIForConf(conf));
      return client.getDelegationToken(endUserName);
    } catch (Exception e) {
      throw new HiveException("Failure building hcat delegation token", e);
    }

  }



}
