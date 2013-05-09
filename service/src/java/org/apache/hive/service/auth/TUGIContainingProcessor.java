package org.apache.hive.service.auth;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.shims.HadoopShims;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSaslServerTransport;

public class TUGIContainingProcessor implements TProcessor{

  private final TProcessor wrapped;
  private final HadoopShims shim;
  private static final Log LOG = LogFactory.getLog(TUGIContainingProcessor.class);

  public TUGIContainingProcessor(TProcessor wrapped, Configuration conf) {
    this.wrapped = wrapped;
    this.shim = ShimLoader.getHadoopShims();

    //This class is used in unsecure mode with impersonation turned on.
    //Each request will result in a new UGI being created, it will fill
    // up hadoop FileSystem.CACHE . Need to change the model of impersonation
    // in unsecure mode to be on lines of the secure mode - ie call
    // closeAllForUGI when the session is closed
    String defaultScheme = FileSystem.getDefaultUri(conf).getScheme();
    String localFSScheme = "file";
    disableCache(conf, localFSScheme, defaultScheme);

  }

  private void disableCache(Configuration conf, String... schemes) {
    for(String scheme : schemes){
      String disableCacheProp =
          String.format("fs.%s.impl.disable.cache", scheme);
      boolean isCacheDisabled = conf.getBoolean(disableCacheProp, false);
      if(!isCacheDisabled){
        LOG.info("Disabling FileSystem cache for scheme: " + scheme
            + " as non-kerberos mode is being used with doAs enabled");
        conf.setBoolean(disableCacheProp, true);
      }
    }
  }

  @Override
  public boolean process(final TProtocol in, final TProtocol out) throws TException {
    UserGroupInformation clientUgi = null;

    try {
      clientUgi = shim.createRemoteUser(((TSaslServerTransport)in.getTransport()).
          getSaslServer().getAuthorizationID(), new ArrayList<String>());
      return shim.doAs(clientUgi, new PrivilegedExceptionAction<Boolean>() {
        public Boolean run() {
          try {
            return wrapped.process(in, out);
          } catch (TException te) {
            throw new RuntimeException(te);
          }
        }
      });
    }
    catch (RuntimeException rte) {
      if (rte.getCause() instanceof TException) {
        throw (TException)rte.getCause();
      }
      throw rte;
    } catch (InterruptedException ie) {
      throw new RuntimeException(ie); // unexpected!
    } catch (IOException ioe) {
      throw new RuntimeException(ioe); // unexpected!
    }
  }
}
