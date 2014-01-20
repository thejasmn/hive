package org.apache.hadoop.hive.ql.security.authorization.plugin;

import org.apache.hadoop.hive.common.classification.InterfaceAudience.Public;

/**
 * Exception thrown by the Authorization plugin api (v2)
 */
@Public
public class HiveAuthorizationPluginException extends Exception{

  private static final long serialVersionUID = 1L;

  public HiveAuthorizationPluginException(){
  }
  
  public HiveAuthorizationPluginException(String msg){
    super(msg);
  }
  
  public HiveAuthorizationPluginException(String msg, Throwable cause){
    super(msg, cause);
  }
  
  public HiveAuthorizationPluginException(Throwable cause){
    super(cause);
  }
  
}
