package org.apache.hive.service.cli.thrift;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TServlet;

public class HttpServlet extends TServlet {

  private static final long serialVersionUID = 1L;
  public static final Log LOG = LogFactory.getLog(HttpServlet.class.getName());

  public HttpServlet(TProcessor processor, TProtocolFactory protocolFactory) {
    super(processor, protocolFactory);
  }

  @Override
  protected void doPost(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {

    logRequestHeader(request);
    super.doPost(request, response);
  }

  protected void logRequestHeader(HttpServletRequest request){

    String authHeaderBase64 = request.getHeader("Authorization");

    if(authHeaderBase64 == null){
      LOG.warn("HttpServlet:  no HTTP Authorization header");
    }
    else {
      if(!authHeaderBase64.startsWith("Basic")){
        LOG.warn("HttpServlet:  HTTP Authorization header exists but is not Basic.");
      }
      else if(LOG.isDebugEnabled()) {
        String authHeaderBase64_Payload = authHeaderBase64.substring("Basic ".length());
        String authHeaderString = org.apache.commons.codec.binary.StringUtils.newStringUtf8(org.apache.commons.codec.binary.Base64.decodeBase64(authHeaderBase64_Payload.getBytes()));
        String[] creds = authHeaderString.split(":");
        String username=null;
        String password=null;
        if(creds.length >= 1 ){
          username = creds[0];
        }
        if(creds.length >= 2){
          password = creds[1];
        }

        if(password == null || password.equals("null") || password.equals("")){
          password = "<no password>";
        }
        else {
          password = "******";  // don't log the actual password.
        }

        LOG.debug("HttpServlet:  HTTP Authorization header:: username=" + username + " password=" + password);
      }
    }
  }
}
