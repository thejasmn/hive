package org.apache.hive.jdbc;

import java.io.IOException;

import org.apache.http.Header;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.protocol.HttpContext;

/**
 * The class is instantiated with the username and password, it is then
 * used to add header with these credentials to HTTP requests
 *
 */
public class HttpBasicAuthInterceptor implements HttpRequestInterceptor {

  Header basicAuthHeader;
  public HttpBasicAuthInterceptor(String username, String password){
    if(username != null){
      UsernamePasswordCredentials creds = new UsernamePasswordCredentials(username, password);
      basicAuthHeader = BasicScheme.authenticate(creds, "UTF-8", false);
    }
  }

  @Override
  public void process(HttpRequest httpRequest, HttpContext httpContext) throws HttpException, IOException {
    if(basicAuthHeader != null){
      httpRequest.addHeader(basicAuthHeader);
    }
  }
}
