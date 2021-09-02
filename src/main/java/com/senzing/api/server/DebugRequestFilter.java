package com.senzing.api.server;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;

public class DebugRequestFilter implements Filter {
  @Override
  public void init(FilterConfig filterConfig) throws ServletException {

  }

  @Override
  public void doFilter(ServletRequest   servletRequest,
                       ServletResponse  servletResponse,
                       FilterChain      filterChain)
      throws IOException, ServletException
  {
    long threadId = Thread.currentThread().getId();
    HttpServletRequest httpRequest = (HttpServletRequest) servletRequest;
    String requestUri   = httpRequest.getRequestURI();
    String queryString  = httpRequest.getQueryString();
    if (queryString != null && queryString.trim().length() > 0) {
      requestUri = requestUri + "?" + queryString;
    }
    long start = System.nanoTime();
    System.out.println("[ " + threadId + " ] RECEIVED REQUEST: " + requestUri);
    try {
      filterChain.doFilter(servletRequest, servletResponse);

    } catch (IOException|ServletException|RuntimeException e) {
      System.out.println("[ " + threadId + " ] REQUEST FAILED: " + requestUri);
      System.out.println(e);
      throw e;

    } finally {
      long end = System.nanoTime();
      long millis = (end - start) / 1000000L;
      System.out.println("[ " + threadId + " ] CONCLUDED REQUEST IN " + millis
                             + "ms : " + requestUri);
    }
  }

  @Override
  public void destroy() {

  }
}
