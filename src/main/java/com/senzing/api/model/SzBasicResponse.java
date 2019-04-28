package com.senzing.api.model;

import com.senzing.util.Timers;

import javax.ws.rs.core.UriInfo;

/**
 * The most basic response from the Senzing REST API.  Also servers as a basis
 * for other responses.
 */
public class SzBasicResponse {
  /**
   * The meta section for this response.
   */
  private SzMeta meta;

  /**
   * The links associated with this response.
   */
  private SzLinks links;

  /**
   * Constructs with the specified HTTP method and self link.
   *
   * @param httpMethod The {@link SzHttpMethod} from the request.
   *
   * @param httpStatusCode The HTTP response code.
   *
   * @param selfLink The self link from the request.
   */
  public SzBasicResponse(SzHttpMethod httpMethod,
                         int          httpStatusCode,
                         String       selfLink,
                         Timers       timers)
  {
    this.meta = new SzMeta(httpMethod, httpStatusCode, timers);
    this.links = new SzLinks(selfLink);
  }

  /**
   * Constructs with the specified HTTP method and {@link UriInfo}.
   *
   * @param httpMethod The {@link SzHttpMethod} from the request.
   *
   * @param httpStatusCode The HTTP response code.
   *
   * @param uriInfo The {@link UriInfo} for the self link.
   */
  public SzBasicResponse(SzHttpMethod httpMethod,
                         int          httpStatusCode,
                         UriInfo      uriInfo,
                         Timers       timers)
  {
    this.meta = new SzMeta(httpMethod, httpStatusCode, timers);
    this.links = new SzLinks(uriInfo);
  }

  /**
   * Returns the meta data associated with this response.
   *
   * @return The meta data associated with this response.
   */
  public SzMeta getMeta() {
    return meta;
  }

  /**
   * Gets the links associated with this response.
   *
   * @return The links associated with this response.
   */
  public SzLinks getLinks() {
    return links;
  }
}
