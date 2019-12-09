package com.senzing.api.model;

import com.senzing.util.Timers;

import javax.ws.rs.core.UriInfo;

/**
 * A response object that contains server info data.
 *
 */
public class SzServerInfoResponse extends SzBasicResponse {
  /**
   * The data for this instance.
   */
  private SzServerInfo serverInfo;

  /**
   * Default constructor.
   */
  SzServerInfoResponse() {
    // do nothing
  }

  /**
   * Constructs with only the HTTP method and the self link, leaving the
   * license info data to be initialized later.
   *
   * @param httpMethod The {@link SzHttpMethod}.
   * @param httpStatusCode The HTTP response code.
   * @param selfLink The string URL link to generate this response.
   * @param timers The {@link Timers} object for the timings that were taken.
   */
  public SzServerInfoResponse(SzHttpMethod httpMethod,
                              int          httpStatusCode,
                              String       selfLink,
                              Timers       timers)
  {
    this(httpMethod, httpStatusCode, selfLink, timers, null);
  }

  /**
   * Constructs with the HTTP method, self link and the {@link SzVersionInfo}
   * describing the version.
   *
   * @param httpMethod The {@link SzHttpMethod}.
   * @param httpStatusCode The HTTP response status code.
   * @param selfLink The string URL link to generate this response.
   * @param timers The {@link Timers} object for the timings that were taken.
   * @param serverInfo The {@link SzServerInfo} describing the version.
   */
  public SzServerInfoResponse(SzHttpMethod   httpMethod,
                              int            httpStatusCode,
                              String         selfLink,
                              Timers         timers,
                              SzServerInfo   serverInfo)
  {
    super(httpMethod, httpStatusCode, selfLink, timers);
    this.serverInfo = serverInfo;
  }

  /**
   * Constructs with only the HTTP method and the {@link UriInfo}, leaving the
   * version info data to be initialized later.
   *
   * @param httpMethod The {@link SzHttpMethod}.
   * @param httpStatusCode The HTTP response code.
   * @param uriInfo The {@link UriInfo} from the request.
   * @param timers The {@link Timers} object for the timings that were taken.
   *
   */
  public SzServerInfoResponse(SzHttpMethod httpMethod,
                              int          httpStatusCode,
                              UriInfo      uriInfo,
                              Timers       timers)
  {
    this(httpMethod, httpStatusCode, uriInfo, timers, null);
  }

  /**
   * Constructs with the HTTP method, {@link UriInfo} and the
   * {@link SzServerInfo} describing the version.
   *
   * @param httpMethod The {@link SzHttpMethod}.
   * @param httpStatusCode The HTTP response status code.
   * @param uriInfo The {@link UriInfo} from the request.
   * @param timers The {@link Timers} object for the timings that were taken.
   * @param serverInfo The {@link SzServerInfo} describing the version.
   */
  public SzServerInfoResponse(SzHttpMethod   httpMethod,
                              int            httpStatusCode,
                              UriInfo        uriInfo,
                              Timers         timers,
                              SzServerInfo   serverInfo)
  {
    super(httpMethod, httpStatusCode, uriInfo, timers);
    this.serverInfo = serverInfo;
  }

  /**
   * Returns the {@link SzServerInfo} associated with this response.
   *
   * @return The data associated with this response.
   */
  public SzServerInfo getData() {
    return this.serverInfo;
  }

  /**
   * Sets the data associated with this response with an {@link SzServerInfo}.
   *
   * @param info The {@link SzServerInfo} describing the license.
   */
  public void setData(SzServerInfo info) {
    this.serverInfo = info;
  }
}
