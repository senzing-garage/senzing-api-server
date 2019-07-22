package com.senzing.api.model;

import com.senzing.util.Timers;

import javax.ws.rs.core.UriInfo;

/**
 * A response object that contains version data.
 *
 */
public class SzVersionResponse extends SzResponseWithRawData {
  /**
   * The data for this instance.
   */
  private SzVersionInfo versionInfo;

  /**
   * Default constructor.
   */
  SzVersionResponse() {
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
  public SzVersionResponse(SzHttpMethod httpMethod,
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
   * @param versionInfo The {@link SzVersionInfo} describing the version.
   */
  public SzVersionResponse(SzHttpMethod   httpMethod,
                           int            httpStatusCode,
                           String         selfLink,
                           Timers         timers,
                           SzVersionInfo  versionInfo)
  {
    super(httpMethod, httpStatusCode, selfLink, timers);
    this.versionInfo = versionInfo;
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
  public SzVersionResponse(SzHttpMethod httpMethod,
                           int          httpStatusCode,
                           UriInfo      uriInfo,
                           Timers       timers)
  {
    this(httpMethod, httpStatusCode, uriInfo, timers, null);
  }

  /**
   * Constructs with the HTTP method, {@link UriInfo} and the
   * {@link SzVersionInfo} describing the version.
   *
   * @param httpMethod The {@link SzHttpMethod}.
   * @param httpStatusCode The HTTP response status code.
   * @param uriInfo The {@link UriInfo} from the request.
   * @param timers The {@link Timers} object for the timings that were taken.
   * @param versionInfo The {@link SzVersionInfo} describing the version.
   */
  public SzVersionResponse(SzHttpMethod   httpMethod,
                           int            httpStatusCode,
                           UriInfo        uriInfo,
                           Timers         timers,
                           SzVersionInfo  versionInfo)
  {
    super(httpMethod, httpStatusCode, uriInfo, timers);
    this.versionInfo = versionInfo;
  }

  /**
   * Returns the {@link SzVersionInfo} associated with this response.
   *
   * @return The data associated with this response.
   */
  public SzVersionInfo getData() {
    return this.versionInfo;
  }

  /**
   * Sets the data associated with this response with an {@link SzVersionInfo}.
   *
   * @param info The {@link SzVersionInfo} describing the license.
   */
  public void setData(SzVersionInfo info) {
    this.versionInfo = info;
  }
}
