package com.senzing.api.model;

import com.senzing.util.Timers;

import javax.ws.rs.core.UriInfo;

/**
 * A response object that contains license data.
 *
 */
public class SzConfigResponse extends SzResponseWithRawData {
  /**
   * Default constructor.
   */
  SzConfigResponse() {
    // do nothing
  }

  /**
   * Constructs with the specified HTTP method and self link.
   *
   * @param httpMethod The {@link SzHttpMethod} from the request.
   *
   * @param httpStatusCode The HTTP response status code.
   *
   * @param selfLink The self link from the request.
   *
   * @param timers The {@link Timers} object for the timings that were taken.
   *
   */
  public SzConfigResponse(SzHttpMethod  httpMethod,
                          int           httpStatusCode,
                          String        selfLink,
                          Timers        timers)
  {
    super(httpMethod, httpStatusCode, selfLink, timers);
  }

  /**
   * Constructs with the specified HTTP method, self link string and
   * object representing the raw data response from the engine.
   *
   * @param httpMethod The {@link SzHttpMethod} from the request.
   *
   * @param httpStatusCode The HTTP response status code.
   *
   * @param selfLink The self link from the request.
   *
   * @param timers The {@link Timers} object for the timings that were taken.
   *
   * @param rawData The raw data to associate with the response.
   */
  public SzConfigResponse(SzHttpMethod  httpMethod,
                          int           httpStatusCode,
                          String        selfLink,
                          Timers        timers,
                          String        rawData)
  {
    super(httpMethod, httpStatusCode, selfLink, timers, rawData);
  }

  /**
   * Constructs with the specified HTTP method and {@link UriInfo}.
   *
   * @param httpMethod The {@link SzHttpMethod} from the request.
   *
   * @param httpStatusCode The HTTP response status code.
   *
   * @param uriInfo The {@link UriInfo} from the request.
   *
   * @param timers The {@link Timers} object for the timings that were taken.
   *
   */
  public SzConfigResponse(SzHttpMethod  httpMethod,
                          int           httpStatusCode,
                          UriInfo       uriInfo,
                          Timers        timers)
  {
    super(httpMethod, httpStatusCode, uriInfo, timers);
  }

  /**
   * Constructs with the specified HTTP method, {@link UriInfo} and
   * object representing the raw data response from the engine.
   *
   * @param httpMethod The {@link SzHttpMethod} from the request.
   *
   * @param httpStatusCode The HTTP response status code.
   *
   * @param uriInfo The {@link UriInfo} from the request.
   *
   * @param timers The {@link Timers} object for the timings that were taken.
   *
   * @param rawConfigData The raw data to associate with the response.
   */
  public SzConfigResponse(SzHttpMethod httpMethod,
                          int          httpStatusCode,
                          UriInfo      uriInfo,
                          Timers       timers,
                          String       rawConfigData)
  {
    super(httpMethod, httpStatusCode, uriInfo, timers, rawConfigData);
  }

}
