package com.senzing.api.model;

import com.senzing.util.Timers;

import javax.ws.rs.core.UriInfo;

/**
 * A response object that contains entity record data.
 *
 */
public class SzBulkLoadResponse extends SzBasicResponse {
  /**
   * The {@link SzBulkLoadResult} describing the record.
   */
  private SzBulkLoadResult bulkLoadResult;

  /**
   * Default constructor.
   */
  public SzBulkLoadResponse() {
    this.bulkLoadResult = null;
  }

  /**
   * Constructs with only the HTTP method and the self link, leaving the
   * record data to be initialized later.
   *
   * @param httpMethod The {@link SzHttpMethod}.
   * @param httpStatusCode The HTTP response status code.
   * @param selfLink The string URL link to generate this response.
   * @param timers The {@link Timers} object for the timings that were taken.
   */
  public SzBulkLoadResponse(SzHttpMethod httpMethod,
                            int          httpStatusCode,
                            String       selfLink,
                            Timers       timers)
  {
    this(httpMethod, httpStatusCode, selfLink, timers, null);
  }

  /**
   * Constructs with the HTTP method, self link and the {@link
   * SzBulkLoadResult} describing the record.
   *
   * @param httpMethod The {@link SzHttpMethod}.
   * @param httpStatusCode The HTTP response status code.
   * @param selfLink The string URL link to generate this response.
   * @param timers The {@link Timers} object for the timings that were taken.
   * @param bulkLoadResult The {@link SzBulkLoadResult} describing the record.
   */
  public SzBulkLoadResponse(SzHttpMethod      httpMethod,
                            int               httpStatusCode,
                            String            selfLink,
                            Timers            timers,
                            SzBulkLoadResult  bulkLoadResult)
  {
    super(httpMethod, httpStatusCode, selfLink, timers);
    this.bulkLoadResult = bulkLoadResult;
  }

  /**
   * Constructs with only the HTTP method and the {@link UriInfo}, leaving the
   * record data to be initialized later.
   *
   * @param httpMethod The {@link SzHttpMethod}.
   * @param httpStatusCode The HTTP response status code.
   * @param uriInfo The {@link UriInfo} from the request.
   * @param timers The {@link Timers} object for the timings that were taken.
   */
  public SzBulkLoadResponse(SzHttpMethod httpMethod,
                            int          httpStatusCode,
                            UriInfo      uriInfo,
                            Timers       timers) {
    this(httpMethod, httpStatusCode, uriInfo, timers, null);
  }

  /**
   * Constructs with the HTTP method, {@link UriInfo} and the {@link
   * SzBulkDataAnalysis} describing the record.
   *
   * @param httpMethod The {@link SzHttpMethod}.
   * @param httpStatusCode The HTTP response status code.
   * @param uriInfo The {@link UriInfo} from the request.
   * @param timers The {@link Timers} object for the timings that were taken.
   * @param bulkLoadResult The {@link SzBulkLoadResult} describing the record.
   */
  public SzBulkLoadResponse(SzHttpMethod      httpMethod,
                            int               httpStatusCode,
                            UriInfo           uriInfo,
                            Timers            timers,
                            SzBulkLoadResult  bulkLoadResult)
  {
    super(httpMethod, httpStatusCode, uriInfo, timers);
    this.bulkLoadResult = bulkLoadResult;
  }

  /**
   * Returns the data associated with this response which is an
   * {@link SzBulkLoadResult}.
   *
   * @return The data associated with this response.
   */
  public SzBulkLoadResult getData() {
    return this.bulkLoadResult;
  }

  /**
   * Sets the data associated with this response with an {@link
   * SzBulkLoadResult}.
   *
   * @param bulkLoadResult The {@link SzBulkLoadResult} describing the record.
   */
  public void setData(SzBulkLoadResult bulkLoadResult) {
    this.bulkLoadResult = bulkLoadResult;
  }
}
