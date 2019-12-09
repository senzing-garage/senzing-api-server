package com.senzing.api.model;

import com.senzing.util.Timers;

import javax.ws.rs.core.UriInfo;

/**
 * A response object that contains entity record data.
 *
 */
public class SzBulkDataAnalysisResponse extends SzBasicResponse {
  /**
   * The {@link SzBulkDataAnalysis} describing the record.
   */
  private SzBulkDataAnalysis bulkDataAnalysis;

  /**
   * Default constructor.
   */
  public SzBulkDataAnalysisResponse() {
    this.bulkDataAnalysis = null;
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
  public SzBulkDataAnalysisResponse(SzHttpMethod httpMethod,
                                    int          httpStatusCode,
                                    String       selfLink,
                                    Timers       timers)
  {
    this(httpMethod, httpStatusCode, selfLink, timers, null);
  }

  /**
   * Constructs with the HTTP method, self link and the {@link
   * SzBulkDataAnalysis} describing the record.
   *
   * @param httpMethod The {@link SzHttpMethod}.
   * @param httpStatusCode The HTTP response status code.
   * @param selfLink The string URL link to generate this response.
   * @param timers The {@link Timers} object for the timings that were taken.
   * @param dataAnalysis The {@link SzEntityRecord} describing the record.
   */
  public SzBulkDataAnalysisResponse(SzHttpMethod        httpMethod,
                                    int                 httpStatusCode,
                                    String              selfLink,
                                    Timers              timers,
                                    SzBulkDataAnalysis  dataAnalysis)
  {
    super(httpMethod, httpStatusCode, selfLink, timers);
    this.bulkDataAnalysis = dataAnalysis;
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
  public SzBulkDataAnalysisResponse(SzHttpMethod httpMethod,
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
   * @param dataAnalysis The {@link SzBulkDataAnalysis} describing the record.
   */
  public SzBulkDataAnalysisResponse(SzHttpMethod        httpMethod,
                                    int                 httpStatusCode,
                                    UriInfo             uriInfo,
                                    Timers              timers,
                                    SzBulkDataAnalysis  dataAnalysis)
  {
    super(httpMethod, httpStatusCode, uriInfo, timers);
    this.bulkDataAnalysis = dataAnalysis;
  }

  /**
   * Returns the data associated with this response which is an
   * {@link SzBulkDataAnalysis}.
   *
   * @return The data associated with this response.
   */
  public SzBulkDataAnalysis getData() {
    return this.bulkDataAnalysis;
  }

  /**
   * Sets the data associated with this response with an {@link
   * SzBulkDataAnalysis}.
   *
   * @param dataAnalysis The {@link SzBulkDataAnalysis} describing the record.
   */
  public void setData(SzBulkDataAnalysis dataAnalysis) {
    this.bulkDataAnalysis = dataAnalysis;
  }
}
