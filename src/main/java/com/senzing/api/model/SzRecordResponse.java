package com.senzing.api.model;

import com.senzing.util.Timers;

import javax.ws.rs.core.UriInfo;

/**
 * A response object that contains entity record data.
 *
 */
public class SzRecordResponse extends SzResponseWithRawData {
  /**
   * The {@link SzEntityRecord} describing the record.
   */
  private SzEntityRecord entityRecord;

  /**
   * Constructs with only the HTTP method and the self link, leaving the
   * record data to be initialized later.
   *
   * @param httpMethod The {@link SzHttpMethod}.
   * @param httpStatusCode The HTTP response status code.
   * @param selfLink The string URL link to generate this response.
   * @param timers The {@link Timers} object for the timings that were taken.
   */
  public SzRecordResponse(SzHttpMethod httpMethod,
                          int          httpStatusCode,
                          String       selfLink,
                          Timers       timers)
  {
    this(httpMethod, httpStatusCode, selfLink, timers, null);
  }

  /**
   * Constructs with the HTTP method, self link and the {@link SzEntityRecord}
   * describing the record.
   *
   * @param httpMethod The {@link SzHttpMethod}.
   * @param httpStatusCode The HTTP response status code.
   * @param selfLink The string URL link to generate this response.
   * @param timers The {@link Timers} object for the timings that were taken.
   * @param data The {@link SzEntityRecord} describing the record.
   */
  public SzRecordResponse(SzHttpMethod   httpMethod,
                          int            httpStatusCode,
                          String         selfLink,
                          Timers         timers,
                          SzEntityRecord data)
  {
    super(httpMethod, httpStatusCode, selfLink, timers);
    this.entityRecord = data;
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
  public SzRecordResponse(SzHttpMethod httpMethod,
                          int          httpStatusCode,
                          UriInfo      uriInfo,
                          Timers       timers) {
    this(httpMethod, httpStatusCode, uriInfo, timers, null);
  }

  /**
   * Constructs with the HTTP method, {@link UriInfo} and the {@link SzEntityRecord}
   * describing the record.
   *
   * @param httpMethod The {@link SzHttpMethod}.
   * @param httpStatusCode The HTTP response status code.
   * @param uriInfo The {@link UriInfo} from the request.
   * @param timers The {@link Timers} object for the timings that were taken.
   * @param data The {@link SzEntityRecord} describing the record.
   */
  public SzRecordResponse(SzHttpMethod   httpMethod,
                          int            httpStatusCode,
                          UriInfo        uriInfo,
                          Timers         timers,
                          SzEntityRecord data)
  {
    super(httpMethod, httpStatusCode, uriInfo, timers);
    this.entityRecord = data;
  }

  /**
   * Returns the data associated with this response which is an
   * {@link SzEntityRecord}.
   *
   * @return The data associated with this response.
   */
  public SzEntityRecord getData() {
    return this.entityRecord;
  }

  /**
   * Sets the data associated with this response with an {@link SzEntityRecord}.
   *
   * @param data The {@link SzEntityRecord} describing the record.
   */
  public void setData(SzEntityRecord data) {
    this.entityRecord = data;
  }
}
