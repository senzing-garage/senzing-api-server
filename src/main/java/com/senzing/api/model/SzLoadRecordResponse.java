package com.senzing.api.model;

import com.senzing.util.Timers;

import javax.ws.rs.core.UriInfo;

/**
 * The response containing the record ID of the record that was loaded.
 *
 */
public class SzLoadRecordResponse extends SzResponseWithRawData
{
  /**
   * The data for this instance.
   */
  private Data data = new Data();

  /**
   * Default constructor.
   */
  public SzLoadRecordResponse() {
    this.data.recordId = null;
  }

  /**
   * Constructs with only the HTTP method and the self link, leaving the
   * record ID to be initialized later.
   *
   * @param httpMethod The {@link SzHttpMethod}.
   *
   * @param httpStatusCode The HTTP response status code.
   *
   * @param selfLink The string URL link to generate this response.
   *
   * @param timers The {@link Timers} object for the timings that were taken.
   */
  public SzLoadRecordResponse(SzHttpMethod httpMethod,
                              int          httpStatusCode,
                              String       selfLink,
                              Timers       timers)
  {
    super(httpMethod, httpStatusCode, selfLink, timers);
  }

  /**
   * Constructs with the HTTP method, the self link, and the record ID.
   *
   * @param httpMethod The {@link SzHttpMethod}.
   *
   * @param httpStatusCode The HTTP response status code.
   *
   * @param selfLink The string URL link to generate this response.
   *
   * @param timers The {@link Timers} object for the timings that were taken.
   *
   * @param recordId The record ID of the record that was loaded.
   */
  public SzLoadRecordResponse(SzHttpMethod httpMethod,
                              int          httpStatusCode,
                              String       selfLink,
                              Timers       timers,
                              String       recordId) {
    super(httpMethod, httpStatusCode, selfLink, timers);
    this.data.recordId = recordId;
  }

  /**
   * Constructs with only the HTTP method and the {@link UriInfo}, leaving the
   * record ID to be initialized later.
   *
   * @param httpMethod The {@link SzHttpMethod}.
   *
   * @param httpStatusCode The HTTP response status code.
   *
   * @param uriInfo The {@link UriInfo} from the request.
   *
   * @param timers The {@link Timers} object for the timings that were taken.
   */
  public SzLoadRecordResponse(SzHttpMethod httpMethod,
                              int          httpStatusCode,
                              UriInfo      uriInfo,
                              Timers       timers)
  {
    super(httpMethod, httpStatusCode, uriInfo, timers);
  }

  /**
   * Constructs with the HTTP method, the {@link UriInfo}, and the record ID.
   *
   * @param httpMethod The {@link SzHttpMethod}.
   *
   * @param httpStatusCode The HTTP response status code.
   *
   * @param uriInfo The {@link UriInfo} from the request.
   *
   * @param recordId The record ID of the record that was loaded.
   */
  public SzLoadRecordResponse(SzHttpMethod httpMethod,
                              int          httpStatusCode,
                              UriInfo      uriInfo,
                              Timers       timers,
                              String       recordId) {
    super(httpMethod, httpStatusCode, uriInfo, timers);
    this.data.recordId = recordId;
  }

  /**
   * Returns the {@link Data} for this instance.
   *
   * @return The {@link Data} for this instance.
   */
  public Data getData() {
    return this.data;
  }

  /**
   * Sets the record ID of the record that was loaded.
   *
   * @param recordId The record ID of the record.
   */
  public void setRecordId(String recordId)
  {
    this.data.recordId = recordId;
  }

  /**
   * Inner class to represent the data section for this response.
   */
  public static class Data {
    /**
     * The record ID of the record that was loaded.
     */
    private String recordId;

    /**
     * Private default constructor.
     */
    private Data() {
      // do nothing
    }

    /**
     * Gets the record ID of the record that was loaded.
     *
     * @return The record ID of the record that was loaded.
     */
    public String getRecordId() {
      return this.recordId;
    }
  }
}
