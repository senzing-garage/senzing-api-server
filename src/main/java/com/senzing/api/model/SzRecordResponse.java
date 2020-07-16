package com.senzing.api.model;

import com.senzing.util.Timers;

import javax.ws.rs.core.UriInfo;
import java.util.*;

/**
 * A response object that contains entity record data.
 *
 */
public class SzRecordResponse extends SzResponseWithRawData {
  /**
   * The data for this instance.
   */
  private Data data = new Data();

  /**
   * Default constructor.
   */
  public SzRecordResponse() {
    // do nothing
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
   * @param record The {@link SzEntityRecord} describing the record.
   */
  public SzRecordResponse(SzHttpMethod   httpMethod,
                          int            httpStatusCode,
                          String         selfLink,
                          Timers         timers,
                          SzEntityRecord record)
  {
    super(httpMethod, httpStatusCode, selfLink, timers);
    this.data.setRecord(record);
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
   * @param record The {@link SzEntityRecord} describing the record.
   */
  public SzRecordResponse(SzHttpMethod   httpMethod,
                          int            httpStatusCode,
                          UriInfo        uriInfo,
                          Timers         timers,
                          SzEntityRecord record)
  {
    super(httpMethod, httpStatusCode, uriInfo, timers);
    this.data.setRecord(record);
  }

  /**
   * Returns the data associated with this response which contains an
   * {@link SzEntityRecord}.
   *
   * @return The data associated with this response.
   */
  public Data getData() {
    return this.data;
  }

  /**
   * Private setter for JSON marshalling.
   */
  private void setData(Data data) {
    this.data = data;
  }

  /**
   * Sets the data associated with this response with an {@link SzEntityRecord}.
   *
   * @param record The {@link SzEntityRecord} describing the record.
   */
  public void setRecord(SzEntityRecord record) {
    this.data.setRecord(record);
  }

  /**
   * Inner class to represent the data section for this response.
   */
  public static class Data {
    /**
     * The {@link SzEntityRecord} describing the record.
     */
    private SzEntityRecord entityRecord;

    /**
     * Private default constructor.
     */
    private Data() {
      this.entityRecord = null;
    }

    /**
     * Gets the {@link SzEntityRecord} describing the record.
     *
     * @return The {@link SzEntityRecord} describing the record.
     */
    public SzEntityRecord getRecord() {
      return this.entityRecord;
    }

    /**
     * Private setter used for deserialization.
     */
    private void setRecord(SzEntityRecord entityRecord) {
      this.entityRecord = entityRecord;
    }
  }

}
