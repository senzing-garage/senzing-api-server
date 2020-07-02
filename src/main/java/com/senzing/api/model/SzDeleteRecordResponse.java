package com.senzing.api.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.senzing.util.Timers;

import javax.ws.rs.core.UriInfo;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;

/**
 * The response for a record deletion operation.
 *
 */
public class SzDeleteRecordResponse extends SzResponseWithRawData
{
  /**
   * The data for this instance.
   */
  private Data data = new Data();

  /**
   * Default constructor.
   */
  public SzDeleteRecordResponse() {
    // do nothing
  }

  /**
   * Constructs with only the HTTP method and the self link.
   *
   * @param httpMethod The {@link SzHttpMethod}.
   *
   * @param httpStatusCode The HTTP response status code.
   *
   * @param selfLink The string URL link to generate this response.
   *
   * @param timers The {@link Timers} object for the timings that were taken.
   */
  public SzDeleteRecordResponse(SzHttpMethod httpMethod,
                                int          httpStatusCode,
                                String       selfLink,
                                Timers       timers)
  {
    super(httpMethod, httpStatusCode, selfLink, timers);
  }

  /**
   * Constructs with the HTTP method, the self link and the info.
   *
   * @param httpMethod The {@link SzHttpMethod}.
   *
   * @param httpStatusCode The HTTP response status code.
   *
   * @param selfLink The string URL link to generate this response.
   *
   * @param timers The {@link Timers} object for the timings that were taken.
   *
   * @param info The {@link SzResolutionInfo} providing the information
   *             associated with the resolution of the record.
   */
  public SzDeleteRecordResponse(SzHttpMethod      httpMethod,
                                int               httpStatusCode,
                                String            selfLink,
                                Timers            timers,
                                SzResolutionInfo  info)
  {
    super(httpMethod, httpStatusCode, selfLink, timers);
    this.data.info = info;
  }

  /**
   * Constructs with only the HTTP method and the {@link UriInfo}.
   *
   * @param httpMethod The {@link SzHttpMethod}.
   *
   * @param httpStatusCode The HTTP response status code.
   *
   * @param uriInfo The {@link UriInfo} from the request.
   *
   * @param timers The {@link Timers} object for the timings that were taken.
   */
  public SzDeleteRecordResponse(SzHttpMethod httpMethod,
                                int          httpStatusCode,
                                UriInfo      uriInfo,
                                Timers       timers)
  {
    super(httpMethod, httpStatusCode, uriInfo, timers);
  }

  /**
   * Constructs with the HTTP method, the {@link UriInfo}, and the info.
   *
   * @param httpMethod The {@link SzHttpMethod}.
   *
   * @param httpStatusCode The HTTP response status code.
   *
   * @param uriInfo The {@link UriInfo} from the request.
   *
   * @param info The {@link SzResolutionInfo} providing the information
   *             associated with the resolution of the record.
   */
  public SzDeleteRecordResponse(SzHttpMethod      httpMethod,
                                int               httpStatusCode,
                                UriInfo           uriInfo,
                                Timers            timers,
                                SzResolutionInfo  info)
  {
    super(httpMethod, httpStatusCode, uriInfo, timers);
    this.data.info = info;
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
   * Sets the @link SzResolutionInfo} providing the information associated
   * with the deletion of the record.
   *
   * @param info The @link SzResolutionInfo} providing the information associated
   *             with the resolution of the record.
   */
  public void setInfo(SzResolutionInfo info) {
    this.data.info = info;
  }

  /**
   * Inner class to represent the data section for this response.
   */
  public static class Data {
    /**
     * The {@link SzResolutionInfo} providing the information associated with
     * the deletion of the record.
     */
    private SzResolutionInfo info;

    /**
     * Private default constructor.
     */
    private Data() {
      this.info = null;
    }

    /**
     * Gets the {@link SzResolutionInfo} providing the information associated
     * with the deletion of the record.
     *
     * @return The {@link SzResolutionInfo} providing the information
     *         associated with the deletion of the record.
     */
    @JsonInclude(NON_NULL)
    public SzResolutionInfo getInfo() {
      return this.info;
    }

  }
}
