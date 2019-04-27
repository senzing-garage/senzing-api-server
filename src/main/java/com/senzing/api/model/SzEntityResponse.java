package com.senzing.api.model;

import com.senzing.util.Timers;

import javax.ws.rs.core.UriInfo;

/**
 * A response object that contains entity data.
 *
 */
public class SzEntityResponse extends SzResponseWithRawData {
  /**
   * The {@link SzEntityData} describing the entity.
   */
  private SzEntityData entityData;

  /**
   * Constructs with only the HTTP method and the self link, leaving the
   * entity data to be initialized later.
   *
   * @param httpMethod The {@link SzHttpMethod}.
   * @param httpStatusCode The HTTP response status code.
   * @param selfLink The string URL link to generate this response.
   * @param timers The {@link Timers} object for the timings that were taken.
   */
  public SzEntityResponse(SzHttpMethod httpMethod,
                          int          httpStatusCode,
                          String       selfLink,
                          Timers       timers) {
    this(httpMethod, httpStatusCode, selfLink, timers, null);
  }

  /**
   * Constructs with the HTTP method, self link and the {@link SzEntityData}
   * describing the record.
   *
   * @param httpMethod The {@link SzHttpMethod}.
   * @param httpStatusCode The HTTP response status code.
   * @param selfLink The string URL link to generate this response.
   * @param timers The {@link Timers} object for the timings that were taken.
   * @param data The {@link SzEntityRecord} describing the record.
   */
  public SzEntityResponse(SzHttpMethod   httpMethod,
                          int            httpStatusCode,
                          String         selfLink,
                          Timers         timers,
                          SzEntityData   data)
  {
    super(httpMethod, httpStatusCode, selfLink, timers);
    this.entityData = data;
  }

  /**
   * Constructs with only the HTTP method and the {@link UriInfo}, leaving the
   * entity data to be initialized later.
   *
   * @param httpMethod The {@link SzHttpMethod}.
   * @param httpStatusCode The HTTP response status code.
   * @param uriInfo The {@link UriInfo} from the request.
   * @param timers The {@link Timers} object for the timings that were taken.
   */
  public SzEntityResponse(SzHttpMethod httpMethod,
                          int          httpStatusCode,
                          UriInfo      uriInfo,
                          Timers       timers)
  {
    this(httpMethod, httpStatusCode, uriInfo, timers, null);
  }

  /**
   * Constructs with the HTTP method, {@link UriInfo} and the
   * {@link SzEntityData} describing the record.
   *
   * @param httpMethod The {@link SzHttpMethod}.
   * @param httpStatusCode The HTTP response status code.
   * @param uriInfo The {@link UriInfo} from the request.
   * @param timers The {@link Timers} object for the timings that were taken.
   * @param data The {@link SzEntityRecord} describing the record.
   */
  public SzEntityResponse(SzHttpMethod   httpMethod,
                          int            httpStatusCode,
                          UriInfo        uriInfo,
                          Timers         timers,
                          SzEntityData   data)
  {
    super(httpMethod, httpStatusCode, uriInfo, timers);
    this.entityData = data;
  }


  /**
   * Returns the data associated with this response which is an
   * {@link SzEntityData}.
   *
   * @return The data associated with this response.
   */
  public SzEntityData getData() {
    return this.entityData;
  }

  /**
   * Sets the data associated with this response with an {@link SzEntityData}.
   *
   * @param data The {@link SzEntityData} describing the record.
   */
  public void setData(SzEntityData data) {
    this.entityData = data;
  }
}
