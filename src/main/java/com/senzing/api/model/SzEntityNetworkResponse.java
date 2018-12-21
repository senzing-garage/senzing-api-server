package com.senzing.api.model;

/**
 * A response object that contains entity path data.
 */
public class SzEntityNetworkResponse extends SzResponseWithRawData {
  /**
   * The {@link SzEntityNetworkData} describing the entity.
   */
  private SzEntityNetworkData entityNetworkData;

  /**
   * Constructs with only the HTTP method and the self link, leaving the
   * entity data to be initialized later.
   *
   * @param httpMethod The {@link SzHttpMethod}.
   * @param httpStatusCode The HTTP response status code.
   * @param selfLink The string URL link to generate this response.
   */
  public SzEntityNetworkResponse(SzHttpMethod httpMethod,
                                 int          httpStatusCode,
                                 String       selfLink) {
    this(httpMethod, httpStatusCode, selfLink, null);
  }

  /**
   * Constructs with the HTTP method, self link and the {@link
   * SzEntityPathData} describing the record.
   *
   * @param httpMethod The {@link SzHttpMethod}.
   * @param httpStatusCode The HTTP response status code.
   * @param selfLink The string URL link to generate this response.
   * @param data The {@link SzEntityRecord} describing the record.
   */
  public SzEntityNetworkResponse(SzHttpMethod         httpMethod,
                                 int                  httpStatusCode,
                                 String               selfLink,
                                 SzEntityNetworkData  data)
  {
    super(httpMethod, httpStatusCode, selfLink);
    this.entityNetworkData = data;
  }

  /**
   * Returns the data associated with this response which is an
   * {@link SzEntityNetworkData}.
   *
   * @return The data associated with this response.
   */
  public SzEntityNetworkData getData() {
    return this.entityNetworkData;
  }

  /**
   * Sets the data associated with this response with an
   * {@link SzEntityNetworkData}.
   *
   * @param data The {@link SzEntityNetworkData} describing the record.
   */
  public void setData(SzEntityNetworkData data) {
    this.entityNetworkData = data;
  }
}
