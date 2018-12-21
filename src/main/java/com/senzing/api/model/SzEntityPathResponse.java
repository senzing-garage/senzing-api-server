package com.senzing.api.model;

/**
 * A response object that contains entity path data.
 */
public class SzEntityPathResponse extends SzResponseWithRawData {
  /**
   * The {@link SzEntityPathData} describing the entity.
   */
  private SzEntityPathData entityPathData;

  /**
   * Constructs with only the HTTP method and the self link, leaving the
   * entity data to be initialized later.
   *
   * @param httpMethod The {@link SzHttpMethod}.
   * @param httpStatusCode The HTTP response status code.
   * @param selfLink The string URL link to generate this response.
   */
  public SzEntityPathResponse(SzHttpMethod httpMethod,
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
  public SzEntityPathResponse(SzHttpMethod      httpMethod,
                              int               httpStatusCode,
                              String            selfLink,
                              SzEntityPathData  data)
  {
    super(httpMethod, httpStatusCode, selfLink);
    this.entityPathData = data;
  }

  /**
   * Returns the data associated with this response which is an
   * {@link SzEntityPathData}.
   *
   * @return The data associated with this response.
   */
  public SzEntityPathData getData() {
    return this.entityPathData;
  }

  /**
   * Sets the data associated with this response with an
   * {@link SzEntityPathData}.
   *
   * @param data The {@link SzEntityPathData} describing the record.
   */
  public void setData(SzEntityPathData data) {
    this.entityPathData = data;
  }
}
