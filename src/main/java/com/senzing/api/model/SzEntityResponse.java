package com.senzing.api.model;

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
   */
  public SzEntityResponse(SzHttpMethod httpMethod,
                          int          httpStatusCode,
                          String       selfLink) {
    this(httpMethod, httpStatusCode, selfLink, null);
  }

  /**
   * Constructs with the HTTP method, self link and the {@link SzEntityData}
   * describing the record.
   *
   * @param httpMethod The {@link SzHttpMethod}.
   * @param httpStatusCode The HTTP response status code.
   * @param selfLink The string URL link to generate this response.
   * @param data The {@link SzEntityRecord} describing the record.
   */
  public SzEntityResponse(SzHttpMethod   httpMethod,
                          int            httpStatusCode,
                          String         selfLink,
                          SzEntityData   data)
  {
    super(httpMethod, httpStatusCode, selfLink);
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
