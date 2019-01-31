package com.senzing.api.model;

import javax.ws.rs.core.UriInfo;

/**
 * A response object that contains attribute type data.
 *
 */
public class SzAttributeTypeResponse extends SzResponseWithRawData {
  /**
   * The {@link SzAttributeType} describing the attribute type.
   */
  private SzAttributeType attributeType;

  /**
   * The data for this instance.
   */
  private Data data = new Data();

  /**
   * Constructs with only the HTTP method and the self link, leaving the
   * attribute type data to be initialized later.
   *
   * @param httpMethod The {@link SzHttpMethod}.
   * @param httpStatusCode The HTTP response code.
   * @param selfLink The string URL link to generate this response.
   */
  public SzAttributeTypeResponse(SzHttpMethod httpMethod,
                                 int          httpStatusCode,
                                 String       selfLink) {
    this(httpMethod, httpStatusCode, selfLink, null);
  }

  /**
   * Constructs with the HTTP method, self link and the {@link SzAttributeType}
   * describing the attribute type.
   *
   * @param httpMethod The {@link SzHttpMethod}.
   * @param httpStatusCode The HTTP response status code.
   * @param selfLink The string URL link to generate this response.
   * @param data The {@link SzAttributeType} describing the attribute type.
   */
  public SzAttributeTypeResponse(SzHttpMethod     httpMethod,
                                 int              httpStatusCode,
                                 String           selfLink,
                                 SzAttributeType  data)
  {
    super(httpMethod, httpStatusCode, selfLink);
    this.attributeType = data;
  }

  /**
   * Constructs with only the HTTP method and the {@link UriInfo}, leaving the
   * attribute type data to be initialized later.
   *
   * @param httpMethod The {@link SzHttpMethod}.
   * @param httpStatusCode The HTTP response code.
   * @param uriInfo The {@link UriInfo} from the request.
   */
  public SzAttributeTypeResponse(SzHttpMethod httpMethod,
                                 int          httpStatusCode,
                                 UriInfo      uriInfo) {
    this(httpMethod, httpStatusCode, uriInfo, null);
  }

  /**
   * Constructs with the HTTP method, {@link UriInfo} and the
   * {@link SzAttributeType} describing the attribute type.
   *
   * @param httpMethod The {@link SzHttpMethod}.
   * @param httpStatusCode The HTTP response status code.
   * @param uriInfo The {@link UriInfo} from the request.
   * @param data The {@link SzAttributeType} describing the attribute type.
   */
  public SzAttributeTypeResponse(SzHttpMethod     httpMethod,
                                 int              httpStatusCode,
                                 UriInfo          uriInfo,
                                 SzAttributeType  data)
  {
    super(httpMethod, httpStatusCode, uriInfo);
    this.attributeType = data;
  }

  /**
   * Returns the {@link Data} associated with this response which contains an
   * {@link SzAttributeType}.
   *
   * @return The data associated with this response.
   */
  public Data getData() {
    return this.data;
  }

  /**
   * Sets the data associated with this response with an
   * {@link SzAttributeType}.
   *
   * @param data The {@link SzAttributeType} describing the attribute type.
   */
  public void setAttributeType(SzAttributeType data) {
    this.attributeType = data;
  }

  /**
   * Inner class to represent the data section for this response.
   */
  public class Data {
    /**
     * Private default constructor.
     */
    private Data() {
      // do nothing
    }

    /**
     * Gets the {@link SzAttributeType} describing the attribute type.
     *
     * @return The {@link SzAttributeType} describing the attributeType.
     */
    public SzAttributeType getAttributeType() {
      return SzAttributeTypeResponse.this.attributeType;
    }
  }

}
