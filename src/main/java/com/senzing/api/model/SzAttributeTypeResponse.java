package com.senzing.api.model;

import com.senzing.util.Timers;

import javax.ws.rs.core.UriInfo;

/**
 * A response object that contains attribute type data.
 *
 */
public class SzAttributeTypeResponse extends SzResponseWithRawData
{
  /**
   * The data for this instance.
   */
  private Data data = new Data();

  /**
   * Package-private default constructor.
   */
  SzAttributeTypeResponse() {
    this.data.attributeType = null;
  }

  /**
   * Constructs with only the HTTP method and the self link, leaving the
   * attribute type data to be initialized later.
   *
   * @param meta The response meta data.
   *
   * @param links The links for the response.
   */
  public SzAttributeTypeResponse(SzMeta meta, SzLinks links) {
    this(meta, links, null);
  }

  /**
   * Constructs with the HTTP method, self link and the {@link SzAttributeType}
   * describing the attribute type.
   *
   * @param meta The response meta data.
   *
   * @param links The links for the response.
   *
   * @param data The {@link SzAttributeType} describing the attribute type.
   */
  public SzAttributeTypeResponse(SzMeta           meta,
                                 SzLinks          links,
                                 SzAttributeType  data)
  {
    super(meta, links);
    this.data.attributeType = data;
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
    this.data.attributeType = data;
  }

  /**
   * Inner class to represent the data section for this response.
   */
  public static class Data {
    /**
     * The {@link SzAttributeType} describing the attribute type.
     */
    private SzAttributeType attributeType;

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
      return this.attributeType;
    }
  }

}
