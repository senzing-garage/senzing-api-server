package com.senzing.api.model;

import java.util.*;

/**
 * The response containing a list of attribute types.  Typically this is the
 * list of all configured attribute types (excluding the "internal" ones).
 *
 */
public class SzAttributeTypesResponse extends SzResponseWithRawData
{
  /**
   * The list of {@link SzAttributeType} instances describing the attribute
   * types.
   */
  private List<SzAttributeType> attributeTypes;

  /**
   * The data for this instance.
   */
  private Data data = new Data();

  /**
   * Constructs with only the HTTP method and the self link, leaving the
   * data sources to be added later.
   *
   * @param httpMethod The {@link SzHttpMethod}.
   *
   * @param httpStatusCode The HTTP response status code.
   *
   * @param selfLink The string URL link to generate this response.
   */
  public SzAttributeTypesResponse(SzHttpMethod httpMethod,
                                  int          httpStatusCode,
                                  String       selfLink) {
    super(httpMethod, httpStatusCode, selfLink);
    this.attributeTypes = new LinkedList<>();
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
   * Adds the specified {@link SzAttributeType}.
   *
   * @param attributeType The {@link SzAttributeType} to add.
   */
  public void addAttributeType(SzAttributeType attributeType) {
    if (attributeType == null) {
      throw new NullPointerException(
          "Cannot add a null attribute type.");
    }
    this.attributeTypes.add(attributeType);
  }

  /**
   * Sets the specified {@link List} of {@linkplain SzAttributeType attribute
   * types} using the specified {@link Collection} of {@link SzAttributeType}
   * instances.
   *
   * @param attributeTypes The {@link Collection} of {@link SzAttributeType}
   *                       instances.
   */
  public void setAttributeTypes(Collection<SzAttributeType> attributeTypes)
  {
    this.attributeTypes.clear();
    if (attributeTypes != null) {
      this.attributeTypes.addAll(attributeTypes);
    }
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
     * Gets the unmodifiable {@link List} of {@link SzAttributeType} instances.
     *
     * @return The unmodifiable {@link List} of {@link SzAttributeType}
     *         instances.
     */
    public List<SzAttributeType> getAttributeTypes() {
      List<SzAttributeType> list = SzAttributeTypesResponse.this.attributeTypes;
      return Collections.unmodifiableList(list);
    }
  }
}
