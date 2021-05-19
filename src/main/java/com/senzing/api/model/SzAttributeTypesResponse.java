package com.senzing.api.model;

import com.senzing.util.Timers;

import javax.ws.rs.core.UriInfo;
import java.util.*;

/**
 * The response containing a list of attribute types.  Typically this is the
 * list of all configured attribute types (excluding the "internal" ones).
 *
 */
public class SzAttributeTypesResponse extends SzResponseWithRawData
{
  /**
   * The data for this instance.
   */
  private Data data = new Data();

  /**
   * Package-private default constructor.
   */
  SzAttributeTypesResponse() {
    // do nothing
  }

  /**
   * Constructs with only the HTTP method and the self link, leaving the
   * data sources to be added later.
   *
   * @param meta The response meta data.
   *
   * @param links The links for the response.
   */
  public SzAttributeTypesResponse(SzMeta meta, SzLinks links) {
    super(meta, links);
    this.data.attributeTypes = new LinkedList<>();
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
    this.data.attributeTypes.add(attributeType);
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
    this.data.attributeTypes.clear();
    if (attributeTypes != null) {
      this.data.attributeTypes.addAll(attributeTypes);
    }
  }

  /**
   * Inner class to represent the data section for this response.
   */
  public static class Data {
    /**
     * The list of {@link SzAttributeType} instances describing the attribute
     * types.
     */
    private List<SzAttributeType> attributeTypes;

    /**
     * Private default constructor.
     */
    private Data() {
      this.attributeTypes = null;
    }

    /**
     * Gets the unmodifiable {@link List} of {@link SzAttributeType} instances.
     *
     * @return The unmodifiable {@link List} of {@link SzAttributeType}
     *         instances.
     */
    public List<SzAttributeType> getAttributeTypes() {
      List<SzAttributeType> list = this.attributeTypes;
      return Collections.unmodifiableList(list);
    }
  }
}
