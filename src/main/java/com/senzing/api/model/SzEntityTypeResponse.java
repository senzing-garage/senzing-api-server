package com.senzing.api.model;

import com.senzing.util.Timers;

import javax.ws.rs.core.UriInfo;

/**
 * The response containing a set of entity type codes.  Typically this is the
 * list of all configured entity type codes.
 *
 */
public class SzEntityTypeResponse extends SzResponseWithRawData
{
  /**
   * The data for this instance.
   */
  private Data data = new Data();

  /**
   * Package-private default constructor.
   */
  SzEntityTypeResponse() {
    // do nothing
  }

  /**
   * Constructs with only the HTTP method and the self link, leaving the
   * entity types to be added later.
   *
   * @param meta The response meta data.
   *
   * @param links The links for the response.
   *
   */
  public SzEntityTypeResponse(SzMeta meta, SzLinks links) {
    super(meta, links);
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
   * Adds the specified entity type providing the specified entity type
   * is not already containe
   *
   * @param entityType The entity type code to add.
   */
  public void setEntityType(SzEntityType entityType) {
    this.data.entityType = entityType;
  }

  /**
   * Inner class to represent the data section for this response.
   */
  public static class Data {
    /**
     * The {@link SzEntityType} for this instance.
     */
    private SzEntityType entityType;

    /**
     * Private default constructor.
     */
    private Data() {
      this.entityType = null;
    }

    /**
     * Gets the {@link SzEntityType} describing the entity type.
     *
     * @return The {@link SzEntityType} describing the entity type.
     */
    public SzEntityType getEntityType() {
      return this.entityType;
    }

    /**
     * Private setter used for deserialization.
     */
    private void setEntityType(SzEntityType entityType) {
      this.entityType = entityType;
    }
  }
}
