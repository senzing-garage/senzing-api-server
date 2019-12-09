package com.senzing.api.model;

import com.senzing.util.Timers;

import javax.ws.rs.core.UriInfo;
import java.util.*;

/**
 * The response containing a set of entity type codes.  Typically this is the
 * list of all configured entity type codes.
 *
 */
public class SzEntityTypesResponse extends SzResponseWithRawData
{
  /**
   * The data for this instance.
   */
  private Data data = new Data();

  /**
   * Package-private default constructor.
   */
  SzEntityTypesResponse() {
    // do nothing
  }

  /**
   * Constructs with only the HTTP method and the self link, leaving the
   * entity types to be added later.
   *
   * @param httpMethod The {@link SzHttpMethod}.
   *
   * @param httpStatusCode The HTTP response status code.
   *
   * @param selfLink The string URL link to generate this response.
   *
   * @param timers The {@link Timers} object for the timings that were taken.
   *
   */
  public SzEntityTypesResponse(SzHttpMethod httpMethod,
                               int          httpStatusCode,
                               String       selfLink,
                               Timers       timers) {
    super(httpMethod, httpStatusCode, selfLink, timers);
  }

  /**
   * Constructs with only the HTTP method and the {@link UriInfo}, leaving the
   * entity types to be added later.
   *
   * @param httpMethod The {@link SzHttpMethod}.
   *
   * @param httpStatusCode The HTTP response status code.
   *
   * @param uriInfo The {@link UriInfo} from the request.
   *
   * @param timers The {@link Timers} object for the timings that were taken.
   *
   */
  public SzEntityTypesResponse(SzHttpMethod httpMethod,
                               int          httpStatusCode,
                               UriInfo      uriInfo,
                               Timers       timers)
  {
    super(httpMethod, httpStatusCode, uriInfo, timers);
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
  public void addEntityType(SzEntityType entityType) {
    this.data.entityTypes.remove(entityType.getEntityTypeCode());
  }

  /**
   * Sets the specified {@link Set} of entity types using the
   * specified {@link Collection} of entity types (removing duplicates).
   *
   * @param entityTypes The {@link Collection} of entity types to set.
   */
  public void setEntityTypes(Collection<SzEntityType> entityTypes)
  {
    this.data.entityTypes.clear();
    if (entityTypes != null) {
      // ensure the entity types are unique
      for (SzEntityType entityType : entityTypes) {
        this.data.entityTypes.put(entityType.getEntityTypeCode(), entityType);
      }
    }
  }

  /**
   * Inner class to represent the data section for this response.
   */
  public static class Data {
    /**
     * The map of {@link String} entity type codes to {@link SzEntityType}
     * instances.
     */
    private Map<String, SzEntityType> entityTypes;

    /**
     * Private default constructor.
     */
    private Data() {
      this.entityTypes = new LinkedHashMap<>();
    }

    /**
     * Gets the unmodifiable {@link Set} of entity type codes.
     *
     * @return The unmodifiable {@link Set} of entity type codes.
     */
    public Set<String> getEntityTypes() {
      Set<String> set = this.entityTypes.keySet();
      return Collections.unmodifiableSet(set);
    }

    /**
     * Private setter used for deserialization.
     */
    private void setEntityTypes(Collection<String> entityTypes) {
      Iterator<Map.Entry<String,SzEntityType>> iter
          = this.entityTypes.entrySet().iterator();

      // remove entries in the map that are not in the specified set
      while (iter.hasNext()) {
        Map.Entry<String,SzEntityType> entry = iter.next();
        if (!entityTypes.contains(entry.getKey())) {
          iter.remove();
        }
      }

      // add place-holder entries to the map for data sources in the set
      for (String entityType: entityTypes) {
        this.entityTypes.put(entityType, null);
      }
    }

    /**
     * Gets the unmodifiable {@link Map} of {@link String} entity type codes
     * to {@link SzEntityType} values describing the configured entity types.
     *
     * @return The unmodifiable {@link Map} of {@link String} entity type codes
     *         to {@link SzEntityType} values describing the configured entity
     *         types.
     */
    public Map<String, SzEntityType> getEntityTypeDetails() {
      return Collections.unmodifiableMap(this.entityTypes);
    }

    /**
     * Private setter used for deserialization.
     */
    private void setEntityTypeDetails(Map<String, SzEntityType> details) {
      this.entityTypes.clear();
      for (SzEntityType entityType: details.values()) {
        this.entityTypes.put(entityType.getEntityTypeCode(), entityType);
      }
    }
  }
}
