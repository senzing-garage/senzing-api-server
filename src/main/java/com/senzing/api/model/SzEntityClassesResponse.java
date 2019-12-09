package com.senzing.api.model;

import com.senzing.util.Timers;

import javax.ws.rs.core.UriInfo;
import java.util.*;

/**
 * The response containing a set of entity class codes.  Typically this is the
 * list of all configured entity class codes.
 *
 */
public class SzEntityClassesResponse extends SzResponseWithRawData
{
  /**
   * The data for this instance.
   */
  private Data data = new Data();

  /**
   * Package-private default constructor.
   */
  SzEntityClassesResponse() {
    // do nothing
  }

  /**
   * Constructs with only the HTTP method and the self link, leaving the
   * entity classs to be added later.
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
  public SzEntityClassesResponse(SzHttpMethod httpMethod,
                                 int          httpStatusCode,
                                 String       selfLink,
                                 Timers       timers) {
    super(httpMethod, httpStatusCode, selfLink, timers);
  }

  /**
   * Constructs with only the HTTP method and the {@link UriInfo}, leaving the
   * entity classs to be added later.
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
  public SzEntityClassesResponse(SzHttpMethod httpMethod,
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
   * Adds the specified entity class providing the specified entity class
   * is not already containe
   *
   * @param entityClass The entity class code to add.
   */
  public void addEntityClass(SzEntityClass entityClass) {
    this.data.entityClasses.remove(entityClass.getEntityClassCode());
  }

  /**
   * Sets the specified {@link Set} of entity classs using the
   * specified {@link Collection} of entity classs (removing duplicates).
   *
   * @param entityClasses The {@link Collection} of entity classs to set.
   */
  public void setEntityClasses(Collection<SzEntityClass> entityClasses)
  {
    this.data.entityClasses.clear();
    if (entityClasses != null) {
      // ensure the entity classs are unique
      for (SzEntityClass entityClass : entityClasses) {
        this.data.entityClasses.put(entityClass.getEntityClassCode(), entityClass);
      }
    }
  }

  /**
   * Inner class to represent the data section for this response.
   */
  public static class Data {
    /**
     * The map of {@link String} entity class codes to {@link SzEntityClass}
     * instances.
     */
    private Map<String, SzEntityClass> entityClasses;

    /**
     * Private default constructor.
     */
    private Data() {
      this.entityClasses = new LinkedHashMap<>();
    }

    /**
     * Gets the unmodifiable {@link Set} of entity class codes.
     *
     * @return The unmodifiable {@link Set} of entity class codes.
     */
    public Set<String> getEntityClasses() {
      Set<String> set = this.entityClasses.keySet();
      return Collections.unmodifiableSet(set);
    }

    /**
     * Private setter used for deserialization.
     */
    private void setEntityClasses(Collection<String> entityClasses) {
      Iterator<Map.Entry<String,SzEntityClass>> iter
          = this.entityClasses.entrySet().iterator();

      // remove entries in the map that are not in the specified set
      while (iter.hasNext()) {
        Map.Entry<String,SzEntityClass> entry = iter.next();
        if (!entityClasses.contains(entry.getKey())) {
          iter.remove();
        }
      }

      // add place-holder entries to the map for data sources in the set
      for (String entityClass: entityClasses) {
        this.entityClasses.put(entityClass, null);
      }
    }

    /**
     * Gets the unmodifiable {@link Map} of {@link String} entity class codes
     * to {@link SzEntityClass} values describing the configured entity classs.
     *
     * @return The unmodifiable {@link Map} of {@link String} entity class codes
     *         to {@link SzEntityClass} values describing the configured entity
     *         classes.
     */
    public Map<String, SzEntityClass> getEntityClassDetails() {
      return Collections.unmodifiableMap(this.entityClasses);
    }

    /**
     * Private setter used for deserialization.
     */
    private void setEntityClassDetails(Map<String, SzEntityClass> details) {
      this.entityClasses.clear();
      for (SzEntityClass entityClass: details.values()) {
        this.entityClasses.put(entityClass.getEntityClassCode(), entityClass);
      }
    }

  }
}
