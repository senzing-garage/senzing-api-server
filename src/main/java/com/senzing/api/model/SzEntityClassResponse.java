package com.senzing.api.model;

import com.senzing.util.Timers;

import javax.ws.rs.core.UriInfo;

/**
 * The response containing a set of entity class codes.  Typically this is the
 * list of all configured entity class codes.
 *
 */
public class SzEntityClassResponse extends SzResponseWithRawData
{
  /**
   * The data for this instance.
   */
  private Data data = new Data();

  /**
   * Package-private default constructor.
   */
  SzEntityClassResponse() {
    // do nothing
  }

  /**
   * Constructs with only the HTTP method and the self link, leaving the
   * entity classes to be added later.
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
  public SzEntityClassResponse(SzHttpMethod httpMethod,
                               int          httpStatusCode,
                               String       selfLink,
                               Timers       timers) {
    super(httpMethod, httpStatusCode, selfLink, timers);
  }

  /**
   * Constructs with only the HTTP method and the {@link UriInfo}, leaving the
   * entity classes to be added later.
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
  public SzEntityClassResponse(SzHttpMethod httpMethod,
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
  public void setEntityClass(SzEntityClass entityClass) {
    this.data.entityClass = entityClass;
  }

  /**
   * Inner class to represent the data section for this response.
   */
  public static class Data {
    /**
     * The {@link SzEntityClass} for this instance.
     */
    private SzEntityClass entityClass;

    /**
     * Private default constructor.
     */
    private Data() {
      this.entityClass = null;
    }

    /**
     * Gets the {@link SzEntityClass} describing the entity class.
     *
     * @return The {@link SzEntityClass} describing the entity class.
     */
    public SzEntityClass getEntityClass() {
      return this.entityClass;
    }

    /**
     * Private setter used for deserialization.
     */
    private void setEntityClass(SzEntityClass entityClass) {
      this.entityClass = entityClass;
    }
  }
}
