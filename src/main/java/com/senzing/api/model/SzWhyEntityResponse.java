package com.senzing.api.model;

import com.senzing.util.Timers;

import javax.ws.rs.core.UriInfo;
import java.util.*;

/**
 * A response object that contains the {@link SzWhyEntityResult} describing
 * why an entity resolved.
 */
public class SzWhyEntityResponse extends SzResponseWithRawData {
  /**
   * The {@link Data} describing the result data.
   */
  private Data data = new Data();

  /**
   * Package-private default constructor.
   */
  SzWhyEntityResponse() {
    // do nothing
  }

  /**
   * Constructs with only the HTTP method and the self link, leaving the
   * entity data to be initialized later.
   *
   * @param httpMethod The {@link SzHttpMethod}.
   * @param httpStatusCode The HTTP response status code.
   * @param selfLink The string URL link to generate this response.
   * @param timers The {@link Timers} object for the timings that were taken.
   */
  public SzWhyEntityResponse(SzHttpMethod httpMethod,
                             int          httpStatusCode,
                             String       selfLink,
                             Timers       timers)
  {
    super(httpMethod, httpStatusCode, selfLink, timers);
  }

  /**
   * Constructs with only the HTTP method and the {@link UriInfo}, leaving the
   * entity data to be initialized later.
   *
   * @param httpMethod The {@link SzHttpMethod}.
   * @param httpStatusCode The HTTP response status code.
   * @param uriInfo The {@link UriInfo} from the request.
   * @param timers The {@link Timers} object for the timings that were taken.
   */
  public SzWhyEntityResponse(SzHttpMethod httpMethod,
                             int          httpStatusCode,
                             UriInfo      uriInfo,
                             Timers       timers)
  {
    super(httpMethod, httpStatusCode, uriInfo, timers);
  }

  /**
   * Returns the data associated with this response.
   *
   * @return The data associated with this response.
   */
  public Data getData() {
    return this.data;
  }

  /**
   * Private setter for the data to support JSON data marshalling.
   *
   * @param data The {@link SzWhyEntityResult} describing the record.
   */
  private void setData(Data data) {
    this.data = data;
  }

  /**
   * Sets the {@link Collection} of {@link SzWhyRecordsResult} describing why
   * the records in the entity resolved.
   */
  public void setWhyResults(Collection<SzWhyEntityResult> results) {
    this.data.setWhyResults(results);
  }

  /**
   * Adds an {@link SzWhyEntityResult} to add to the list of results describing
   * why the records in the entity resolved.
   *
   * @param result The {@link SzWhyEntityResult} to add to the list of results
   *               describing why the records in the entity resolved.
   */
  public void addWhyResult(SzWhyEntityResult result) {
    this.data.whyResults.add(result);
  }

  /**
   * Adds an entity to the list of entities associated with the {@link
   * SzWhyRecordsResult}.
   *
   * @param entity The {@link SzEntityData} describing the entity to add.
   */
  public void addEntity(SzEntityData entity) {
    this.data.entities.add(entity);
  }

  /**
   * Sets the list of {@link SzEntityData} instances describing the entities
   * associated with the why result.
   */
  public void setEntities(Collection<SzEntityData> entities) {
    this.data.setEntities(entities);
  }

  /**
   * Inner class to represent the data section for this response.
   */
  public static class Data {
    /**
     * The {@link List} of {@link SzWhyEntityResult} instances for the entity.
     */
    private List<SzWhyEntityResult> whyResults;

    /**
     * The {@link List} of {@link SzEntityData} instances decribing the
     * entities in the response.
     */
    private List<SzEntityData> entities;

    /**
     * Private default constructor.
     */
    private Data() {
      this.whyResults = new LinkedList<>();
      this.entities   = new LinkedList<>();
    }

    /**
     * Gets the {@link List} of {@link SzWhyRecordsResult} instances describing
     * why the records in the entity resolved.
     *
     * @return The {@link List} of {@link SzWhyRecordsResult} instances
     *         describing why the records in the entity resolved.
     */
    public List<SzWhyEntityResult> getWhyResults() {
      return Collections.unmodifiableList(this.whyResults);
    }

    /**
     * Sets the {@link List} of {@link SzWhyEntityResult} instances describing
     * why the records in the entity resolved.
     *
     * @param whyResults The {@link Collection} of {@link SzWhyEntityResult}
     *                   instances describing why the records in the entity
     *                   resolved.
     */
    private void setWhyResults(Collection<SzWhyEntityResult> whyResults) {
      this.whyResults.clear();
      this.whyResults.addAll(whyResults);
    }

    /**
     * Gets the unmodifiable {@link List} of {@link SzEntityData} instances
     * describing the entities involved in this why operation.
     *
     * @return The unmodifiable {@link Map} of {@link String} data source codes
     *         to {@link SzDataSource} values describing the configured data
     *         sources.
     */
    public List<SzEntityData> getEntities() {
      return Collections.unmodifiableList(this.entities);
    }

    /**
     * Private setter used for deserialization.
     */
    private void setEntities(Collection<SzEntityData> entities) {
      this.entities.clear();
      if (entities != null) this.entities.addAll(entities);
    }
  }
}
