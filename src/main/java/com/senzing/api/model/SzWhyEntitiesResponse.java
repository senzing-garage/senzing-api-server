package com.senzing.api.model;

import com.senzing.util.Timers;

import javax.ws.rs.core.UriInfo;
import java.util.*;

/**
 * A response object that contains the {@link SzWhyEntitiesResult} describing
 * why two entities related or did not resolve.
 */
public class SzWhyEntitiesResponse extends SzResponseWithRawData {
  /**
   * The {@link Data} describing the result data.
   */
  private Data data = new Data();

  /**
   * Package-private default constructor.
   */
  SzWhyEntitiesResponse() {
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
  public SzWhyEntitiesResponse(SzHttpMethod httpMethod,
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
  public SzWhyEntitiesResponse(SzHttpMethod httpMethod,
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
   * Sets the {@link SzWhyEntitiesResult} describing why the entities did not
   * resolve or why they related.
   *
   * @param result The {@link SzWhyEntitiesResult} the result describing why
   *               the entities did not resolve or why they related.
   */
  public void setWhyResult(SzWhyEntitiesResult result) {
    this.data.setWhyResult(result);
  }

  /**
   * Adds an entity to the list of entities associated with the {@link
   * SzWhyEntitiesResult}.
   *
   * @param entity The {@link SzEntityData} describing the entity to add.
   */
  public void addEntity(SzEntityData entity) {
    this.data.addEntity(entity);
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
     * The {@link List} of {@link SzWhyEntitiesResult} instances for the
     * entities.
     */
    private SzWhyEntitiesResult whyResult;

    /**
     * The {@link List} of {@link SzEntityData} instances decribing the
     * entities in the response.
     */
    private List<SzEntityData> entities;

    /**
     * Private default constructor.
     */
    private Data() {
      this.whyResult  = null;
      this.entities   = new LinkedList<>();
    }

    /**
     * Gets the {@link SzWhyEntitiesResult} describing why the entities did not
     * resolve or why they related.
     *
     * @return The {@link SzWhyEntitiesResult} describing why the entities did
     *         not resolve or why they related.
     */
    public SzWhyEntitiesResult getWhyResult() {
      return this.whyResult;
    }

    /**
     * Sets the {@link SzWhyEntitiesResult} describing why the entities did not
     * resolve or why they related.
     *
     * @param whyResult The {@link SzWhyEntitiesResult} describing why the
     *                  entities did not resolve or why they related.
     */
    private void setWhyResult(SzWhyEntitiesResult whyResult) {
      this.whyResult = whyResult;
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
     * Adds the specified {@link SzEntityData} to the list of entities involved
     * in this why operation.
     *
     * @param entity The specified {@link SzEntityData} to the list of entities
     *               involved in this why operation.
     */
    private void addEntity(SzEntityData entity) {
      this.entities.add(entity);
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
