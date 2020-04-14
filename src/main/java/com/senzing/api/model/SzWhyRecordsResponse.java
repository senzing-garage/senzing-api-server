package com.senzing.api.model;

import com.senzing.util.Timers;

import javax.ws.rs.core.UriInfo;
import java.util.*;

/**
 * A response object that contains the {@link SzWhyRecordsResult} describing
 * why or why not two records did or did not resolve.
 */
public class SzWhyRecordsResponse extends SzResponseWithRawData {
  /**
   * The data for this response.
   */
  private Data data = new Data();

  /**
   * Package-private default constructor.
   */
  SzWhyRecordsResponse() {
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
  public SzWhyRecordsResponse(SzHttpMethod httpMethod,
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
  public SzWhyRecordsResponse(SzHttpMethod httpMethod,
                              int          httpStatusCode,
                              UriInfo      uriInfo,
                              Timers       timers)
  {
    super(httpMethod, httpStatusCode, uriInfo, timers);
  }

  /**
   * Returns the data associated with this response which is an
   * {@link SzWhyRecordsResult}.
   *
   * @return The data associated with this response.
   */
  public Data getData() {
    return this.data;
  }

  /**
   * Private setter for JSON marshalling.
   */
  private void setData(Data data) {
    this.data = data;
  }

  /**
   * Sets the {@link SzWhyRecordsResult} describing why the two records did or
   * did not resolve to one another or why they do or do not relate.
   */
  public void setWhyResult(SzWhyRecordsResult result) {
    this.data.whyResult = result;
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
     * The {@link SzWhyRecordsResult} instance for the records.
     */
    private SzWhyRecordsResult whyResult;

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
     * Gets the {@link SzWhyRecordsResult} instance describing why two records
     * did or did not resolve.
     *
     * @return The {@link SzWhyRecordsResult} instance describing why two
     *         records did or did not resolve.
     */
    public SzWhyRecordsResult getWhyResult() {
      return this.whyResult;
    }

    /**
     * Sets the {@link SzWhyRecordsResult} instance describing why two records
     * did or did not resolve.
     *
     * @param whyResult The {@link SzWhyRecordsResult} instance describing why
     *                  two records did or did not resolve.
     */
    private void setWhyResult(SzWhyRecordsResult whyResult) {
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
     * Private setter used for deserialization.
     */
    private void setEntities(Collection<SzEntityData> entities) {
      this.entities.clear();
      if (entities != null) this.entities.addAll(entities);
    }
  }

}
