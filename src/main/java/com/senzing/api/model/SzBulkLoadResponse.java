package com.senzing.api.model;

import com.senzing.util.Timers;

import javax.ws.rs.core.UriInfo;

/**
 * A response object that contains entity record data.
 *
 */
public class SzBulkLoadResponse extends SzBasicResponse {
  /**
   * The {@link SzBulkLoadResult} describing the record.
   */
  private SzBulkLoadResult bulkLoadResult;

  /**
   * Default constructor.
   */
  public SzBulkLoadResponse() {
    this.bulkLoadResult = null;
  }

  /**
   * Constructs with only the HTTP method and the self link, leaving the
   * record data to be initialized later.
   *
   * @param meta The response meta data.
   *
   * @param links The links for the response.
   */
  public SzBulkLoadResponse(SzMeta meta, SzLinks links)
  {
    this(meta, links, null);
  }

  /**
   * Constructs with the HTTP method, self link and the {@link
   * SzBulkLoadResult} describing the record.
   *
   * @param meta The response meta data.
   *
   * @param links The links for the response.
   *
   * @param bulkLoadResult The {@link SzBulkLoadResult} describing the record.
   */
  public SzBulkLoadResponse(SzMeta            meta,
                            SzLinks           links,
                            SzBulkLoadResult  bulkLoadResult)
  {
    super(meta, links);
    this.bulkLoadResult = bulkLoadResult;
  }

  /**
   * Returns the data associated with this response which is an
   * {@link SzBulkLoadResult}.
   *
   * @return The data associated with this response.
   */
  public SzBulkLoadResult getData() {
    return this.bulkLoadResult;
  }

  /**
   * Sets the data associated with this response with an {@link
   * SzBulkLoadResult}.
   *
   * @param bulkLoadResult The {@link SzBulkLoadResult} describing the record.
   */
  public void setData(SzBulkLoadResult bulkLoadResult) {
    this.bulkLoadResult = bulkLoadResult;
  }
}
