package com.senzing.api.model;

import com.senzing.util.Timers;

import javax.ws.rs.core.UriInfo;

/**
 * A response object that contains entity record data.
 *
 */
public class SzBulkDataAnalysisResponse extends SzBasicResponse {
  /**
   * The {@link SzBulkDataAnalysis} describing the record.
   */
  private SzBulkDataAnalysis bulkDataAnalysis;

  /**
   * Default constructor.
   */
  public SzBulkDataAnalysisResponse() {
    this.bulkDataAnalysis = null;
  }

  /**
   * Constructs with only the HTTP method and the self link, leaving the
   * record data to be initialized later.
   *
   * @param meta The response meta data.
   *
   * @param links The links for the response.
   */
  public SzBulkDataAnalysisResponse(SzMeta meta, SzLinks links)
  {
    this(meta, links, null);
  }

  /**
   * Constructs with the HTTP method, self link and the {@link
   * SzBulkDataAnalysis} describing the record.
   *
   * @param meta The response meta data.
   *
   * @param links The links for the response.
   *
   * @param dataAnalysis The {@link SzEntityRecord} describing the record.
   */
  public SzBulkDataAnalysisResponse(SzMeta              meta,
                                    SzLinks             links,
                                    SzBulkDataAnalysis  dataAnalysis)
  {
    super(meta, links);
    this.bulkDataAnalysis = dataAnalysis;
  }

  /**
   * Returns the data associated with this response which is an
   * {@link SzBulkDataAnalysis}.
   *
   * @return The data associated with this response.
   */
  public SzBulkDataAnalysis getData() {
    return this.bulkDataAnalysis;
  }

  /**
   * Sets the data associated with this response with an {@link
   * SzBulkDataAnalysis}.
   *
   * @param dataAnalysis The {@link SzBulkDataAnalysis} describing the record.
   */
  public void setData(SzBulkDataAnalysis dataAnalysis) {
    this.bulkDataAnalysis = dataAnalysis;
  }
}
