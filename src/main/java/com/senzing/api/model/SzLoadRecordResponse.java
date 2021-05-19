package com.senzing.api.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.senzing.util.Timers;

import javax.ws.rs.core.UriInfo;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_EMPTY;
import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;

/**
 * The response containing the record ID of the record that was loaded.
 *
 */
public class SzLoadRecordResponse extends SzResponseWithRawData
{
  /**
   * The data for this instance.
   */
  private Data data = new Data();

  /**
   * Default constructor.
   */
  public SzLoadRecordResponse() {
    this.data.recordId = null;
  }

  /**
   * Constructs with only the HTTP method and the self link, leaving the
   * record ID to be initialized later.
   *
   * @param meta The response meta data.
   *
   * @param links The links for the response.
   */
  public SzLoadRecordResponse(SzMeta meta, SzLinks links)
  {
    super(meta, links);
  }

  /**
   * Constructs with the HTTP method, the self link, and the record ID.
   *
   * @param meta The response meta data.
   *
   * @param links The links for the response.
   *
   * @param recordId The record ID of the record that was loaded.
   *
   * @param info The {@link SzResolutionInfo} providing the information
   *             associated with the resolution of the record.
   */
  public SzLoadRecordResponse(SzMeta            meta,
                              SzLinks           links,
                              String            recordId,
                              SzResolutionInfo  info)
  {
    super(meta, links);
    this.data.recordId  = recordId;
    this.data.info      = info;
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
   * Sets the record ID of the record that was loaded.
   *
   * @param recordId The record ID of the record.
   */
  public void setRecordId(String recordId) {
    this.data.recordId = recordId;
  }

  /**
   * Sets the @link SzResolutionInfo} providing the information associated
   * with the resolution of the record.
   *
   * @param info The @link SzResolutionInfo} providing the information associated
   *             with the resolution of the record.
   */
  public void setInfo(SzResolutionInfo info) {
    this.data.info = info;
  }

  /**
   * Inner class to represent the data section for this response.
   */
  public static class Data {
    /**
     * The record ID of the record that was loaded.
     */
    private String recordId;

    /**
     * The {@link SzResolutionInfo} providing the information associated with
     * the resolution of the record.
     */
    private SzResolutionInfo info;

    /**
     * Private default constructor.
     */
    private Data() {
      this.recordId = null;
      this.info     = null;
    }

    /**
     * Gets the record ID of the record that was loaded.
     *
     * @return The record ID of the record that was loaded.
     */
    @JsonInclude(NON_NULL)
    public String getRecordId() {
      return this.recordId;
    }

    /**
     * Gets the {@link SzResolutionInfo} providing the information associated
     * with the resolution of the record.
     *
     * @return The {@link SzResolutionInfo} providing the information
     *         associated with the resolution of the record.
     */
    @JsonInclude(NON_NULL)
    public SzResolutionInfo getInfo() {
      return this.info;
    }

  }
}
