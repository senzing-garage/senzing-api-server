package com.senzing.api.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.senzing.util.Timers;

import javax.ws.rs.core.UriInfo;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;

/**
 * The response for a record deletion operation.
 *
 */
public class SzDeleteRecordResponse extends SzResponseWithRawData
{
  /**
   * The data for this instance.
   */
  private Data data = new Data();

  /**
   * Default constructor.
   */
  public SzDeleteRecordResponse() {
    // do nothing
  }

  /**
   * Constructs with only the HTTP method and the self link.
   *
   * @param meta The response meta data.
   *
   * @param links The links for the response.
   */
  public SzDeleteRecordResponse(SzMeta meta, SzLinks links) {
    super(meta, links);
  }

  /**
   * Constructs with the HTTP method, the self link and the info.
   *
   * @param meta The response meta data.
   *
   * @param links The links for the response.
   *
   * @param info The {@link SzResolutionInfo} providing the information
   *             associated with the resolution of the record.
   */
  public SzDeleteRecordResponse(SzMeta            meta,
                                SzLinks           links,
                                SzResolutionInfo  info)
  {
    super(meta, links);
    this.data.info = info;
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
   * Sets the @link SzResolutionInfo} providing the information associated
   * with the deletion of the record.
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
     * The {@link SzResolutionInfo} providing the information associated with
     * the deletion of the record.
     */
    private SzResolutionInfo info;

    /**
     * Private default constructor.
     */
    private Data() {
      this.info = null;
    }

    /**
     * Gets the {@link SzResolutionInfo} providing the information associated
     * with the deletion of the record.
     *
     * @return The {@link SzResolutionInfo} providing the information
     *         associated with the deletion of the record.
     */
    @JsonInclude(NON_NULL)
    public SzResolutionInfo getInfo() {
      return this.info;
    }

  }
}
