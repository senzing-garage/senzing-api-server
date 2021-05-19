package com.senzing.api.model;

import com.senzing.util.Timers;

import javax.ws.rs.core.UriInfo;
import java.util.*;

/**
 * A response object that contains entity record data.
 *
 */
public class SzRecordResponse extends SzResponseWithRawData {
  /**
   * The data for this instance.
   */
  private Data data = new Data();

  /**
   * Default constructor.
   */
  public SzRecordResponse() {
    // do nothing
  }

  /**
   * Constructs with only the HTTP method and the self link, leaving the
   * record data to be initialized later.
   *
   * @param meta The response meta data.
   *
   * @param links The links for the response.
   */
  public SzRecordResponse(SzMeta meta, SzLinks links)
  {
    this(meta, links, null);
  }

  /**
   * Constructs with the HTTP method, self link and the {@link SzEntityRecord}
   * describing the record.
   *
   * @param meta The response meta data.
   *
   * @param links The links for the response.
   *
   * @param record The {@link SzEntityRecord} describing the record.
   */
  public SzRecordResponse(SzMeta         meta,
                          SzLinks        links,
                          SzEntityRecord record)
  {
    super(meta, links);
    this.data.setRecord(record);
  }

  /**
   * Returns the data associated with this response which contains an
   * {@link SzEntityRecord}.
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
   * Sets the data associated with this response with an {@link SzEntityRecord}.
   *
   * @param record The {@link SzEntityRecord} describing the record.
   */
  public void setRecord(SzEntityRecord record) {
    this.data.setRecord(record);
  }

  /**
   * Inner class to represent the data section for this response.
   */
  public static class Data {
    /**
     * The {@link SzEntityRecord} describing the record.
     */
    private SzEntityRecord entityRecord;

    /**
     * Private default constructor.
     */
    private Data() {
      this.entityRecord = null;
    }

    /**
     * Gets the {@link SzEntityRecord} describing the record.
     *
     * @return The {@link SzEntityRecord} describing the record.
     */
    public SzEntityRecord getRecord() {
      return this.entityRecord;
    }

    /**
     * Private setter used for deserialization.
     */
    private void setRecord(SzEntityRecord entityRecord) {
      this.entityRecord = entityRecord;
    }
  }

}
