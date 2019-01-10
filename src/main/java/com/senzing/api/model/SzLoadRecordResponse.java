package com.senzing.api.model;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * The response containing the record ID of the record that was loaded.
 *
 */
public class SzLoadRecordResponse extends SzBasicResponse
{
  /**
   * The record ID of the record that was loaded.
   */
  private String recordId;

  /**
   * The data for this instance.
   */
  private Data data = new Data();

  /**
   * Constructs with only the HTTP method and the self link, leaving the
   * record ID to be initialized later.
   *
   * @param httpMethod The {@link SzHttpMethod}.
   *
   * @param httpStatusCode The HTTP response status code.
   *
   * @param selfLink The string URL link to generate this response.
   */
  public SzLoadRecordResponse(SzHttpMethod httpMethod,
                              int          httpStatusCode,
                              String       selfLink) {
    super(httpMethod, httpStatusCode, selfLink);
  }

  /**
   * Constructs with the HTTP method, the self link, and the record ID.
   *
   * @param httpMethod The {@link SzHttpMethod}.
   *
   * @param httpStatusCode The HTTP response status code.
   *
   * @param selfLink The string URL link to generate this response.
   *
   * @param recordId The record ID of the record that was loaded.
   */
  public SzLoadRecordResponse(SzHttpMethod httpMethod,
                              int          httpStatusCode,
                              String       selfLink,
                              String       recordId) {
    super(httpMethod, httpStatusCode, selfLink);
    this.recordId = recordId;
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
  public void setRecordId(String recordId)
  {
    this.recordId = recordId;
  }

  /**
   * Inner class to represent the data section for this response.
   */
  public class Data {
    /**
     * Private default constructor.
     */
    private Data() {
      // do nothing
    }

    /**
     * Gets the record ID of the record that was loaded.
     *
     * @return The record ID of the record that was loaded.
     */
    public String getRecordId() {
      return SzLoadRecordResponse.this.recordId;
    }
  }
}
