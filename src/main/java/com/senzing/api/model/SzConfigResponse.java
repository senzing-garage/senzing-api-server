package com.senzing.api.model;

import com.senzing.util.Timers;

import javax.ws.rs.core.UriInfo;

/**
 * A response object that contains license data.
 *
 */
public class SzConfigResponse extends SzResponseWithRawData {
  /**
   * Default constructor.
   */
  SzConfigResponse() {
    // do nothing
  }

  /**
   * Constructs with the specified HTTP method and self link.
   *
   * @param meta The response meta data.
   *
   * @param links The links for the response.
   */
  public SzConfigResponse(SzMeta meta, SzLinks links)
  {
    super(meta, links);
  }

  /**
   * Constructs with the specified HTTP method, self link string and
   * object representing the raw data response from the engine.
   *
   * @param meta The response meta data.
   *
   * @param links The links for the response.
   *
   * @param rawData The raw data to associate with the response.
   */
  public SzConfigResponse(SzMeta meta, SzLinks links, String rawData)
  {
    super(meta, links, rawData);
  }
}
