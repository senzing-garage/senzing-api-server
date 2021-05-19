package com.senzing.api.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.senzing.util.JsonUtils;
import com.senzing.util.Timers;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonValue;
import javax.ws.rs.core.UriInfo;
import java.io.StringReader;
import java.util.LinkedHashMap;
import java.util.Map;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;

/**
 * Extends the {@link SzBasicResponse} to add the optional raw data section
 * that is common to most responses that leverage the native Senzing APIs.
 *
 */
public class SzResponseWithRawData extends SzBasicResponse {
  /**
   * The raw data associated with the response.
   */
  private Object rawData;

  /**
   * Default constructor.
   */
  SzResponseWithRawData() {
    this.rawData = null;
  }

  /**
   * Constructs with the specified HTTP method and self link.
   *
   * @param meta The response meta data.
   *
   * @param links The links for the response.
   *
   */
  public SzResponseWithRawData(SzMeta meta, SzLinks links)
  {
    this(meta, links, null);
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
  public SzResponseWithRawData(SzMeta meta, SzLinks links, String rawData)
  {
    super(meta, links);
    this.rawData = JsonUtils.normalizeJsonText(rawData);
  }

  /**
   * Returns the raw data associated with this response.
   *
   * @return The raw data associated with this response.
   */
  @JsonInclude(NON_NULL)
  public Object getRawData() {
    return this.rawData;
  }

  /**
   * Sets the raw data associated with this response.
   *
   * @param rawData The raw data associated with this response.
   */
  public void setRawData(Object rawData) {
    if (rawData instanceof String) {
      this.rawData = JsonUtils.normalizeJsonText((String) rawData);
    } else {
      this.rawData = rawData;
    }
  }
}
