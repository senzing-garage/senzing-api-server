package com.senzing.api.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.senzing.util.JsonUtils;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonValue;
import javax.ws.rs.core.UriInfo;
import java.io.StringReader;
import java.util.LinkedHashMap;
import java.util.Map;

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
   * Constructs with the specified HTTP method and self link.
   *
   * @param httpMethod The {@link SzHttpMethod} from the request.
   *
   * @param httpStatusCode The HTTP response status code.
   *
   * @param selfLink The self link from the request.
   */
  public SzResponseWithRawData(SzHttpMethod httpMethod,
                               int          httpStatusCode,
                               String       selfLink)
  {
    this(httpMethod, httpStatusCode, selfLink, null);
  }

  /**
   * Constructs with the specified HTTP method, self link string and
   * object representing the raw data response from the engine.
   *
   * @param httpMethod The {@link SzHttpMethod} from the request.
   *
   * @param httpStatusCode The HTTP response status code.
   *
   * @param selfLink The self link from the request.
   *
   * @param rawData The raw data to associate with the response.
   */
  public SzResponseWithRawData(SzHttpMethod httpMethod,
                               int          httpStatusCode,
                               String       selfLink,
                               String       rawData)
  {
    super(httpMethod, httpStatusCode, selfLink);

    this.rawData = JsonUtils.normalizeJsonText(rawData);
  }

  /**
   * Constructs with the specified HTTP method and {@link UriInfo}.
   *
   * @param httpMethod The {@link SzHttpMethod} from the request.
   *
   * @param httpStatusCode The HTTP response status code.
   *
   * @param uriInfo The {@link UriInfo} from the request.
   */
  public SzResponseWithRawData(SzHttpMethod httpMethod,
                               int          httpStatusCode,
                               UriInfo      uriInfo)
  {
    this(httpMethod, httpStatusCode, uriInfo, null);
  }

  /**
   * Constructs with the specified HTTP method, {@link UriInfo} and
   * object representing the raw data response from the engine.
   *
   * @param httpMethod The {@link SzHttpMethod} from the request.
   *
   * @param httpStatusCode The HTTP response status code.
   *
   * @param uriInfo The {@link UriInfo} from the request.
   *
   * @param rawData The raw data to associate with the response.
   */
  public SzResponseWithRawData(SzHttpMethod httpMethod,
                               int          httpStatusCode,
                               UriInfo      uriInfo,
                               String       rawData)
  {
    super(httpMethod, httpStatusCode, uriInfo);

    this.rawData = JsonUtils.normalizeJsonText(rawData);
  }

  /**
   * Returns the raw data associated with this response.
   *
   * @return The raw data associated with this response.
   */
  public Object getRawData() {
    return this.rawData;
  }

  /**
   * Sets the raw data associated with this response.
   *
   * @param rawData The raw data associated with this response.
   */
  public void setRawData(String rawData) {
    this.rawData = JsonUtils.normalizeJsonText(rawData);
  }
}
