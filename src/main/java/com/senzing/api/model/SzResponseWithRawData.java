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
   * @param httpMethod The {@link SzHttpMethod} from the request.
   *
   * @param httpStatusCode The HTTP response status code.
   *
   * @param selfLink The self link from the request.
   *
   * @param timers The {@link Timers} object for the timings that were taken.
   *
   */
  public SzResponseWithRawData(SzHttpMethod httpMethod,
                               int          httpStatusCode,
                               String       selfLink,
                               Timers       timers)
  {
    this(httpMethod, httpStatusCode, selfLink, timers, null);
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
   * @param timers The {@link Timers} object for the timings that were taken.
   *
   * @param rawData The raw data to associate with the response.
   */
  public SzResponseWithRawData(SzHttpMethod httpMethod,
                               int          httpStatusCode,
                               String       selfLink,
                               Timers       timers,
                               String       rawData)
  {
    super(httpMethod, httpStatusCode, selfLink, timers);

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
   *
   * @param timers The {@link Timers} object for the timings that were taken.
   *
   */
  public SzResponseWithRawData(SzHttpMethod httpMethod,
                               int          httpStatusCode,
                               UriInfo      uriInfo,
                               Timers       timers)
  {
    this(httpMethod, httpStatusCode, uriInfo, timers, null);
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
   * @param timers The {@link Timers} object for the timings that were taken.
   *
   * @param rawData The raw data to associate with the response.
   */
  public SzResponseWithRawData(SzHttpMethod httpMethod,
                               int          httpStatusCode,
                               UriInfo      uriInfo,
                               Timers       timers,
                               String       rawData)
  {
    super(httpMethod, httpStatusCode, uriInfo, timers);

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
