package com.senzing.api.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.senzing.api.BuildInfo;
import com.senzing.util.Timers;

import java.util.*;

public class SzMeta {
  /**
   * The HTTP method that was executed.
   */
  private SzHttpMethod httpMethod;

  /**
   * The HTTP response code.
   */
  private int httpStatusCode;

  /**
   * The timings for the operation.
   */
  private Timers timers;

  /**
   * The cached {@link Map} of timings which once initialized is not modified.
   */
  private Map<String,Long> timings;

  /**
   * The timestamp associated with the response.
   */
  @JsonFormat(shape = JsonFormat.Shape.STRING,
              pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'",
              locale = "en_GB")
  private Date timestamp;

  /**
   * Constructs with the specified HTTP method.
   *
   * @param httpMethod The HTTP method with which to construct.
   *
   * @param httpStatusCode The HTTP response code.
   */
  public SzMeta(SzHttpMethod httpMethod, int httpStatusCode, Timers timers) {
    this.httpMethod = httpMethod;
    this.httpStatusCode = httpStatusCode;
    this.timestamp  = new Date();
    this.timers = timers;
    this.timings = null;
  }

  /**
   * The HTTP method for the REST request.
   *
   * @return HTTP method for the REST request.
   */
  public SzHttpMethod getHttpMethod() {
    return this.httpMethod;
  }

  /**
   * The HTTP response status code for the REST request.
   *
   * @return The HTTP response status code for the REST request.
   */
  public int getHttpStatusCode() { return this.httpStatusCode; }

  /**
   * Returns the timestamp that the request was completed.
   *
   * @return The timestamp that the request was completed.
   */
  public Date getTimestamp() {
    return this.timestamp;
  }

  /**
   * Returns the build version of the server implementation.
   *
   * @return The build version of the server implementation.
   */
  public String getVersion() { return BuildInfo.MAVEN_VERSION; }

  /**
   * Returns the timings that were recorded for the operation as an
   * unmodifiable {@link Map} of {@link String} keys to {@link Long}
   * millisecond values.
   *
   * @return The timings that were recorded for the operation.
   */
  public Map<String, Long> getTimings() {
    try {
      if (this.timings != null) {
        return this.timings.size() == 0 ? null : this.timings;
      }
      if (this.timers == null) {
        this.timings = Collections.emptyMap();
      } else {
        this.timings = Collections.unmodifiableMap(this.timers.getTimings());
      }
      return this.timings.size() == 0 ? null : this.timings;

    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
  }
}
