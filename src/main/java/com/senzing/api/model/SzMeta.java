package com.senzing.api.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.senzing.api.BuildInfo;
import com.senzing.util.Timers;

import javax.validation.constraints.NotNull;
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
   * The server version number for the response.
   */
  private String version;

  /**
   * The Senzing REST API version implemented by the server.
   */
  private String restApiVersion;

  /**
   * Default constructor for reconstructing from JSON.
   */
  SzMeta() {
    this.httpMethod     = null;
    this.httpStatusCode = 0;
    this.timestamp      = null;
    this.timers         = null;
    this.timings        = null;
    this.version        = null;
    this.restApiVersion = null;
  }

  /**
   * Constructs with the specified HTTP method.
   *
   * @param httpMethod The HTTP method with which to construct.
   *
   * @param httpStatusCode The HTTP response code.
   */
  public SzMeta(SzHttpMethod httpMethod, int httpStatusCode, Timers timers) {
    this.httpMethod     = httpMethod;
    this.httpStatusCode = httpStatusCode;
    this.timestamp      = new Date();
    this.timers         = timers;
    this.timings        = null;
    this.version        = BuildInfo.MAVEN_VERSION;
    this.restApiVersion = BuildInfo.REST_API_VERSION;
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
  public String getVersion() { return this.version; }

  /**
   * Returns the Senzing REST API version implemented by the server.
   *
   * @return The Senzing REST API version implemented by the server.
   */
  public String getRestApiVersion() { return this.restApiVersion; }

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

  /**
   * If any of the response's timers are still accumulating time, this
   * causes them to cease.  Generally, this is only used in testing since
   * converting the object to JSON to serialize the response will trigger
   * a call to {@link #getTimings()} which will have the effect of concluding
   * all timers.
   *
   * If the timers are already concluded then this method does nothing.
   */
  public void concludeTimers() {
    if (this.timings != null) return;
    this.getTimings();
  }

}
