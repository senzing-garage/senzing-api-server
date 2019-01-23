package com.senzing.api.model;

import com.fasterxml.jackson.annotation.JsonFormat;

import java.util.Date;

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
  public SzMeta(SzHttpMethod httpMethod, int httpStatusCode) {
    this.httpMethod = httpMethod;
    this.httpStatusCode = httpStatusCode;
    this.timestamp  = new Date();
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
}
