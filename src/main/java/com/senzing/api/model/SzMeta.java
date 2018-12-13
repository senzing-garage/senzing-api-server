package com.senzing.api.model;

import com.fasterxml.jackson.annotation.JsonFormat;

import java.util.Date;

public class SzMeta {
  /**
   * The HTTP method that was executed.
   */
  private SzHttpMethod httpMethod;

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
   */
  public SzMeta(SzHttpMethod httpMethod) {
    this.httpMethod = httpMethod;
    this.timestamp  = new Date();
  }

  /**
   * The HTTP method for the REST request.
   *
   * @return HTTP method for the REST request.
   */
  public SzHttpMethod getHttpMethod() {
    return httpMethod;
  }

  /**
   * Returns the timestamp that the request was completed.
   *
   * @return The timestamp that the request was completed.
   */
  public Date getTimestamp() {
    return timestamp;
  }
}
