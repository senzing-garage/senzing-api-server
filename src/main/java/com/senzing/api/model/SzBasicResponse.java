package com.senzing.api.model;

import com.fasterxml.jackson.databind.annotation.JsonTypeResolver;

import java.util.Objects;

/**
 * The most basic response from the Senzing REST API.  Also servers as a basis
 * for other responses.
 */
public class SzBasicResponse {
  /**
   * The meta section for this response.
   */
  private SzMeta meta;

  /**
   * The links associated with this response.
   */
  private SzLinks links;

  /**
   * Default constructor.
   */
  protected SzBasicResponse() {
    this.meta = null;
    this.links = null;
  }

  /**
   * Constructs with the specified HTTP method and self link.
   *
   * @param meta The response meta data.
   *
   * @param links The links for the response.
   */
  public SzBasicResponse(SzMeta meta, SzLinks links) {
    Objects.requireNonNull(meta, "The meta data cannot be null");
    Objects.requireNonNull(links, "The links cannot be null");
    this.meta   = meta;
    this.links  = links;
  }

  /**
   * Returns the meta data associated with this response.
   *
   * @return The meta data associated with this response.
   */
  public SzMeta getMeta() {
    return meta;
  }

  /**
   * Gets the links associated with this response.
   *
   * @return The links associated with this response.
   */
  public SzLinks getLinks() {
    return links;
  }

  /**
   * If any of the response's timers are still accumulating time, this
   * causes them to cease.  Generally, this is only used in testing since
   * converting the object to JSON to serialize the response will have the
   * effect of concluding all timers.
   *
   * If timers are already concluded then this method does nothing.
   */
  public void concludeTimers() {
    this.getMeta().concludeTimers();
  }
}
