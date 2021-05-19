package com.senzing.api.model.impl;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.senzing.api.model.SzLinks;
import javax.ws.rs.core.UriInfo;

/**
 * Provides the default {@link SzLinks} implementation.
 */
@JsonDeserialize
public class SzLinksImpl implements SzLinks {
  /**
   * The self link.
   */
  private String self;

  /**
   * Default constructor.
   */
  public SzLinksImpl() {
    this.self = null;
  }

  /**
   * Constructs with the specified link.
   *
   * @param self The self link.
   */
  public SzLinksImpl(String self) {
    this.self = self;
  }

  /**
   * Constructs with the specified {@link UriInfo}.
   *
   * @param uriInfo The {@link UriInfo} for extracting the self link.
   */
  public SzLinksImpl(UriInfo uriInfo) {
    this(uriInfo.getRequestUri().toString());
  }

  /**
   * Gets the self link.
   *
   * @return The self link.
   */
  public String getSelf() {
    return self;
  }

  /**
   * Sets the self link.
   *
   * @param self The self link.
   */
  public void setSelf(String self) {
    this.self = self;
  }
}
