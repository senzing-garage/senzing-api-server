package com.senzing.api.model;

import javax.ws.rs.core.UriInfo;

public class SzLinks {
  private String self;

  /**
   * Default constructor.
   */
  public SzLinks() {
    this.self = null;
  }

  /**
   * Constructs with the specified link.
   *
   * @param self The self link.
   */
  public SzLinks(String self) {
    this.self = self;
  }

  /**
   * Constructs with the specified {@link UriInfo}.
   *
   * @param uriInfo The {@link UriInfo} for extracting the self link.
   */
  public SzLinks(UriInfo uriInfo) {
    this(uriInfo.getRequestUri().toString());
  }

  public String getSelf() {
    return self;
  }

  public void setSelf(String self) {
    this.self = self;
  }
}
