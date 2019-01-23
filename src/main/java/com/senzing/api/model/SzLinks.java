package com.senzing.api.model;

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

  public String getSelf() {
    return self;
  }

  public void setSelf(String self) {
    this.self = self;
  }
}
