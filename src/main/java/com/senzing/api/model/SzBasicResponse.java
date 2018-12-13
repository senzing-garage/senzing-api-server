package com.senzing.api.model;

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
   * Constructs with the specified HTTP method and self link.
   *
   * @param httpMethod The {@link SzHttpMethod} from the request.
   *
   * @param selfLink The self link from the request.
   */
  public SzBasicResponse(SzHttpMethod httpMethod,
                         String       selfLink)
  {
    this.meta = new SzMeta(httpMethod);
    this.links = new SzLinks(selfLink);
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
}
