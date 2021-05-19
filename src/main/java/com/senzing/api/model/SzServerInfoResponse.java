package com.senzing.api.model;

import com.senzing.util.Timers;

import javax.ws.rs.core.UriInfo;

/**
 * A response object that contains server info data.
 *
 */
public class SzServerInfoResponse extends SzBasicResponse {
  /**
   * The data for this instance.
   */
  private SzServerInfo serverInfo;

  /**
   * Default constructor.
   */
  SzServerInfoResponse() {
    // do nothing
  }

  /**
   * Constructs with only the HTTP method and the self link, leaving the
   * license info data to be initialized later.
   *
   * @param meta The response meta data.
   *
   * @param links The links for the response.
   */
  public SzServerInfoResponse(SzMeta meta, SzLinks links)
  {
    this(meta, links, null);
  }

  /**
   * Constructs with the HTTP method, self link and the {@link SzVersionInfo}
   * describing the version.
   *
   * @param meta The response meta data.
   *
   * @param links The links for the response.
   *
   * @param serverInfo The {@link SzServerInfo} describing the version.
   */
  public SzServerInfoResponse(SzMeta        meta,
                              SzLinks       links,
                              SzServerInfo  serverInfo)
  {
    super(meta, links);
    this.serverInfo = serverInfo;
  }

  /**
   * Returns the {@link SzServerInfo} associated with this response.
   *
   * @return The data associated with this response.
   */
  public SzServerInfo getData() {
    return this.serverInfo;
  }

  /**
   * Sets the data associated with this response with an {@link SzServerInfo}.
   *
   * @param info The {@link SzServerInfo} describing the license.
   */
  public void setData(SzServerInfo info) {
    this.serverInfo = info;
  }
}
