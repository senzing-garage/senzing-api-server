package com.senzing.api.model;

import com.senzing.util.Timers;

import javax.ws.rs.core.UriInfo;

/**
 * A response object that contains version data.
 *
 */
public class SzVersionResponse extends SzResponseWithRawData {
  /**
   * The data for this instance.
   */
  private SzVersionInfo versionInfo;

  /**
   * Default constructor.
   */
  SzVersionResponse() {
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
  public SzVersionResponse(SzMeta meta, SzLinks links)
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
   * @param versionInfo The {@link SzVersionInfo} describing the version.
   */
  public SzVersionResponse(SzMeta         meta,
                           SzLinks        links,
                           SzVersionInfo  versionInfo)
  {
    super(meta, links);
    this.versionInfo = versionInfo;
  }

  /**
   * Returns the {@link SzVersionInfo} associated with this response.
   *
   * @return The data associated with this response.
   */
  public SzVersionInfo getData() {
    return this.versionInfo;
  }

  /**
   * Sets the data associated with this response with an {@link SzVersionInfo}.
   *
   * @param info The {@link SzVersionInfo} describing the license.
   */
  public void setData(SzVersionInfo info) {
    this.versionInfo = info;
  }
}
