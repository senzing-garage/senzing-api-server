package com.senzing.api.model;

import com.senzing.util.Timers;

import javax.ws.rs.core.UriInfo;

/**
 * A response object that contains entity path data.
 */
public class SzEntityNetworkResponse extends SzResponseWithRawData {
  /**
   * The {@link SzEntityNetworkData} describing the entity.
   */
  private SzEntityNetworkData entityNetworkData;

  /**
   * Package-private default constructor.
   */
  SzEntityNetworkResponse() {
    this.entityNetworkData = null;
  }

  /**
   * Constructs with only the HTTP method and the self link, leaving the
   * entity data to be initialized later.
   *
   * @param meta The response meta data.
   *
   * @param links The links for the response.
   */
  public SzEntityNetworkResponse(SzMeta meta, SzLinks links)
  {
    this(meta, links, null);
  }

  /**
   * Constructs with the HTTP method, self link and the {@link
   * SzEntityPathData} describing the record.
   *
   * @param meta The response meta data.
   *
   * @param links The links for the response.
   *
   * @param data The {@link SzEntityRecord} describing the record.
   */
  public SzEntityNetworkResponse(SzMeta               meta,
                                 SzLinks              links,
                                 SzEntityNetworkData  data)
  {
    super(meta, links);
    this.entityNetworkData = data;
  }

  /**
   * Returns the data associated with this response which is an
   * {@link SzEntityNetworkData}.
   *
   * @return The data associated with this response.
   */
  public SzEntityNetworkData getData() {
    return this.entityNetworkData;
  }

  /**
   * Sets the data associated with this response with an
   * {@link SzEntityNetworkData}.
   *
   * @param data The {@link SzEntityNetworkData} describing the record.
   */
  public void setData(SzEntityNetworkData data) {
    this.entityNetworkData = data;
  }
}
