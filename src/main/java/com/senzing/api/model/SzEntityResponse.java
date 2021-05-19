package com.senzing.api.model;

import com.senzing.util.Timers;

import javax.ws.rs.core.UriInfo;

/**
 * A response object that contains entity data.
 *
 */
public class SzEntityResponse extends SzResponseWithRawData {
  /**
   * The {@link SzEntityData} describing the entity.
   */
  private SzEntityData entityData;

  /**
   * Package-private default constructor.
   */
  SzEntityResponse() {
    this.entityData = null;
  }

  /**
   * Constructs with only the HTTP method and the self link, leaving the
   * entity data to be initialized later.
   *
   * @param meta The response meta data.
   *
   * @param links The links for the response.
   */
  public SzEntityResponse(SzMeta meta, SzLinks links) {
    this(meta, links, null);
  }

  /**
   * Constructs with the HTTP method, self link and the {@link SzEntityData}
   * describing the record.
   *
   * @param meta The response meta data.
   *
   * @param links The links for the response.
   *
   * @param data The {@link SzEntityRecord} describing the record.
   */
  public SzEntityResponse(SzMeta         meta,
                          SzLinks        links,
                          SzEntityData   data)
  {
    super(meta, links);
    this.entityData = data;
  }

  /**
   * Returns the data associated with this response which is an
   * {@link SzEntityData}.
   *
   * @return The data associated with this response.
   */
  public SzEntityData getData() {
    return this.entityData;
  }

  /**
   * Sets the data associated with this response with an {@link SzEntityData}.
   *
   * @param data The {@link SzEntityData} describing the record.
   */
  public void setData(SzEntityData data) {
    this.entityData = data;
  }
}
