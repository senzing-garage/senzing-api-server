package com.senzing.api.model.impl;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.senzing.api.model.*;

/**
 * Provides a default implementation of {@link SzEntityTypeResponse}.
 */
@JsonDeserialize
public class SzEntityTypeResponseImpl extends SzResponseWithRawDataImpl
  implements SzEntityTypeResponse
{
  /**
   * The data for this instance.
   */
  private SzEntityTypeResponseData data = null;
  /**
   * Package-private default constructor.
   */
  protected SzEntityTypeResponseImpl() {
    this.data = null;
  }

  /**
   * Constructs with only the HTTP method and the self link, leaving the
   * entity types to be added later.
   *
   * @param meta The response meta data.
   *
   * @param links The links for the response.
   *
   */
  public SzEntityTypeResponseImpl(SzMeta meta, SzLinks links) {
    this(meta, links, SzEntityTypeResponseData.FACTORY.create());
  }

  /**
   * Constructs with only the HTTP method and the self link, leaving the
   * entity types to be added later.
   *
   * @param meta The response meta data.
   *
   * @param links The links for the response.
   *
   * @param data The data for the response.
   */
  public SzEntityTypeResponseImpl(SzMeta                    meta,
                                  SzLinks                   links,
                                  SzEntityTypeResponseData  data)
  {
    super(meta, links);
    this.data = data;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public SzEntityTypeResponseData getData() {
    return this.data;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setData(SzEntityTypeResponseData data) {
    this.data = data;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setEntityType(SzEntityType entityType) {
    this.data.setEntityType(entityType);
  }
}
