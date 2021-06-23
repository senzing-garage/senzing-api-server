package com.senzing.api.model.impl;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.senzing.api.model.*;

import java.util.*;

/**
 * Provides a default implementation of {@link SzEntityTypesResponse}.
 */
@JsonDeserialize
public class SzEntityTypesResponseImpl extends SzResponseWithRawDataImpl
  implements SzEntityTypesResponse
{
  /**
   * The data for this instance.
   */
  private SzEntityTypesResponseData data = null;

  /**
   * Package-private default constructor.
   */
  protected SzEntityTypesResponseImpl() {
    this.data = null;
  }

  /**
   * Constructs with the specified {@link SzMeta} and {@link SzLinks}.
   *
   * @param meta The response meta data.
   *
   * @param links The links for the response.
   *
   */
  public SzEntityTypesResponseImpl(SzMeta meta, SzLinks links) {
    this(meta, links, SzEntityTypesResponseData.FACTORY.create());
  }

  /**
   * Constructs with the specified {@link SzMeta}, {@link SzLinks} and
   * {@link SzEntityTypesResponseData}.
   *
   * @param meta The response meta data.
   *
   * @param links The links for the response.
   *
   * @param data The data for thie response.
   */
  public SzEntityTypesResponseImpl(SzMeta                     meta,
                                   SzLinks                    links,
                                   SzEntityTypesResponseData  data)
  {
    super(meta, links);
    this.data = data;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public SzEntityTypesResponseData getData() {
    return this.data;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setData(SzEntityTypesResponseData data) {
    this.data = data;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void addEntityType(SzEntityType entityType) {
    this.data.addEntityType(entityType);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setEntityTypes(Collection<SzEntityType> entityTypes)
  {
    this.data.setEntityTypes(entityTypes);
  }
}
