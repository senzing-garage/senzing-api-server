package com.senzing.api.model.impl;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.senzing.api.model.*;

/**
 * Provides a default implementation of {@link SzEntityClassResponse}.
 */
@JsonDeserialize
public class SzEntityClassResponseImpl extends SzResponseWithRawDataImpl
  implements SzEntityClassResponse
{
  /**
   * The data for this instance.
   */
  private SzEntityClassResponseData data = null;

  /**
   * Package-private default constructor.
   */
  protected SzEntityClassResponseImpl() {
    this.data = null;
  }

  /**
   * Constructs with only the HTTP method and the self link, leaving the
   * entity classes to be added later.
   *
   * @param meta The response meta data.
   *
   * @param links The links for the response.
   *
   */
  public SzEntityClassResponseImpl(SzMeta meta, SzLinks links) {
    this(meta, links, SzEntityClassResponseData.FACTORY.create());
  }

  /**
   * Constructs with only the HTTP method and the self link, leaving the
   * entity classes to be added later.
   *
   * @param meta The response meta data.
   *
   * @param links The links for the response.
   *
   */
  public SzEntityClassResponseImpl(SzMeta                     meta,
                                   SzLinks                    links,
                                   SzEntityClassResponseData  data)
  {
    super(meta, links);
    this.data = data;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public SzEntityClassResponseData getData() {
    return this.data;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setData(SzEntityClassResponseData data) {
    this.data = data;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setEntityClass(SzEntityClass entityClass) {
    this.data.setEntityClass(entityClass);
  }
}
