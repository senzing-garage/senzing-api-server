package com.senzing.api.model.impl;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.senzing.api.model.SzEntityClass;
import com.senzing.api.model.SzEntityClassResponseData;

/**
 * Provides a default implementation of {@link SzEntityClassResponseData}.
 */
@JsonDeserialize
public class SzEntityClassResponseDataImpl implements SzEntityClassResponseData
{
  /**
   * The {@link SzEntityClass} for this instance.
   */
  private SzEntityClass entityClass;

  /**
   * Default constructor.
   */
  public SzEntityClassResponseDataImpl() {
      this.entityClass = null;
    }

  /**
   * Constructs with the specified {@link SzEntityClass} describing the entity
   * class.
   *
   * @param entityClass The {@link SzEntityClass} describing the entity class.
   */
  public SzEntityClassResponseDataImpl(SzEntityClass entityClass) {
    this.entityClass = entityClass;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public SzEntityClass getEntityClass() {
    return this.entityClass;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setEntityClass(SzEntityClass entityClass) {
    this.entityClass = entityClass;
  }
}
