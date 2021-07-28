package com.senzing.api.model.impl;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.senzing.api.model.SzEntityType;
import com.senzing.api.model.SzEntityTypeResponseData;

/**
 * Provides a default implementation of {@link SzEntityTypeResponseData}.
 */
@JsonDeserialize
public class SzEntityTypeResponseDataImpl implements SzEntityTypeResponseData
{
  /**
   * The {@link SzEntityType} for this instance.
   */
  private SzEntityType entityType;

  /**
   * Default constructor.
   */
  public SzEntityTypeResponseDataImpl() {
      this.entityType = null;
    }

  /**
   * Constructs with the specified {@link SzEntityType} describing the entity
   * type.
   *
   * @param entityType The {@link SzEntityType} describing the entity type.
   */
  public SzEntityTypeResponseDataImpl(SzEntityType entityType) {
    this.entityType = entityType;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public SzEntityType getEntityType() {
    return this.entityType;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setEntityType(SzEntityType entityType) {
    this.entityType = entityType;
  }
}
