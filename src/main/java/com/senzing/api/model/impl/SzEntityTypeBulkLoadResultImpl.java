package com.senzing.api.model.impl;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.senzing.api.model.SzEntityTypeBulkLoadResult;

/**
 * Provides a default implementation of {@link SzEntityTypeBulkLoadResult}.
 */
@JsonDeserialize
public class SzEntityTypeBulkLoadResultImpl
    extends SzBaseBulkLoadResultImpl
    implements SzEntityTypeBulkLoadResult
{
  /**
   * The associated entity type or <tt>null</tt>.
   */
  private String entityType;

  /**
   * Default constructor that constructs with a <tt>null</tt> entity type.
   */
  public SzEntityTypeBulkLoadResultImpl() {
    this(null);
  }

  /**
   * Constructs with the specified entity type.
   *
   * @param entityType The entity type or <tt>null</tt> if the constructed
   *                   instance is associated with those records that have
   *                   no entity type.
   */
  public SzEntityTypeBulkLoadResultImpl(String entityType) {
    super();
    this.entityType = entityType;
  }

  /**
   * Returns the entity type with which this instance was constructed.
   *
   * @return The entity type with which this instance was constructed.
   */
  @Override
  public String getEntityType() {
    return entityType;
  }

  /**
   * Sets the entity type for this instance.
   *
   * @return The entity type for this instance.
   */
  void setEntityType(String entityType) {
    this.entityType = entityType;
  }

  @Override
  public String toString() {
    return "SzEntityTypeBulkLoadResult{" +
        "entityType='" + this.getEntityType() + '\'' +
        ", recordCount=" + this.getRecordCount() +
        ", loadedRecordCount=" + this.getLoadedRecordCount() +
        ", incompleteRecordCount=" + this.getIncompleteRecordCount() +
        ", failedRecordCount=" + this.getFailedRecordCount() +
        ", topErrors=[ " + this.getTopErrors() +
        " ]}";
  }
}

