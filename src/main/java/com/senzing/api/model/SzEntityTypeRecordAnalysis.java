package com.senzing.api.model;

/**
 * Describes an analysis of bulk data records associated with a specific
 * entity type (or no entity type at all).
 */
public class SzEntityTypeRecordAnalysis {
  /**
   * The associated entity type or <tt>null</tt>.
   */
  private String entityType;

  /**
   * The number of records with the associated entity type.
   */
  private int recordCount;

  /**
   * The number of records with the associated entity type that have a
   * <tt>"RECORD_ID"</tt> specified.
   */
  private int recordIdCount;

  /**
   * The number of records with the associated entity type that have a
   * <tt>"DATA_SOURCE"</tt> specified.
   */
  private int dataSourceCount;

  /**
   * Default constructor that constructs with a <tt>null</tt> entity type.
   */
  public SzEntityTypeRecordAnalysis() {
    this(null);
  }

  /**
   * Constructs with the specified entity type.
   *
   * @param entityType The entity type or <tt>null</tt> if the constructed
   *                   instance is associated with those records that have
   *                   no entity type.
   */
  public SzEntityTypeRecordAnalysis(String entityType) {
    this.entityType       = entityType;
    this.recordCount      = 0;
    this.recordIdCount    = 0;
    this.dataSourceCount  = 0;
  }

  /**
   * Returns the entity type with which this instance was constructed.
   *
   * @return The entity type with which this instance was constructed.
   */
  public String getEntityType() {
    return entityType;
  }

  /**
   * Gets the number of records that have the associated entity type.
   *
   * @return The number of records that have the associated entity type.
   */
  public int getRecordCount() {
    return recordCount;
  }

  /**
   * Sets the number of records that have the associated entity type.
   *
   * @param recordCount The number of records that have the associated
   *                    entity type.
   */
  void setRecordCount(int recordCount) {
    this.recordCount = recordCount;
  }

  /**
   * Increments the number of records that have the associated entity type
   * and returns the new count.
   *
   * @return The new count after incrementing.
   */
  long incrementRecordCount() {
    return ++this.recordCount;
  }

  /**
   * Increments the number of records that have the associated entity type
   * and returns the new count.
   *
   * @param increment The number of records to increment by.
   *
   * @return The new count after incrementing.
   */
  long incrementRecordCount(int increment) {
    this.recordCount += increment;
    return this.recordCount;
  }

  /**
   * Gets the number of records that have the associated entity type and also
   * have a <tt>"RECORD_ID"</tt>.
   *
   * @return The number of records that have the associated entity type and
   *         also have a <tt>"RECORD_ID"</tt>.
   */
  public int getRecordsWithRecordIdCount() {
    return recordIdCount;
  }

  /**
   * Sets the number of records that have the associated entity type and also
   * have a <tt>"RECORD_ID"</tt>.
   *
   * @param recordIdCount The number of records that have the associated
   *                      entity type and also have a <tt>"RECORD_ID"</tt>.
   */
  void setRecordsWithRecordIdCount(int recordIdCount) {
    this.recordIdCount = recordIdCount;
  }

  /**
   * Increments the number of records that have the associated data source
   * and also have a <tt>"RECORD_ID"</tt> and returns the new count.
   *
   * @return The new count after incrementing.
   */
  int incrementRecordsWithRecordIdCount() {
    return ++this.recordIdCount;
  }

  /**
   * Increments the number of records that have the associated data source
   * and also have a <tt>"RECORD_ID"</tt> and returns the new count.
   *
   * @param increment The number of records to increment by.
   *
   * @return The new count after incrementing.
   */
  int incrementRecordsWithRecordIdCount(int increment) {
    this.recordIdCount += increment;
    return this.recordIdCount;
  }

  /**
   * Gets the number of records that have the associated entity type and also
   * have a <tt>"DATA_SOURCE"</tt>.
   *
   * @return The number of records that have the associated entity type and
   *         also have a <tt>"DATA_SOURCE"</tt>.
   */
  public int getRecordsWithDataSourceCount() {
    return this.dataSourceCount;
  }

  /**
   * Sets the number of records that have the associated entity type and also
   * have a <tt>"DATA_SOURCE"</tt>.
   *
   * @param dataSourceCount The number of records that have the associated
   *                        entity type and also have a <tt>"DATA_SOURCE"</tt>.
   */
  void setRecordsWithDataSourceCount(int dataSourceCount) {
    this.dataSourceCount = dataSourceCount;
  }

  /**
   * Increments the number of records that have the associated data source
   * and also have a <tt>"DATA_SOURCE"</tt> and returns the new count.
   *
   * @return The new count after incrementing.
   */
  int incrementRecordsWithDataSourceCount() {
    return ++this.dataSourceCount;
  }

  /**
   * Increments the number of records that have the associated data source
   * and also have a <tt>"DATA_SOURCE"</tt> and returns the new count.
   *
   * @param increment The number of records to increment by.
   *
   * @return The new count after incrementing.
   */
  int incrementRecordsWithDataSourceCount(int increment) {
    this.dataSourceCount += increment;
    return this.dataSourceCount;
  }

  @Override
  public String toString() {
    return "SzEntityTypeRecordAnalysis{" +
        "entityType='" + entityType + '\'' +
        ", recordCount=" + recordCount +
        ", recordIdCount=" + recordIdCount +
        ", dataSourceCount=" + dataSourceCount +
        '}';
  }
}

