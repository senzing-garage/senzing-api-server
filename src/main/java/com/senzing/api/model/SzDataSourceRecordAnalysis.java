package com.senzing.api.model;

/**
 * Describes an analysis of bulk data records associated with a specific
 * data source (or no data source at all).
 */
public class SzDataSourceRecordAnalysis {
  /**
   * The associated data source or <tt>null</tt>.
   */
  private String dataSource;

  /**
   * The number of records with the associated data source.
   */
  private int recordCount;

  /**
   * The number of records with the associated data source that have a
   * <tt>"RECORD_ID"</tt> specified.
   */
  private int recordIdCount;

  /**
   * The number of records with the associated data source that have an
   * <tt>"ENTITY_TYPE"</tt> specified.
   */
  private int entityTypeCount;

  /**
   * Default constructor that constructs with a <tt>null</tt> data source.
   */
  public SzDataSourceRecordAnalysis() {
    this(null);
  }

  /**
   * Constructs with the specified data source.
   *
   * @param dataSource The data source or <tt>null</tt> if the constructed
   *                   instance is associated with those records that have
   *                   no data source.
   */
  public SzDataSourceRecordAnalysis(String dataSource) {
    this.dataSource       = dataSource;
    this.recordCount      = 0;
    this.recordIdCount    = 0;
    this.entityTypeCount  = 0;
  }

  /**
   * Returns the data source with which this instance was constructed.
   *
   * @return The data source with which this instance was constructed.
   */
  public String getDataSource() {
    return dataSource;
  }

  /**
   * Gets the number of records that have the associated data source.
   *
   * @return The number of records that have the associated data source.
   */
  public int getRecordCount() {
    return recordCount;
  }

  /**
   * Sets the number of records that have the associated data source.
   *
   * @param recordCount The number of records that have the associated
   *                    data source.
   */
  void setRecordCount(int recordCount) {
    this.recordCount = recordCount;
  }

  /**
   * Increments the number of records that have the associated data source
   * and returns the new count.
   *
   * @return The new count after incrementing.
   */
  long incrementRecordCount() {
    return ++this.recordCount;
  }

  /**
   * Increments the number of records that have the associated data source
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
   * Gets the number of records that have the associated data source and also
   * have a <tt>"RECORD_ID"</tt>.
   *
   * @return The number of records that have the associated data source and
   *         also have a <tt>"RECORD_ID"</tt>.
   */
  public int getRecordsWithRecordIdCount() {
    return recordIdCount;
  }

  /**
   * Sets the number of records that have the associated data source and also
   * have a <tt>"RECORD_ID"</tt>.
   *
   * @param recordIdCount The number of records that have the associated
   *                      data source and also have a <tt>"RECORD_ID"</tt>.
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
   * Gets the number of records that have the associated data source and also
   * have an <tt>"ENTITY_TYPE"</tt>.
   *
   * @return The number of records that have the associated data source and
   *         also have an <tt>"ENTITY_TYPE"</tt>.
   */
  public int getRecordsWithEntityTypeCount() {
    return this.entityTypeCount;
  }

  /**
   * Sets the number of records that have the associated data source and also
   * have an <tt>"ENTITY_TYPE"</tt>.
   *
   * @param entityTypeCount The number of records that have the associated data
   *                        source and also have an <tt>"ENTITY_TYPE"</tt>.
   */
  void setRecordsWithEntityTypeCount(int entityTypeCount) {
    this.entityTypeCount = entityTypeCount;
  }

  /**
   * Increments the number of records that have the associated data source
   * and also have an <tt>"ENTITY_TYPE"</tt> and returns the new count.
   *
   * @return The new count after incrementing.
   */
  int incrementRecordsWithEntityTypeCount() {
    return ++this.entityTypeCount;
  }

  /**
   * Increments the number of records that have the associated data source
   * and also have an <tt>"ENTITY_TYPE"</tt> and returns the new count.
   *
   * @param increment The number of records to increment by.
   *
   * @return The new count after incrementing.
   */
  int incrementRecordsWithEntityTypeCount(int increment) {
    this.entityTypeCount += increment;
    return this.entityTypeCount;
  }

  @Override
  public String toString() {
    return "SzDataSourceRecordAnalysis{" +
        "dataSource='" + dataSource + '\'' +
        ", recordCount=" + recordCount +
        ", recordIdCount=" + recordIdCount +
        ", entityTypeCount=" + entityTypeCount +
        '}';
  }
}

