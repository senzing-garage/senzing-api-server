package com.senzing.api.model;

/**
 * Describes an analysis of bulk data records associated with a specific
 * data source (or no data source at all).
 */
public class SzDataSourceBulkLoadResult extends SzBaseBulkLoadResult {
  /**
   * The associated data source or <tt>null</tt>.
   */
  private String dataSource;

  /**
   * Default constructor that constructs with a <tt>null</tt> data source.
   */
  public SzDataSourceBulkLoadResult() {
    this(null);
  }

  /**
   * Constructs with the specified data source.
   *
   * @param dataSource The data source or <tt>null</tt> if the constructed
   *                   instance is associated with those records that have
   *                   no data source.
   */
  public SzDataSourceBulkLoadResult(String dataSource) {
    super();
    this.dataSource = dataSource;
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
   * Sets the data source for this instance.
   *
   * @return The data source for this instance.
   */
  void setDataSource(String dataSource) {
    this.dataSource = dataSource;
  }

  @Override
  public String toString() {
    return "SzDataSourceBulkLoadResult{" +
        "dataSource='" + this.getDataSource() + '\'' +
        ", recordCount=" + this.getRecordCount() +
        ", loadedRecordCount=" + this.getLoadedRecordCount() +
        ", incompleteRecordCount=" + this.getIncompleteRecordCount() +
        ", failedRecordCount=" + this.getFailedRecordCount() +
        ", topErrors=[ " + this.getTopErrors() +
        " ]}";
  }
}

