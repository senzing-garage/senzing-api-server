package com.senzing.api.model;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import static com.senzing.api.model.SzBulkLoadResult.*;

/**
 * Describes an analysis of bulk data records associated with a specific
 * data source (or no data source at all).
 */
public class SzDataSourceBulkLoadResult {
  /**
   * The associated data source or <tt>null</tt>.
   */
  private String dataSource;

  /**
   * The number of records with the associated data source.
   */
  private int recordCount;

  /**
   * The number of records with the associated data source that were
   * successfully loaded.
   */
  private int loadedRecordCount;

  /**
   * The number of records with the associated data source that failed to load.
   */
  private int failedRecordCount;

  /**
   * The tracker for instances of {@link SzBulkLoadError}.
   */
  private SzBulkLoadErrorTracker errorTracker;

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
    this.dataSource         = dataSource;
    this.recordCount        = 0;
    this.loadedRecordCount  = 0;
    this.failedRecordCount  = 0;
    this.errorTracker       = new SzBulkLoadErrorTracker();
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
   * @return The number of records associated with the specified data source.
   */
  long incrementRecordCount() {
    return ++this.recordCount;
  }

  /**
   * Gets the number of records with the associated data source that were
   * successfully loaded.
   *
   * @return The number of records with the associated data source that were
   *         successfully loaded.
   */
  public int getLoadedRecordCount() {
    return this.loadedRecordCount;
  }

  /**
   * Sets the number of records with the associated data source that were
   * successfully loaded.
   *
   * @param recordCount The number of records with the associated data source
   *                    that were successfully loaded.
   */
  void setLoadedRecordCount(int recordCount) {
    this.loadedRecordCount = recordCount;
  }

  /**
   * Increments the number of records with the associated data source that
   * were successfully loaded and returns the new count.
   *
   * @return The number of records with the associated data source that were
   *         successfully loaded.
   */
  long incrementLoadedRecordCount() {
    return ++this.loadedRecordCount;
  }

  /**
   * Gets the number of records with the associated data source that failed to
   * load.
   *
   * @return The number of records with the associated data source that failed
   *         to load.
   */
  public int getFailedRecordCount() {
    return this.failedRecordCount;
  }

  /**
   * Sets the number of records with the associated data source that failed
   * to load.
   *
   * @param recordCount The number of records with the associated data source
   *                    that failed to load.
   */
  void setFailedRecordCount(int recordCount) {
    this.failedRecordCount = recordCount;
  }

  /**
   * Increments the number of records with the assocaited data source that
   * failed to load.
   *
   * @param error The {@link SzError} describing the failure.
   *
   * @return The number of records with the associated data source that failed
   *         to load.
   */
  long incrementFailedRecordCount(SzError error) {
    this.errorTracker.trackError(error);
    return ++this.failedRecordCount;
  }

  /**
   * Gets the unmodifiable {@link List} of {@link SzBulkLoadError} instances
   * describing the top errors.
   *
   * @return The {@link List} of {@link SzBulkLoadError} instances describing
   * the top errors.
   */
  public List<SzBulkLoadError> getTopErrors() {
    return this.errorTracker.getTopErrors();
  }

  /**
   * Sets the {@link List} of {@link SzBulkLoadError} instances describing the
   * top errors.
   *
   * @param errors The list of top errors.
   */
  void setTopErrors(Collection<SzBulkLoadError> errors) {
    this.errorTracker.setTopErrors(errors);
  }

  @Override
  public String toString() {
    return "SzDataSourceBulkLoadResult{" +
        "dataSource='" + this.getDataSource() + '\'' +
        ", recordCount=" + this.getRecordCount() +
        ", loadedRecordCount=" + this.getLoadedRecordCount() +
        ", failedRecordCount=" + this.getFailedRecordCount() +
        ", topErrors=[ " + this.getTopErrors() +
        " ]}";
  }
}

