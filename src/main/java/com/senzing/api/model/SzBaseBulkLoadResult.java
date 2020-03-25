package com.senzing.api.model;

import java.util.Collection;
import java.util.List;

/**
 * Provides the minimum set of properties for describing the load of a set of
 * bulk data records either in entirety or by some aggregate group (e.g.: by
 * data source or entity type).
 */
public abstract class SzBaseBulkLoadResult {
  /**
   * The total number of records.
   */
  private int recordCount;

  /**
   * The number of records that were successfully loaded.
   */
  private int loadedRecordCount;

  /**
   * The number of records that are incomplete.
   */
  private int incompleteRecordCount;

  /**
   * The number of records that failed to load.
   */
  private int failedRecordCount;

  /**
   * The tracker for instances of {@link SzBulkLoadError}.
   */
  private SzBulkLoadErrorTracker errorTracker;

  /**
   * Default constructor.
   */
  protected SzBaseBulkLoadResult() {
    this.recordCount            = 0;
    this.loadedRecordCount      = 0;
    this.incompleteRecordCount  = 0;
    this.failedRecordCount      = 0;
    this.errorTracker           = new SzBulkLoadErrorTracker();
  }

  /**
   * Gets the total number of records.
   *
   * @return The total number of records.
   */
  public int getRecordCount() {
    return recordCount;
  }

  /**
   * Sets the total number of records.
   *
   * @param recordCount The total number of records.
   */
  protected void setRecordCount(int recordCount) {
    this.recordCount = recordCount;
  }

  /**
   * Increments the total number of records and returns the new count.
   *
   * @return The total number of records after incrementing.
   */
  protected long incrementRecordCount() {
    return ++this.recordCount;
  }

  /**
   * Gets the number of records that were successfully loaded.
   *
   * @return The number of records that were successfully loaded.
   */
  public int getLoadedRecordCount() {
    return this.loadedRecordCount;
  }

  /**
   * Sets the number of records that were successfully loaded.
   *
   * @param recordCount The number of records that were successfully loaded.
   */
  protected void setLoadedRecordCount(int recordCount) {
    this.loadedRecordCount = recordCount;
  }

  /**
   * Increments the number of records that were successfully loaded and
   * returns the new count.
   *
   * @return The number of records that were successfully loaded after
   *         incrementing.
   */
  protected long incrementLoadedRecordCount() {
    return ++this.loadedRecordCount;
  }

  /**
   * Return the number of records associated that are deemed incomplete.
   *
   * @return The number of records that are incomplete.
   */
  public int getIncompleteRecordCount() {
    return this.incompleteRecordCount;
  }

  /**
   * Sets the number of records that are incomplete.
   *
   * @param recordCount The number of records that are incomplete.
   */
  protected void setIncompleteRecordCount(int recordCount) {
    this.incompleteRecordCount = recordCount;
  }

  /**
   * Increments the number of records that are incomplete.
   *
   * @return The incremented incomplete record count.
   */
  protected int incrementIncompleteRecordCount() {
    return ++this.incompleteRecordCount;
  }

  /**
   * Gets the number of records that failed to load.
   *
   * @return The number of records that failed to load.
   */
  public int getFailedRecordCount() {
    return this.failedRecordCount;
  }

  /**
   * Sets the number of records that failed to load.
   *
   * @param recordCount The number of records that failed to load.
   */
  protected void setFailedRecordCount(int recordCount) {
    this.failedRecordCount = recordCount;
  }

  /**
   * Tracks the specified error and increments the number of records failed
   * to load.
   *
   * @param error The {@link SzError} describing the failure.
   *
   * @return The number of records that failed to load after incrementing.
   */
  protected long trackFailedRecord(SzError error) {
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
  protected void setTopErrors(Collection<SzBulkLoadError> errors) {
    this.errorTracker.setTopErrors(errors);
  }

  @Override
  public String toString() {
    return "SzAbstractBulkLoadResult{" +
        "recordCount=" + this.getRecordCount() +
        ", loadedRecordCount=" + this.getLoadedRecordCount() +
        ", incompleteRecordCount=" + this.getIncompleteRecordCount() +
        ", failedRecordCount=" + this.getFailedRecordCount() +
        ", topErrors=[ " + this.getTopErrors() +
        " ]}";
  }
}

