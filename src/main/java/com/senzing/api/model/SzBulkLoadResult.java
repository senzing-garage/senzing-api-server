package com.senzing.api.model;

import com.senzing.g2.engine.G2Fallible;
import java.util.*;

import static com.senzing.api.model.SzBulkDataStatus.*;

/**
 * Describes an analysis of bulk data records that are being prepared for
 * loading.
 */
public class SzBulkLoadResult {
  /**
   * The character encoding used to interpret the bulk data file.
   */
  private String characterEncoding;

  /**
   * The media type of the bulk data file.
   */
  private String mediaType;

  /**
   * The number of records discovered.
   */
  private int recordCount;

  /**
   * The number of records that were loaded.
   */
  private int loadedRecordCount;

  /**
   * The number of records for which the attempt to load failed with an error.
   */
  private int failedRecordCount;

  /**
   * The number of records missing a DATA_SOURCE so they could not be loaded.
   */
  private int incompleteRecordCount;

  /**
   * The status of the bulk load.
   */
  private SzBulkDataStatus status;

  /**
   * Internal {@link Map} for tracking the analysis by data source.
   */
  private Map<String, SzDataSourceBulkLoadResult> resultsByDataSource;

  /**
   * The tracker for instances of {@link SzBulkLoadError}.
   */
  private SzBulkLoadErrorTracker errorTracker;

  /**
   * Default constructor.
   */
  public SzBulkLoadResult() {
    this.recordCount = 0;
    this.loadedRecordCount = 0;
    this.failedRecordCount = 0;
    this.incompleteRecordCount = 0;
    this.status = NOT_STARTED;
    this.resultsByDataSource = new HashMap<>();
    this.errorTracker = new SzBulkLoadErrorTracker();
  }

  /**
   * Gets the {@linkplain SzBulkDataStatus status} of the bulk load.
   *
   * @return The status of the bulk load.
   */
  public SzBulkDataStatus getStatus() {
    return status;
  }

  /**
   * Sets the {@linkplain SzBulkDataStatus status} of the bulk load.
   *
   * @param status The status of the bulk load.
   */
  public void setStatus(SzBulkDataStatus status) {
    this.status = status;
  }

  /**
   * Gets the character encoding with which the records were processed.
   *
   * @return The character encoding with which the records were processed.
   */
  public String getCharacterEncoding() {
    return this.characterEncoding;
  }

  /**
   * Sets the character encoding with which the bulk data was processed.
   *
   * @param encoding The character encoding used to process the bulk data.
   */
  public void setCharacterEncoding(String encoding) {
    this.characterEncoding = encoding;
  }

  /**
   * Gets the media type of the bulk record data.
   *
   * @return The media type of the bulk record data.
   */
  public String getMediaType() {
    return this.mediaType;
  }

  /**
   * Sets the media type of the bulk record data.
   *
   * @param mediaType The media type of the bulk record data.
   */
  public void setMediaType(String mediaType) {
    this.mediaType = mediaType;
  }

  /**
   * Return the number of records in the bulk data set.
   *
   * @return The number of records in the bulk data set.
   */
  public int getRecordCount() {
    return recordCount;
  }

  /**
   * Sets the number of records in the bulk data set.
   *
   * @param recordCount The number of records in the bulk data set.
   */
  private void setRecordCount(int recordCount) {
    this.recordCount = recordCount;
  }

  /**
   * Increments the number of records in the bulk data set and returns
   * the new record count.
   *
   * @return The incremented record count.
   */
  private int incrementRecordCount() {
    return ++this.recordCount;
  }

  /**
   * Return the number of records in the bulk data set that were successfully
   * loaded.
   *
   * @return The number of records in the bulk data set that were successfully
   * loaded.
   */
  public int getLoadedRecordCount() {
    return this.loadedRecordCount;
  }

  /**
   * Sets the number of records in the bulk data set that were successfully
   * loaded.
   *
   * @param recordCount The number of records in the bulk data set that were
   *                    successfully loaded.
   */
  private void setLoadedRecordCount(int recordCount) {
    this.loadedRecordCount = recordCount;
  }

  /**
   * Increments the number of records in the bulk data set that were loaded
   * and returns the new loaded record count.
   *
   * @return The incremented loaded record count.
   */
  private int incrementLoadedRecordCount() {
    return ++this.loadedRecordCount;
  }

  /**
   * Return the number of records in the bulk data set that failed to load due
   * to an error.
   *
   * @return The number of records in the bulk data set that failed to load due
   * to an error.
   */
  public int getFailedRecordCount() {
    return this.failedRecordCount;
  }

  /**
   * Sets the number of records in the bulk data set that failed to load due
   * to an error.
   *
   * @param recordCount The number of records in the bulk data set that failed
   *                    to load due to an error.
   */
  private void setFailedRecordCount(int recordCount) {
    this.failedRecordCount = recordCount;
  }

  /**
   * Increments the number of records in the bulk data set that failed to load
   * due to an error and returns the new failed record count.
   *
   * @return The incremented failed record count.
   */
  private int incrementFailedRecordCount() {
    return ++this.failedRecordCount;
  }

  /**
   * Return the number of records that are incomplete because they are missing
   * a <tt>"DATA_SOURCE"</tt> field.
   *
   * @return The number of records that are incomplete because they are missing
   *         a <tt>"DATA_SOURCE"</tt> field.
   */
  public int getIncompleteRecordCount() {
    return this.incompleteRecordCount;
  }

  /**
   * Sets the number of records that are incomplete because they are missing
   * a <tt>"DATA_SOURCE"</tt> field.
   *
   * @param recordCount The number of records that are incomplete because they
   *                    are missing a <tt>"DATA_SOURCE"</tt> field.
   */
  private void setIncompleteRecordCount(int recordCount) {
    this.incompleteRecordCount = recordCount;
  }

  /**
   * Increments the number of records that are incomplete because they are
   * missing a <tt>"DATA_SOURCE"</tt> field.
   *
   * @return The incremented incomplete record count.
   */
  private int incrementIncompleteRecordCount() {
    return ++this.incompleteRecordCount;
  }

  /**
   * Gets the list of {@link SzDataSourceRecordAnalysis} instances for the
   * bulk data describing the statistics by data source (including those with
   * no data source).
   *
   * @return A {@link List} of {@link SzDataSourceRecordAnalysis} instances
   * describing the statistics for the bulk data.
   */
  public List<SzDataSourceBulkLoadResult> getResultsByDataSource() {
    List<SzDataSourceBulkLoadResult> list
        = new ArrayList<>(this.resultsByDataSource.values());
    list.sort((r1, r2) -> {
      int diff = r1.getRecordCount() - r2.getRecordCount();
      if (diff != 0) return diff;
      diff = r1.getLoadedRecordCount() - r2.getLoadedRecordCount();
      if (diff != 0) return diff;
      diff = r1.getFailedRecordCount() - r2.getFailedRecordCount();
      if (diff == 0) {
        String ds1 = r1.getDataSource();
        String ds2 = r2.getDataSource();
        if (ds1 == null) return -1;
        if (ds2 == null) return 1;
        return ds1.compareTo(ds2);
      } else {
        return diff;
      }
    });
    return list;
  }

  /**
   * Set the bulk load results by data source for this instance.  This will
   * reset the top-level counts according to what is discovered in the specified
   * collection of {@link SzDataSourceBulkLoadResult} instances.
   *
   * @param resultList The {@link Collection} of
   *                   {@link SzDataSourceBulkLoadResult} instances.
   */
  private void setResultsByDataSource(
      Collection<SzDataSourceBulkLoadResult> resultList) {
    // count the records
    int recordCount = resultList.stream()
        .mapToInt(SzDataSourceBulkLoadResult::getRecordCount).sum();
    this.setRecordCount(recordCount);

    // count the records that have been loaded
    int loadedCount = resultList.stream()
        .filter(a -> a.getDataSource() != null)
        .mapToInt(SzDataSourceBulkLoadResult::getLoadedRecordCount)
        .sum();
    this.setLoadedRecordCount(loadedCount);

    // count the records that failed to load
    int failedCount = resultList.stream()
        .filter(a -> a.getDataSource() != null)
        .mapToInt(SzDataSourceBulkLoadResult::getFailedRecordCount)
        .sum();
    this.setFailedRecordCount(failedCount);

    // clear the current analysis map and repopulate it
    this.resultsByDataSource.clear();
    for (SzDataSourceBulkLoadResult loadResult : resultList) {
      this.resultsByDataSource.put(loadResult.getDataSource(), loadResult);
    }
  }

  /**
   * Utility method for tracking the successful loading of a record with the
   * specified non-null data source.
   *
   * @param dataSource The non-null data source for the record.
   * @throws NullPointerException If the specified parameter is <tt>null</tt>.
   */
  public void trackLoadedRecord(String dataSource) {
    Objects.requireNonNull(dataSource, "The data source cannot be null");

    // get the analysis for that data source
    SzDataSourceBulkLoadResult result = this.getResult(dataSource);

    // increment the global and data-source specified
    result.incrementRecordCount();
    this.incrementRecordCount();

    result.incrementLoadedRecordCount();
    this.incrementLoadedRecordCount();
    this.status = IN_PROGRESS;
  }

  /**
   * Utility method for tracking a failed attempt to load a record with the
   * specified non-null data source.  The failure is recorded with the specified
   * error code and error message.
   *
   * @param dataSource The data source for the record, or <tt>null</tt> if it
   *                   does not have a <tt>"DATA_SOURCE"</tt> property.
   * @param errorCode  The error code for the failure.
   * @param errorMsg   The error message associated with the failure.
   */
  public void trackFailedRecord(String dataSource,
                                String errorCode,
                                String errorMsg) {
    this.trackFailedRecord(dataSource, new SzError(errorCode, errorMsg));
  }

  /**
   * Utility method for tracking a failed attempt to load a record with the
   * specified non-null data source.  The failure is recorded with the specified
   * error code and error message.
   *
   * @param dataSource The data source for the record, or <tt>null</tt> if it
   *                   does not have a <tt>"DATA_SOURCE"</tt> property.
   * @param g2Fallible The {@link G2Fallible} instance that had the failure.
   */
  public void trackFailedRecord(String dataSource, G2Fallible g2Fallible) {
    this.trackFailedRecord(dataSource, new SzError(g2Fallible));
  }

  /**
   * Utility method for tracking a failed attempt to load a record with the
   * specified non-null data source.  The failure is recorded with the specified
   * error code and error message.
   *
   * @param dataSource The data source for the record, or <tt>null</tt> if it
   *                   does not have a <tt>"DATA_SOURCE"</tt> property.
   * @param error      The {@link SzError} describing the error that occurred.
   */
  public void trackFailedRecord(String dataSource, SzError error) {
    // get the analysis for that data source
    SzDataSourceBulkLoadResult result = this.getResult(dataSource);

    // increment the global and data-source specified
    result.incrementRecordCount();
    this.incrementRecordCount();

    // increment the failed record count
    result.incrementFailedRecordCount(error);
    this.incrementFailedRecordCount();

    // track the error
    this.errorTracker.trackError(error);
    this.status = IN_PROGRESS;
  }

  /**
   * Tracks the occurrence of an incomplete record.
   */
  public void trackIncompleteRecord() {
    this.incrementIncompleteRecordCount();
    this.status = IN_PROGRESS;
  }

  /**
   * Gets the {@link SzDataSourceBulkLoadResult} for the specified data source.
   *
   * @param dataSource The data source.
   * @return The {@link SzDataSourceBulkLoadResult} for the data source.
   */
  private SzDataSourceBulkLoadResult getResult(String dataSource) {
    // check if data source is empty string and if so, normalize it to null
    if (dataSource != null && dataSource.trim().length() == 0) {
      dataSource = null;
    }

    // get the analysis for that data source
    SzDataSourceBulkLoadResult result
        = this.resultsByDataSource.get(dataSource);

    // check if it does not yet exist
    if (result == null) {
      // if not, create it and store it for later
      result = new SzDataSourceBulkLoadResult(dataSource);
      this.resultsByDataSource.put(dataSource, result);
    }

    return result;
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
  public void setTopErrors(Collection<SzBulkLoadError> errors) {
    this.errorTracker.setTopErrors(errors);
  }
}
