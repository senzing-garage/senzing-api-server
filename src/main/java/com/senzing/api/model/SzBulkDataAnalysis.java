package com.senzing.api.model;

import com.senzing.io.RecordReader;
import com.sun.java.accessibility.util.EventID;

import java.util.*;

/**
 * Describes an analysis of bulk data records that are being prepared for
 * loading.
 */
public class SzBulkDataAnalysis {
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
   * The number of records having a record ID.
   */
  private int recordIdCount;

  /**
   * The number of records having a data source.
   */
  private int dataSourceCount;

  /**
   * Internal {@link Map} for tracking the analysis by data source.
   */
  private Map<String, SzDataSourceRecordAnalysis> analysisByDataSource;

  /**
   * Default constructor.
   */
  public SzBulkDataAnalysis() {
    this.recordCount = 0;
    this.recordIdCount  = 0;
    this.analysisByDataSource = new HashMap<>();
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
   * Gets the number of records in the bulk data set that have a
   * <tt>"RECORD_ID"</tt> property.
   *
   * @return The number of records in the bulk data set that have a
   *         <tt>"RECORD_ID"</tt> property.
   */
  public int getRecordsWithRecordIdCount() {
    return recordIdCount;
  }

  /**
   * Sets the number of records in the bulk data set that have a
   * <tt>"RECORD_ID"</tt> property.
   *
   * @param recordIdCount The number of records in the bulk data set that have
   *                      a <tt>"RECORD_ID"</tt> property.
   */
  private void setRecordsWithRecordIdCount(int recordIdCount) {
    this.recordIdCount = recordIdCount;
  }

  /**
   * Increments the number of records in the bulk data set that have a
   * <tt>"RECORD_ID"</tt> property and returns the new count.
   *
   * @return The newly incremented count of records in the bulk data set that
   *         have a <tt>"RECORD_ID"</tt> property.
   */
  private int incrementRecordsWithRecordIdCount() {
    return ++this.recordIdCount;
  }

  /**
   * Gets the number of records in the bulk data set that have a
   * <tt>"DATA_SOURCE"</tt> property.
   *
   * @return The number of records in the bulk data set that have a
   *         <tt>"DATA_SOURCE"</tt> property.
   */
  public int getRecordsWithDataSourceCount() {
    return dataSourceCount;
  }

  /**
   * Sets the number of records in the bulk data set that have a
   * <tt>"DATA_SOURCE"</tt> property.
   *
   * @param dataSourceCount The number of records in the bulk data set that
   *                        have a <tt>"RECORD_ID"</tt> property.
   */
  private void setRecordsWithDataSourceCount(int dataSourceCount) {
    this.dataSourceCount = dataSourceCount;
  }

  /**
   * Increments the number of records in the bulk data set that have a
   * <tt>"DATA_SOURCE"</tt> property and returns the new count.
   *
   * @return The newly incremented count of records in the bulk data set that
   *         have a <tt>"DATA_SOURCE"</tt> property.
   */
  private int incrementRecordsWithDataSourceCount() {
    return ++this.dataSourceCount;
  }

  /**
   * Gets the list of {@link SzDataSourceRecordAnalysis} instances for the
   * bulk data describing the statistics by data source (including those with
   * no data source).
   *
   * @return A {@link List} of {@link SzDataSourceRecordAnalysis} instances
   *         describing the statistics for the bulk data.
   */
  public List<SzDataSourceRecordAnalysis> getAnalysisByDataSource() {
    List<SzDataSourceRecordAnalysis> list
        = new ArrayList<>(this.analysisByDataSource.values());
    list.sort((a1, a2) -> {
      int diff = a1.getRecordCount() - a2.getRecordCount();
      if (diff != 0) return diff;
      diff = a1.getRecordsWithRecordIdCount() - a2.getRecordsWithRecordIdCount();
      if (diff != 0) return diff;
      String ds1 = a1.getDataSource();
      String ds2 = a2.getDataSource();
      if (ds1 == null) return -1;
      if (ds2 == null) return 1;
      return ds1.compareTo(ds2);
    });
    return list;
  }

  /**
   * Set the analysis by data source for this instance.  This will reset the
   * top-level counts according to what is discovered in the specified
   * collection of {@link SzDataSourceRecordAnalysis} instances.
   *
   * @param analysisList The {@link Collection} of
   *                     {@link SzDataSourceRecordAnalysis} instances.
   */
  private void setAnalysisByDataSource(
      Collection<SzDataSourceRecordAnalysis> analysisList)
  {
    // count the records
    int recordCount = analysisList.stream()
        .mapToInt(SzDataSourceRecordAnalysis::getRecordCount).sum();
    this.setRecordCount(recordCount);

    // count the records having a data source specified
    int dataSourceCount = analysisList.stream()
        .filter(a -> a.getDataSource() != null)
        .mapToInt(SzDataSourceRecordAnalysis::getRecordCount)
        .sum();
    this.setRecordsWithDataSourceCount(dataSourceCount);

    // count the records having a record ID specified
    int recordIdCount = analysisList.stream()
        .mapToInt(SzDataSourceRecordAnalysis::getRecordsWithRecordIdCount)
        .sum();
    this.setRecordsWithRecordIdCount(recordIdCount);

    // clear the current analysis map and repopulate it
    this.analysisByDataSource.clear();
    for (SzDataSourceRecordAnalysis analysis : analysisList) {
      this.analysisByDataSource.put(analysis.getDataSource(), analysis);
    }
  }

  /**
   * Utility method for tracking a record that has been analyzed with the
   * specified data source and record ID (which may be <tt>null</tt> to indicate
   * if they are absent in the record).
   *
   * @param dataSource The data source for the record, or <tt>null</tt> if it
   *                   does not have a <tt>"DATA_SOURCE"</tt> property.
   * @param recordId The record ID for the record, or <tt>null</tt> if it does
   *                 not have a <tt>"RECORD_ID"</tt> property.
   */
  public void trackRecord(String dataSource, String recordId) {
    // check if data source is empty string and if so, normalize it to null
    if (dataSource != null && dataSource.trim().length() == 0) {
      dataSource = null;
    }

    // get the analysis for that data source
    SzDataSourceRecordAnalysis analysis
        = this.analysisByDataSource.get(dataSource);

    // check if it does not yet exist
    if (analysis == null) {
      // if not, create it and store it for later
      analysis = new SzDataSourceRecordAnalysis(dataSource);
      this.analysisByDataSource.put(dataSource, analysis);
    }

    // increment the global and data-source specified
    analysis.incrementRecordCount();
    this.incrementRecordCount();

    if (recordId != null) {
      analysis.incrementRecordsWithRecordIdCount();
      this.incrementRecordsWithRecordIdCount();
    }

    if (dataSource != null) {
      this.incrementRecordsWithDataSourceCount();
    }
  }
}
