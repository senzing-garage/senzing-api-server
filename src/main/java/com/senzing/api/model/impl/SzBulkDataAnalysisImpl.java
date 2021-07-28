package com.senzing.api.model.impl;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.senzing.api.model.*;

import java.util.*;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_EMPTY;
import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;
import static com.senzing.api.model.SzBulkDataStatus.IN_PROGRESS;
import static com.senzing.api.model.SzBulkDataStatus.NOT_STARTED;

/**
 * Provides a default implementation of {@link SzBulkDataAnalysis}.
 */
@JsonDeserialize
public class SzBulkDataAnalysisImpl implements SzBulkDataAnalysis {
  /**
   * The status of the analysis.
   */
  private SzBulkDataStatus status;

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
   * The number of records having an entity type.
   */
  private int entityTypeCount;

  /**
   * Internal {@link Map} for tracking the analysis by data source.
   */
  private Map<String, SzDataSourceRecordAnalysis> analysisByDataSource;

  /**
   * Internal {@link Map} for tracking the analysis by entity type.
   */
  private Map<String, SzEntityTypeRecordAnalysis> analysisByEntityType;

  /**
   * Default constructor.
   */
  public SzBulkDataAnalysisImpl() {
    this.recordCount      = 0;
    this.recordIdCount    = 0;
    this.dataSourceCount  = 0;
    this.entityTypeCount  = 0;
    this.analysisByDataSource = new HashMap<>();
    this.analysisByEntityType = new HashMap<>();
    this.status = NOT_STARTED;
  }

  /**
   * Gets the {@linkplain SzBulkDataStatus status} of the bulk data analysis.
   *
   * @return The status of the bulk data analysis.
   */
  @Override
  public SzBulkDataStatus getStatus() {
    return status;
  }

  /**
   * Sets the {@linkplain SzBulkDataStatus status} of the bulk data analysis.
   *
   * @param status The status of the bulk data analysis.
   */
  @Override
  public void setStatus(SzBulkDataStatus status) {
    this.status = status;
  }

  /**
   * Gets the character encoding with which the records were processed.
   *
   * @return The character encoding with which the records were processed.
   */
  @Override
  public String getCharacterEncoding() {
    return this.characterEncoding;
  }

  /**
   * Sets the character encoding with which the bulk data was processed.
   *
   * @param encoding The character encoding used to process the bulk data.
   */
  @Override
  public void setCharacterEncoding(String encoding) {
    this.characterEncoding = encoding;
  }

  /**
   * Gets the media type of the bulk record data.
   *
   * @return The media type of the bulk record data.
   */
  @JsonInclude(NON_NULL)
  @Override
  public String getMediaType() {
    return this.mediaType;
  }

  /**
   * Sets the media type of the bulk record data.
   *
   * @param mediaType The media type of the bulk record data.
   */
  @Override
  public void setMediaType(String mediaType) {
    this.mediaType = mediaType;
  }

  /**
   * Return the number of records in the bulk data set.
   *
   * @return The number of records in the bulk data set.
   */
  @Override
  public int getRecordCount() {
    return recordCount;
  }

  /**
   * Sets the number of records in the bulk data set.
   *
   * @param recordCount The number of records in the bulk data set.
   */
  @Override
  public void setRecordCount(int recordCount) {
    this.recordCount = recordCount;
  }

  /**
   * Increments the number of records in the bulk data set and returns
   * the new record count.
   *
   * @return The incremented record count.
   */
  @Override
  public int incrementRecordCount() {
    return ++this.recordCount;
  }

  /**
   * Increments the number of records in the bulk data set and returns
   * the new record count.
   *
   * @param increment The number of records to increment by.
   *
   * @return The incremented record count.
   */
  @Override
  public int incrementRecordCount(int increment) {
    this.recordCount += increment;
    return this.recordCount;
  }

  /**
   * Gets the number of records in the bulk data set that have a
   * <tt>"RECORD_ID"</tt> property.
   *
   * @return The number of records in the bulk data set that have a
   *         <tt>"RECORD_ID"</tt> property.
   */
  @Override
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
  @Override
  public void setRecordsWithRecordIdCount(int recordIdCount) {
    this.recordIdCount = recordIdCount;
  }

  /**
   * Increments the number of records in the bulk data set that have a
   * <tt>"RECORD_ID"</tt> property and returns the new count.
   *
   * @return The newly incremented count of records in the bulk data set that
   *         have a <tt>"RECORD_ID"</tt> property.
   */
  @Override
  public int incrementRecordsWithRecordIdCount() {
    return ++this.recordIdCount;
  }

  /**
   * Increments the number of records in the bulk data set that have a
   * <tt>"RECORD_ID"</tt> property and returns the new count.
   *
   * @param increment The number of records to increment by.
   *
   * @return The newly incremented count of records in the bulk data set that
   *         have a <tt>"RECORD_ID"</tt> property.
   */
  @Override
  public int incrementRecordsWithRecordIdCount(int increment) {
    this.recordIdCount += increment;
    return this.recordIdCount;
  }

  /**
   * Gets the number of records in the bulk data set that have a
   * <tt>"DATA_SOURCE"</tt> property.
   *
   * @return The number of records in the bulk data set that have a
   *         <tt>"DATA_SOURCE"</tt> property.
   */
  @Override
  public int getRecordsWithDataSourceCount() {
    return dataSourceCount;
  }

  /**
   * Sets the number of records in the bulk data set that have a
   * <tt>"DATA_SOURCE"</tt> property.
   *
   * @param dataSourceCount The number of records in the bulk data set that
   *                        have a <tt>"DATA_SOURCE"</tt> property.
   */
  @Override
  public void setRecordsWithDataSourceCount(int dataSourceCount) {
    this.dataSourceCount = dataSourceCount;
  }

  /**
   * Increments the number of records in the bulk data set that have a
   * <tt>"DATA_SOURCE"</tt> property and returns the new count.
   *
   * @return The newly incremented count of records in the bulk data set that
   *         have a <tt>"DATA_SOURCE"</tt> property.
   */
  @Override
  public int incrementRecordsWithDataSourceCount() {
    return ++this.dataSourceCount;
  }

  /**
   * Increments the number of records in the bulk data set that have a
   * <tt>"DATA_SOURCE"</tt> property and returns the new count.
   *
   * @param increment The number of records to increment by.
   * @return The newly incremented count of records in the bulk data set that
   *         have a <tt>"DATA_SOURCE"</tt> property.
   */
  @Override
  public int incrementRecordsWithDataSourceCount(int increment) {
    this.dataSourceCount += increment;
    return this.dataSourceCount;
  }

  /**
   * Gets the number of records in the bulk data set that have a
   * <tt>"ENTITY_TYPE"</tt> property.
   *
   * @return The number of records in the bulk data set that have a
   *         <tt>"ENTITY_TYPE"</tt> property.
   */
  @Override
  public int getRecordsWithEntityTypeCount() {
    return this.entityTypeCount;
  }

  /**
   * Sets the number of records in the bulk data set that have a
   * <tt>"ENTITY_TYPE"</tt> property.
   *
   * @param entityTypeCount The number of records in the bulk data set that
   *                        have a <tt>"ENTITY_TYPE"</tt> property.
   */
  @Override
  public void setRecordsWithEntityTypeCount(int entityTypeCount) {
    this.entityTypeCount = entityTypeCount;
  }

  /**
   * Increments the number of records in the bulk data set that have a
   * <tt>"ENTITY_TYPE"</tt> property and returns the new count.
   *
   * @return The newly incremented count of records in the bulk data set that
   *         have a <tt>"ENTITY_TYPE"</tt> property.
   */
  @Override
  public int incrementRecordsWithEntityTypeCount() {
    return ++this.entityTypeCount;
  }

  /**
   * Increments the number of records in the bulk data set that have a
   * <tt>"ENTITY_TYPE"</tt> property and returns the new count.
   *
   * @param increment The number of records to increment by.
   *
   * @return The newly incremented count of records in the bulk data set that
   *         have a <tt>"ENTITY_TYPE"</tt> property.
   */
  @Override
  public int incrementRecordsWithEntityTypeCount(int increment) {
    this.entityTypeCount += increment;
    return this.entityTypeCount;
  }

  /**
   * Gets the list of {@link SzDataSourceRecordAnalysis} instances for the
   * bulk data describing the statistics by data source (including those with
   * no data source).
   *
   * @return A {@link List} of {@link SzDataSourceRecordAnalysis} instances
   *         describing the statistics for the bulk data.
   */
  @JsonInclude(NON_EMPTY)
  @Override
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
  @Override
  public void setAnalysisByDataSource(
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
   * Gets the list of {@link SzEntityTypeRecordAnalysis} instances for the
   * bulk data describing the statistics by entity type (including those with
   * no entity type).
   *
   * @return A {@link List} of {@link SzEntityTypeRecordAnalysis} instances
   *         describing the statistics for the bulk data.
   */
  @JsonInclude(NON_EMPTY)
  @Override
  public List<SzEntityTypeRecordAnalysis> getAnalysisByEntityType() {
    List<SzEntityTypeRecordAnalysis> list
        = new ArrayList<>(this.analysisByEntityType.values());
    list.sort((a1, a2) -> {
      int diff = a1.getRecordCount() - a2.getRecordCount();
      if (diff != 0) return diff;
      diff = a1.getRecordsWithRecordIdCount() - a2.getRecordsWithRecordIdCount();
      if (diff != 0) return diff;
      String et1 = a1.getEntityType();
      String et2 = a2.getEntityType();
      if (et1 == null) return -1;
      if (et2 == null) return 1;
      return et1.compareTo(et2);
    });
    return list;
  }

  /**
   * Set the analysis by entity type for this instance.  This will reset the
   * top-level counts according to what is discovered in the specified
   * collection of {@link SzEntityTypeRecordAnalysis} instances.
   *
   * @param analysisList The {@link Collection} of
   *                     {@link SzEntityTypeRecordAnalysis} instances.
   */
  @Override
  public void setAnalysisByEntityType(
      Collection<SzEntityTypeRecordAnalysis> analysisList)
  {
    // count the records
    int recordCount = analysisList.stream()
        .mapToInt(SzEntityTypeRecordAnalysis::getRecordCount).sum();
    this.setRecordCount(recordCount);

    // count the records having a data source specified
    int entityTypeCount = analysisList.stream()
        .filter(a -> a.getEntityType() != null)
        .mapToInt(SzEntityTypeRecordAnalysis::getRecordCount)
        .sum();
    this.setRecordsWithEntityTypeCount(entityTypeCount);

    // count the records having a record ID specified
    int recordIdCount = analysisList.stream()
        .mapToInt(SzEntityTypeRecordAnalysis::getRecordsWithRecordIdCount)
        .sum();
    this.setRecordsWithRecordIdCount(recordIdCount);

    // clear the current analysis map and repopulate it
    this.analysisByEntityType.clear();
    for (SzEntityTypeRecordAnalysis analysis : analysisList) {
      this.analysisByEntityType.put(analysis.getEntityType(), analysis);
    }
  }

  /**
   * Utility method for tracking a record that has been analyzed with the
   * specified data source, entity type and record ID (any of which may be
   * <tt>null</tt> to indicate if they are absent in the record).
   *
   * @param dataSource The data source for the record, or <tt>null</tt> if it
   *                   does not have a <tt>"DATA_SOURCE"</tt> property.
   * @param entityType The entity type for the record, or <tt>null</tt> if it
   *                   does not have a <tt>"ENTITY_TYPE"</tt> property.
   * @param recordId The record ID for the record, or <tt>null</tt> if it does
   *                 not have a <tt>"RECORD_ID"</tt> property.
   */
  @Override
  public void trackRecord(String dataSource, String entityType, String recordId)
  {
    this.trackRecords(1, dataSource, entityType, (recordId != null));
  }

  /**
   * Utility method for tracking a record that has been analyzed with the
   * specified data source, entity type and record ID (any of which may be
   * <tt>null</tt> to indicate if they are absent in the record).
   *
   * @param recordCount The number of records being tracked.
   * @param dataSource The data source for the record, or <tt>null</tt> if it
   *                   does not have a <tt>"DATA_SOURCE"</tt> property.
   * @param entityType The entity type for the record, or <tt>null</tt> if it
   *                   does not have a <tt>"ENTITY_TYPE"</tt> property.
   * @param withRecordId <tt>true</tt> if the records being tracked have record
   *                     ID's, and <tt>false</tt> if they do not.
   */
  @Override
  public void trackRecords(int      recordCount,
                           String   dataSource,
                           String   entityType,
                           boolean  withRecordId)
  {
    if (recordCount < 0) {
      throw new IllegalArgumentException(
          "The record count cannot be negative: " + recordCount);
    }
    if (recordCount == 0) return;

    // check if entity type is empty string and if so, normalize it to null
    if (entityType != null && entityType.trim().length() == 0) {
      entityType = null;
    }

    // check if data source is empty string and if so, normalize it to null
    if (dataSource != null && dataSource.trim().length() == 0) {
      dataSource = null;
    }

    // get the analysis for that entity type
    SzEntityTypeRecordAnalysis etypeAnalysis
        = this.analysisByEntityType.get(entityType);

    // check if it does not yet exist
    if (etypeAnalysis == null) {
      // if not, create it and store it for later
      etypeAnalysis = SzEntityTypeRecordAnalysis.FACTORY.create(entityType);
      this.analysisByEntityType.put(entityType, etypeAnalysis);
    }

    // get the analysis for that data source
    SzDataSourceRecordAnalysis dsrcAnalysis
        = this.analysisByDataSource.get(dataSource);

    // check if it does not yet exist
    if (dsrcAnalysis == null) {
      // if not, create it and store it for later
      dsrcAnalysis = SzDataSourceRecordAnalysis.FACTORY.create(dataSource);
      this.analysisByDataSource.put(dataSource, dsrcAnalysis);
    }

    // increment the global count, data-source count and entity type count
    dsrcAnalysis.incrementRecordCount(recordCount);
    etypeAnalysis.incrementRecordCount(recordCount);
    this.incrementRecordCount(recordCount);

    if (withRecordId) {
      dsrcAnalysis.incrementRecordsWithRecordIdCount(recordCount);
      etypeAnalysis.incrementRecordsWithRecordIdCount(recordCount);
      this.incrementRecordsWithRecordIdCount(recordCount);
    }

    if (dataSource != null) {
      this.incrementRecordsWithDataSourceCount(recordCount);
      etypeAnalysis.incrementRecordsWithDataSourceCount(recordCount);
    }
    if (entityType != null) {
      this.incrementRecordsWithEntityTypeCount(recordCount);
      dsrcAnalysis.incrementRecordsWithEntityTypeCount(recordCount);
    }
    if (this.status == NOT_STARTED) this.status = IN_PROGRESS;
  }

}
