package com.senzing.api.model;

import javax.json.JsonArray;
import javax.json.JsonObject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * Describes a record summary by data source.
 */
public class SzDataSourceRecordSummary {
  /**
   * The data source for the record summary.
   */
  private String dataSource;

  /**
   * The number of records in the entity from the data source.
   */
  private int recordCount;

  /**
   * The list of record IDs for the entity from the data source.
   */
  private List<String> topRecordIds;

  /**
   * Default constructor.
   */
  public SzDataSourceRecordSummary() {
    this(null, 0);
  }

  /**
   * Constructs with the specified data source and record count.
   *
   * @param dataSource The data source associated with the summary.
   * @param recordCount The number of records from the data source in
   *                    the entity.
   */
  public SzDataSourceRecordSummary(String dataSource, int recordCount) {
    this.dataSource   = dataSource;
    this.recordCount  = recordCount;
    this.topRecordIds = new LinkedList<>();
  }

  /**
   * Returns the associated data source.
   *
   * @return The associated data source.
   */
  public String getDataSource() {
    return this.dataSource;
  }

  /**
   * Sets the associated data source.
   *
   * @param dataSource The data source for the summary.
   */
  public void setDataSource(String dataSource) {
    this.dataSource = dataSource;
  }

  /**
   * Returns the record count for the summary.
   *
   * @return The record count for the summary.
   */
  public int getRecordCount() {
    return this.recordCount;
  }

  /**
   * Sets the record count for the summary.
   *
   * @param recordCount The number of records in the entity from the
   *                    data source.
   */
  public void setRecordCount(int recordCount) {
    this.recordCount = recordCount;
  }

  /**
   * Returns an unmodifiable {@link List} of the top record IDs.
   *
   * @return An unmodifiable {@link List} of the top record IDs.
   */
  public List<String> getTopRecordIds() {
    return Collections.unmodifiableList(this.topRecordIds);
  }

  /**
   * Sets the top record IDs to the specified {@link List} of record IDs.
   *
   * @param topRecordIds The top record IDs for the data source.
   */
  public void setTopRecordIds(List<String> topRecordIds) {
    this.topRecordIds.clear();
    if (topRecordIds != null) {
      this.topRecordIds.addAll(topRecordIds);
    }
  }

  /**
   * Adds a record ID to the {@link List} of top record IDs for the summary.
   *
   * @param recordId The record ID to add to the list of top record IDs.
   */
  public void addTopRecordId(String recordId) {
    this.topRecordIds.add(recordId);
  }

  /**
   * Parses a list of {@link SzDataSourceRecordSummary} instances from native API JSON
   * format and populates the specified {@link List} or creates a new {@link
   * List} if the specified {@link List} is <tt>null</tt>.
   *
   * @param list The {@link List} to populate or <tt>null</tt> if a new list
   *             should be created.
   *
   * @param jsonArray The {@link JsonArray} describing the list of record
   *                  summaries in the Senzing native API JSON format.
   *
   * @return The specified {@link List} that was populated or the new
   *         {@link List} that was created.
   */
  public static List<SzDataSourceRecordSummary> parseRecordSummaryList(
      List<SzDataSourceRecordSummary> list,
      JsonArray             jsonArray)
  {
    if (list == null) {
      list = new ArrayList<>(jsonArray.size());
    }
    for (JsonObject jsonObject : jsonArray.getValuesAs(JsonObject.class)) {
      list.add(parseRecordSummary(null, jsonObject));
    }
    return list;
  }

  /**
   * Parses the native API JSON and creates or populates an
   * {@link SzDataSourceRecordSummary}.
   *
   * @param summary The summary to populate or <tt>null</tt> if a new
   *                instance should be created.
   *
   * @param jsonObject The {@link JsonObject} to parse.
   *
   * @return The {@link SzDataSourceRecordSummary} that was specified or created.
   */
  public static SzDataSourceRecordSummary parseRecordSummary(SzDataSourceRecordSummary summary,
                                                             JsonObject      jsonObject)
  {
    if (summary == null) summary = new SzDataSourceRecordSummary();

    String dataSource  = jsonObject.getString("DATA_SOURCE");
    int    recordCount = jsonObject.getJsonNumber("RECORD_COUNT").intValue();

    summary.setDataSource(dataSource);
    summary.setRecordCount(recordCount);

    return summary;
  }

  @Override
  public String toString() {
    return "SzRecordSummary{" +
        "dataSource='" + dataSource + '\'' +
        ", recordCount=" + recordCount +
        ", topRecordIds=" + topRecordIds +
        '}';
  }
}
