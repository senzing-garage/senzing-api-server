package com.senzing.api.model;

import com.senzing.util.JsonUtils;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Describes a record ID with a data source.
 */
public class SzFocusRecordId {
  /**
   * The data source code.
   */
  private String dataSource;

  /**
   * The record ID identifying the record within the data source.
   */
  private String recordId;

  /**
   * Default constructor.
   */
  private SzFocusRecordId() {
    this.dataSource = null;
    this.recordId = null;
  }

  /**
   * Constructs with the specified data source code and record ID.
   *
   * @param dataSource The data source code.
   * @param recordId The record ID identifying the record.
   */
  public SzFocusRecordId(String dataSource, String recordId) {
    this.dataSource = (dataSource != null)
        ? dataSource.toUpperCase().trim() : null;
    this.recordId = (recordId != null) ? recordId.trim() : null;
  }

  /**
   * Gets the data source code for the record.
   *
   * @return The data source code for the record.
   */
  public String getDataSource() {
    return dataSource;
  }

  /**
   * Sets the data source code for the record.
   *
   * @param dataSource The data source code for the record.
   */
  private void setDataSource(String dataSource) {
    this.dataSource = dataSource.toUpperCase().trim();
  }

  /**
   * Return the record ID identifying the record.
   *
   * @return The record ID identifying the record.
   */
  public String getRecordId() {
    return recordId;
  }

  /**
   * Sets the record ID identifying the record.
   *
   * @param recordId The record ID identifying the record.
   */
  private void setRecordId(String recordId) {
    this.recordId = recordId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SzFocusRecordId recordId1 = (SzFocusRecordId) o;
    return Objects.equals(getDataSource(), recordId1.getDataSource()) &&
        Objects.equals(getRecordId(), recordId1.getRecordId());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getDataSource(), getRecordId());
  }

  @Override
  public String toString() {
    return "SzFocusRecordId{" +
        "dataSource='" + this.getDataSource() + '\'' +
        ", recordId='" + this.getRecordId() + '\'' +
        '}';
  }

  /**
   * Parses the {@link SzFocusRecordId} from a {@link JsonObject} describing
   * JSON for the Senzing native API format for record ID to create a new
   * instance.
   *
   * @param jsonObject The {@link JsonObject} to parse.
   *
   * @return The {@link SzFocusRecordId} that was created.
   */
  public static SzFocusRecordId parseFocusRecordId(JsonObject jsonObject) {
    String src  = JsonUtils.getString(jsonObject, "DATA_SOURCE");
    String id   = JsonUtils.getString(jsonObject, "RECORD_ID");
    return new SzFocusRecordId(src, id);
  }

  /**
   * Parses and populates a {@link List} of {@link SzFocusRecordId} instances
   * from a {@link JsonArray} of {@link JsonObject} instances describing JSON
   * for the Senzing native API format for record ID to create new instances.
   *
   * @param jsonArray The {@link JsonArray} to parse.
   *
   * @return The {@link List} of {@link SzFocusRecordId} instances that were
   *         populated.
   */
  public static List<SzFocusRecordId> parseFocusRecordIdList(
      JsonArray jsonArray)
  {
    return parseRecordIdList(null, jsonArray);
  }

  /**
   * Parses and populates a {@link List} of {@link SzFocusRecordId} instances
   * from a {@link JsonArray} of {@link JsonObject} instances describing JSON
   * for the Senzing native API format for record ID to create new instances.
   *
   * @param list The {@link List} to populate or <tt>null</tt> if a new {@link
   *             List} should be created.
   *
   * @param jsonArray The {@link JsonArray} to parse.
   *
   * @return The {@link List} of {@link SzFocusRecordId} instances that were
   *         populated.
   */
  public static List<SzFocusRecordId> parseRecordIdList(
      List<SzFocusRecordId> list,
      JsonArray             jsonArray)
  {
    if (list == null) {
      list = new ArrayList<>(jsonArray.size());
    }

    for (JsonObject jsonObject : jsonArray.getValuesAs(JsonObject.class)) {
      list.add(parseFocusRecordId(jsonObject));
    }

    return list;
  }
}
