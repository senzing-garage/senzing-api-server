package com.senzing.api.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.senzing.util.JsonUtils;

import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonString;
import java.util.*;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_EMPTY;
import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;

/**
 * Describes a record for a flagged entity.
 */
public class SzFlaggedRecord {
  /**
   * The data source for the record.
   */
  private String dataSource;

  /**
   * The record ID for the record.
   */
  private String recordId;

  /**
   * The {@link Set} of flags for the record.
   */
  private Set<String> flags;

  /**
   * Default constructor.
   */
  public SzFlaggedRecord() {
    this.dataSource = null;
    this.recordId = null;
    this.flags = new LinkedHashSet<>();
  }

  /**
   * Gets the data source for the flagged record.
   *
   * @return The data source for the flagged record.
   */
  @JsonInclude(NON_NULL)
  public String getDataSource() {
    return dataSource;
  }

  /**
   * Sets the data source for the flagged record.
   *
   * @param dataSource The data source for the flagged record.
   */
  public void setDataSource(String dataSource) {
    this.dataSource = dataSource;
  }

  /**
   * Gets the record ID for the flagged record.
   *
   * @return The record ID for the flagged record.
   */
  @JsonInclude(NON_NULL)
  public String getRecordId() {
    return recordId;
  }

  /**
   * Sets the record ID for the flagged record.
   *
   * @param recordId The record ID for the flagged record.
   */
  public void setRecordId(String recordId) {
    this.recordId = recordId;
  }

  /**
   * Returns the {@link Set} of {@link String} flags that were flagged for
   * this record.
   *
   * @return The {@link Set} of {@link String} flags that were flagged for
   * this record, or <tt>null</tt> if none.
   */
  @JsonInclude(NON_EMPTY)
  public Set<String> getFlags() {
    return Collections.unmodifiableSet(this.flags);
  }

  /**
   * Adds the specified {@link String} flag as one flagged for this record.
   *
   * @param flag The flag to add to this instance.
   */
  public void addFlag(String flag) {
    this.flags.add(flag);
  }

  /**
   * Sets the {@link String} flags that were flagged for this record.
   *
   * @param flags The {@link Collection} of {@link String} flags that were
   *              flagged for this record.
   */
  public void setFlags(Collection<String> flags) {
    this.flags.clear();
    if (flags != null) {
      for (String flag : flags) {
        if (flag != null) this.flags.add(flag);
      }
    }
  }

  @Override
  public String toString() {
    return "SzFlaggedRecord{" +
        "dataSource='" + dataSource + '\'' +
        ", recordId='" + recordId + '\'' +
        ", flags=" + flags +
        '}';
  }

  /**
   * Parses a list of flagged records from a {@link JsonArray} describing a
   * JSON array in the Senzing native API format for flagged record info and
   * populates the specified {@link List} or creates a new {@link List}.
   *
   * @param list      The {@link List} of {@link SzFlaggedRecord} instances to
   *                  populate, or <tt>null</tt> if a new {@link List}
   *                  should be created.
   * @param jsonArray The {@link JsonArray} describing the JSON in the
   *                  Senzing native API format.
   * @return The populated (or created) {@link List} of {@link
   * SzFlaggedRecord} instances.
   */
  public static List<SzFlaggedRecord> parseFlaggedRecordList(
      List<SzFlaggedRecord> list,
      JsonArray jsonArray) {
    if (list == null) {
      list = new ArrayList<>(jsonArray == null ? 0 : jsonArray.size());
    }

    if (jsonArray == null) return list;

    for (JsonObject jsonObject : jsonArray.getValuesAs(JsonObject.class)) {
      list.add(parseFlaggedRecord(null, jsonObject));
    }
    return list;
  }

  /**
   * Parses the flagged record from a {@link JsonObject} describing JSON
   * for the Senzing native API format for flagged record info and populates
   * the specified {@link SzFlaggedRecord} or creates a new instance.
   *
   * @param record     The {@link SzFlaggedRecord} instance to populate, or
   *                   <tt>null</tt> if a new instance should be created.
   * @param jsonObject The {@link JsonObject} describing the JSON in the
   *                   Senzing native API format.
   * @return The populated (or created) {@link SzFlaggedRecord}.
   */
  public static SzFlaggedRecord parseFlaggedRecord(
      SzFlaggedRecord record,
      JsonObject jsonObject) {
    if (record == null) record = new SzFlaggedRecord();

    record.setDataSource(JsonUtils.getString(jsonObject, "DATA_SOURCE"));
    record.setRecordId(JsonUtils.getString(jsonObject, "RECORD_ID"));

    JsonArray jsonArray = JsonUtils.getJsonArray(jsonObject, "FLAGS");
    if (jsonArray != null) {
      for (JsonString flag : jsonArray.getValuesAs(JsonString.class)) {
        record.addFlag(flag.getString());
      }
    }
    return record;
  }
}
