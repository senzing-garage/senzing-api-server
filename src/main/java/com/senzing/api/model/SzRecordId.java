package com.senzing.api.model;

import com.senzing.util.JsonUtils;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import java.util.Objects;

/**
 * Describes a record ID with a data source.
 */
public class SzRecordId implements SzEntityIdentifier {
  /**
   * The data source code.
   */
  private String dataSourceCode;

  /**
   * The record ID identifying the record within the data source.
   */
  private String recordId;

  /**
   * Default constructor.
   */
  public SzRecordId() {
    this.dataSourceCode = null;
    this.recordId = null;
  }

  /**
   * Constructs with the specified data source code and record ID.
   *
   * @param dataSourceCode The data source code.
   *
   * @param recordId The record ID identifying the record.
   */
  public SzRecordId(String dataSourceCode, String recordId) {
    this.dataSourceCode = dataSourceCode.toUpperCase().trim();
    this.recordId = recordId.trim();
  }

  /**
   * Gets the data source code for the record.
   *
   * @return The data source code for the record.
   */
  public String getDataSourceCode() {
    return dataSourceCode;
  }

  /**
   * Sets the data source code for the record.
   *
   * @param dataSourceCode The data source code for the record.
   */
  public void setDataSourceCode(String dataSourceCode) {
    this.dataSourceCode = dataSourceCode.toUpperCase().trim();
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
  public void setRecordId(String recordId) {
    this.recordId = recordId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SzRecordId recordId1 = (SzRecordId) o;
    return Objects.equals(getDataSourceCode(), recordId1.getDataSourceCode()) &&
        Objects.equals(getRecordId(), recordId1.getRecordId());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getDataSourceCode(), getRecordId());
  }

  @Override
  public String toString() {
    JsonObjectBuilder builder = Json.createObjectBuilder();
    builder.add("src", this.getDataSourceCode());
    builder.add("id", this.getRecordId());
    return JsonUtils.toJsonText(builder);
  }

  /**
   * Parses the specified JSON text for the record ID.
   *
   * @param text The JSON text to parse.
   *
   * @return The {@link SzRecordId} that was created.
   */
  public static SzRecordId valueOf(String text) {
    JsonObject  jsonObject  = JsonUtils.parseJsonObject(text);
    String      source      = jsonObject.getString("src");
    String      id          = jsonObject.getString("id");
    return new SzRecordId(source, id);
  }
}
