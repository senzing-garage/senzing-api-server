package com.senzing.api.model;

import com.senzing.util.JsonUtils;

import javax.json.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Describes a data source in its entirety.
 */
public class SzDataSource implements SzDataSourceDescriptor {
  /**
   * The data source code.
   */
  private String dataSourceCode;

  /**
   * The data source ID.
   */
  private Integer dataSourceId;

  /**
   * Default constructor.
   */
  SzDataSource() {
    this.dataSourceCode = null;
    this.dataSourceId   = null;
  }

  /**
   * Constructs with the specified data source code and a <tt>null</tt>
   * data source ID.
   *
   * @param dataSourceCode The data source code for the data source.
   */
  public SzDataSource(String dataSourceCode) {
    this(dataSourceCode, null);
  }

  /**
   * Constructs with the specified data source code and data source ID.
   *
   * @param dataSourceCode The data source code for the data source.
   * @param dataSourceId The data source ID for the data source, or
   *                     <tt>null</tt> if the data source ID is not
   *                     specified.
   */
  public SzDataSource(String dataSourceCode, Integer dataSourceId) {
    this.dataSourceCode = dataSourceCode.toUpperCase().trim();
    this.dataSourceId   = dataSourceId;
  }

  /**
   * Gets the data source code for the data source.
   *
   * @return The data source code for the data source.
   */
  public String getDataSourceCode() {
    return this.dataSourceCode;
  }

  /**
   * Sets the data source code for the data source.
   *
   * @param code The data source code for the data source.
   */
  void setDataSourceCode(String code) {
    this.dataSourceCode = code;
    if (this.dataSourceCode != null) {
      this.dataSourceCode = this.dataSourceCode.toUpperCase().trim();
    }
  }

  /**
   * Return the data source ID associated with the data source.
   *
   * @return The data source ID associated with the data source.
   */
  public Integer getDataSourceId() {
    return this.dataSourceId;
  }

  /**
   * Sets the data source ID associated with the data source.
   *
   * @param dataSourceId The data source ID associated with the data source.
   */
  public void setDataSourceId(Integer dataSourceId) {
    this.dataSourceId = dataSourceId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SzDataSource dataSource = (SzDataSource) o;
    return Objects.equals(getDataSourceCode(), dataSource.getDataSourceCode())
        && Objects.equals(this.getDataSourceId(), dataSource.getDataSourceId());
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.getDataSourceCode(), this.getDataSourceId());
  }

  @Override
  public String toString() {
    return this.toJson();
  }

  /**
   * Parses the specified JSON text for the data source.
   *
   * @param text The JSON text to parse.
   * @return The {@link SzDataSource} that was created.
   */
  public static SzDataSource valueOf(String text) {
    try {
      JsonObject jsonObject = JsonUtils.parseJsonObject(text.trim());

      return SzDataSource.parse(jsonObject);

    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid data source: " + text);
    }
  }

  /**
   * Parses the specified {@link JsonObject} as a data source.
   *
   * @param jsonObject The {@link JsonObject} to parse.
   *
   * @return The {@link SzDataSource} that was created.
   */
  public static SzDataSource parse(JsonObject jsonObject) {
    String code = JsonUtils.getString(jsonObject, "dataSourceCode");
    if (code == null) {
      code = JsonUtils.getString(jsonObject, "DSRC_CODE");
    }
    Integer id = JsonUtils.getInteger(jsonObject, "dataSourceId");
    if (id == null) {
      id = JsonUtils.getInteger(jsonObject, "DSRC_ID");
    }
    return new SzDataSource(code, id);
  }

  /**
   * Parses a JSON array of the engine API JSON to create or populate a
   * {@link List} of {@link SzDataSource} instances.
   *
   * @param list The {@link List} to populate or <tt>null</tt> if a new
   *             {@link List} should be created.
   *
   * @param jsonArray The {@link JsonArray} of {@link JsonObject} instances
   *                  to parse from the engine API.
   *
   * @return The specified (or newly created) {@link List} of 
   *         {@link SzDataSource} instances.
   */
  public static List<SzDataSource> parseDataSourceList(
      List<SzDataSource>  list,
      JsonArray           jsonArray)
  {
    if (list == null) {
      list = new ArrayList<>(jsonArray.size());
    }
    for (JsonObject jsonObject : jsonArray.getValuesAs(JsonObject.class)) {
      list.add(parseDataSource(null, jsonObject));
    }
    return list;
  }

  /**
   * Parses the engine API JSON to create an instance of {@link SzDataSource}.
   *
   * @param dataSource The {@link SzDataSource} object to initialize or
   *                   <tt>null</tt> if a new one should be created.
   *
   * @param jsonObject The {@link JsonObject} to parse from the engine API.
   *
   * @return The specified (or newly created) {@link SzDataSource}
   */
  public static SzDataSource parseDataSource(SzDataSource dataSource,
                                             JsonObject   jsonObject)
  {
    if (dataSource == null) dataSource = new SzDataSource();

    String  code  = JsonUtils.getString(jsonObject, "DSRC_CODE");
    Integer id    = JsonUtils.getInteger(jsonObject, "DSRC_ID");

    if (code == null) {
      throw new IllegalArgumentException(
          "Could not find the DSRC_CODE property");
    }
    if (id == null) {
      throw new IllegalArgumentException(
          "Could not find the DSRC_ID property");
    }

    dataSource.setDataSourceCode(code);
    dataSource.setDataSourceId(id);

    return dataSource;
  }

  /**
   * Returns a reference to this instance.
   *
   * @return The a reference to this instance.
   */
  public SzDataSource toDataSource() {
    return this;
  }

  /**
   * Adds the JSON properties to the specified {@link JsonObjectBuilder} to
   * describe this instance in its standard JSON format.
   *
   * @param builder The {@link JsonObjectBuilder} to add the properties.
   */
  public void buildJson(JsonObjectBuilder builder) {
    builder.add("dataSourceCode", this.getDataSourceCode());
    Integer sourceId = this.getDataSourceId();
    if (sourceId != null) {
      builder.add("dataSourceId", sourceId);
    }
  }

  /**
   * Converts this instance to a {@link JsonObject} representation of this
   * object as native JSON.
   *
   * @param builder The {@link JsonObjectBuilder} to add the JSON properties to.
   */
  public void buildNativeJson(JsonObjectBuilder builder) {
    builder.add("DSRC_CODE", this.getDataSourceCode());
    if (this.getDataSourceId() != null) {
      builder.add("DSRC_ID", this.getDataSourceId());
    }
  }
}
