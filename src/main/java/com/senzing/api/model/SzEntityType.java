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
 * Describes a entity type in its entirety.
 */
public class SzEntityType implements SzEntityTypeDescriptor {
  /**
   * The entity type code.
   */
  private String entityTypeCode;

  /**
   * The entity type ID.
   */
  private Integer entityTypeId;

  /**
   * The entity class code identifying the associated entity class.
   */
  private String entityClassCode;

  /**
   * Default constructor.
   */
  SzEntityType() {
    this.entityTypeCode = null;
    this.entityTypeId   = null;
    this.entityTypeCode = null;
  }

  /**
   * Constructs with only the entity type code, leaving the entity class code
   * and entity type ID as <tt>null</tt>.
   *
   * @param entityTypeCode The entity type code for the entity type.
   */
  public SzEntityType(String entityTypeCode) {
    this(entityTypeCode, null, null);
  }

  /**
   * Constructs with the specified entity tfype code and entity type ID.
   *
   * @param entityTypeCode The entity type code for the entity type.
   * @param entityTypeId The entity type ID for the entity type.
   * @param entityTypeCode The entity class code for the associated entity
   *                        class.
   */
  public SzEntityType(String    entityTypeCode,
                      Integer   entityTypeId,
                      String    entityClassCode)
  {
    this.entityTypeCode   = entityTypeCode.toUpperCase().trim();
    this.entityTypeId     = entityTypeId;
    this.entityClassCode  = entityClassCode;
    if (this.entityClassCode != null) {
      this.entityClassCode = this.entityClassCode.toUpperCase().trim();
    }
  }

  /**
   * Gets the entity type code for the entity type.
   *
   * @return The entity type code for the entity type.
   */
  public String getEntityTypeCode() {
    return this.entityTypeCode;
  }

  /**
   * Sets the entity type code for the entity type.
   *
   * @param code The entity type code for the entity type.
   */
  void setEntityTypeCode(String code) {
    this.entityTypeCode = code;
    if (this.entityTypeCode != null) {
      this.entityTypeCode = this.entityTypeCode.toUpperCase().trim();
    }
  }

  /**
   * Return the entity type ID associated with the entity type.
   *
   * @return The entity type ID associated with the entity type.
   */
  public Integer getEntityTypeId() {
    return this.entityTypeId;
  }

  /**
   * Sets the entity type ID associated with the entity type.
   *
   * @param entityTypeId The entity type ID associated with the entity type.
   */
  public void setEntityTypeId(Integer entityTypeId) {
    this.entityTypeId = entityTypeId;
  }

  /**
   * Gets the entity class code for the entity class to which this entity type
   * belongs.
   *
   * @return The entity class code for the entity class to which this entity
   *         type belongs.
   */
  public String getEntityClassCode() {
    return this.entityClassCode;
  }

  /**
   * Sets the entity class code for the entity class to which this entity type
   * belongs.
   *
   * @param code The entity class code for the entity class to which this entity
   *             type belongs.
   */
  public void setEntityClassCode(String code) {
    this.entityClassCode = code;
    if (this.entityClassCode != null) {
      this.entityClassCode = this.entityClassCode.toUpperCase().trim();
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SzEntityType ec = (SzEntityType) o;
    return Objects.equals(this.getEntityTypeCode(), ec.getEntityTypeCode())
        && Objects.equals(this.getEntityTypeId(), ec.getEntityTypeId())
        && Objects.equals(this.getEntityClassCode(), ec.getEntityClassCode());
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.getEntityTypeCode(),
                        this.getEntityTypeId(),
                        this.getEntityClassCode());
  }

  @Override
  public String toString() {
    return this.toJson();
  }

  /**
   * Adds the JSON properties to the specified {@link JsonObjectBuilder} to
   * describe this instance in its standard JSON format.
   *
   * @param builder The {@link JsonObjectBuilder} to add the properties.
   */
  public void buildJson(JsonObjectBuilder builder) {
    builder.add("entityTypeCode", this.getEntityTypeCode());
    Integer typeId = this.getEntityTypeId();
    if (typeId != null) {
      builder.add("entityTypeId", typeId);
    }
    String classCode = this.getEntityClassCode();
    if (classCode != null) {
      builder.add("entityClassCode", classCode);
    }
  }

  /**
   * Parses the specified JSON text for the entity class.
   *
   * @param text The JSON text to parse.
   * @return The {@link SzEntityType} that was created.
   */
  public static SzEntityType valueOf(String text) {
    try {
      JsonObject jsonObject = JsonUtils.parseJsonObject(text.trim());

      return SzEntityType.parse(jsonObject);

    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid entity type: " + text);
    }
  }

  /**
   * Parses the specified {@link JsonObject} as a entity class.
   *
   * @param jsonObject The {@link JsonObject} to parse.
   *
   * @return The {@link SzEntityType} that was created.
   */
  public static SzEntityType parse(JsonObject jsonObject) {
    String typeCode = JsonUtils.getString(jsonObject, "entityTypeCode");
    if (typeCode == null) {
      typeCode = JsonUtils.getString(jsonObject, "ETYPE_CODE");
    }
    Integer id = JsonUtils.getInteger(jsonObject, "entityTypeId");
    if (id == null) {
      id = JsonUtils.getInteger(jsonObject, "ETYPE_ID");
    }
    String classCode = JsonUtils.getString(jsonObject, "entityClassCode");
    if (classCode == null) {
      classCode = JsonUtils.getString(jsonObject, "ECLASS_CODE");
    }
    return new SzEntityType(typeCode, id, classCode);
  }

  /**
   * Parses a JSON array of the engine API JSON to create or populate a
   * {@link List} of {@link SzEntityType} instances.
   *
   * @param list The {@link List} to populate or <tt>null</tt> if a new
   *             {@link List} should be created.
   *
   * @param jsonArray The {@link JsonArray} of {@link JsonObject} instances
   *                  to parse from the engine API.
   *
   * @return The specified (or newly created) {@link List} of 
   *         {@link SzEntityType} instances.
   */
  public static List<SzEntityType> parseEntityTypeList(
      List<SzEntityType> list,
      JsonArray           jsonArray)
  {
    if (list == null) {
      list = new ArrayList<>(jsonArray.size());
    }
    for (JsonObject jsonObject : jsonArray.getValuesAs(JsonObject.class)) {
      list.add(parseEntityType(null, jsonObject));
    }
    return list;
  }

  /**
   * Parses the engine API JSON to create an instance of {@link SzEntityType}.
   *
   * @param entityType The {@link SzEntityType} object to initialize or
   *                    <tt>null</tt> if a new one should be created.
   *
   * @param jsonObject The {@link JsonObject} to parse from the engine API.
   *
   * @return The specified (or newly created) {@link SzEntityType}
   */
  public static SzEntityType parseEntityType(SzEntityType entityType,
                                             JsonObject   jsonObject)
  {
    if (entityType == null) entityType = new SzEntityType();

    String  typeCode  = JsonUtils.getString(jsonObject, "ETYPE_CODE");
    Integer id        = JsonUtils.getInteger(jsonObject, "ETYPE_ID");
    String  classCode = JsonUtils.getString(jsonObject, "ECLASS_CODE");

    if (typeCode == null) {
      throw new IllegalArgumentException(
          "Could not find the ETYPE_CODE property");
    }
    if (id == null) {
      throw new IllegalArgumentException(
          "Could not find the ETYPE_ID property");
    }
    if (classCode == null) {
      throw new IllegalArgumentException(
          "Could not find the ECLASS_CODE property");
    }

    entityType.setEntityTypeCode(typeCode);
    entityType.setEntityTypeId(id);
    entityType.setEntityClassCode(classCode);

    return entityType;
  }

  /**
   * Implemented to return a reference to this instance.
   *
   * @return A reference to this instance.
   */
  public SzEntityType toEntityType() {
    return this;
  }

  /**
   * Converts this instance to Senzing native JSON representation.
   *
   * @param builder The {@link JsonObjectBuilder} to add the properties to.
   */
  public void buildNativeJson(JsonObjectBuilder builder) {
    builder.add("ETYPE_CODE", this.getEntityTypeCode());
    if (this.getEntityTypeId() != null) {
      builder.add("ETYPE_ID", this.getEntityTypeId());
    }
    if (this.getEntityClassCode() != null) {
      builder.add("ECLASS_CODE", this.getEntityClassCode());
    }
  }
}
