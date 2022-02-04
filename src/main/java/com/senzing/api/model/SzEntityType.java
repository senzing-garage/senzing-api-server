package com.senzing.api.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.senzing.api.model.impl.SzEntityTypeImpl;
import com.senzing.util.JsonUtilities;

import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import java.util.ArrayList;
import java.util.List;

/**
 * Describes a entity type in its entirety.
 */
@JsonDeserialize(using=SzEntityType.Factory.class)
public interface SzEntityType extends SzEntityTypeDescriptor {
  /**
   * Gets the entity type code for the entity type.
   *
   * @return The entity type code for the entity type.
   */
  String getEntityTypeCode();

  /**
   * Sets the entity type code for the entity type.
   *
   * @param code The entity type code for the entity type.
   */
  void setEntityTypeCode(String code);

  /**
   * Return the entity type ID associated with the entity type.
   *
   * @return The entity type ID associated with the entity type.
   */
  Integer getEntityTypeId();

  /**
   * Sets the entity type ID associated with the entity type.
   *
   * @param entityTypeId The entity type ID associated with the entity type.
   */
  void setEntityTypeId(Integer entityTypeId);

  /**
   * Gets the entity class code for the entity class to which this entity type
   * belongs.
   *
   * @return The entity class code for the entity class to which this entity
   *         type belongs.
   */
  String getEntityClassCode();

  /**
   * Sets the entity class code for the entity class to which this entity type
   * belongs.
   *
   * @param code The entity class code for the entity class to which this entity
   *             type belongs.
   */
  void setEntityClassCode(String code);

  /**
   * A {@link ModelProvider} for instances of {@link SzEntityType}.
   */
  interface Provider extends ModelProvider<SzEntityType> {
    /**
     * Default creator method.
     */
    SzEntityType create();

    /**
     * Creates an instance with only the entity type code, leaving the entity
     * class code and entity type ID as <tt>null</tt>.
     *
     * @param entityTypeCode The entity type code for the entity type.
     */
    SzEntityType create(String entityTypeCode);

    /**
     * Creates an instance with the specified entity tfype code and entity
     * type ID.
     *
     * @param entityTypeCode The entity type code for the entity type.
     * @param entityTypeId The entity type ID for the entity type.
     * @param entityClassCode The entity class code for the associated entity
     *                        class.
     */
    SzEntityType create(String    entityTypeCode,
                        Integer   entityTypeId,
                        String    entityClassCode);
  }

  /**
   * Provides a default {@link Provider} implementation for {@link
   * SzEntityType} that produces instances of {@link SzEntityTypeImpl}.
   */
  class DefaultProvider extends AbstractModelProvider<SzEntityType>
      implements Provider
  {
    /**
     * Default constructor.
     */
    public DefaultProvider() {
      super(SzEntityType.class, SzEntityTypeImpl.class);
    }

    @Override
    public SzEntityType create() {
      return new SzEntityTypeImpl();
    }

    @Override
    public SzEntityType create(String entityTypeCode) {
      return new SzEntityTypeImpl(entityTypeCode);
    }

    @Override
    public SzEntityType create(String    entityTypeCode,
                               Integer   entityTypeId,
                               String    entityClassCode) {
      return new SzEntityTypeImpl(
          entityTypeCode, entityTypeId, entityClassCode);
    }
  }

  /**
   * Provides a {@link ModelFactory} implementation for {@link SzEntityType}.
   */
  class Factory extends ModelFactory<SzEntityType, Provider> {
    /**
     * Default constructor.  This is public and can only be called after the
     * singleton master instance is created as it inherits the same state from
     * the master instance.
     */
    public Factory() {
      super(SzEntityType.class);
    }

    /**
     * Constructs with the default provider.  This constructor is private and
     * is used for the master singleton instance.
     * @param defaultProvider The default provider.
     */
    private Factory(Provider defaultProvider) {
      super(defaultProvider);
    }

    /**
     * Default creator method.
     */
    public SzEntityType create() {
      return this.getProvider().create();
    }

    /**
     * Creates an instance with only the entity type code, leaving the entity
     * class code and entity type ID as <tt>null</tt>.
     *
     * @param entityTypeCode The entity type code for the entity type.
     */
    public SzEntityType create(String entityTypeCode) {
      return this.getProvider().create(entityTypeCode);
    }

    /**
     * Creates an instance with the specified entity tfype code and entity
     * type ID.
     *
     * @param entityTypeCode The entity type code for the entity type.
     * @param entityTypeId The entity type ID for the entity type.
     * @param entityClassCode The entity class code for the associated entity
     *                        class.
     */
    public SzEntityType create(String   entityTypeCode,
                               Integer  entityTypeId,
                               String   entityClassCode) {
      return this.getProvider().create(
          entityTypeCode, entityTypeId, entityClassCode);
    }

  }

  /**
   * The {@link Factory} instance for this interface.
   */
  Factory FACTORY = new Factory(new DefaultProvider());

  /**
   * Parses the specified JSON text for the entity class.
   *
   * @param text The JSON text to parse.
   * @return The {@link SzEntityType} that was created.
   */
  static SzEntityType valueOf(String text) {
    try {
      JsonObject jsonObject = JsonUtilities.parseJsonObject(text.trim());

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
  static SzEntityType parse(JsonObject jsonObject) {
    String typeCode = JsonUtilities.getString(jsonObject, "entityTypeCode");
    if (typeCode == null) {
      typeCode = JsonUtilities.getString(jsonObject, "ETYPE_CODE");
    }
    Integer id = JsonUtilities.getInteger(jsonObject, "entityTypeId");
    if (id == null) {
      id = JsonUtilities.getInteger(jsonObject, "ETYPE_ID");
    }
    String classCode = JsonUtilities.getString(jsonObject, "entityClassCode");
    if (classCode == null) {
      classCode = JsonUtilities.getString(jsonObject, "ECLASS_CODE");
    }
    return SzEntityType.FACTORY.create(typeCode, id, classCode);
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
  static List<SzEntityType> parseEntityTypeList(
      List<SzEntityType>  list,
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
  static SzEntityType parseEntityType(SzEntityType entityType,
                                             JsonObject   jsonObject)
  {
    if (entityType == null) entityType = SzEntityType.FACTORY.create();

    String  typeCode  = JsonUtilities.getString(jsonObject, "ETYPE_CODE");
    Integer id        = JsonUtilities.getInteger(jsonObject, "ETYPE_ID");
    String  classCode = JsonUtilities.getString(jsonObject, "ECLASS_CODE");

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
  default SzEntityType toEntityType() {
    return this;
  }

  /**
   * Adds the JSON properties to the specified {@link JsonObjectBuilder} to
   * describe this instance in its standard JSON format.
   *
   * @param builder The {@link JsonObjectBuilder} to add the properties.
   */
  default void buildJson(JsonObjectBuilder builder) {
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
   * Converts this instance to Senzing native JSON representation.
   *
   * @param builder The {@link JsonObjectBuilder} to add the properties to.
   */
  default void buildNativeJson(JsonObjectBuilder builder) {
    builder.add("ETYPE_CODE", this.getEntityTypeCode());
    if (this.getEntityTypeId() != null) {
      builder.add("ETYPE_ID", this.getEntityTypeId());
    }
    if (this.getEntityClassCode() != null) {
      builder.add("ECLASS_CODE", this.getEntityClassCode());
    }
  }
}
