package com.senzing.api.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.senzing.api.model.impl.SzEntityClassImpl;
import com.senzing.util.JsonUtils;

import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import java.util.ArrayList;
import java.util.List;

/**
 * Describes a entity class in its entirety.
 */
@JsonDeserialize(using=SzEntityClass.Factory.class)
public interface SzEntityClass extends SzEntityClassDescriptor {
  /**
   * Gets the entity class code for the entity class.
   *
   * @return The entity class code for the entity class.
   */
  String getEntityClassCode();

  /**
   * Sets the entity class code for the entity class.
   *
   * @param code The entity class code for the entity class.
   */
  void setEntityClassCode(String code);

  /**
   * Return the entity class ID associated with the entity class.
   *
   * @return The entity class ID associated with the entity class.
   */
  Integer getEntityClassId();

  /**
   * Sets the entity class ID associated with the entity class.
   *
   * @param entityClassId The entity class ID associated with the entity class.
   */
  void setEntityClassId(Integer entityClassId);

  /**
   * Checks if entities having this entity class will resolve against each
   * other.  This returns <tt>null</tt> if the instance is not fully
   * initialized.
   *
   * @return <tt>true</tt> if entities of this entity class will resolve against
   *         each other and <tt>false</tt> if not.  This returns <tt>null</tt>
   *         if not initialized.
   */
  Boolean isResolving();

  /**
   * Sets the flag indicating if entities of this class will resolve against
   * each other.
   *
   * @param resolving <tt>true</tt> if the entities of this class resolve, and
   *                  <tt>false</tt> if they do not.
   */
  void setResolving(Boolean resolving);

  /**
   * A {@link ModelProvider} for instances of {@link SzEntityClass}.
   */
  interface Provider extends ModelProvider<SzEntityClass> {
    /**
     * Constructs an uninitialized instance.
     */
    SzEntityClass create();

    /**
     * Creates an instance with the specified entity class code leaving the
     * entity class ID and resolving flag <tt>null</tt>.
     *
     * @param entityClassCode The entity class code for the entity class.
     */
    SzEntityClass create(String entityClassCode);

    /**
     * Creates an instance with the specified entity class code, entity class ID
     * and flag indicating whether or not entities having entity types belonging
     * to this entity class will resolve against each other.
     *
     * @param entityClassCode The entity class code for the entity class.
     * @param entityClassId The entity class ID for the entity class.
     * @param resolving <tt>true</tt> if entities having entity types belonging
     *                  to this entity class will resolve against each other,
     *                  and <tt>false</tt> if they will not resolve.
     */
    SzEntityClass create(String   entityClassCode,
                         Integer  entityClassId,
                         Boolean  resolving);
  }

  /**
   * Provides a default {@link Provider} implementation for {@link
   * SzEntityClass} that produces instances of {@link SzEntityClassImpl}.
   */
  class DefaultProvider extends AbstractModelProvider<SzEntityClass>
      implements Provider
  {
    /**
     * Default constructor.
     */
    public DefaultProvider() {
      super(SzEntityClass.class, SzEntityClassImpl.class);
    }

    @Override
    public SzEntityClass create() {
      return new SzEntityClassImpl();
    }

    @Override
    public SzEntityClass create(String entityClassCode) {
      return new SzEntityClassImpl(entityClassCode);
    }

    @Override
    public SzEntityClass create(String  entityClassCode,
                                Integer entityClassId,
                                Boolean resolving) {
      return new SzEntityClassImpl(entityClassCode, entityClassId, resolving);
    }
  }

  /**
   * Provides a {@link ModelFactory} implementation for {@link SzEntityClass}.
   */
  class Factory extends ModelFactory<SzEntityClass, Provider> {
    /**
     * Default constructor.  This is public and can only be called after the
     * singleton master instance is created as it inherits the same state from
     * the master instance.
     */
    public Factory() {
      super(SzEntityClass.class);
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
     * Constructs an uninitialized instance.
     */
    public SzEntityClass create() {
      return this.getProvider().create();
    }

    /**
     * Creates an instance with the specified entity class code leaving the
     * entity class ID and resolving flag <tt>null</tt>.
     *
     * @param entityClassCode The entity class code for the entity class.
     */
    public SzEntityClass create(String entityClassCode) {
      return this.getProvider().create(entityClassCode);
    }

    /**
     * Creates an instance with the specified entity class code, entity class ID
     * and flag indicating whether or not entities having entity types belonging
     * to this entity class will resolve against each other.
     *
     * @param entityClassCode The entity class code for the entity class.
     * @param entityClassId The entity class ID for the entity class.
     * @param resolving <tt>true</tt> if entities having entity types belonging
     *                  to this entity class will resolve against each other,
     *                  and <tt>false</tt> if they will not resolve.
     */
    public SzEntityClass create(String  entityClassCode,
                                Integer entityClassId,
                                Boolean resolving) {

      return this.getProvider().create(entityClassCode,
                                       entityClassId,
                                       resolving);
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
   * @return The {@link SzEntityClass} that was created.
   */
  static SzEntityClass valueOf(String text) {
    try {
      JsonObject jsonObject = JsonUtils.parseJsonObject(text.trim());

      return SzEntityClass.parse(jsonObject);

    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid entity class: " + text);
    }
  }

  /**
   * Parses the specified {@link JsonObject} as a entity class.
   *
   * @param jsonObject The {@link JsonObject} to parse.
   *
   * @return The {@link SzEntityClass} that was created.
   */
  static SzEntityClass parse(JsonObject jsonObject) {
    String code = JsonUtils.getString(jsonObject, "entityClassCode");
    if (code == null) {
      code = JsonUtils.getString(jsonObject, "ECLASS_CODE");
    }
    Integer id = JsonUtils.getInteger(jsonObject, "entityClassId");
    if (id == null) {
      id = JsonUtils.getInteger(jsonObject, "ECLASS_ID");
    }
    Boolean resolving = JsonUtils.getBoolean(jsonObject, "resolving");
    if (resolving == null) {
      String flag = JsonUtils.getString(jsonObject, "RESOLVE");
      if (flag != null) {
        resolving = flag.trim().toUpperCase().equals("YES");
      }
    }
    return SzEntityClass.FACTORY.create(code, id, resolving);
  }

  /**
   * Parses a JSON array of the engine API JSON to create or populate a
   * {@link List} of {@link SzEntityClass} instances.
   *
   * @param list The {@link List} to populate or <tt>null</tt> if a new
   *             {@link List} should be created.
   *
   * @param jsonArray The {@link JsonArray} of {@link JsonObject} instances
   *                  to parse from the engine API.
   *
   * @return The specified (or newly created) {@link List} of 
   *         {@link SzEntityClass} instances.
   */
  static List<SzEntityClass> parseEntityClassList(
      List<SzEntityClass> list,
      JsonArray           jsonArray)
  {
    if (list == null) {
      list = new ArrayList<>(jsonArray.size());
    }
    for (JsonObject jsonObject : jsonArray.getValuesAs(JsonObject.class)) {
      list.add(parseEntityClass(null, jsonObject));
    }
    return list;
  }

  /**
   * Parses the engine API JSON to create an instance of {@link SzEntityClass}.
   *
   * @param entityClass The {@link SzEntityClass} object to initialize or
   *                    <tt>null</tt> if a new one should be created.
   *
   * @param jsonObject The {@link JsonObject} to parse from the engine API.
   *
   * @return The specified (or newly created) {@link SzEntityClass}
   */
  static SzEntityClass parseEntityClass(SzEntityClass entityClass,
                                        JsonObject    jsonObject)
  {
    if (entityClass == null) entityClass = SzEntityClass.FACTORY.create();

    String  code    = JsonUtils.getString(jsonObject, "ECLASS_CODE");
    Integer id      = JsonUtils.getInteger(jsonObject, "ECLASS_ID");
    String  resolve = JsonUtils.getString(jsonObject, "RESOLVE");

    if (code == null) {
      throw new IllegalArgumentException(
          "Could not find the ECLASS_CODE property");
    }
    if (id == null) {
      throw new IllegalArgumentException(
          "Could not find the ECLASS_ID property");
    }
    if (resolve == null) {
      throw new IllegalArgumentException(
          "Could not find the RESOLVE property");
    }

    entityClass.setEntityClassCode(code);
    entityClass.setEntityClassId(id);
    entityClass.setResolving(
        (resolve == null) ? null : resolve.trim().toUpperCase().equals("YES"));

    return entityClass;
  }

  /**
   * Implemented to return a reference to this instance.
   *
   * @return A reference to this instance.
   */
  default SzEntityClass toEntityClass() {
    return this;
  }

  /**
   * Adds the JSON properties to the specified {@link JsonObjectBuilder} to
   * describe this instance in its standard JSON format.
   *
   * @param builder The {@link JsonObjectBuilder} to add the properties.
   */
  default void buildJson(JsonObjectBuilder builder) {
    builder.add("entityClassCode", this.getEntityClassCode());
    Integer classId = this.getEntityClassId();
    if (classId != null) {
      builder.add("entityClassId", classId);
    }
    Boolean resolve = this.isResolving();
    if (resolve != null) {
      builder.add("resolving", resolve);
    }
  }

  /**
   * Adds the native Senzing JSON properties to the specified {@link
   * JsonObjectBuilder} describing this instance.
   *
   * @param builder The {@link JsonObjectBuilder} to add the properties to.
   */
  default void buildNativeJson(JsonObjectBuilder builder) {
    builder.add("ECLASS_CODE", this.getEntityClassCode());
    if (this.getEntityClassId() != null) {
      builder.add("ECLASS_ID", this.getEntityClassId());
    }
    boolean resolve = (this.isResolving() == null) ? true : this.isResolving();
    if (this.isResolving() != null) {
      builder.add("RESOLVE", (resolve ? "Yes" : "No"));
    }
  }
}
