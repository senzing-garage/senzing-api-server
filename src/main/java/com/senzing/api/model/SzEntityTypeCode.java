package com.senzing.api.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.senzing.api.model.impl.SzEntityTypeCodeImpl;
import com.senzing.util.JsonUtils;

import javax.json.Json;
import javax.json.JsonObjectBuilder;
import java.util.Objects;

/**
 * Describes a entity type code to identify an entity type.
 */
@JsonDeserialize(using=SzEntityTypeCode.Factory.class)
public interface SzEntityTypeCode extends SzEntityTypeDescriptor {
  /**
   * Return the entity type code identifying the entity type.
   *
   * @return The entity type code identifying the entity type.
   */
  String getValue();

  /**
   * A {@link ModelProvider} for instances of {@link SzDataSourceCode}.
   */
  interface Provider extends ModelProvider<SzEntityTypeCode> {
    /**
     * Constructs with the specified entity type code.
     *
     * @param entityTypeCode The entity type code for the entity type.
     */
    SzEntityTypeCode create(String entityTypeCode);
  }

  /**
   * Provides a default {@link Provider} implementation for {@link
   * SzEntityTypeCode} that produces instances of {@link SzEntityTypeCodeImpl}.
   */
  class DefaultProvider extends AbstractModelProvider<SzEntityTypeCode>
      implements Provider
  {
    /**
     * Default constructor.
     */
    public DefaultProvider() {
      super(SzEntityTypeCode.class, SzEntityTypeCodeImpl.class);
    }

    @Override
    public SzEntityTypeCode create(String entityTypeCode) {
      return new SzEntityTypeCodeImpl(entityTypeCode);
    }
  }

  /**
   * Provides a {@link ModelFactory} implementation for {@link
   * SzEntityTypeCode}.
   */
  class Factory extends ModelFactory<SzEntityTypeCode, Provider> {
    /**
     * Default constructor.  This is public and can only be called after the
     * singleton master instance is created as it inherits the same state from
     * the master instance.
     */
    public Factory() {
      super(SzEntityTypeCode.class);
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
     * Constructs with the specified entity type code.
     *
     * @param entityTypeCode The entity type code for the entity type.
     */
    public SzEntityTypeCode create(String entityTypeCode) {
      return this.getProvider().create(entityTypeCode);
    }
  }

  /**
   * The {@link Factory} instance for this interface.
   */
  Factory FACTORY = new Factory(new DefaultProvider());

  /**
   * Parses text as a entity type code.  The specified text is trimmed of
   * leading and trailing white space and converted to upper case.  If the
   * specified text is enclosed in double quotes, they are stripped off.
   *
   * @param text The to parse.
   *
   * @return The {@link SzEntityTypeCode} that was created.
   *
   * @throws NullPointerException If the specified text is <tt>null</tt>.
   */
  static SzEntityTypeCode valueOf(String text)
    throws NullPointerException
  {
    if (text.length() > 2 && text.startsWith("\"") && text.endsWith("\"")) {
      text = text.substring(1, text.length() - 1);
    }
    return SzEntityTypeCode.FACTORY.create(text);
  }

  /**
   * Converts this instance to an instance of {@link SzEntityType}
   * which completely describes a entity type with the same
   * entity type code and a <tt>null</tt> entity type ID and entity
   * class code.
   *
   * @return The {@link SzEntityType} describing the entity type.
   */
  default SzEntityType toEntityType() {
    return SzEntityType.FACTORY.create(this.getValue());
  }

  /**
   * Adds the JSON properties to the specified {@link JsonObjectBuilder} to
   * describe this instance in its standard JSON format.
   *
   * @param builder The {@link JsonObjectBuilder} to add the properties.
   */
  default void buildJson(JsonObjectBuilder builder) {
    builder.add("entityTypeCode", this.getValue());
  }

  /**
   * Adds the Senzing native JSON properties to the specified {@link
   * JsonObjectBuilder}.
   *
   * @param builder The {@link JsonObjectBuilder} to add the properties to.
   */
  default void buildNativeJson(JsonObjectBuilder builder) {
    builder.add("ETYPE_CODE", this.getValue());
  }
}
