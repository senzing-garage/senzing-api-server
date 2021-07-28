package com.senzing.api.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.senzing.api.model.impl.SzEntityClassCodeImpl;
import com.senzing.util.JsonUtils;

import javax.json.Json;
import javax.json.JsonObjectBuilder;
import java.util.Objects;

/**
 * Describes a entity class code to identify an entity class.
 */
@JsonDeserialize(using=SzEntityClassCode.Factory.class)
public interface SzEntityClassCode extends SzEntityClassDescriptor {
  /**
   * Return the entity class code identifying the entity class.
   *
   * @return The entity class code identifying the entity class.
   */
  String getValue();

  /**
   * A {@link ModelProvider} for instances of {@link SzEntityClassCode}.
   */
  interface Provider extends ModelProvider<SzEntityClassCode> {
    /**
     * Constructs with the specified entity class code.
     *
     * @param entityClassCode The data source code for the data source.
     */
    SzEntityClassCode create(String entityClassCode);
  }

  /**
   * Provides a default {@link Provider} implementation for {@link
   * SzEntityClassCode} that produces instances of {@link
   * SzEntityClassCodeImpl}.
   */
  class DefaultProvider extends AbstractModelProvider<SzEntityClassCode>
      implements Provider
  {
    /**
     * Default constructor.
     */
    public DefaultProvider() {
      super(SzEntityClassCode.class, SzEntityClassCodeImpl.class);
    }

    @Override
    public SzEntityClassCode create(String entityClassCode) {
      return new SzEntityClassCodeImpl(entityClassCode);
    }
  }

  /**
   * Provides a {@link ModelFactory} implementation for {@link
   * SzEntityClassCode}.
   */
  class Factory extends ModelFactory<SzEntityClassCode, Provider> {
    /**
     * Default constructor.  This is public and can only be called after the
     * singleton master instance is created as it inherits the same state from
     * the master instance.
     */
    public Factory() {
      super(SzEntityClassCode.class);
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
     * Creates an instance with the specified entity class code.
     *
     * @param entityClassCode The entity class code for the entity class.
     */
    public SzEntityClassCode create(String entityClassCode) {
      return this.getProvider().create(entityClassCode);
    }
  }

  /**
   * The {@link Factory} instance for this interface.
   */
  Factory FACTORY = new Factory(new DefaultProvider());


  /**
   * Parses text as a entity class code.  The specified text is trimmed of
   * leading and trailing white space and converted to upper case.  If the
   * specified text is enclosed in double quotes, they are stripped off.
   *
   * @param text The to parse.
   *
   * @return The {@link SzEntityClassCode} that was created.
   *
   * @throws NullPointerException If the specified text is <tt>null</tt>.
   */
  static SzEntityClassCode valueOf(String text)
    throws NullPointerException
  {
    if (text.length() > 2 && text.startsWith("\"") && text.endsWith("\"")) {
      text = text.substring(1, text.length() - 1);
    }
    return SzEntityClassCode.FACTORY.create(text);
  }

  /**
   * Converts this instance to an instance of {@link SzEntityClass}
   * which completely describes a entity class with the same
   * entity class code and a <tt>null</tt> entity class ID and resolving flag.
   *
   * @return The {@link SzEntityClass} describing the entity class.
   */
  default SzEntityClass toEntityClass() {
    return SzEntityClass.FACTORY.create(this.getValue());
  }

  /**
   * Adds the JSON properties to the specified {@link JsonObjectBuilder} to
   * describe this instance in its standard JSON format.
   *
   * @param builder The {@link JsonObjectBuilder} to add the properties.
   */
  default void buildJson(JsonObjectBuilder builder) {
    builder.add("entityClassCode", this.getValue());
  }

  /**
   * Adds the native Senzing JSON properties to the specified {@link
   * JsonObjectBuilder} describing this instance.
   *
   * @param builder The {@link JsonObjectBuilder} to add the properties to.
   */
  default void buildNativeJson(JsonObjectBuilder builder) {
    builder.add("ECLASS_CODE", this.getValue());
  }
}
