package com.senzing.api.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.senzing.api.model.impl.SzEntityTypeDescriptorsImpl;
import com.senzing.util.JsonUtilities;

import javax.json.*;
import java.util.*;

/**
 * Used to represent a {@link List} of zero or more {@link
 * SzEntityTypeDescriptor} instances.
 *
 */
@JsonDeserialize(using=SzEntityTypeDescriptors.Factory.class)
public interface SzEntityTypeDescriptors {
  /**
   * Checks if all the {@link SzEntityTypeDescriptor} instances contained are
   * of the same type (e.g.: either {@link SzEntityId} or {@link SzRecordId}).
   *
   * @return <tt>true</tt> if the {@link SzEntityTypeDescriptor} instances are
   *         of the same type otherwise <tt>false</tt>.
   */
  default boolean isHomogeneous() {
    Class<? extends SzEntityTypeDescriptor> c = null;
    for (SzEntityTypeDescriptor i : this.getDescriptors()) {
      if (c == null) {
        c = i.getClass();
        continue;
      }
      if (c != i.getClass()) return false;
    }
    return true;
  }

  /**
   * Checks if there are no entity descriptors specified for this instance.
   *
   * @return <tt>true</tt> if no entity descriptors are specified, otherwise
   *         <tt>false</tt>.
   */
  default boolean isEmpty() {
    List<SzEntityTypeDescriptor> descriptors = this.getDescriptors();
    return (descriptors == null || descriptors.size() == 0);
  }

  /**
   * Returns the number of entity descriptors.
   *
   * @return The number of entity descriptors.
   */
  default int getCount() {
    List<SzEntityTypeDescriptor> descriptors = this.getDescriptors();
    return (descriptors == null ? 0 : descriptors.size());
  }

  /**
   * Returns the unmodifiable {@link List} of {@link SzEntityTypeDescriptor}
   * instances that were specified.
   *
   * @return The unmodifiable {@link List} of {@link SzEntityTypeDescriptor}
   *         instances that were specified.
   */
  List<SzEntityTypeDescriptor> getDescriptors();
  
  /**
   * A {@link ModelProvider} for instances of {@link SzEntityTypeDescriptors}.
   */
  interface Provider extends ModelProvider<SzEntityTypeDescriptors> {
    /**
     * Constructs an instance with no {@link SzEntityTypeDescriptor} instances.
     */
    SzEntityTypeDescriptors create();

    /**
     * Constructs an instance with a single {@link SzEntityTypeDescriptor}
     * instance.
     *
     * @param identifier The single non-null {@link SzEntityTypeDescriptor}
     *                   instance.
     *
     * @throws NullPointerException If the specified parameter is null.
     */
    SzEntityTypeDescriptors create(SzEntityTypeDescriptor identifier)
        throws NullPointerException;

    /**
     * Constructs with the specified {@link Collection} of {@link
     * SzEntityTypeDescriptor} instances.  The specified {@link Collection} will be
     * copied.
     *
     * @param descriptors The non-null {@link Collection} of {@link
     *                    SzEntityTypeDescriptor} instances.
     *
     * @throws NullPointerException If the specified parameter is null.
     */
    SzEntityTypeDescriptors create(
        Collection<? extends SzEntityTypeDescriptor> descriptors)
        throws NullPointerException;
  }

  /**
   * Provides a default {@link Provider} implementation for {@link
   * SzEntityTypeDescriptor} that produces instances of {@link
   * SzEntityTypeDescriptorsImpl}.
   */
  class DefaultProvider extends AbstractModelProvider<SzEntityTypeDescriptors>
      implements Provider
  {
    /**
     * Default constructor.
     */
    public DefaultProvider() {
      super(SzEntityTypeDescriptors.class, SzEntityTypeDescriptorsImpl.class);
    }

    @Override
    public SzEntityTypeDescriptors create() {
      return new SzEntityTypeDescriptorsImpl();
    }

    @Override
    public SzEntityTypeDescriptors create(SzEntityTypeDescriptor identifier)
        throws NullPointerException
    {
      return new SzEntityTypeDescriptorsImpl(identifier);
    }

    @Override
    public SzEntityTypeDescriptors create(
        Collection<? extends SzEntityTypeDescriptor> descriptors)
        throws NullPointerException
    {
      return new SzEntityTypeDescriptorsImpl(descriptors);
    }
  }

  /**
   * Provides a {@link ModelFactory} implementation for {@link
   * SzEntityTypeDescriptors}.
   */
  class Factory extends ModelFactory<SzEntityTypeDescriptors, Provider> {
    /**
     * Default constructor.  This is public and can only be called after the
     * singleton master instance is created as it inherits the same state from
     * the master instance.
     */
    public Factory() {
      super(SzEntityTypeDescriptors.class);
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
     * Constructs an instance with no {@link SzEntityTypeDescriptor} instances.
     */
    public SzEntityTypeDescriptors create() {
      return this.getProvider().create();
    }

    /**
     * Constructs an instance with a single {@link SzEntityTypeDescriptor}
     * instance.
     *
     * @param identifier The single non-null {@link SzEntityTypeDescriptor}
     *                   instance.
     *
     * @throws NullPointerException If the specified parameter is null.
     */
    public SzEntityTypeDescriptors create(SzEntityTypeDescriptor identifier)
        throws NullPointerException
    {
      return this.getProvider().create(identifier);
    }

    /**
     * Constructs with the specified {@link Collection} of {@link
     * SzEntityTypeDescriptor} instances.  The specified {@link Collection} will be
     * copied.
     *
     * @param descriptors The non-null {@link Collection} of {@link
     *                    SzEntityTypeDescriptor} instances.
     *
     * @throws NullPointerException If the specified parameter is null.
     */
    public SzEntityTypeDescriptors create(
        Collection<? extends SzEntityTypeDescriptor> descriptors)
        throws NullPointerException
    {
      return this.getProvider().create(descriptors);
    }
  }

  /**
   * The {@link Factory} instance for this interface.
   */
  Factory FACTORY = new Factory(new DefaultProvider());
  
  /**
   * Parses the specified text as a {@link List} of homogeneous
   * {@link SzEntityTypeDescriptor} instances.
   *
   * @param text The text to parse.
   *
   * @return The {@link SzEntityTypeDescriptors} instance representing the
   *         {@link List} of {@link SzEntityTypeDescriptor} instances.
   */
  static SzEntityTypeDescriptors valueOf(String text) {
    if (text != null) text = text.trim();
    int               length  = (text == null) ? 0 : text.length();
    char              first   = (length == 0) ? 0 : text.charAt(0);
    char              last    = (length <= 1) ? 0 : text.charAt(length-1);

    // check if no descriptors
    if (length == 0) {
      // no descriptors
      return SzEntityTypeDescriptors.FACTORY.create();
    }

    // check if it looks like a JSON array
    if (first == '[' && last == ']') {
      try {
        return parseAsJsonArray(text);

      } catch (RuntimeException e) {
        // ignore
      }
    }

    // try to convert it to a JSON array
    if (first != '[' && last != ']') {
      // check if it is a quoted string or a JSON object
      if ((first == '{' && last == '}') || (first == '"' && last == '"')) {
        // if its already a quoted string or object , surround it in brackets
        try {
          return SzEntityTypeDescriptors.parseAsJsonArray("[" + text + "]");

        } catch (RuntimeException e) {
          // ignore
        }
      }

      // if we get here then assume it is an unquoted string representing a
      // data source code and build a JSON string array
      JsonArrayBuilder jab = Json.createArrayBuilder();
      jab.add(text);
      String jsonText = JsonUtilities.toJsonText(jab);
      return SzEntityTypeDescriptors.parseAsJsonArray(jsonText);
    }

    // if we get here then check for a failure
    throw new IllegalArgumentException(
        "Unable to interpret the text as a list of entity type descriptors: "
        + text);
  }

  /**
   * Parses the specified text as a JSON array.
   *
   * @param text The text to parse
   * @return The {@link SzEntityTypeDescriptors} that was parsed.
   */
  private static SzEntityTypeDescriptors parseAsJsonArray(String text) {
    // it appears we have a JSON array of entity descriptors
    JsonArray jsonArray = JsonUtilities.parseJsonArray(text);
    List<SzEntityTypeDescriptor> descriptors
        = new ArrayList<>(jsonArray.size());
    JsonValue.ValueType valueType = null;
    for (JsonValue value : jsonArray) {
      JsonValue.ValueType vt = value.getValueType();
      SzEntityTypeDescriptor descriptor;
      switch (vt) {
        case STRING:
          descriptor = SzEntityTypeCode.FACTORY.create(
              ((JsonString) value).getString());
          break;

        case OBJECT:
          descriptor = SzEntityType.parse((JsonObject) value);
          break;

        default:
          throw new IllegalArgumentException(
              "Unexpected element in entity descriptor array: valueType=[ "
                  + valueType + " ], value=[ " + value + " ]");
      }
      descriptors.add(descriptor);
    }

    // make the list unmodifiable
    return SzEntityTypeDescriptors.FACTORY.create(
        Collections.unmodifiableList(descriptors));
  }

}
