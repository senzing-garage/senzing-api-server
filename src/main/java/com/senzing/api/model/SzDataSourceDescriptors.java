package com.senzing.api.model;

import com.senzing.util.JsonUtils;

import javax.json.*;
import java.util.*;

/**
 * Used to represent a {@link List} of zero or more {@link SzDataSourceDescriptor}
 * instances.
 *
 */
public class SzDataSourceDescriptors {
  /**
   * The {@link List} of {@link SzDataSourceDescriptor} instances.
   */
  private List<SzDataSourceDescriptor> descriptors;

  /**
   * Constructs with no {@link SzDataSourceDescriptor} instances.
   */
  public SzDataSourceDescriptors() throws NullPointerException
  {
    this.descriptors = Collections.emptyList();
  }

  /**
   * Constructs with a single {@link SzDataSourceDescriptor} instance.
   *
   * @param identifier The single non-null {@link SzDataSourceDescriptor} instance.
   *
   * @throws NullPointerException If the specified parameter is null.
   */
  public SzDataSourceDescriptors(SzDataSourceDescriptor identifier)
      throws NullPointerException
  {
    Objects.requireNonNull(identifier, "Identifier cannot be null.");
    this.descriptors = Collections.singletonList(identifier);
  }

  /**
   * Constructs with the specified {@link Collection} of {@link
   * SzDataSourceDescriptor} instances.  The specified {@link Collection} will be
   * copied.
   *
   * @param descriptors The non-null {@link Collection} of {@link
   *                    SzDataSourceDescriptor} instances.
   *
   * @throws NullPointerException If the specified parameter is null.
   */
  public SzDataSourceDescriptors(
      Collection<? extends SzDataSourceDescriptor> descriptors)
    throws NullPointerException
  {
    Objects.requireNonNull(descriptors, "Identifiers cannot be null.");
    this.descriptors = Collections.unmodifiableList(
        new ArrayList<>(descriptors));
  }

  /**
   * Private constructor to use when the collection of {@link
   * SzDataSourceDescriptor} instances may not need to be copied.
   *
   * @param descriptors The {@link List} of {@link SzDataSourceDescriptor}
   *                    instances.
   *
   * @param copy <tt>true</tt> if the specified list should be copied or
   *             used directly.
   */
  private SzDataSourceDescriptors(List<SzDataSourceDescriptor>  descriptors,
                                  boolean                       copy)
  {
    if (copy) {
      if (descriptors == null || descriptors.size() == 0) {
        this.descriptors = Collections.emptyList();
      } else {
        this.descriptors = Collections.unmodifiableList(
            new ArrayList<>(descriptors));
      }
    } else {
      this.descriptors = descriptors;
    }
  }

  /**
   * Checks if all the {@link SzDataSourceDescriptor} instances contained are of the
   * same type (e.g.: either {@link SzEntityId} or {@link SzRecordId}).
   *
   * @return <tt>true</tt> if the {@link SzDataSourceDescriptor} instances are
   *         of the same type otherwise <tt>false</tt>.
   */
  public boolean isHomogeneous() {
    Class<? extends SzDataSourceDescriptor> c = null;
    for (SzDataSourceDescriptor i : this.descriptors) {
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
  public boolean isEmpty() {
    return (this.descriptors == null || this.descriptors.size() == 0);
  }

  /**
   * Returns the number of entity descriptors.
   *
   * @return The number of entity descriptors.
   */
  public int getCount() {
    return (this.descriptors == null ? 0 : this.descriptors.size());
  }

  /**
   * Returns the unmodifiable {@link List} of {@link SzDataSourceDescriptor}
   * instances that were specified.
   *
   * @return The unmodifiable {@link List} of {@link SzDataSourceDescriptor}
   *         instances that were specified.
   */
  public List<SzDataSourceDescriptor> getDescriptors() {
    return this.descriptors;
  }

  /**
   * Overridden to convert the {@link SzDataSourceDescriptors} instance to a JSON
   * array string.
   *
   * @return The JSON array string representation of this instance.
   *
   */
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    String prefix = "";
    for (SzDataSourceDescriptor identifier : this.getDescriptors()) {
      sb.append(prefix).append(identifier.toString());
      prefix = ",";
    }
    sb.append("]");
    return sb.toString();
  }

  /**
   * Parses the specified text as a {@link List} of homogeneous
   * {@link SzDataSourceDescriptor} instances.
   *
   * @param text The text to parse.
   *
   * @return The {@link SzDataSourceDescriptors} instance representing the {@link
   *         List} of {@link SzDataSourceDescriptor} instances.
   */
  public static SzDataSourceDescriptors valueOf(String text) {
    text = text.trim();
    int               length  = text.length();
    char              first   = text.charAt(0);
    char              last    = text.charAt(length-1);

    // check if no descriptors
    if (length == 0) {
      // no descriptors
      return new SzDataSourceDescriptors();
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
      if ((first == '"' && last == '"') || (first == '{' && last == '}')) {
        // if its already a quoted string or object, surround it in brackets
        try {
          return SzDataSourceDescriptors.parseAsJsonArray("[" + text + "]");

        } catch (RuntimeException e) {
          // ignore
        }
      }

      // if we get here then assume it is an unquoted string representing a
      // data source code and build a JSON string array
      JsonArrayBuilder jab = Json.createArrayBuilder();
      jab.add(text);
      String jsonText = JsonUtils.toJsonText(jab);
      return SzDataSourceDescriptors.parseAsJsonArray(jsonText);
    }

    // if we get here then check for a failure
    throw new IllegalArgumentException(
        "Unable to interpret the text as a list of data source descriptors: "
        + text);
  }

  /**
   * Parses the specified text as a JSON array.
   *
   * @param text The text to parse
   * @return The {@link SzDataSourceDescriptors} that was parsed.
   */
  private static SzDataSourceDescriptors parseAsJsonArray(String text) {
    // it appears we have a JSON array of entity descriptors
    JsonArray jsonArray = JsonUtils.parseJsonArray(text);
    List<SzDataSourceDescriptor> descriptors
        = new ArrayList<>(jsonArray.size());
    JsonValue.ValueType valueType = null;
    for (JsonValue value : jsonArray) {
      JsonValue.ValueType vt = value.getValueType();
      SzDataSourceDescriptor descriptor;
      switch (vt) {
        case STRING:
          descriptor = new SzDataSourceCode(((JsonString) value).getString());
          break;

        case OBJECT:
          descriptor = SzDataSource.parse((JsonObject) value);
          break;

        default:
          throw new IllegalArgumentException(
              "Unexpected element in entity descriptor array: valueType=[ "
                  + valueType + " ], value=[ " + value + " ]");
      }
      descriptors.add(descriptor);
    }

    // make the list unmodifiable
    return new SzDataSourceDescriptors(
        Collections.unmodifiableList(descriptors), false);
  }

  /**
   * Test main function.
   */
  public static void main(String[] args) {
    for (String arg : args) {
      System.out.println();
      System.out.println("- - - - - - - - - - - - - - - - - - - - - ");
      System.out.println("PARSING: " + arg);
      try {
        SzDataSourceDescriptors descriptors = SzDataSourceDescriptors.valueOf(arg);
        System.out.println(descriptors.getDescriptors());

      } catch (Exception e) {
        e.printStackTrace(System.out);
      }

    }
  }
}
