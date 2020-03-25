package com.senzing.api.model;

import com.senzing.util.JsonUtils;

import javax.json.Json;
import javax.json.JsonObjectBuilder;
import java.util.Objects;

/**
 * Describes a entity class code to identify an entity class.
 */
public class SzEntityClassCode implements SzEntityClassDescriptor {
  /**
   * The entity class code that identifiers the entity class.
   */
  private String value;

  /**
   * Constructs with the specified entity class code.  The specified entity
   * class code is trimmed of leading and trailing white space and converted
   * to all upper case.
   *
   * @param code The non-null entity class code which will be trimmed and
   *             converted to upper-case.
   *
   * @throws NullPointerException If the specified code is <tt>null</tt>.
   */
  public SzEntityClassCode(String code)
    throws NullPointerException
  {
    Objects.requireNonNull(code, "The entity class code cannot be null");
    this.value = code.trim().toUpperCase();
  }

  /**
   * Return the entity class code identifying the entity class.
   *
   * @return The entity class code identifying the entity class.
   */
  public String getValue() {
    return this.value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SzEntityClassCode that = (SzEntityClassCode) o;
    return this.value.equals(that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.value);
  }

  @Override
  public String toString() {
    return this.getValue();
  }

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
  public static SzEntityClassCode valueOf(String text)
    throws NullPointerException
  {
    if (text.length() > 2 && text.startsWith("\"") && text.endsWith("\"")) {
      text = text.substring(1, text.length() - 1);
    }
    return new SzEntityClassCode(text);
  }

  /**
   * Converts this instance to an instance of {@link SzEntityClass}
   * which completely describes a entity class with the same
   * entity class code and a <tt>null</tt> entity class ID and resolving flag.
   *
   * @return The {@link SzEntityClass} describing the entity class.
   */
  public SzEntityClass toEntityClass() {
    return new SzEntityClass(this.getValue());
  }

  /**
   * Adds the JSON properties to the specified {@link JsonObjectBuilder} to
   * describe this instance in its standard JSON format.
   *
   * @param builder The {@link JsonObjectBuilder} to add the properties.
   */
  public void buildJson(JsonObjectBuilder builder) {
    builder.add("entityClassCode", this.getValue());
  }

  /**
   * Adds the native Senzing JSON properties to the specified {@link
   * JsonObjectBuilder} describing this instance.
   *
   * @param builder The {@link JsonObjectBuilder} to add the properties to.
   */
  public void buildNativeJson(JsonObjectBuilder builder) {
    builder.add("ECLASS_CODE", this.getValue());
  }
}
