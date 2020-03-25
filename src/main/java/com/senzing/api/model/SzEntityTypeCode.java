package com.senzing.api.model;

import com.senzing.util.JsonUtils;

import javax.json.Json;
import javax.json.JsonObjectBuilder;
import java.util.Objects;

/**
 * Describes a entity type code to identify an entity type.
 */
public class SzEntityTypeCode implements SzEntityTypeDescriptor {
  /**
   * The entity type code that identifiers the entity type.
   */
  private String value;

  /**
   * Constructs with the specified entity type code.  The specified entity
   * type code is trimmed of leading and trailing white space and converted
   * to all upper case.
   *
   * @param code The non-null entity type code which will be trimmed and
   *             converted to upper-case.
   *
   * @throws NullPointerException If the specified code is <tt>null</tt>.
   */
  public SzEntityTypeCode(String code)
    throws NullPointerException
  {
    Objects.requireNonNull(code, "The entity type code cannot be null");
    this.value = code.trim().toUpperCase();
  }

  /**
   * Return the entity type code identifying the entity type.
   *
   * @return The entity type code identifying the entity type.
   */
  public String getValue() {
    return this.value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SzEntityTypeCode that = (SzEntityTypeCode) o;
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
  public static SzEntityTypeCode valueOf(String text)
    throws NullPointerException
  {
    if (text.length() > 2 && text.startsWith("\"") && text.endsWith("\"")) {
      text = text.substring(1, text.length() - 1);
    }
    return new SzEntityTypeCode(text);
  }

  /**
   * Converts this instance to an instance of {@link SzEntityType}
   * which completely describes a entity type with the same
   * entity type code and a <tt>null</tt> entity type ID and entity
   * class code.
   *
   * @return The {@link SzEntityType} describing the entity type.
   */
  public SzEntityType toEntityType() {
    return new SzEntityType(this.getValue());
  }

  /**
   * Adds the JSON properties to the specified {@link JsonObjectBuilder} to
   * describe this instance in its standard JSON format.
   *
   * @param builder The {@link JsonObjectBuilder} to add the properties.
   */
  public void buildJson(JsonObjectBuilder builder) {
    builder.add("entityTypeCode", this.getValue());
  }

  /**
   * Adds the Senzing native JSON properties to the specified {@link
   * JsonObjectBuilder}.
   *
   * @param builder The {@link JsonObjectBuilder} to add the properties to.
   */
  public void buildNativeJson(JsonObjectBuilder builder) {
    builder.add("ETYPE_CODE", this.getValue());
  }
}
