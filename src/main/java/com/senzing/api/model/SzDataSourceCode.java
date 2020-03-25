package com.senzing.api.model;

import com.senzing.util.JsonUtils;

import javax.json.Json;
import javax.json.JsonObjectBuilder;
import java.util.Objects;

/**
 * Describes a data source code to identify an data source.
 */
public class SzDataSourceCode implements SzDataSourceDescriptor {
  /**
   * The data source code that identifiers the data source.
   */
  private String value;

  /**
   * Constructs with the specified data source code.  The specified data
   * source code is trimmed of leading and trailing white space and converted
   * to all upper case.
   *
   * @param code The non-null data source code which will be trimmed and
   *             converted to upper-case.
   *
   * @throws NullPointerException If the specified code is <tt>null</tt>.
   */
  public SzDataSourceCode(String code)
    throws NullPointerException
  {
    Objects.requireNonNull(code, "The data source code cannot be null");
    this.value = code.trim().toUpperCase();
  }

  /**
   * Return the data source code identifying the data source.
   *
   * @return The data source code identifying the data source.
   */
  public String getValue() {
    return this.value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SzDataSourceCode that = (SzDataSourceCode) o;
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
   * Parses text as a data source code.  The specified text is trimmed of
   * leading and trailing white space and converted to upper case.  If the
   * specified text is enclosed in double quotes, they are stripped off.
   *
   * @param text The to parse.
   *
   * @return The {@link SzDataSourceCode} that was created.
   *
   * @throws NullPointerException If the specified text is <tt>null</tt>.
   */
  public static SzDataSourceCode valueOf(String text)
    throws NullPointerException
  {
    if (text.length() > 2 && text.startsWith("\"") && text.endsWith("\"")) {
      text = text.substring(1, text.length() - 1);
    }
    return new SzDataSourceCode(text);
  }

  /**
   * Converts this instance to an instance of {@link SzDataSource}
   * which completely describes a data source with the same
   * data source code and a <tt>null</tt> data source ID.
   *
   * @return The {@link SzDataSource} describing the data source.
   */
  public SzDataSource toDataSource() {
    return new SzDataSource(this.getValue());
  }

  /**
   * Adds the JSON properties to the specified {@link JsonObjectBuilder} to
   * describe this instance in its standard JSON format.
   *
   * @param builder The {@link JsonObjectBuilder} to add the properties.
   */
  public void buildJson(JsonObjectBuilder builder) {
    builder.add("dataSourceCode", this.getValue());
  }

  /**
   * Implemented to add the <tt>"DSRC_CODE"</tt> field to the specified
   * {@link JsonObjectBuilder}.
   *
   * @param builder The {@link JsonObjectBuilder} to add the properties to.
   */
  public void buildNativeJson(JsonObjectBuilder builder) {
    builder.add("DSRC_CODE", this.getValue());
  }

}
