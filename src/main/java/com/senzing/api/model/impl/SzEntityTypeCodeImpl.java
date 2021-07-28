package com.senzing.api.model.impl;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.senzing.api.model.SzEntityType;
import com.senzing.api.model.SzEntityTypeCode;
import com.senzing.api.model.SzEntityTypeDescriptor;

import javax.json.JsonObjectBuilder;
import java.util.Objects;

/**
 * Describes a entity type code to identify an entity type.
 */
@JsonDeserialize
public class SzEntityTypeCodeImpl implements SzEntityTypeCode {
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
  public SzEntityTypeCodeImpl(String code)
    throws NullPointerException
  {
    Objects.requireNonNull(code, "The entity type code cannot be null");
    this.value = code.trim().toUpperCase();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getValue() {
    return this.value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SzEntityTypeCodeImpl that = (SzEntityTypeCodeImpl) o;
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
}
