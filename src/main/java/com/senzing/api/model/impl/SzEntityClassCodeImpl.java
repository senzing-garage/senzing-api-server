package com.senzing.api.model.impl;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.senzing.api.model.SzEntityClass;
import com.senzing.api.model.SzEntityClassCode;
import com.senzing.api.model.SzEntityClassDescriptor;

import javax.json.JsonObjectBuilder;
import java.util.Objects;

/**
 * Describes a entity class code to identify an entity class.
 */
@JsonDeserialize
public class SzEntityClassCodeImpl implements SzEntityClassCode {
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
  public SzEntityClassCodeImpl(String code)
    throws NullPointerException
  {
    Objects.requireNonNull(code, "The entity class code cannot be null");
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
    SzEntityClassCodeImpl that = (SzEntityClassCodeImpl) o;
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
