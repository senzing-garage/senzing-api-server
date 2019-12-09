package com.senzing.api.model;

import java.util.Objects;

/**
 * Describes an entity ID to identify an entity.
 */
public class SzEntityId implements SzEntityIdentifier {
  /**
   * The entity ID that identifies the entity.
   */
  private long value;

  /**
   * Constructs with the specified entity ID.
   * @param id The entity ID.
   */
  public SzEntityId(long id) {
    this.value = id;
  }

  /**
   * Return the entity ID identifying the entity.
   *
   * @return The entity ID identifying the entity.
   */
  public long getValue() {
    return this.value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SzEntityId that = (SzEntityId) o;
    return value == that.value;
  }

  @Override
  public int hashCode() {
    return Objects.hash(value);
  }

  @Override
  public String toString() {
    return String.valueOf(this.getValue());
  }

  /**
   * Parses text as the entity ID (same format as a long integer).
   *
   * @param text The to parse.
   *
   * @return The {@link SzEntityId} that was created.
   */
  public static SzEntityId valueOf(String text) {
    Long id = Long.valueOf(text);
    return new SzEntityId(id);
  }
}
