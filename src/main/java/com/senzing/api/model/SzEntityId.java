package com.senzing.api.model;

import com.senzing.util.JsonUtils;

import javax.json.JsonObject;
import java.util.Objects;

/**
 * Describes an entity ID to identify an entity.
 */
public class SzEntityId implements SzEntityIdentifier {
  /**
   * The entity ID that identifies the entity.
   */
  private long entityId;

  /**
   * Default constructor.
   */
  public SzEntityId(long id) {
    this.entityId = id;
  }

  /**
   * Return the entity ID identifying the entity.
   *
   * @return The entity ID identifying the entity.
   */
  public long getValue() {
    return this.entityId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SzEntityId that = (SzEntityId) o;
    return entityId == that.entityId;
  }

  @Override
  public int hashCode() {
    return Objects.hash(entityId);
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
