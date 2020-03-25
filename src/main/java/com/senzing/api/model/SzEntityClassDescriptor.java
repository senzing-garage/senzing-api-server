package com.senzing.api.model;

/**
 * A tagging interface for entity identifiers.
 */
public interface SzEntityClassDescriptor extends SzJsonConvertible {
  /**
   * Implemented to return either an instance of {@link SzEntityClassCode}
   * or {@link SzEntityClass}.
   *
   * @param text The text to parse.
   *
   * @return The {@link SzEntityClassDescriptor} for the specified text.
   */
  static SzEntityClassDescriptor valueOf(String text) {
    text = text.trim();
    if (text.length() > 2 && text.startsWith("{") && text.endsWith("}")) {
      return SzEntityClass.valueOf(text);
    } else {
      return SzEntityClassCode.valueOf(text);
    }
  }

  /**
   * Converts this instance to an instance of {@link SzEntityClass}
   * which completely describes an entity class.
   *
   * @return The {@link SzEntityClass} describing the entity class.
   */
  SzEntityClass toEntityClass();
}
