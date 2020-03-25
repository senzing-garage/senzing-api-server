package com.senzing.api.model;

/**
 * A tagging interface for entity identifiers.
 */
public interface SzEntityTypeDescriptor extends SzJsonConvertible {
  /**
   * Implemented to return either an instance of {@link SzEntityTypeCode}
   * or {@link SzEntityType}.
   *
   * @param text The text to parse.
   *
   * @return The {@link SzEntityTypeDescriptor} for the specified text.
   */
   static SzEntityTypeDescriptor valueOf(String text) {
     text = text.trim();
     if (text.length() > 2 && text.startsWith("{") && text.endsWith("}")) {
       return SzEntityType.valueOf(text);
     } else {
       return SzEntityTypeCode.valueOf(text);
     }
   }

  /**
   * Converts this instance to an instance of {@link SzEntityType}
   * which completely describes an entity type.
   *
   * @return The {@link SzEntityType} describing the entity type.
   */
  SzEntityType toEntityType();
}
