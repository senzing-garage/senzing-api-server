package com.senzing.datagen;

/**
 * The strategy for creating the <tt>ENTITY_TYPE</tt> field for the generated
 * records.
 */
public enum EntityTypeStrategy {
  /**
   * No entity type should be generated for the record.
   */
  NONE,

  /**
   * The generic entity type should be used.
   */
  GENERIC,

  /**
   * The entity type should be set the same as the {@link RecordType}.
   */
  RECORD_TYPE,

  /**
   * The entity type should be set the same as the data source.
   */
  DATA_SOURCE;
}
