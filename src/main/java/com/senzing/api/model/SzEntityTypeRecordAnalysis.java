package com.senzing.api.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.senzing.api.model.impl.SzEntityTypeRecordAnalysisImpl;

/**
 * Describes an analysis of bulk data records associated with a specific
 * entity type (or no entity type at all).
 */
@JsonDeserialize(using=SzEntityTypeRecordAnalysis.Factory.class)
public interface SzEntityTypeRecordAnalysis {
  /**
   * Returns the entity type with which this instance was constructed.
   *
   * @return The entity type with which this instance was constructed.
   */
  String getEntityType();

  /**
   * Gets the number of records that have the associated entity type.
   *
   * @return The number of records that have the associated entity type.
   */
  int getRecordCount();

  /**
   * Sets the number of records that have the associated entity type.
   *
   * @param recordCount The number of records that have the associated
   *                    entity type.
   */
  void setRecordCount(int recordCount);

  /**
   * Increments the number of records that have the associated entity type
   * and returns the new count.
   *
   * @return The new count after incrementing.
   */
  long incrementRecordCount();

  /**
   * Increments the number of records that have the associated entity type
   * and returns the new count.
   *
   * @param increment The number of records to increment by.
   *
   * @return The new count after incrementing.
   */
  long incrementRecordCount(int increment);

  /**
   * Gets the number of records that have the associated entity type and also
   * have a <tt>"RECORD_ID"</tt>.
   *
   * @return The number of records that have the associated entity type and
   *         also have a <tt>"RECORD_ID"</tt>.
   */
  int getRecordsWithRecordIdCount();

  /**
   * Sets the number of records that have the associated entity type and also
   * have a <tt>"RECORD_ID"</tt>.
   *
   * @param recordIdCount The number of records that have the associated
   *                      entity type and also have a <tt>"RECORD_ID"</tt>.
   */
  void setRecordsWithRecordIdCount(int recordIdzzount);

  /**
   * Increments the number of records that have the associated data source
   * and also have a <tt>"RECORD_ID"</tt> and returns the new count.
   *
   * @return The new count after incrementing.
   */
  int incrementRecordsWithRecordIdCount();

  /**
   * Increments the number of records that have the associated data source
   * and also have a <tt>"RECORD_ID"</tt> and returns the new count.
   *
   * @param increment The number of records to increment by.
   *
   * @return The new count after incrementing.
   */
  int incrementRecordsWithRecordIdCount(int increment);

  /**
   * Gets the number of records that have the associated entity type and also
   * have a <tt>"DATA_SOURCE"</tt>.
   *
   * @return The number of records that have the associated entity type and
   *         also have a <tt>"DATA_SOURCE"</tt>.
   */
  int getRecordsWithDataSourceCount();

  /**
   * Sets the number of records that have the associated entity type and also
   * have a <tt>"DATA_SOURCE"</tt>.
   *
   * @param dataSourceCount The number of records that have the associated
   *                        entity type and also have a <tt>"DATA_SOURCE"</tt>.
   */
  void setRecordsWithDataSourceCount(int dataSourceCount);

  /**
   * Increments the number of records that have the associated data source
   * and also have a <tt>"DATA_SOURCE"</tt> and returns the new count.
   *
   * @return The new count after incrementing.
   */
  int incrementRecordsWithDataSourceCount();

  /**
   * Increments the number of records that have the associated data source
   * and also have a <tt>"DATA_SOURCE"</tt> and returns the new count.
   *
   * @param increment The number of records to increment by.
   *
   * @return The new count after incrementing.
   */
  int incrementRecordsWithDataSourceCount(int increment);

    /**
   * A {@link ModelProvider} for instances of {@link
   * SzEntityTypeRecordAnalysis}.
   */
  interface Provider extends ModelProvider<SzEntityTypeRecordAnalysis> {
    /**
     * Creates a new instance of {@link SzEntityTypeRecordAnalysis}.
     * @param entityType The entity type code for the new instance.
     * @return The new instance of {@link SzEntityTypeRecordAnalysis}
     */
    SzEntityTypeRecordAnalysis create(String entityType);
  }

  /**
   * Provides a default {@link Provider} implementation for {@link
   * SzEntityTypeRecordAnalysis} that produces instances of {@link
   * SzEntityTypeRecordAnalysisImpl}.
   */
  class DefaultProvider
      extends AbstractModelProvider<SzEntityTypeRecordAnalysis>
      implements Provider
  {
    /**
     * Default constructor.
     */
    public DefaultProvider() {
      super(SzEntityTypeRecordAnalysis.class,
            SzEntityTypeRecordAnalysisImpl.class);
    }

    @Override
    public SzEntityTypeRecordAnalysis create(String entityType) {
      return new SzEntityTypeRecordAnalysisImpl(entityType);
    }
  }

  /**
   * Provides a {@link ModelFactory} implementation for {@link
   * SzEntityTypeRecordAnalysis}.
   */
  class Factory extends ModelFactory<SzEntityTypeRecordAnalysis, Provider> {
    /**
     * Default constructor.  This is public and can only be called after the
     * singleton master instance is created as it inherits the same state from
     * the master instance.
     */
    public Factory() {
      super(SzEntityTypeRecordAnalysis.class);
    }

    /**
     * Constructs with the default provider.  This constructor is private and
     * is used for the master singleton instance.
     * @param defaultProvider The default provider.
     */
    private Factory(Provider defaultProvider) {
      super(defaultProvider);
    }

    /**
     * Creates a new instance of {@link SzEntityTypeRecordAnalysis}.
     * @param entityType The entity type code for the new instance.
     * @return The new instance of {@link SzEntityTypeRecordAnalysis}.
     */
    public SzEntityTypeRecordAnalysis create(String entityType)
    {
      return this.getProvider().create(entityType);
    }
  }

  /**
   * The {@link Factory} instance for this interface.
   */
  Factory FACTORY = new Factory(new DefaultProvider());

}

