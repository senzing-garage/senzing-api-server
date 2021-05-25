package com.senzing.api.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.senzing.api.model.impl.SzEntityTypeBulkLoadResultImpl;

/**
 * Describes an analysis of bulk data records associated with a specific
 * entity type (or no entity type at all).
 */
@JsonDeserialize(using=SzEntityTypeBulkLoadResult.Factory.class)
public interface SzEntityTypeBulkLoadResult extends SzBaseBulkLoadResult {
  /**
   * Returns the entity type with which this instance was constructed.
   *
   * @return The entity type with which this instance was constructed.
   */
  String getEntityType();

  /**
   * A {@link ModelProvider} for instances of {@link
   * SzEntityTypeBulkLoadResult}.
   */
  interface Provider extends ModelProvider<SzEntityTypeBulkLoadResult> {
    /**
     * Creates a new instance of {@link SzEntityTypeBulkLoadResult}.
     * @param entityType The entity type code for the new instance.
     * @return The new instance of {@link SzEntityTypeBulkLoadResult}
     */
    SzEntityTypeBulkLoadResult create(String entityType);
  }

  /**
   * Provides a default {@link Provider} implementation for {@link
   * SzEntityTypeBulkLoadResult} that produces instances of {@link
   * SzEntityTypeBulkLoadResultImpl}.
   */
  class DefaultProvider
      extends AbstractModelProvider<SzEntityTypeBulkLoadResult>
      implements Provider
  {
    /**
     * Default constructor.
     */
    public DefaultProvider() {
      super(SzEntityTypeBulkLoadResult.class,
            SzEntityTypeBulkLoadResultImpl.class);
    }

    @Override
    public SzEntityTypeBulkLoadResult create(String entityType) {
      return new SzEntityTypeBulkLoadResultImpl(entityType);
    }
  }

  /**
   * Provides a {@link ModelFactory} implementation for {@link
   * SzEntityTypeBulkLoadResult}.
   */
  class Factory extends ModelFactory<SzEntityTypeBulkLoadResult, Provider> {
    /**
     * Default constructor.  This is public and can only be called after the
     * singleton master instance is created as it inherits the same state from
     * the master instance.
     */
    public Factory() {
      super(SzEntityTypeBulkLoadResult.class);
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
     * Creates a new instance of {@link SzEntityTypeBulkLoadResult}.
     * @param entityType The entity type code for the new instance.
     * @return The new instance of {@link SzEntityTypeBulkLoadResult}.
     */
    public SzEntityTypeBulkLoadResult create(String entityType)
    {
      return this.getProvider().create(entityType);
    }
  }

  /**
   * The {@link Factory} instance for this interface.
   */
  Factory FACTORY = new Factory(new DefaultProvider());
}

