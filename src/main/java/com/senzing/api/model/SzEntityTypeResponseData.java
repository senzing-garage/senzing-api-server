package com.senzing.api.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.senzing.api.model.impl.SzEntityTypeResponseDataImpl;
import com.senzing.api.model.impl.SzEntityTypeResponseImpl;

/**
 * Represents the data for {@link SzEntityTypeResponse}.
 *
 */
@JsonDeserialize(using= SzEntityTypeResponseData.Factory.class)
public interface SzEntityTypeResponseData {
  /**
   * Gets the {@link SzEntityType} describing the entity type.
   *
   * @return The {@link SzEntityType} describing the entity type.
   */
  SzEntityType getEntityType();

  /**
   * Sets the {@link SzEntityType} describing the entity type.
   *
   * @param entityType The {@link SzEntityType} describing the entity type.
   */
  void setEntityType(SzEntityType entityType);

  /**
   * A {@link ModelProvider} for instances of {@link SzEntityTypeResponseData}.
   */
  interface Provider extends ModelProvider<SzEntityTypeResponseData> {
    /**
     * Creates an instance with no entity type.
     *
     * @return The {@link SzEntityTypeResponseData} instance that was created.
     */
    SzEntityTypeResponseData create();

    /**
     * Creates an instance with the specified {@link SzEntityType}.
     *
     * @param entityType The {@link SzEntityType} for the new instance.
     *
     * @return The {@link SzEntityTypeResponseData} instance that was created.
     */
    SzEntityTypeResponseData create(SzEntityType entityType);
  }

  /**
   * Provides a default {@link Provider} implementation for {@link
   * SzEntityTypeResponseData} that produces instances of
   * {@link SzEntityTypeResponseImpl}.
   */
  class DefaultProvider extends AbstractModelProvider<SzEntityTypeResponseData>
      implements Provider
  {
    /**
     * Default constructor.
     */
    public DefaultProvider() {
      super(SzEntityTypeResponseData.class, SzEntityTypeResponseDataImpl.class);
    }

    @Override
    public SzEntityTypeResponseData create() {
      return new SzEntityTypeResponseDataImpl();
    }

    @Override
    public SzEntityTypeResponseData create(SzEntityType entityType) {
      return new SzEntityTypeResponseDataImpl(entityType);
    }
  }

  /**
   * Provides a {@link ModelFactory} implementation for
   * {@link SzEntityTypeResponseData}.
   */
  class Factory extends ModelFactory<SzEntityTypeResponseData, Provider> {
    /**
     * Default constructor.  This is public and can only be called after the
     * singleton master instance is created as it inherits the same state from
     * the master instance.
     */
    public Factory() {
      super(SzEntityTypeResponseData.class);
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
     * Creates an instance with no entity type.
     *
     * @return The {@link SzEntityTypeResponseData} instance that was created.
     */
    public SzEntityTypeResponseData create() {
      return this.getProvider().create();
    }

    /**
     * Creates an instance with the specified {@link SzEntityType}.
     *
     * @param entityType The {@link SzEntityType} for the new instance.
     *
     * @return The {@link SzEntityTypeResponseData} instance that was created.
     */
    public SzEntityTypeResponseData create(SzEntityType entityType) {
      return this.getProvider().create(entityType);
    }
  }

  /**
   * The {@link Factory} instance for this interface.
   */
  Factory FACTORY = new Factory(new DefaultProvider());
}
