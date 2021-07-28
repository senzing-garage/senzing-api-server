package com.senzing.api.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.senzing.api.model.impl.SzEntityClassResponseDataImpl;
import com.senzing.api.model.impl.SzEntityClassResponseImpl;

/**
 * Represents the data for {@link SzEntityClassResponse}.
 *
 */
@JsonDeserialize(using= SzEntityClassResponseData.Factory.class)
public interface SzEntityClassResponseData {
  /**
   * Gets the {@link SzEntityClass} describing the entity class.
   *
   * @return The {@link SzEntityClass} describing the entity class.
   */
  SzEntityClass getEntityClass();

  /**
   * Sets the {@link SzEntityClass} describing the entity class.
   *
   * @param entityClass The {@link SzEntityClass} describing the entity class.
   */
  void setEntityClass(SzEntityClass entityClass);

  /**
   * A {@link ModelProvider} for instances of {@link SzEntityClassResponseData}.
   */
  interface Provider extends ModelProvider<SzEntityClassResponseData> {
    /**
     * Creates an instance with no entity class.
     *
     * @return The {@link SzEntityClassResponseData} instance that was created.
     */
    SzEntityClassResponseData create();

    /**
     * Creates an instance with the specified {@link SzEntityClass}.
     *
     * @param entityClass The {@link SzEntityClass} for the new instance.
     *
     * @return The {@link SzEntityClassResponseData} instance that was created.
     */
    SzEntityClassResponseData create(SzEntityClass entityClass);
  }

  /**
   * Provides a default {@link Provider} implementation for {@link
   * SzEntityClassResponseData} that produces instances of
   * {@link SzEntityClassResponseImpl}.
   */
  class DefaultProvider extends AbstractModelProvider<SzEntityClassResponseData>
      implements Provider
  {
    /**
     * Default constructor.
     */
    public DefaultProvider() {
      super(SzEntityClassResponseData.class, SzEntityClassResponseDataImpl.class);
    }

    @Override
    public SzEntityClassResponseData create() {
      return new SzEntityClassResponseDataImpl();
    }

    @Override
    public SzEntityClassResponseData create(SzEntityClass entityClass) {
      return new SzEntityClassResponseDataImpl(entityClass);
    }
  }

  /**
   * Provides a {@link ModelFactory} implementation for
   * {@link SzEntityClassResponseData}.
   */
  class Factory extends ModelFactory<SzEntityClassResponseData, Provider> {
    /**
     * Default constructor.  This is public and can only be called after the
     * singleton master instance is created as it inherits the same state from
     * the master instance.
     */
    public Factory() {
      super(SzEntityClassResponseData.class);
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
     * Creates an instance with no entity class.
     *
     * @return The {@link SzEntityClassResponseData} instance that was created.
     */
    public SzEntityClassResponseData create() {
      return this.getProvider().create();
    }

    /**
     * Creates an instance with the specified {@link SzEntityClass}.
     *
     * @param entityClass The {@link SzEntityClass} for the new instance.
     *
     * @return The {@link SzEntityClassResponseData} instance that was created.
     */
    public SzEntityClassResponseData create(SzEntityClass entityClass) {
      return this.getProvider().create(entityClass);
    }
  }

  /**
   * The {@link Factory} instance for this interface.
   */
  Factory FACTORY = new Factory(new DefaultProvider());
}
