package com.senzing.api.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.senzing.api.model.impl.SzEntityTypesResponseDataImpl;
import com.senzing.api.model.impl.SzEntityTypesResponseImpl;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Describes the data segment for {@link SzEntityTypesResponse}.
 */
@JsonDeserialize(using= SzEntityTypesResponseData.Factory.class)
public interface SzEntityTypesResponseData {
  /**
   * Gets the unmodifiable {@link Set} of entity type codes.
   *
   * @return The unmodifiable {@link Set} of entity type codes.
   */
  Set<String> getEntityTypes();

  /**
   * Gets the unmodifiable {@link Map} of {@link String} entity type codes
   * to {@link SzEntityType} values describing the configured entity types.
   *
   * @return The unmodifiable {@link Map} of {@link String} entity type codes
   *         to {@link SzEntityType} values describing the configured entity
   *         types.
   */
  Map<String, SzEntityType> getEntityTypeDetails();

  /**
   * Adds the specified {@link SzEntityType} to the entity types for this
   * instance.
   *
   * @param entityType The {@link SzEntityType} to add to the entity types
   *                   for this instance.
   */
  void addEntityType(SzEntityType entityType);

  /**
   * Sets the entity types for this instance to those in the specified of
   * {@link Collection} of {@link SzEntityType} instances.
   *
   * @param entityTypes The {@link Collection} of entity type codes.
   */
  void setEntityTypes(Collection<? extends SzEntityType> entityTypes);

  /**
   * A {@link ModelProvider} for instances of {@link SzEntityTypesResponseData}.
   */
  interface Provider extends ModelProvider<SzEntityTypesResponseData> {
    /**
     * Creates an instance of {@link SzEntityTypesResponseData} with no
     * entity types.
     *
     * @return The {@link SzEntityTypesResponseData} instance that was
     *         created.
     */
    SzEntityTypesResponseData create();

    /**
     * Creates an instance of {@link SzEntityTypesResponseData} with the
     * entity types described by the {@link SzEntityType} instances in the
     * specified {@link Collection}.
     *
     * @param entityTypes The {@link Collection} of {@link SzEntityType}
     *                    instances describing the entity types.
     *
     * @return The {@link SzEntityTypesResponseData} instance that was
     *         created.
     */
    SzEntityTypesResponseData create(
        Collection<? extends SzEntityType> entityTypes);
  }

  /**
   * Provides a default {@link Provider} implementation for {@link
   * SzEntityTypesResponseData} that produces instances of
   * {@link SzEntityTypesResponseImpl}.
   */
  class DefaultProvider extends AbstractModelProvider<SzEntityTypesResponseData>
      implements Provider
  {
    /**
     * Default constructor.
     */
    public DefaultProvider() {
      super(SzEntityTypesResponseData.class,
            SzEntityTypesResponseDataImpl.class);
    }

    @Override
    public SzEntityTypesResponseData create() {
      return new SzEntityTypesResponseDataImpl();
    }

    @Override
    public SzEntityTypesResponseData create(
        Collection<? extends SzEntityType> entityTypes)
    {
      return new SzEntityTypesResponseDataImpl(entityTypes);
    }
  }

  /**
   * Provides a {@link ModelFactory} implementation for
   * {@link SzEntityTypesResponseData}.
   */
  class Factory extends ModelFactory<SzEntityTypesResponseData, Provider> {
    /**
     * Default constructor.  This is public and can only be called after the
     * singleton master instance is created as it inherits the same state from
     * the master instance.
     */
    public Factory() {
      super(SzEntityTypesResponseData.class);
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
     * Creates an instance of {@link SzEntityTypesResponseData} with no
     * entity types.
     *
     * @return The {@link SzEntityTypesResponseData} instance that was
     *         created.
     */
    public SzEntityTypesResponseData create() {
      return this.getProvider().create();
    }

    /**
     * Creates an instance of {@link SzEntityTypesResponseData} with the
     * entity types described by the {@link SzEntityType} instances in the
     * specified {@link Collection}.
     *
     * @param entityTypes The {@link Collection} of {@link SzEntityType}
     *                    instances describing the entity types.
     *
     * @return The {@link SzEntityTypesResponseData} instance that was
     *         created.
     */
    public SzEntityTypesResponseData create(
        Collection<? extends SzEntityType> entityTypes)
    {
      return this.getProvider().create(entityTypes);
    }
  }

  /**
   * The {@link Factory} instance for this interface.
   */
  Factory FACTORY = new Factory(new DefaultProvider());
}
