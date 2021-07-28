package com.senzing.api.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.senzing.api.model.impl.SzEntityClassesResponseDataImpl;
import com.senzing.api.model.impl.SzEntityClassesResponseImpl;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Describes the data segment for {@link SzEntityClassesResponse}.
 */
@JsonDeserialize(using= SzEntityClassesResponseData.Factory.class)
public interface SzEntityClassesResponseData {
  /**
   * Gets the unmodifiable {@link Set} of entity class codes.
   *
   * @return The unmodifiable {@link Set} of entity class codes.
   */
  Set<String> getEntityClasses();

  /**
   * Gets the unmodifiable {@link Map} of {@link String} entity class codes
   * to {@link SzEntityClass} values describing the configured entity classes.
   *
   * @return The unmodifiable {@link Map} of {@link String} entity class codes
   *         to {@link SzEntityClass} values describing the configured entity
   *         classes.
   */
  Map<String, SzEntityClass> getEntityClassDetails();

  /**
   * Adds the specified {@link SzEntityClass} to the entity classes for this
   * instance.
   *
   * @param entityClass The {@link SzEntityClass} to add to the entity classes
   *                   for this instance.
   */
  void addEntityClass(SzEntityClass entityClass);

  /**
   * Sets the entity classes for this instance to those in the specified of
   * {@link Collection} of {@link SzEntityClass} instances.
   *
   * @param entityClasses The {@link Collection} of entity class codes.
   */
  void setEntityClasses(Collection<? extends SzEntityClass> entityClasses);

  /**
   * A {@link ModelProvider} for instances of {@link SzEntityClassesResponseData}.
   */
  interface Provider extends ModelProvider<SzEntityClassesResponseData> {
    /**
     * Creates an instance of {@link SzEntityClassesResponseData} with no
     * entity classes.
     *
     * @return The {@link SzEntityClassesResponseData} instance that was
     *         created.
     */
    SzEntityClassesResponseData create();

    /**
     * Creates an instance of {@link SzEntityClassesResponseData} with the
     * entity classes described by the {@link SzEntityClass} instances in the
     * specified {@link Collection}.
     *
     * @param entityClasses The {@link Collection} of {@link SzEntityClass}
     *                    instances describing the entity classes.
     *
     * @return The {@link SzEntityClassesResponseData} instance that was
     *         created.
     */
    SzEntityClassesResponseData create(
        Collection<? extends SzEntityClass> entityClasses);
  }

  /**
   * Provides a default {@link Provider} implementation for {@link
   * SzEntityClassesResponseData} that produces instances of
   * {@link SzEntityClassesResponseImpl}.
   */
  class DefaultProvider extends AbstractModelProvider<SzEntityClassesResponseData>
      implements Provider
  {
    /**
     * Default constructor.
     */
    public DefaultProvider() {
      super(SzEntityClassesResponseData.class,
            SzEntityClassesResponseDataImpl.class);
    }

    @Override
    public SzEntityClassesResponseData create() {
      return new SzEntityClassesResponseDataImpl();
    }

    @Override
    public SzEntityClassesResponseData create(
        Collection<? extends SzEntityClass> entityClasses)
    {
      return new SzEntityClassesResponseDataImpl(entityClasses);
    }
  }

  /**
   * Provides a {@link ModelFactory} implementation for
   * {@link SzEntityClassesResponseData}.
   */
  class Factory extends ModelFactory<SzEntityClassesResponseData, Provider> {
    /**
     * Default constructor.  This is public and can only be called after the
     * singleton master instance is created as it inherits the same state from
     * the master instance.
     */
    public Factory() {
      super(SzEntityClassesResponseData.class);
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
     * Creates an instance of {@link SzEntityClassesResponseData} with no
     * entity classes.
     *
     * @return The {@link SzEntityClassesResponseData} instance that was
     *         created.
     */
    public SzEntityClassesResponseData create() {
      return this.getProvider().create();
    }

    /**
     * Creates an instance of {@link SzEntityClassesResponseData} with the
     * entity classes described by the {@link SzEntityClass} instances in the
     * specified {@link Collection}.
     *
     * @param entityClasses The {@link Collection} of {@link SzEntityClass}
     *                    instances describing the entity classes.
     *
     * @return The {@link SzEntityClassesResponseData} instance that was
     *         created.
     */
    public SzEntityClassesResponseData create(
        Collection<? extends SzEntityClass> entityClasses)
    {
      return this.getProvider().create(entityClasses);
    }
  }

  /**
   * The {@link Factory} instance for this interface.
   */
  Factory FACTORY = new Factory(new DefaultProvider());
}
