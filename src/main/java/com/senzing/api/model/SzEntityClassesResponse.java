package com.senzing.api.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.senzing.api.model.impl.SzEntityClassesResponseImpl;
import java.util.*;

/**
 * The response containing a set of entity class codes.  Typically this is the
 * list of all configured entity class codes.
 *
 */
@JsonDeserialize(using=SzEntityClassesResponse.Factory.class)
public interface SzEntityClassesResponse extends SzResponseWithRawData
{
  /**
   * Returns the {@link SzEntityClassesResponseData} for this instance.
   *
   * @return The {@link SzEntityClassesResponseData} for this instance.
   */
  SzEntityClassesResponseData getData();

  /**
   * Sets the {@link SzEntityClassesResponseData} for this instance.
   *
   * @param data The {@link SzEntityClassesResponseData} for this instance.
   */
  void setData(SzEntityClassesResponseData data);

  /**
   * Convenience method to add the specified entity class to the list of
   * entity classes of the underlying {@link SzEntityClassesResponseData}.
   *
   * @param entityClass The entity class code to add.
   */
  void addEntityClass(SzEntityClass entityClass);

  /**
   * Convenience method to set the entity classes on the underlying
   * {@link SzEntityClassesResponseData} to those in the specified collection.
   *
   * @param entityClasses The {@link Collection} of entity classs to set.
   */
  void setEntityClasses(Collection<? extends SzEntityClass> entityClasses);

  /**
   * A {@link ModelProvider} for instances of {@link SzEntityClassesResponse}.
   */
  interface Provider extends ModelProvider<SzEntityClassesResponse> {
    /**
     * Creates an instance of {@link SzEntityClassesResponse} with the specified
     * {@link SzMeta} and {@link SzLinks}.
     *
     * @param meta The response meta data.
     *
     * @param links The links for the response.
     */
    SzEntityClassesResponse create(SzMeta meta, SzLinks links);

    /**
     * Creates an instance of {@link SzEntityClassesResponse} with the specified
     * {@link SzMeta}, {@link SzLinks} and {@link SzEntityClassesResponseData}.
     *
     * @param meta The response meta data.
     *
     * @param links The links for the response.
     *
     * @param data The data for the response.
     */
    SzEntityClassesResponse create(SzMeta                       meta,
                                   SzLinks                      links,
                                   SzEntityClassesResponseData  data);
  }

  /**
   * Provides a default {@link Provider} implementation for {@link
   * SzEntityClassesResponse} that produces instances of
   * {@link SzEntityClassesResponseImpl}.
   */
  class DefaultProvider extends AbstractModelProvider<SzEntityClassesResponse>
      implements Provider
  {
    /**
     * Default constructor.
     */
    public DefaultProvider() {
      super(SzEntityClassesResponse.class, SzEntityClassesResponseImpl.class);
    }

    @Override
    public SzEntityClassesResponse create(SzMeta meta, SzLinks links) {
      return new SzEntityClassesResponseImpl(meta, links);
    }

    @Override
    public SzEntityClassesResponse create(SzMeta                      meta,
                                          SzLinks                     links,
                                          SzEntityClassesResponseData data)
    {
      return new SzEntityClassesResponseImpl(meta, links, data);
    }
  }

  /**
   * Provides a {@link ModelFactory} implementation for
   * {@link SzEntityClassesResponse}.
   */
  class Factory extends ModelFactory<SzEntityClassesResponse, Provider> {
    /**
     * Default constructor.  This is public and can only be called after the
     * singleton master instance is created as it inherits the same state from
     * the master instance.
     */
    public Factory() {
      super(SzEntityClassesResponse.class);
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
     * Creates an instance of {@link SzEntityClassesResponse} with the specified
     * {@link SzMeta} and {@link SzLinks}.
     *
     * @param meta The response meta data.
     *
     * @param links The links for the response.
     */
    public SzEntityClassesResponse create(SzMeta meta, SzLinks links) {
      return this.getProvider().create(meta, links);
    }

    /**
     * Creates an instance of {@link SzEntityClassesResponse} with the specified
     * {@link SzMeta} and {@link SzLinks}.
     *
     * @param meta The response meta data.
     *
     * @param links The links for the response.
     *
     * @param data The data for the response.
     */
    public SzEntityClassesResponse create(SzMeta                      meta,
                                          SzLinks                     links,
                                          SzEntityClassesResponseData data)
    {
      return this.getProvider().create(meta, links, data);
    }
  }

  /**
   * The {@link Factory} instance for this interface.
   */
  Factory FACTORY = new Factory(new DefaultProvider());
}
