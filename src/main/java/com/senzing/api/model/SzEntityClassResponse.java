package com.senzing.api.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.senzing.api.model.impl.SzEntityClassResponseImpl;
import com.senzing.util.Timers;

import javax.ws.rs.core.UriInfo;

/**
 * The response containing a set of entity class codes.  Typically this is the
 * list of all configured entity class codes.
 *
 */
@JsonDeserialize(using=SzEntityClassResponse.Factory.class)
public interface SzEntityClassResponse extends SzResponseWithRawData
{
  /**
   * Returns the {@link SzEntityClassResponseData} for this instance.
   *
   * @return The {@link SzEntityClassResponseData} for this instance.
   */
  SzEntityClassResponseData getData();

  /**
   * Sets the {@link SzEntityClassResponseData} for this instance.
   *
   * @param data The {@link SzEntityClassResponseData} for this instance.
   */
  void setData(SzEntityClassResponseData data);

  /**
   * Convenience method to set the {@link SzEntityClass} on the underlying
   * {@link SzEntityClassResponseData}.
   *
   * @param entityClass The entity class code to add.
   */
  void setEntityClass(SzEntityClass entityClass);

  /**
   * A {@link ModelProvider} for instances of {@link SzEntityClassResponse}.
   */
  interface Provider extends ModelProvider<SzEntityClassResponse> {
    /**
     * Creates an instance of {@link SzEntityClassResponse} with the specified
     * {@link SzMeta} and {@link SzLinks}.
     *
     * @param meta The response meta data.
     *
     * @param links The links for the response.
     */
    SzEntityClassResponse create(SzMeta meta, SzLinks links);

    /**
     * Creates an instance of {@link SzEntityClassResponse} with the specified
     * {@link SzMeta}, {@link SzLinks} and {@link SzEntityClassResponseData}.
     *
     * @param meta The response meta data.
     *
     * @param links The links for the response.
     *
     * @param data The data for this response.
     */
    SzEntityClassResponse create(SzMeta                     meta,
                                 SzLinks                    links,
                                 SzEntityClassResponseData  data);
  }

  /**
   * Provides a default {@link Provider} implementation for {@link
   * SzEntityClassResponse} that produces instances of
   * {@link SzEntityClassResponseImpl}.
   */
  class DefaultProvider extends AbstractModelProvider<SzEntityClassResponse>
      implements Provider
  {
    /**
     * Default constructor.
     */
    public DefaultProvider() {
      super(SzEntityClassResponse.class, SzEntityClassResponseImpl.class);
    }

    @Override
    public SzEntityClassResponse create(SzMeta meta, SzLinks links) {
      return new SzEntityClassResponseImpl(meta, links);
    }

    @Override
    public SzEntityClassResponse create(SzMeta                    meta,
                                        SzLinks                   links,
                                        SzEntityClassResponseData data)
    {
      return new SzEntityClassResponseImpl(meta, links, data);
    }

  }

  /**
   * Provides a {@link ModelFactory} implementation for
   * {@link SzEntityClassResponse}.
   */
  class Factory extends ModelFactory<SzEntityClassResponse, Provider> {
    /**
     * Default constructor.  This is public and can only be called after the
     * singleton master instance is created as it inherits the same state from
     * the master instance.
     */
    public Factory() {
      super(SzEntityClassResponse.class);
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
     * Creates an instance of {@link SzEntityClassResponse} with the specified
     * {@link SzMeta} and {@link SzLinks}.
     *
     * @param meta The response meta data.
     *
     * @param links The links for the response.
     */
    public SzEntityClassResponse create(SzMeta meta, SzLinks links) {
      return this.getProvider().create(meta, links);
    }

    /**
     * Creates an instance of {@link SzEntityClassResponse} with the specified
     * {@link SzMeta} and {@link SzLinks}.
     *
     * @param meta The response meta data.
     *
     * @param links The links for the response.
     *
     * @param data The data for this response.
     */
    public SzEntityClassResponse create(SzMeta                    meta,
                                        SzLinks                   links,
                                        SzEntityClassResponseData data)
    {
      return this.getProvider().create(meta, links, data);
    }

  }

  /**
   * The {@link Factory} instance for this interface.
   */
  Factory FACTORY = new Factory(new DefaultProvider());
}
