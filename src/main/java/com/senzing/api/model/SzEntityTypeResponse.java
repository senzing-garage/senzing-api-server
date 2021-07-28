package com.senzing.api.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.senzing.api.model.impl.SzEntityTypeResponseImpl;
import com.senzing.util.Timers;

import javax.ws.rs.core.UriInfo;

/**
 * The response containing a set of entity type codes.  Typically this is the
 * list of all configured entity type codes.
 *
 */
@JsonDeserialize(using=SzEntityTypeResponse.Factory.class)
public interface SzEntityTypeResponse extends SzResponseWithRawData {
  /**
   * Returns the {@link SzEntityTypeResponseData} for this instance.
   *
   * @return The {@link SzEntityTypeResponseData} for this instance.
   */
  SzEntityTypeResponseData getData();

  /**
   * Sets the {@link SzEntityTypeResponseData} for this instance.
   *
   * @param data The {@link SzEntityTypeResponseData} for this instance.
   */
  void setData(SzEntityTypeResponseData data);

  /**
   * Convenience method to set the entity type on the underlying {@link
   * SzEntityTypeResponseData} to the specified {@link SzEntityType}.
   *
   * @param entityType The entity type code to add.
   */
  void setEntityType(SzEntityType entityType);

  /**
   * A {@link ModelProvider} for instances of {@link SzEntityTypeResponse}.
   */
  interface Provider extends ModelProvider<SzEntityTypeResponse> {
    /**
     * Creates an instance of {@link SzEntityTypeResponse} with the specified
     * {@link SzMeta} and {@link SzLinks}.
     *
     * @param meta The response meta data.
     *
     * @param links The links for the response.
     */
    SzEntityTypeResponse create(SzMeta meta, SzLinks links);

    /**
     * Creates an instance of {@link SzEntityTypeResponse} with the specified
     * {@link SzMeta}, {@link SzLinks} and {@link SzEntityTypeResponseData}.
     *
     * @param meta The response meta data.
     *
     * @param links The links for the response.
     *
     * @param data The data for the response.
     */
    SzEntityTypeResponse create(SzMeta                    meta,
                                SzLinks                   links,
                                SzEntityTypeResponseData  data);
  }

  /**
   * Provides a default {@link Provider} implementation for {@link
   * SzEntityTypeResponse} that produces instances of
   * {@link SzEntityTypeResponseImpl}.
   */
  class DefaultProvider extends AbstractModelProvider<SzEntityTypeResponse>
      implements Provider
  {
    /**
     * Default constructor.
     */
    public DefaultProvider() {
      super(SzEntityTypeResponse.class, SzEntityTypeResponseImpl.class);
    }

    @Override
    public SzEntityTypeResponse create(SzMeta meta, SzLinks links) {
      return new SzEntityTypeResponseImpl(meta, links);
    }

    @Override
    public SzEntityTypeResponse create(SzMeta                    meta,
                                       SzLinks                   links,
                                       SzEntityTypeResponseData  data)
    {
      return new SzEntityTypeResponseImpl(meta, links, data);
    }

  }

  /**
   * Provides a {@link ModelFactory} implementation for
   * {@link SzEntityTypeResponse}.
   */
  class Factory extends ModelFactory<SzEntityTypeResponse, Provider> {
    /**
     * Default constructor.  This is public and can only be called after the
     * singleton master instance is created as it inherits the same state from
     * the master instance.
     */
    public Factory() {
      super(SzEntityTypeResponse.class);
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
     * Creates an instance of {@link SzEntityTypeResponse} with the specified
     * {@link SzMeta} and {@link SzLinks}.
     *
     * @param meta The response meta data.
     *
     * @param links The links for the response.
     */
    public SzEntityTypeResponse create(SzMeta meta, SzLinks links) {
      return this.getProvider().create(meta, links);
    }

    /**
     * Creates an instance of {@link SzEntityTypeResponse} with the specified
     * {@link SzMeta}, {@link SzLinks} and {@link SzEntityTypeResponseData}.
     *
     * @param meta The response meta data.
     *
     * @param links The links for the response.
     *
     * @param data The data for the response.
     */
    public SzEntityTypeResponse create(SzMeta                   meta,
                                       SzLinks                  links,
                                       SzEntityTypeResponseData data)
    {
      return this.getProvider().create(meta, links, data);
    }
  }

  /**
   * The {@link Factory} instance for this interface.
   */
  Factory FACTORY = new Factory(new DefaultProvider());
}
