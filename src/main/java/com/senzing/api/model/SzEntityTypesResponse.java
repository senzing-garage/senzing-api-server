package com.senzing.api.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.senzing.api.model.impl.SzEntityTypesResponseImpl;
import com.senzing.util.Timers;

import javax.ws.rs.core.UriInfo;
import java.util.*;

/**
 * The response containing a set of entity type codes.  Typically this is the
 * list of all configured entity type codes.
 *
 */
@JsonDeserialize(using=SzEntityTypesResponse.Factory.class)
public interface SzEntityTypesResponse extends SzResponseWithRawData
{
  /**
   * Returns the {@link SzEntityTypesResponseData} for this instance.
   *
   * @return The {@link SzEntityTypesResponseData} for this instance.
   */
  SzEntityTypesResponseData getData();

  /**
   * Sets the {@link SzEntityTypesResponseData} for this instance.
   *
   * @param data The {@link SzEntityTypesResponseData} for this instance.
   */
  void setData(SzEntityTypesResponseData data);

  /**
   * Adds the specified entity type providing the specified entity type
   * is not already containe
   *
   * @param entityType The entity type code to add.
   */
  void addEntityType(SzEntityType entityType);

  /**
   * Sets the specified {@link Set} of entity types using the
   * specified {@link Collection} of entity types (removing duplicates).
   *
   * @param entityTypes The {@link Collection} of entity types to set.
   */
  void setEntityTypes(Collection<SzEntityType> entityTypes);

  /**
   * A {@link ModelProvider} for instances of {@link SzEntityTypesResponse}.
   */
  interface Provider extends ModelProvider<SzEntityTypesResponse> {
    /**
     * Creates an instance of {@link SzEntityTypesResponse} with the specified
     * {@link SzMeta} and {@link SzLinks}.
     *
     * @param meta The response meta data.
     *
     * @param links The links for the response.
     */
    SzEntityTypesResponse create(SzMeta meta, SzLinks links);

    /**
     * Creates an instance of {@link SzEntityTypesResponse} with the specified
     * {@link SzMeta}, {@link SzLinks} and {@link SzEntityTypesResponseData}.
     *
     * @param meta The response meta data.
     *
     * @param links The links for the response.
     *
     * @param data The data for the response.
     */
    SzEntityTypesResponse create(SzMeta                     meta,
                                 SzLinks                    links,
                                 SzEntityTypesResponseData  data);
  }

  /**
   * Provides a default {@link Provider} implementation for {@link
   * SzEntityTypesResponse} that produces instances of
   * {@link SzEntityTypesResponseImpl}.
   */
  class DefaultProvider extends AbstractModelProvider<SzEntityTypesResponse>
      implements Provider
  {
    /**
     * Default constructor.
     */
    public DefaultProvider() {
      super(SzEntityTypesResponse.class, SzEntityTypesResponseImpl.class);
    }

    @Override
    public SzEntityTypesResponse create(SzMeta meta, SzLinks links) {
      return new SzEntityTypesResponseImpl(meta, links);
    }

    @Override
    public SzEntityTypesResponse create(SzMeta                    meta,
                                        SzLinks                   links,
                                        SzEntityTypesResponseData data)
    {
      return new SzEntityTypesResponseImpl(meta, links, data);
    }

  }

  /**
   * Provides a {@link ModelFactory} implementation for
   * {@link SzEntityTypesResponse}.
   */
  class Factory extends ModelFactory<SzEntityTypesResponse, Provider> {
    /**
     * Default constructor.  This is public and can only be called after the
     * singleton master instance is created as it inherits the same state from
     * the master instance.
     */
    public Factory() {
      super(SzEntityTypesResponse.class);
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
     * Creates an instance of {@link SzEntityTypesResponse} with the specified
     * {@link SzMeta} and {@link SzLinks}.
     *
     * @param meta The response meta data.
     *
     * @param links The links for the response.
     */
    public SzEntityTypesResponse create(SzMeta meta, SzLinks links) {
      return this.getProvider().create(meta, links);
    }

    /**
     * Creates an instance of {@link SzEntityTypesResponse} with the specified
     * {@link SzMeta}, {@link SzLinks} and {@link SzEntityTypesResponseData}.
     *
     * @param meta The response meta data.
     *
     * @param links The links for the response.
     *
     * @param data The data for the response.
     */
    public SzEntityTypesResponse create(SzMeta                    meta,
                                        SzLinks                   links,
                                        SzEntityTypesResponseData data)
    {
      return this.getProvider().create(meta, links, data);
    }

  }

  /**
   * The {@link Factory} instance for this interface.
   */
  Factory FACTORY = new Factory(new DefaultProvider());
}
