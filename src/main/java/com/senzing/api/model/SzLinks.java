package com.senzing.api.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.senzing.api.model.impl.SzLinksImpl;

import javax.ws.rs.core.UriInfo;

/**
 * Describes the links section of the response.
 */
@JsonDeserialize(using=SzLinks.Factory.class)
public interface SzLinks {
  /**
   * Gets the self link.
   *
   * @return The self link.
   */
  String getSelf();

  /**
   * Sets the self link.
   *
   * @param self The self link.
   */
  void setSelf(String self);

  /**
   * A {@link ModelProvider} for instances of {@link SzLinks}.
   */
  interface Provider extends ModelProvider<SzLinks> {
    /**
     * Creates a new instance of {@link SzLinks} with the specified self link.
     *
     * @param self The self link.
     */
    SzLinks create(String self);

    /**
     * Creates a new instance of {@link SzLinks} with the specified {@link
     * UriInfo} for obtaining the self link.
     *
     * @param uriInfo The {@link UriInfo} for obtaining the self link.
     *
     */
    SzLinks create(UriInfo uriInfo);
  }

  /**
   * Provides a default {@link Provider} implementation for {@link SzLinks}
   * that produces instances of {@link SzLinksImpl}.
   */
  class DefaultProvider extends AbstractModelProvider<SzLinks>
      implements Provider
  {
    /**
     * Default constructor.
     */
    public DefaultProvider() {
      super(SzLinks.class, SzLinksImpl.class);
    }

    @Override
    public SzLinks create(String self) {
      return new SzLinksImpl(self);
    }

    @Override
    public SzLinks create(UriInfo uriInfo) {
      return new SzLinksImpl(uriInfo);
    }
  }

  /**
   * Provides a {@link ModelFactory} implementation for {@link SzMeta}.
   */
  class Factory extends ModelFactory<SzLinks, Provider> {
    /**
     * Default constructor.  This is public and can only be called after the
     * singleton master instance is created as it inherits the same state from
     * the master instance.
     */
    public Factory() {
      super(SzLinks.class);
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
     * Creates a new instance of {@link SzLinks} with the specified parameters.
     *
     * @param self The self link.
     *
     * @return The new instance of {@link SzLinks}
     */
    public SzLinks create(String self)
    {
      return this.getProvider().create(self);
    }

    /**
     * Creates a new instance of {@link SzLinks} with the specified parameters.
     *
     * @param uriInfo The {@link UriInfo} for obtaining the self link.
     *
     * @return The new instance of {@link SzLinks}
     */
    public SzLinks create(UriInfo uriInfo)
    {
      return this.getProvider().create(uriInfo);
    }
  }

  /**
   * The {@link Factory} instance for this interface.
   */
  Factory FACTORY = new Factory(new DefaultProvider());
}
