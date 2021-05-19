package com.senzing.api.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.senzing.util.Timers;

import javax.ws.rs.core.UriInfo;

/**
 * A response object that contains license data.
 *
 */
public class SzLicenseResponse extends SzResponseWithRawData {
  /**
   * The data for this instance.
   */
  private Data data = new Data();

  /**
   * Default constructor.
   */
  SzLicenseResponse() {
    // do nothing
  }

  /**
   * Constructs with only the HTTP method and the self link, leaving the
   * license info data to be initialized later.
   *
   * @param meta The response meta data.
   *
   * @param links The links for the response.
   */
  public SzLicenseResponse(SzMeta meta, SzLinks links)
  {
    this(meta, links, null);
  }

  /**
   * Constructs with the HTTP method, self link and the {@link SzLicenseInfo}
   * describing the license.
   *
   * @param meta The response meta data.
   *
   * @param links The links for the response.
   *
   * @param data The {@link SzLicenseInfo} describing the license.
   */
  public SzLicenseResponse(SzMeta meta, SzLinks links, SzLicenseInfo data)
  {
    super(meta, links);
    this.data.license = data;
  }

  /**
   * Returns the {@link Data} associated with this response which contains an
   * {@link SzLicenseInfo}.
   *
   * @return The data associated with this response.
   */
  public Data getData() {
    return this.data;
  }

  /**
   * Sets the data associated with this response with an {@link SzLicenseInfo}.
   *
   * @param data The {@link SzLicenseInfo} describing the license.
   */
  public void setLicense(SzLicenseInfo data) {
    this.data.license = data;
  }

  /**
   * Inner class to represent the data section for this response.
   */
  public static class Data {
    /**
     * The {@link SzLicenseInfo} describing the license.
     */
    private SzLicenseInfo license;

    /**
     * Private default constructor.
     */
    private Data() {
      // do nothing
    }

    /**
     * Gets the {@link SzLicenseInfo} describing the license.
     *
     * @return The {@link SzLicenseInfo} describing the license.
     */
    public SzLicenseInfo getLicense() {
      return this.license;
    }
  }

}
