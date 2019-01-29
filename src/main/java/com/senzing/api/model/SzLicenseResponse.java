package com.senzing.api.model;

import javax.ws.rs.core.UriInfo;

/**
 * A response object that contains license data.
 *
 */
public class SzLicenseResponse extends SzResponseWithRawData {
  /**
   * The {@link SzLicenseInfo} describing the license.
   */
  private SzLicenseInfo licenseInfo;

  /**
   * The data for this instance.
   */
  private Data data = new Data();

  /**
   * Constructs with only the HTTP method and the self link, leaving the
   * license info data to be initialized later.
   *
   * @param httpMethod The {@link SzHttpMethod}.
   * @param httpStatusCode The HTTP response code.
   * @param selfLink The string URL link to generate this response.
   */
  public SzLicenseResponse(SzHttpMethod httpMethod,
                           int          httpStatusCode,
                           String       selfLink) {
    this(httpMethod, httpStatusCode, selfLink, null);
  }

  /**
   * Constructs with the HTTP method, self link and the {@link SzLicenseInfo}
   * describing the license.
   *
   * @param httpMethod The {@link SzHttpMethod}.
   * @param httpStatusCode The HTTP response status code.
   * @param selfLink The string URL link to generate this response.
   * @param data The {@link SzLicenseInfo} describing the license.
   */
  public SzLicenseResponse(SzHttpMethod   httpMethod,
                           int            httpStatusCode,
                           String         selfLink,
                           SzLicenseInfo  data)
  {
    super(httpMethod, httpStatusCode, selfLink);
    this.licenseInfo = data;
  }

  /**
   * Constructs with only the HTTP method and the {@link UriInfo}, leaving the
   * license info data to be initialized later.
   *
   * @param httpMethod The {@link SzHttpMethod}.
   * @param httpStatusCode The HTTP response code.
   * @param uriInfo The {@link UriInfo} from the request.
   */
  public SzLicenseResponse(SzHttpMethod httpMethod,
                           int          httpStatusCode,
                           UriInfo      uriInfo) {
    this(httpMethod, httpStatusCode, uriInfo, null);
  }

  /**
   * Constructs with the HTTP method, {@link UriInfo} and the
   * {@link SzLicenseInfo} describing the license.
   *
   * @param httpMethod The {@link SzHttpMethod}.
   * @param httpStatusCode The HTTP response status code.
   * @param uriInfo The {@link UriInfo} from the request.
   * @param data The {@link SzLicenseInfo} describing the license.
   */
  public SzLicenseResponse(SzHttpMethod   httpMethod,
                           int            httpStatusCode,
                           UriInfo        uriInfo,
                           SzLicenseInfo  data)
  {
    super(httpMethod, httpStatusCode, uriInfo);
    this.licenseInfo = data;
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
    this.licenseInfo = data;
  }

  /**
   * Inner class to represent the data section for this response.
   */
  public class Data {
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
      return SzLicenseResponse.this.licenseInfo;
    }
  }

}
