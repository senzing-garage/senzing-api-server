package com.senzing.api.model;

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
   * Returns the data associated with this response which is an
   * {@link SzLicenseInfo}.
   *
   * @return The data associated with this response.
   */
  public SzLicenseInfo getData() {
    return this.licenseInfo;
  }

  /**
   * Sets the data associated with this response with an {@link SzLicenseInfo}.
   *
   * @param data The {@link SzLicenseInfo} describing the license.
   */
  public void setData(SzLicenseInfo data) {
    this.licenseInfo = data;
  }
}
