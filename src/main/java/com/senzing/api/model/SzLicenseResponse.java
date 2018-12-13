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
   * @param method The {@link SzHttpMethod}.
   * @param selfLink The string URL link to generate this response.
   */
  public SzLicenseResponse(SzHttpMethod method,
                           String       selfLink) {
    this(method, selfLink, null);
  }

  /**
   * Constructs with the HTTP method, self link and the {@link SzLicenseInfo}
   * describing the license.
   *
   * @param method The {@link SzHttpMethod}.
   * @param selfLink The string URL link to generate this response.
   * @param data The {@link SzLicenseInfo} describing the license.
   */
  public SzLicenseResponse(SzHttpMethod   method,
                           String         selfLink,
                           SzLicenseInfo  data)
  {
    super(method, selfLink);
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
