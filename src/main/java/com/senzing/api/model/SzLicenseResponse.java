package com.senzing.api.model;

import com.senzing.util.Timers;

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
   * @param timers The {@link Timers} object for the timings that were taken.
   */
  public SzLicenseResponse(SzHttpMethod httpMethod,
                           int          httpStatusCode,
                           String       selfLink,
                           Timers       timers)
  {
    this(httpMethod, httpStatusCode, selfLink, timers, null);
  }

  /**
   * Constructs with the HTTP method, self link and the {@link SzLicenseInfo}
   * describing the license.
   *
   * @param httpMethod The {@link SzHttpMethod}.
   * @param httpStatusCode The HTTP response status code.
   * @param selfLink The string URL link to generate this response.
   * @param timers The {@link Timers} object for the timings that were taken.
   * @param data The {@link SzLicenseInfo} describing the license.
   */
  public SzLicenseResponse(SzHttpMethod   httpMethod,
                           int            httpStatusCode,
                           String         selfLink,
                           Timers         timers,
                           SzLicenseInfo  data)
  {
    super(httpMethod, httpStatusCode, selfLink, timers);
    this.licenseInfo = data;
  }

  /**
   * Constructs with only the HTTP method and the {@link UriInfo}, leaving the
   * license info data to be initialized later.
   *
   * @param httpMethod The {@link SzHttpMethod}.
   * @param httpStatusCode The HTTP response code.
   * @param uriInfo The {@link UriInfo} from the request.
   * @param timers The {@link Timers} object for the timings that were taken.
   *
   */
  public SzLicenseResponse(SzHttpMethod httpMethod,
                           int          httpStatusCode,
                           UriInfo      uriInfo,
                           Timers       timers)
  {
    this(httpMethod, httpStatusCode, uriInfo, timers, null);
  }

  /**
   * Constructs with the HTTP method, {@link UriInfo} and the
   * {@link SzLicenseInfo} describing the license.
   *
   * @param httpMethod The {@link SzHttpMethod}.
   * @param httpStatusCode The HTTP response status code.
   * @param uriInfo The {@link UriInfo} from the request.
   * @param timers The {@link Timers} object for the timings that were taken.
   * @param data The {@link SzLicenseInfo} describing the license.
   */
  public SzLicenseResponse(SzHttpMethod   httpMethod,
                           int            httpStatusCode,
                           UriInfo        uriInfo,
                           Timers         timers,
                           SzLicenseInfo  data)
  {
    super(httpMethod, httpStatusCode, uriInfo, timers);
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
