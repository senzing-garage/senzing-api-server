package com.senzing.api.services;

import com.senzing.api.BuildInfo;
import com.senzing.api.model.*;

import javax.json.JsonObject;
import javax.ws.rs.core.UriInfo;

import com.senzing.g2.engine.G2Product;
import com.senzing.g2.engine.G2ProductJNI;
import com.senzing.util.JsonUtils;
import org.junit.jupiter.api.*;

import static com.senzing.api.model.SzHttpMethod.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.TestInstance.*;

@TestInstance(Lifecycle.PER_CLASS)
public class AdminServicesTest extends AbstractServiceTest {
  private AdminServices adminServices;

  @BeforeAll public void initializeEnvironment() {
    this.initializeTestEnvironment();
    this.adminServices = new AdminServices();
  }

  @AfterAll public void teardownEnvironment() {
    this.teardownTestEnvironment(true);
  }

  @Test public void heartbeatTest() {
    this.assumeNativeApiAvailable();
    String  uriText = this.formatServerUri("heartbeat");
    UriInfo uriInfo = this.newProxyUriInfo(uriText);

    long            before    = System.currentTimeMillis();
    SzBasicResponse response  = this.adminServices.heartbeat(uriInfo);
    response.concludeTimers();
    long            after     = System.currentTimeMillis();

    this.validateBasics(response, uriText, before, after);
  }

  @Test public void heartbeatViaHttpTest() {
    this.assumeNativeApiAvailable();
    String  uriText = this.formatServerUri("heartbeat");
    long    before  = System.currentTimeMillis();
    SzBasicResponse response
        = this.invokeServerViaHttp(GET, uriText, SzBasicResponse.class);
    long after = System.currentTimeMillis();

    this.validateBasics(response, uriText, before, after);
  }

  @Test public void licenseTest() {
    this.assumeNativeApiAvailable();
    String  uriText = this.formatServerUri("license");
    UriInfo uriInfo = this.newProxyUriInfo(uriText);

    long              before    = System.currentTimeMillis();
    SzLicenseResponse response  = this.adminServices.license(false, uriInfo);
    response.concludeTimers();
    long              after     = System.currentTimeMillis();

    this.validateLicenseResponse(response,
                                 before,
                                 after,
                                 null,
                                 "EVAL",
                                 10000);
  }

  @Test public void licenseViaHttpTest() {
    this.assumeNativeApiAvailable();
    String  uriText = this.formatServerUri("license");
    UriInfo uriInfo = this.newProxyUriInfo(uriText);

    long before = System.currentTimeMillis();
    SzLicenseResponse response
        = this.invokeServerViaHttp(GET, uriText, SzLicenseResponse.class);
    long after = System.currentTimeMillis();

    this.validateLicenseResponse(response,
                                 before,
                                 after,
                                 null,
                                 "EVAL",
                                 10000);
  }

  @Test public void licenseWithoutRawTest() {
    this.assumeNativeApiAvailable();
    String  uriText = this.formatServerUri("license?withRaw=false");
    UriInfo uriInfo = this.newProxyUriInfo(uriText);

    long              before    = System.currentTimeMillis();
    SzLicenseResponse response  = this.adminServices.license(false, uriInfo);
    response.concludeTimers();
    long              after     = System.currentTimeMillis();

    this.validateLicenseResponse(response,
                                 before,
                                 after,
                                 false,
                                 "EVAL",
                                 10000);
  }

  @Test public void licenseWithoutRawViaHttpTest() {
    this.assumeNativeApiAvailable();
    String  uriText = this.formatServerUri("license?withRaw=false");

    long before = System.currentTimeMillis();
    SzLicenseResponse response
        = this.invokeServerViaHttp(GET, uriText, SzLicenseResponse.class);
    long after = System.currentTimeMillis();

    this.validateLicenseResponse(response,
                                 before,
                                 after,
                                 false,
                                 "EVAL",
                                 10000);
  }

  @Test public void licenseWithRawTest() {
    this.assumeNativeApiAvailable();
    String  uriText = this.formatServerUri("license?withRaw=true");
    UriInfo uriInfo = this.newProxyUriInfo(uriText);

    long              before    = System.currentTimeMillis();
    SzLicenseResponse response  = this.adminServices.license(true, uriInfo);
    response.concludeTimers();
    long              after     = System.currentTimeMillis();

    this.validateLicenseResponse(response,
                                 before,
                                 after,
                                 true,
                                 "EVAL",
                                 10000);
  }

  @Test public void licenseWithRawViaHttpTest() {
    this.assumeNativeApiAvailable();
    String uriText = this.formatServerUri("license?withRaw=true");

    long before = System.currentTimeMillis();
    SzLicenseResponse response
        = this.invokeServerViaHttp(GET, uriText, SzLicenseResponse.class);
    long after = System.currentTimeMillis();

    this.validateLicenseResponse(response,
                                 before,
                                 after,
                                 true,
                                 "EVAL",
                                 10000);
  }


  private void validateLicenseResponse(SzLicenseResponse  response,
                                       long               beforeTimestamp,
                                       long               afterTimestamp,
                                       Boolean            expectRawData,
                                       String             expectedLicenseType,
                                       long               expectedRecordLimit)
  {
    String selfLink = this.formatServerUri("license");
    if (expectRawData != null) {
      selfLink += "?withRaw=" + expectRawData;
    } else {
      expectRawData = false;
    }

    this.validateBasics(
        response, selfLink, beforeTimestamp, afterTimestamp, expectRawData);

    SzLicenseResponse.Data data = response.getData();

    assertNotNull(data, "Response data is null");

    SzLicenseInfo licenseInfo = data.getLicense();

    assertNotNull(licenseInfo, "License data is null");

    assertEquals(10000,
                 licenseInfo.getRecordLimit(),
                 "Record limit wrong");

    assertEquals("EVAL",
                 licenseInfo.getLicenseType(),
                 "Unexpected license type");

    if (expectRawData) {
      this.validateRawDataMap(
          response.getRawData(),
          "customer", "contract", "issueDate", "licenseType",
          "licenseLevel", "billing", "expireDate", "recordLimit");

    }
  }

  @Test public void versionTest() {
    this.assumeNativeApiAvailable();
    String  uriText = this.formatServerUri("version");
    UriInfo uriInfo = this.newProxyUriInfo(uriText);

    long              before    = System.currentTimeMillis();
    SzVersionResponse response  = this.adminServices.version(false,
                                                             uriInfo);
    response.concludeTimers();
    long              after     = System.currentTimeMillis();

    this.validateVersionResponse(response, before, after, null);
  }

  @Test public void versionViaHttpTest() {
    this.assumeNativeApiAvailable();
    String  uriText = this.formatServerUri("version");

    long before = System.currentTimeMillis();
    SzVersionResponse response
        = this.invokeServerViaHttp(GET, uriText, SzVersionResponse.class);
    long after = System.currentTimeMillis();

    this.validateVersionResponse(response, before, after, null);
  }

  @Test public void versionWithoutRawTest() {
    this.assumeNativeApiAvailable();
    String  uriText = this.formatServerUri("version?withRaw=false");
    UriInfo uriInfo = this.newProxyUriInfo(uriText);

    long              before    = System.currentTimeMillis();
    SzVersionResponse response  = this.adminServices.version(false, uriInfo);
    response.concludeTimers();
    long              after     = System.currentTimeMillis();

    this.validateVersionResponse(response, before, after, false);
  }

  @Test public void versionWithoutRawViaHttpTest() {
    this.assumeNativeApiAvailable();
    String  uriText = this.formatServerUri("version?withRaw=false");

    long before = System.currentTimeMillis();
    SzVersionResponse response
        = this.invokeServerViaHttp(GET, uriText, SzVersionResponse.class);
    long after = System.currentTimeMillis();

    this.validateVersionResponse(response, before, after,false);
  }

  @Test public void versionWithRawTest() {
    this.assumeNativeApiAvailable();
    String  uriText = this.formatServerUri("version?withRaw=true");
    UriInfo uriInfo = this.newProxyUriInfo(uriText);

    long              before    = System.currentTimeMillis();
    SzVersionResponse response  = this.adminServices.version(true, uriInfo);
    response.concludeTimers();
    long              after     = System.currentTimeMillis();

    this.validateVersionResponse(response,
                                 before,
                                 after,
                                 true);
  }

  @Test public void versionWithRawViaHttpTest() {
    this.assumeNativeApiAvailable();
    String uriText = this.formatServerUri("version?withRaw=true");

    long before = System.currentTimeMillis();
    SzVersionResponse response
        = this.invokeServerViaHttp(GET, uriText, SzVersionResponse.class);
    long after = System.currentTimeMillis();

    this.validateVersionResponse(response, before, after, true);
  }

  private void validateVersionResponse(SzVersionResponse  response,
                                       long               beforeTimestamp,
                                       long               afterTimestamp,
                                       Boolean            expectRawData)
  {
    String selfLink = this.formatServerUri("version");
    if (expectRawData != null) {
      selfLink += "?withRaw=" + expectRawData;
    } else {
      expectRawData = false;
    }

    this.validateBasics(
        response, selfLink, beforeTimestamp, afterTimestamp, expectRawData);

    SzVersionInfo info = response.getData();

    assertNotNull(info, "Response data is null");

    assertEquals(BuildInfo.MAVEN_VERSION,
                 info.getApiServerVersion(),
                 "API Server Version wrong");

    assertEquals(BuildInfo.REST_API_VERSION,
                 info.getRestApiVersion(),
                 "REST API Version wrong");

    // assume we can reinitialize the product API since it does not really do
    // anything when we initialize it
    String    initJson  = readInitJsonFile();
    G2Product product   = new G2ProductJNI();
    product.initV2("testApiServer", initJson, false);
    try {
      String versionJson = product.version();

      JsonObject jsonObject = JsonUtils.parseJsonObject(versionJson);
      String expectedVersion = JsonUtils.getString(jsonObject, "VERSION");
      String expectedBuildNum = JsonUtils.getString(jsonObject, "BUILD_NUMBER");

      JsonObject subObject = JsonUtils.getJsonObject(
          jsonObject, "COMPATIBILITY_VERSION");

      int configCompatVers = Integer.parseInt(JsonUtils.getString(
          subObject, "CONFIG_VERSION"));

      assertEquals(expectedVersion, info.getNativeApiVersion(),
                   "Native API Version wrong");

      assertEquals(expectedBuildNum, info.getNativeApiBuildNumber(),
                   "Native API Build Number wrong");

      assertEquals(configCompatVers, info.getConfigCompatibilityVersion(),
                   "Native API Config Compatibility wrong");
    } finally {
      product.destroy();

    }
    if (expectRawData) {
      this.validateRawDataMap(
          response.getRawData(),
          false,
          "VERSION", "BUILD_NUMBER", "BUILD_DATE", "COMPATIBILITY_VERSION");
    }
  }

}
