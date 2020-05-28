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
import static com.senzing.api.services.ResponseValidators.*;

@TestInstance(Lifecycle.PER_CLASS)
public class AdminServicesTest extends AbstractServiceTest {
  public static final int TEST_LICENSE_RECORD_LIMIT = 10000;
  private AdminServices adminServices;

  @BeforeAll public void initializeEnvironment() {
    this.beginTests();
    this.initializeTestEnvironment();
    this.adminServices = new AdminServices();
  }

  @AfterAll public void teardownEnvironment() {
    try {
      this.teardownTestEnvironment();
      this.conditionallyLogCounts(true);
    } finally {
      this.endTests();
    }
  }

  @Test public void heartbeatTest() {
    this.performTest(() -> {
      String uriText = this.formatServerUri("heartbeat");
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      long before = System.currentTimeMillis();
      SzBasicResponse response = this.adminServices.heartbeat(uriInfo);
      response.concludeTimers();
      long after = System.currentTimeMillis();

      validateBasics(response, uriText, before, after);
    });
  }

  @Test public void heartbeatViaHttpTest() {
    this.performTest(() -> {
      String  uriText = this.formatServerUri("heartbeat");
      long    before  = System.currentTimeMillis();
      SzBasicResponse response
          = this.invokeServerViaHttp(GET, uriText, SzBasicResponse.class);
      long after = System.currentTimeMillis();

      validateBasics(response, uriText, before, after);
    });
  }

  @Test public void licenseTest() {
    this.performTest(() -> {
      String  uriText = this.formatServerUri("license");
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      long              before    = System.currentTimeMillis();
      SzLicenseResponse response  = this.adminServices.license(false, uriInfo);
      response.concludeTimers();
      long              after     = System.currentTimeMillis();

      validateLicenseResponse(response,
                              uriText,
                              before,
                              after,
                              null,
                              "EVAL",
                              TEST_LICENSE_RECORD_LIMIT);
    });
  }

  @Test public void licenseViaHttpTest() {
    this.performTest(() -> {
      String  uriText = this.formatServerUri("license");
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      long before = System.currentTimeMillis();
      SzLicenseResponse response
          = this.invokeServerViaHttp(GET, uriText, SzLicenseResponse.class);
      long after = System.currentTimeMillis();

      validateLicenseResponse(response,
                              uriText,
                              before,
                              after,
                              null,
                              "EVAL",
                              TEST_LICENSE_RECORD_LIMIT);
    });
  }

  @Test public void licenseWithoutRawTest() {
    this.performTest(() -> {
      String  uriText = this.formatServerUri("license?withRaw=false");
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      long              before    = System.currentTimeMillis();
      SzLicenseResponse response  = this.adminServices.license(false, uriInfo);
      response.concludeTimers();
      long              after     = System.currentTimeMillis();

      validateLicenseResponse(response,
                              uriText,
                              before,
                              after,
                              false,
                              "EVAL",
                              TEST_LICENSE_RECORD_LIMIT);

    });
  }

  @Test public void licenseWithoutRawViaHttpTest() {
    this.performTest(() -> {
      String  uriText = this.formatServerUri("license?withRaw=false");

      long before = System.currentTimeMillis();
      SzLicenseResponse response
          = this.invokeServerViaHttp(GET, uriText, SzLicenseResponse.class);
      long after = System.currentTimeMillis();

      validateLicenseResponse(response,
                              uriText,
                              before,
                              after,
                              false,
                              "EVAL",
                              TEST_LICENSE_RECORD_LIMIT);
    });
  }

  @Test public void licenseWithRawTest() {
    this.performTest(() -> {
      String  uriText = this.formatServerUri("license?withRaw=true");
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      long              before    = System.currentTimeMillis();
      SzLicenseResponse response  = this.adminServices.license(true, uriInfo);
      response.concludeTimers();
      long              after     = System.currentTimeMillis();

      validateLicenseResponse(response,
                              uriText,
                              before,
                              after,
                              true,
                              "EVAL",
                              TEST_LICENSE_RECORD_LIMIT);
    });
  }

  @Test public void licenseWithRawViaHttpTest() {
    this.performTest(()-> {
      String uriText = this.formatServerUri("license?withRaw=true");

      long before = System.currentTimeMillis();
      SzLicenseResponse response
          = this.invokeServerViaHttp(GET, uriText, SzLicenseResponse.class);
      long after = System.currentTimeMillis();

      validateLicenseResponse(response,
                              uriText,
                              before,
                              after,
                              true,
                              "EVAL",
                              TEST_LICENSE_RECORD_LIMIT);
    });
  }

  @Test public void versionTest() {
    this.performTest(()-> {
      String  uriText = this.formatServerUri("version");
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      long              before    = System.currentTimeMillis();
      SzVersionResponse response  = this.adminServices.version(false,
                                                               uriInfo);
      response.concludeTimers();
      long              after     = System.currentTimeMillis();

      validateVersionResponse(response,
                              uriText,
                              before,
                              after,
                              null,
                              this.readInitJsonFile());
    });
  }

  @Test public void versionViaHttpTest() {
    this.performTest(() -> {
      String  uriText = this.formatServerUri("version");

      long before = System.currentTimeMillis();
      SzVersionResponse response
          = this.invokeServerViaHttp(GET, uriText, SzVersionResponse.class);
      long after = System.currentTimeMillis();

      validateVersionResponse(response,
                              uriText,
                              before,
                              after,
                              null,
                              this.readInitJsonFile());

    });
  }

  @Test public void versionWithoutRawTest() {
    this.performTest(() -> {
      String  uriText = this.formatServerUri("version?withRaw=false");
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      long              before    = System.currentTimeMillis();
      SzVersionResponse response  = this.adminServices.version(false, uriInfo);
      response.concludeTimers();
      long              after     = System.currentTimeMillis();

      validateVersionResponse(response,
                              uriText,
                              before,
                              after,
                              false,
                              this.readInitJsonFile());
    });
  }

  @Test public void versionWithoutRawViaHttpTest() {
    this.performTest(() -> {
      String  uriText = this.formatServerUri("version?withRaw=false");

      long before = System.currentTimeMillis();
      SzVersionResponse response
          = this.invokeServerViaHttp(GET, uriText, SzVersionResponse.class);
      long after = System.currentTimeMillis();

      validateVersionResponse(response,
                              uriText,
                              before,
                              after,
                              false,
                              this.readInitJsonFile());
    });
  }

  @Test public void versionWithRawTest() {
    this.performTest(() -> {
      String  uriText = this.formatServerUri("version?withRaw=true");
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      long              before    = System.currentTimeMillis();
      SzVersionResponse response  = this.adminServices.version(true, uriInfo);
      response.concludeTimers();
      long              after     = System.currentTimeMillis();

      validateVersionResponse(response,
                              uriText,
                              before,
                              after,
                              true,
                              this.readInitJsonFile());
    });
  }

  @Test public void versionWithRawViaHttpTest() {
    this.performTest(() -> {
      String uriText = this.formatServerUri("version?withRaw=true");

      long before = System.currentTimeMillis();
      SzVersionResponse response
          = this.invokeServerViaHttp(GET, uriText, SzVersionResponse.class);
      long after = System.currentTimeMillis();

      validateVersionResponse(response,
                              uriText,
                              before,
                              after,
                              true,
                              this.readInitJsonFile());
    });
  }
}
