package com.senzing.api.services;

import com.senzing.api.model.*;

import javax.ws.rs.core.UriInfo;
import java.util.*;

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

  @Test public void heartbeatTestViaHttp() {
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

  @Test public void licenseTestViaHttp() {
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

  @Test public void licenseWithoutRawTestViaHttp() {
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

  @Test public void licenseWithRawTestViaHttp() {
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
      Object rawData = response.getRawData();
      if (!(rawData instanceof Map)) {
        fail("Raw data is not a JSON object: " + rawData);
      }
      Map<String, Object> map = (Map<String, Object>) rawData;
      String[] expectedKeys = {
          "customer", "contract", "issueDate", "licenseType",
          "licenseLevel", "billing", "expireDate", "recordLimit"
      };
      Set<String> expectedKeySet = new HashSet<>();
      Set<String> actualKeySet = map.keySet();
      for (String key : expectedKeys) {
        expectedKeySet.add(key);
        if (!actualKeySet.contains(key)) {
          fail("JSON property missing from raw data: " + key + " / " + map);
        }
      }
      if (expectedKeySet.size() != actualKeySet.size()) {
        Set<String> extraKeySet = new HashSet<>(actualKeySet);
        extraKeySet.removeAll(expectedKeySet);
        fail("Unexpected JSON properties in raw data: " + extraKeySet);
      }
    }
  }
}
