package com.senzing.api.services;

import com.senzing.api.model.SzErrorResponse;
import com.senzing.api.model.SzLoadRecordResponse;
import com.senzing.api.model.SzRecordId;
import com.senzing.api.model.SzReevaluateResponse;
import com.senzing.api.server.SzApiServer;
import com.senzing.api.server.SzApiServerOptions;
import com.senzing.util.JsonUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.web.client.HttpStatusCodeException;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.core.UriInfo;
import java.util.*;

import static com.senzing.api.model.SzHttpMethod.POST;
import static com.senzing.api.model.SzHttpMethod.PUT;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.TestInstance.Lifecycle;
import static com.senzing.api.services.ResponseValidators.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@TestInstance(Lifecycle.PER_CLASS)
public class ReadOnlyEntityDataWriteServicesTest
    extends EntityDataWriteServicesTest
{
  /**
   * Sets the desired options for the {@link SzApiServer} during server
   * initialization.
   *
   * @param options The {@link SzApiServerOptions} to initialize.
   */
  protected void initializeServerOptions(SzApiServerOptions options) {
    super.initializeServerOptions(options);
    options.setReadOnly(true);
  }

  @Test public void postRecordTest() {
    this.performTest(() -> {
      String  uriText = this.formatServerUri(
          "data-sources/" + WATCHLIST_DATA_SOURCE + "/records");
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      JsonObjectBuilder job = Json.createObjectBuilder();
      job.add("NAME_FIRST", "Joe");
      job.add("NAME_LAST", "Schmoe");
      job.add("PHONE_NUMBER", "702-555-1212");
      job.add("ADDR_FULL", "101 Main Street, Las Vegas, NV 89101");
      JsonObject  jsonObject  = job.build();
      String      jsonText    = JsonUtils.toJsonText(jsonObject);

      long before = System.currentTimeMillis();
      try {
        this.entityDataServices.loadRecord(
            WATCHLIST_DATA_SOURCE, null, false, false, uriInfo, jsonText);
        fail("Did not get expected 403 ForbiddenException in read-only mode");

      } catch (ForbiddenException expected) {
        SzErrorResponse response
            = (SzErrorResponse) expected.getResponse().getEntity();
        response.concludeTimers();
        long after = System.currentTimeMillis();
        validateBasics(response, 403, POST, uriText, before, after);
      }
    });
  }

  @Test public void postRecordViaHttpTest() {
    this.performTest(() -> {
      String  uriText = this.formatServerUri(
          "data-sources/" + WATCHLIST_DATA_SOURCE + "/records");
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      Map recordBody = new HashMap();
      recordBody.put("NAME_FIRST", "Joanne");
      recordBody.put("NAME_LAST", "Smith");
      recordBody.put("PHONE_NUMBER", "212-555-1212");
      recordBody.put("ADDR_FULL", "101 Fifth Ave, Las Vegas, NV 10018");

      long before = System.currentTimeMillis();
      SzErrorResponse response = this.invokeServerViaHttp(
          POST, uriText, null, recordBody, SzErrorResponse.class);
      response.concludeTimers();
      long after = System.currentTimeMillis();

      validateBasics(response, 403, POST, uriText, before, after);
    });
  }

  @Test public void postMismatchedDataSourceTest() {
    this.performTest(() -> {
      String  uriText = this.formatServerUri(
          "data-sources/" + CUSTOMER_DATA_SOURCE + "/records");
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      JsonObjectBuilder job = Json.createObjectBuilder();
      job.add("DATA_SOURCE", WATCHLIST_DATA_SOURCE);
      job.add("NAME_FIRST", "John");
      job.add("NAME_LAST", "Doe");
      job.add("PHONE_NUMBER", "818-555-1313");
      job.add("ADDR_FULL", "100 Main Street, Los Angeles, CA 90012");
      JsonObject  jsonObject  = job.build();
      String      jsonText    = JsonUtils.toJsonText(jsonObject);

      long before = System.currentTimeMillis();
      try {
        this.entityDataServices.loadRecord(CUSTOMER_DATA_SOURCE,
                                           null,
                                           false,
                                           false,
                                           uriInfo,
                                           jsonText);

        fail("Expected BadRequestException for mismatched DATA_SOURCE");

      } catch (ForbiddenException expected) {
        SzErrorResponse response
            = (SzErrorResponse) expected.getResponse().getEntity();
        response.concludeTimers();
        long after = System.currentTimeMillis();
        validateBasics(response, 403, POST, uriText, before, after);
      }
    });
  }

  @Test public void postMismatchedDataSourceViaHttpTest() {
    this.performTest(() -> {
      String  uriText = this.formatServerUri(
          "data-sources/" + CUSTOMER_DATA_SOURCE + "/records");
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      Map recordBody = new HashMap();
      recordBody.put("DATA_SOURCE", WATCHLIST_DATA_SOURCE);
      recordBody.put("NAME_FIRST", "Jane");
      recordBody.put("NAME_LAST", "Doe");
      recordBody.put("PHONE_NUMBER", "818-555-1212");
      recordBody.put("ADDR_FULL", "500 First Street, Los Angeles, CA 90033");

      long before = System.currentTimeMillis();
      SzErrorResponse response = this.invokeServerViaHttp(
          POST, uriText, null, recordBody, SzErrorResponse.class);
      response.concludeTimers();
      long after = System.currentTimeMillis();

      validateBasics(response,
                     403,
                     POST,
                     uriText,
                     before,
                     after);
    });
  }

  @ParameterizedTest
  @MethodSource("withInfoParams")
  public void postRecordWithInfoTest(Boolean  withInfo,
                                     Boolean  withRaw)
  {
    this.performTest(() -> {
      Map<String, Object> queryParams = new LinkedHashMap<>();
      if (withInfo != null) queryParams.put("withInfo", withInfo);
      if (withRaw != null) queryParams.put("withRaw", withRaw);

      String uriText = this.formatServerUri(
          "data-sources/" + WATCHLIST_DATA_SOURCE + "/records",
          queryParams);
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      JsonObjectBuilder job = Json.createObjectBuilder();
      job.add("NAME_FIRST", "James");
      job.add("NAME_LAST", "Moriarty");
      job.add("PHONE_NUMBER", "702-555-1212");
      job.add("ADDR_FULL", "101 Main Street, Las Vegas, NV 89101");
      JsonObject  jsonObject  = job.build();
      String      jsonText    = JsonUtils.toJsonText(jsonObject);

      long before = System.currentTimeMillis();
      try {
        this.entityDataServices.loadRecord(
            WATCHLIST_DATA_SOURCE,
            null,
            (withInfo != null ? withInfo : false),
            (withRaw != null ? withRaw : false),
            uriInfo,
            jsonText);

        fail("Did not get expected 403 ForbiddenException in read-only mode");

      } catch (ForbiddenException expected) {
        SzErrorResponse response
            = (SzErrorResponse) expected.getResponse().getEntity();
        response.concludeTimers();
        long after = System.currentTimeMillis();
        validateBasics(response, 403, POST, uriText, before, after);
      }
    });
  }

  @ParameterizedTest
  @MethodSource("withInfoParams")
  public void postRecordWithInfoViaHttpTest(Boolean withInfo,
                                            Boolean withRaw)
  {
    this.performTest(() -> {
      Map<String, Object> queryParams = new LinkedHashMap<>();
      if (withInfo != null) queryParams.put("withInfo", withInfo);
      if (withRaw != null) queryParams.put("withRaw", withRaw);

      String uriText = this.formatServerUri(
          "data-sources/" + WATCHLIST_DATA_SOURCE + "/records",
          queryParams);

      Map recordBody = new HashMap();
      recordBody.put("NAME_FIRST", "James");
      recordBody.put("NAME_LAST", "Moriarty");
      recordBody.put("PHONE_NUMBER", "702-555-1212");
      recordBody.put("ADDR_FULL", "101 Fifth Ave, Las Vegas, NV 10018");

      long before = System.currentTimeMillis();
      SzErrorResponse response = this.invokeServerViaHttp(
          POST, uriText, null, recordBody, SzErrorResponse.class);
      response.concludeTimers();
      long after = System.currentTimeMillis();

      validateBasics(response, 403, POST, uriText, before, after);
    });
  }

  @ParameterizedTest
  @MethodSource("withInfoParams")
  public void postRecordWithInfoViaJavaClientTest(Boolean withInfo,
                                                  Boolean withRaw)
  {
    this.performTest(() -> {
      Map<String, Object> queryParams = new LinkedHashMap<>();
      if (withInfo != null) queryParams.put("withInfo", withInfo);
      if (withRaw != null) queryParams.put("withRaw", withRaw);

      String uriText = this.formatServerUri(
          "data-sources/" + WATCHLIST_DATA_SOURCE + "/records",
          queryParams);

      Map recordBody = new HashMap();
      recordBody.put("NAME_FIRST", "James");
      recordBody.put("NAME_LAST", "Moriarty");
      recordBody.put("PHONE_NUMBER", "702-555-1212");
      recordBody.put("ADDR_FULL", "101 Fifth Ave, Las Vegas, NV 10018");

      long before = System.currentTimeMillis();
      try {
        this.entityDataApi.addRecordWithReturnedRecordId(recordBody,
                                                         WATCHLIST_DATA_SOURCE,
                                                         null,
                                                         withInfo,
                                                         withRaw);
      } catch (HttpStatusCodeException expected) {
        long after = System.currentTimeMillis();
        com.senzing.gen.api.model.SzErrorResponse clientResponse
            = jsonParse(expected.getResponseBodyAsString(),
                        com.senzing.gen.api.model.SzErrorResponse.class);

        SzErrorResponse response = jsonCopy(clientResponse,
                                            SzErrorResponse.class);

        validateBasics(response,
                       403,
                       POST,
                       uriText,
                       before,
                       after);
      }

      uriText = this.formatServerUri(
          "data-sources/" + CUSTOMER_DATA_SOURCE + "/records",
          queryParams);

      recordBody = new HashMap();
      recordBody.put("NAME_FIRST", "Joe");
      recordBody.put("NAME_LAST", "Schmoe");
      recordBody.put("PHONE_NUMBER", "702-555-1212");
      recordBody.put("ADDR_FULL", "101 Fifth Ave, Las Vegas, NV 10018");

      before = System.currentTimeMillis();
      try {
        this.entityDataApi.addRecordWithReturnedRecordId(
            recordBody, CUSTOMER_DATA_SOURCE, null, withInfo, withRaw);

      } catch (HttpStatusCodeException expected) {
        long after = System.currentTimeMillis();
        com.senzing.gen.api.model.SzErrorResponse clientResponse
            = jsonParse(expected.getResponseBodyAsString(),
                        com.senzing.gen.api.model.SzErrorResponse.class);

        SzErrorResponse response = jsonCopy(clientResponse,
                                            SzErrorResponse.class);

        validateBasics(response,
                       403,
                       POST,
                       uriText,
                       before,
                       after);
      }
    });
  }

  @Test public void putRecordTest() {
    this.performTest(() -> {
      final String recordId = "ABC123";

      String  uriText = this.formatServerUri(
          "data-sources/" + WATCHLIST_DATA_SOURCE + "/records/" + recordId);
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      JsonObjectBuilder job = Json.createObjectBuilder();
      job.add("NAME_FIRST", "John");
      job.add("NAME_LAST", "Doe");
      job.add("PHONE_NUMBER", "818-555-1313");
      job.add("ADDR_FULL", "100 Main Street, Los Angeles, CA 90012");
      JsonObject  jsonObject  = job.build();
      String      jsonText    = JsonUtils.toJsonText(jsonObject);

      long before = System.currentTimeMillis();
      try {
        this.entityDataServices.loadRecord(
            WATCHLIST_DATA_SOURCE, recordId, null, false, false, uriInfo, jsonText);

        fail("Did not get expected 403 ForbiddenException in read-only mode");
      } catch (ForbiddenException expected) {
        SzErrorResponse response
            = (SzErrorResponse) expected.getResponse().getEntity();
        response.concludeTimers();
        long after = System.currentTimeMillis();
        validateBasics(response, 403, PUT, uriText, before, after);
      }
    });
  }

  @Test public void putRecordViaHttpTest() {
    this.performTest(() -> {
      final String recordId = "XYZ456";

      String  uriText = this.formatServerUri(
          "data-sources/" + WATCHLIST_DATA_SOURCE + "/records/" + recordId);
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      Map recordBody = new HashMap();
      recordBody.put("NAME_FIRST", "Jane");
      recordBody.put("NAME_LAST", "Doe");
      recordBody.put("PHONE_NUMBER", "818-555-1212");
      recordBody.put("ADDR_FULL", "500 First Street, Los Angeles, CA 90033");

      long before = System.currentTimeMillis();
      SzErrorResponse response = this.invokeServerViaHttp(
          PUT, uriText, null, recordBody, SzErrorResponse.class);
      response.concludeTimers();
      long after = System.currentTimeMillis();

      validateBasics(response, 403, PUT, uriText, before, after);
    });
  }

  @Test public void putMismatchedRecordTest() {
    this.performTest(() -> {
      final String recordId = "ABC123";

      String  uriText = this.formatServerUri(
          "data-sources/" + CUSTOMER_DATA_SOURCE + "/records/" + recordId);
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      JsonObjectBuilder job = Json.createObjectBuilder();
      job.add("RECORD_ID", "DEF456");
      job.add("NAME_FIRST", "John");
      job.add("NAME_LAST", "Doe");
      job.add("PHONE_NUMBER", "818-555-1313");
      job.add("ADDR_FULL", "100 Main Street, Los Angeles, CA 90012");
      JsonObject  jsonObject  = job.build();
      String      jsonText    = JsonUtils.toJsonText(jsonObject);

      long before = System.currentTimeMillis();
      try {
        this.entityDataServices.loadRecord(CUSTOMER_DATA_SOURCE,
                                           recordId,
                                           null,
                                           false,
                                           false,
                                           uriInfo,
                                           jsonText);

        fail("Did not get expected 403 ForbiddenException in read-only mode");

      } catch (ForbiddenException expected) {
        SzErrorResponse response
            = (SzErrorResponse) expected.getResponse().getEntity();
        response.concludeTimers();
        long after = System.currentTimeMillis();
        validateBasics(response, 403, PUT, uriText, before, after);
      }
    });
  }

  @Test public void putMismatchedRecordViaHttpTest() {
    this.performTest(() -> {
      final String recordId = "XYZ456";

      String  uriText = this.formatServerUri(
          "data-sources/" + CUSTOMER_DATA_SOURCE + "/records/" + recordId);
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      Map recordBody = new HashMap();
      recordBody.put("RECORD_ID", "DEF456");
      recordBody.put("NAME_FIRST", "Jane");
      recordBody.put("NAME_LAST", "Doe");
      recordBody.put("PHONE_NUMBER", "818-555-1212");
      recordBody.put("ADDR_FULL", "500 First Street, Los Angeles, CA 90033");

      long before = System.currentTimeMillis();
      SzErrorResponse response = this.invokeServerViaHttp(
          PUT, uriText, null, recordBody, SzErrorResponse.class);
      response.concludeTimers();
      long after = System.currentTimeMillis();

      validateBasics(response,
                     403,
                     PUT,
                     uriText,
                     before,
                     after);
    });
  }

  @Test public void putMismatchedRecordViaJavaClientTest() {
    this.performTest(() -> {
      final String recordId = "XYZ456";

      String  uriText = this.formatServerUri(
          "data-sources/" + CUSTOMER_DATA_SOURCE + "/records/" + recordId);
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      Map recordBody = new HashMap();
      recordBody.put("RECORD_ID", "DEF456");
      recordBody.put("NAME_FIRST", "Jane");
      recordBody.put("NAME_LAST", "Doe");
      recordBody.put("PHONE_NUMBER", "818-555-1212");
      recordBody.put("ADDR_FULL", "500 First Street, Los Angeles, CA 90033");

      long before = System.currentTimeMillis();
      try {
        com.senzing.gen.api.model.SzLoadRecordResponse clientResponse
            = this.entityDataApi.addRecord(recordBody,
                                           CUSTOMER_DATA_SOURCE,
                                           recordId,
                                           null,
                                           null,
                                           null);

        fail("Expected failure, but got success for mismatched record: "
                 + "dataSource=[ " + CUSTOMER_DATA_SOURCE
                 + " ], urlRecordId=[ " + recordId
                 + " ], bodyRecordId=[ DEF456 ], response=[ "
                 + clientResponse + " ]");

      } catch (HttpStatusCodeException expected) {
        long after = System.currentTimeMillis();
        com.senzing.gen.api.model.SzErrorResponse clientResponse
            = jsonParse(expected.getResponseBodyAsString(),
                        com.senzing.gen.api.model.SzErrorResponse.class);

        SzErrorResponse response = jsonCopy(clientResponse,
                                            SzErrorResponse.class);

        validateBasics(response,
                       403,
                       PUT,
                       uriText,
                       before,
                       after);
      }
    });
  }

  @Test public void putMismatchedDataSourceTest() {
    this.performTest(() -> {
      final String recordId = "ABC123";

      String  uriText = this.formatServerUri(
          "data-sources/" + CUSTOMER_DATA_SOURCE + "/records/" + recordId);
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      JsonObjectBuilder job = Json.createObjectBuilder();
      job.add("DATA_SOURCE", WATCHLIST_DATA_SOURCE);
      job.add("NAME_FIRST", "John");
      job.add("NAME_LAST", "Doe");
      job.add("PHONE_NUMBER", "818-555-1313");
      job.add("ADDR_FULL", "100 Main Street, Los Angeles, CA 90012");
      JsonObject  jsonObject  = job.build();
      String      jsonText    = JsonUtils.toJsonText(jsonObject);

      long before = System.currentTimeMillis();
      try {
        this.entityDataServices.loadRecord(CUSTOMER_DATA_SOURCE,
                                           recordId,
                                           null,
                                           false,
                                           false,
                                           uriInfo,
                                           jsonText);

        fail("Did not get expected 403 ForbiddenException in read-only mode");

      } catch (ForbiddenException expected) {
        SzErrorResponse response
            = (SzErrorResponse) expected.getResponse().getEntity();
        response.concludeTimers();
        long after = System.currentTimeMillis();
        validateBasics(response, 403, PUT, uriText, before, after);
      }
    });
  }

  @Test public void putMismatchedDataSourceViaHttpTest() {
    this.performTest(() -> {
      final String recordId = "XYZ456";

      String  uriText = this.formatServerUri(
          "data-sources/" + CUSTOMER_DATA_SOURCE + "/records/" + recordId);
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      Map recordBody = new HashMap();
      recordBody.put("DATA_SOURCE", WATCHLIST_DATA_SOURCE);
      recordBody.put("NAME_FIRST", "Jane");
      recordBody.put("NAME_LAST", "Doe");
      recordBody.put("PHONE_NUMBER", "818-555-1212");
      recordBody.put("ADDR_FULL", "500 First Street, Los Angeles, CA 90033");

      long before = System.currentTimeMillis();
      SzErrorResponse response = this.invokeServerViaHttp(
          PUT, uriText, null, recordBody, SzErrorResponse.class);
      response.concludeTimers();
      long after = System.currentTimeMillis();

      validateBasics(response,
                     403,
                     PUT,
                     uriText,
                     before,
                     after);
    });
  }

  @Test public void putMismatchedDataSourceViaJavaClientTest() {
    this.performTest(() -> {
      final String recordId = "XYZ456";

      String  uriText = this.formatServerUri(
          "data-sources/" + CUSTOMER_DATA_SOURCE + "/records/" + recordId);
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      Map recordBody = new HashMap();
      recordBody.put("DATA_SOURCE", WATCHLIST_DATA_SOURCE);
      recordBody.put("NAME_FIRST", "Jane");
      recordBody.put("NAME_LAST", "Doe");
      recordBody.put("PHONE_NUMBER", "818-555-1212");
      recordBody.put("ADDR_FULL", "500 First Street, Los Angeles, CA 90033");

      long before = System.currentTimeMillis();
      try {
        com.senzing.gen.api.model.SzLoadRecordResponse clientResponse
            = this.entityDataApi.addRecord(recordBody,
                                           CUSTOMER_DATA_SOURCE,
                                           recordId,
                                           null,
                                           null,
                                           null);

        fail("Expected forbidden failure, but got success for mismatched data "
                 + "source: urlDataSource=[ " + CUSTOMER_DATA_SOURCE
                 + " ], bodyDataSource=[ " + WATCHLIST_DATA_SOURCE
                 + " ], response=[ "
                 + clientResponse + " ]");

      } catch (HttpStatusCodeException expected) {
        long after = System.currentTimeMillis();
        com.senzing.gen.api.model.SzErrorResponse clientResponse
            = jsonParse(expected.getResponseBodyAsString(),
                        com.senzing.gen.api.model.SzErrorResponse.class);

        SzErrorResponse response = jsonCopy(clientResponse,
                                            SzErrorResponse.class);

        validateBasics(response,
                       403,
                       PUT,
                       uriText,
                       before,
                       after);
      }
    });
  }

  @ParameterizedTest
  @MethodSource("withInfoParams")
  public void putRecordWithInfoTest(Boolean  withInfo,
                                    Boolean  withRaw)
  {
    this.performTest(() -> {
      Map<String, Object> queryParams = new LinkedHashMap<>();
      if (withInfo != null) queryParams.put("withInfo", withInfo);
      if (withRaw != null) queryParams.put("withRaw", withRaw);

      String uriText = this.formatServerUri(
          "data-sources/" + WATCHLIST_DATA_SOURCE + "/records/ABC123",
          queryParams);
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      JsonObjectBuilder job = Json.createObjectBuilder();
      job.add("DATA_SOURCE", WATCHLIST_DATA_SOURCE);
      job.add("RECORD_ID", "ABC123");
      job.add("NAME_FIRST", "James");
      job.add("NAME_LAST", "Moriarty");
      job.add("PHONE_NUMBER", "702-555-1212");
      job.add("ADDR_FULL", "101 Main Street, Las Vegas, NV 89101");
      JsonObject  jsonObject  = job.build();
      String      jsonText    = JsonUtils.toJsonText(jsonObject);

      long before = System.currentTimeMillis();
      try {
        this.entityDataServices.loadRecord(
            WATCHLIST_DATA_SOURCE,
            "ABC123",
            null,
            (withInfo != null ? withInfo : false),
            (withRaw != null ? withRaw : false),
            uriInfo,
            jsonText);

        fail("Did not get expected 403 ForbiddenException in read-only mode");

      } catch (ForbiddenException expected) {
        SzErrorResponse response
            = (SzErrorResponse) expected.getResponse().getEntity();
        response.concludeTimers();
        long after = System.currentTimeMillis();
        validateBasics(response, 403, PUT, uriText, before, after);
      }
    });
  }

  @ParameterizedTest
  @MethodSource("withInfoParams")
  public void putRecordWithInfoViaHttpTest(Boolean withInfo,
                                           Boolean withRaw)
  {
    this.performTest(() -> {
      Map<String, Object> queryParams = new LinkedHashMap<>();
      if (withInfo != null) queryParams.put("withInfo", withInfo);
      if (withRaw != null) queryParams.put("withRaw", withRaw);

      String uriText = this.formatServerUri(
          "data-sources/" + WATCHLIST_DATA_SOURCE + "/records/ABC123",
          queryParams);

      Map recordBody = new HashMap();
      recordBody.put("DATA_SOURCE", WATCHLIST_DATA_SOURCE);
      recordBody.put("RECORD_ID", "ABC123");
      recordBody.put("NAME_FIRST", "James");
      recordBody.put("NAME_LAST", "Moriarty");
      recordBody.put("PHONE_NUMBER", "702-555-1212");
      recordBody.put("ADDR_FULL", "101 Fifth Ave, Las Vegas, NV 10018");

      long before = System.currentTimeMillis();
      SzErrorResponse response = this.invokeServerViaHttp(
          PUT, uriText, null, recordBody, SzErrorResponse.class);
      response.concludeTimers();
      long after = System.currentTimeMillis();

      validateBasics(response,
                     403,
                     PUT,
                     uriText,
                     before,
                     after);

    });
  }

  @ParameterizedTest
  @MethodSource("withInfoParams")
  public void putRecordWithInfoViaJavaClientTest(Boolean withInfo,
                                                 Boolean withRaw)
  {
    this.performTest(() -> {
      final String recordId1 = "ABC123";
      final String recordId2 = "DEF456";
      Map<String, Object> queryParams = new LinkedHashMap<>();
      if (withInfo != null) queryParams.put("withInfo", withInfo);
      if (withRaw != null) queryParams.put("withRaw", withRaw);

      String uriText = this.formatServerUri(
          "data-sources/" + WATCHLIST_DATA_SOURCE + "/records/"
              + recordId1, queryParams);

      Map recordBody = new HashMap();
      recordBody.put("DATA_SOURCE", WATCHLIST_DATA_SOURCE);
      recordBody.put("RECORD_ID", recordId1);
      recordBody.put("NAME_FIRST", "James");
      recordBody.put("NAME_LAST", "Moriarty");
      recordBody.put("PHONE_NUMBER", "702-555-1212");
      recordBody.put("ADDR_FULL", "101 Fifth Ave, Las Vegas, NV 10018");

      long before = System.currentTimeMillis();
      try {
        this.entityDataApi.addRecord(recordBody,
                                     WATCHLIST_DATA_SOURCE,
                                     recordId1,
                                     null,
                                     withInfo,
                                     withRaw);

      } catch (HttpStatusCodeException expected) {
        long after = System.currentTimeMillis();
        com.senzing.gen.api.model.SzErrorResponse clientResponse
            = jsonParse(expected.getResponseBodyAsString(),
                        com.senzing.gen.api.model.SzErrorResponse.class);

        SzErrorResponse response = jsonCopy(clientResponse,
                                            SzErrorResponse.class);

        validateBasics(response,
                       403,
                       PUT,
                       uriText,
                       before,
                       after);
      }

      uriText = this.formatServerUri(
          "data-sources/" + CUSTOMER_DATA_SOURCE + "/records/"
              + recordId2, queryParams);

      recordBody = new HashMap();
      recordBody.put("NAME_FIRST", "Joe");
      recordBody.put("NAME_LAST", "Schmoe");
      recordBody.put("PHONE_NUMBER", "702-555-1212");
      recordBody.put("ADDR_FULL", "101 Fifth Ave, Las Vegas, NV 10018");

      before = System.currentTimeMillis();
      try {
        this.entityDataApi.addRecord(recordBody,
                                     CUSTOMER_DATA_SOURCE,
                                     recordId2,
                                     null,
                                     withInfo,
                                     withRaw);

      } catch (HttpStatusCodeException expected) {
        long after = System.currentTimeMillis();
        com.senzing.gen.api.model.SzErrorResponse clientResponse
            = jsonParse(expected.getResponseBodyAsString(),
                        com.senzing.gen.api.model.SzErrorResponse.class);

        SzErrorResponse response = jsonCopy(clientResponse,
                                            SzErrorResponse.class);

        validateBasics(response,
                       403,
                       PUT,
                       uriText,
                       before,
                       after);
      }
    });
  }

  @ParameterizedTest
  @MethodSource("withInfoParams")
  public void reevaluateRecordTest(Boolean withInfo, Boolean withRaw)
  {
    this.performTest(() -> {
      final String recordId1 = "ABC123";
      Map<String, Object> queryParams = new LinkedHashMap<>();
      if (withInfo != null) queryParams.put("withInfo", withInfo);
      if (withRaw != null) queryParams.put("withRaw", withRaw);

      String uriText = this.formatServerUri(
          "data-sources/" + WATCHLIST_DATA_SOURCE
              + "/records/" + recordId1 + "/reevaluate", queryParams);
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      long before = System.currentTimeMillis();
      try {
        this.entityDataServices.reevaluateRecord(
            WATCHLIST_DATA_SOURCE,
            recordId1,
            (withInfo != null ? withInfo : false),
            (withRaw != null ? withRaw : false),
            uriInfo);
        fail("Did not get expected 403 ForbiddenException in read-only mode");

      } catch (ForbiddenException expected) {
        SzErrorResponse response
            = (SzErrorResponse) expected.getResponse().getEntity();
        response.concludeTimers();
        long after = System.currentTimeMillis();
        validateBasics(response, 403, POST, uriText, before, after);
      }
    });
  }

  @ParameterizedTest
  @MethodSource("withInfoParams")
  public void reevaluateRecordViaHttpTest(Boolean withInfo, Boolean withRaw)
  {
    this.performTest(() -> {
      final String recordId1 = "ABC123";
      Map<String, Object> queryParams = new LinkedHashMap<>();
      if (withInfo != null) queryParams.put("withInfo", withInfo);
      if (withRaw != null) queryParams.put("withRaw", withRaw);

      String uriText = this.formatServerUri(
          "data-sources/" + WATCHLIST_DATA_SOURCE
              + "/records/" + recordId1 + "/reevaluate", queryParams);

      long before = System.currentTimeMillis();
      SzErrorResponse response = this.invokeServerViaHttp(
          POST, uriText, SzErrorResponse.class);
      response.concludeTimers();
      long after = System.currentTimeMillis();

      validateBasics(response,
                     403,
                     POST,
                     uriText,
                     before,
                     after);
    });
  }

  @ParameterizedTest
  @MethodSource("withInfoParams")
  public void reevaluateEntityTest(Boolean withInfo, Boolean withRaw)
  {
    this.performTest(() -> {
      final String recordId1 = "ABC123";
      Map<String, Object> queryParams = new LinkedHashMap<>();
      if (withInfo != null) queryParams.put("withInfo", withInfo);
      if (withRaw != null) queryParams.put("withRaw", withRaw);
      queryParams.put("entityId", 100L);

      String uriText = this.formatServerUri("reevaluate-entity", queryParams);
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      long before = System.currentTimeMillis();
      try {
        this.entityDataServices.reevaluateEntity(
            100L,
            (withInfo != null ? withInfo : false),
            (withRaw != null ? withRaw : false),
            uriInfo);
        fail("Did not get expected 403 ForbiddenException in read-only mode");

      } catch (ForbiddenException expected) {
        SzErrorResponse response
            = (SzErrorResponse) expected.getResponse().getEntity();
        response.concludeTimers();
        long after = System.currentTimeMillis();
        validateBasics(response, 403, POST, uriText, before, after);
      }
    });
  }

  @ParameterizedTest
  @MethodSource("withInfoParams")
  public void reevaluateRecordViaJavaClientTest(Boolean withInfo,
                                                Boolean withRaw)
  {
    this.performTest(() -> {
      final String recordId1 = "ABC123";
      final String recordId2 = "DEF456";
      Map<String, Object> queryParams = new LinkedHashMap<>();
      if (withInfo != null) queryParams.put("withInfo", withInfo);
      if (withRaw != null) queryParams.put("withRaw", withRaw);

      String uriText = this.formatServerUri(
          "data-sources/" + WATCHLIST_DATA_SOURCE + "/records/"
              + recordId1, queryParams);

      Map recordBody = new HashMap();
      recordBody.put("DATA_SOURCE", WATCHLIST_DATA_SOURCE);
      recordBody.put("RECORD_ID", recordId1);
      recordBody.put("NAME_FIRST", "James");
      recordBody.put("NAME_LAST", "Moriarty");
      recordBody.put("PHONE_NUMBER", "702-555-1212");
      recordBody.put("ADDR_FULL", "101 Fifth Ave, Las Vegas, NV 10018");

      long before = System.currentTimeMillis();
      try {
        this.entityDataApi.addRecord(recordBody,
                                     WATCHLIST_DATA_SOURCE,
                                     recordId1,
                                     null,
                                     withInfo,
                                     withRaw);

      } catch (HttpStatusCodeException expected) {
        long after = System.currentTimeMillis();
        com.senzing.gen.api.model.SzErrorResponse clientResponse
            = jsonParse(expected.getResponseBodyAsString(),
                        com.senzing.gen.api.model.SzErrorResponse.class);

        SzErrorResponse response = jsonCopy(clientResponse,
                                            SzErrorResponse.class);

        validateBasics(response,
                       403,
                       PUT,
                       uriText,
                       before,
                       after);
      }

      uriText = this.formatServerUri(
          "data-sources/" + WATCHLIST_DATA_SOURCE
              + "/records/" + recordId1 + "/reevaluate", queryParams);

      before = System.currentTimeMillis();
      try {
        this.entityDataApi.reevaluateRecord(WATCHLIST_DATA_SOURCE,
                                            recordId1,
                                            withInfo,
                                            withRaw);

      } catch (HttpStatusCodeException expected) {
        long after = System.currentTimeMillis();
        com.senzing.gen.api.model.SzErrorResponse clientResponse
            = jsonParse(expected.getResponseBodyAsString(),
                        com.senzing.gen.api.model.SzErrorResponse.class);

        SzErrorResponse response = jsonCopy(clientResponse,
                                            SzErrorResponse.class);

        validateBasics(response,
                       403,
                       POST,
                       uriText,
                       before,
                       after);
      }

      uriText = this.formatServerUri(
          "data-sources/" + CUSTOMER_DATA_SOURCE + "/records/"
              + recordId2, queryParams);

      recordBody = new HashMap();
      recordBody.put("NAME_FIRST", "Joe");
      recordBody.put("NAME_LAST", "Schmoe");
      recordBody.put("PHONE_NUMBER", "702-555-1212");
      recordBody.put("ADDR_FULL", "101 Fifth Ave, Las Vegas, NV 10018");

      before = System.currentTimeMillis();
      try {
        this.entityDataApi.addRecord(recordBody,
                                     CUSTOMER_DATA_SOURCE,
                                     recordId2,
                                     null,
                                     withInfo,
                                     withRaw);

      } catch (HttpStatusCodeException expected) {
        long after = System.currentTimeMillis();
        com.senzing.gen.api.model.SzErrorResponse clientResponse
            = jsonParse(expected.getResponseBodyAsString(),
                        com.senzing.gen.api.model.SzErrorResponse.class);

        SzErrorResponse response = jsonCopy(clientResponse,
                                            SzErrorResponse.class);

        validateBasics(response,
                       403,
                       PUT,
                       uriText,
                       before,
                       after);
      }

      uriText = this.formatServerUri(
          "data-sources/" + CUSTOMER_DATA_SOURCE + "/records/"
              + recordId2 + "/reevaluate", queryParams);

      before = System.currentTimeMillis();
      try {
        this.entityDataApi.reevaluateRecord(CUSTOMER_DATA_SOURCE,
                                            recordId2,
                                            withInfo,
                                            withRaw);

      } catch (HttpStatusCodeException expected) {
        long after = System.currentTimeMillis();
        com.senzing.gen.api.model.SzErrorResponse clientResponse
            = jsonParse(expected.getResponseBodyAsString(),
                        com.senzing.gen.api.model.SzErrorResponse.class);

        SzErrorResponse response = jsonCopy(clientResponse,
                                            SzErrorResponse.class);

        validateBasics(response,
                       403,
                       POST,
                       uriText,
                       before,
                       after);
      }
    });
  }

  @ParameterizedTest
  @MethodSource("withInfoParams")
  public void reevaluateEntityViaHttpTest(Boolean withInfo, Boolean withRaw)
  {
    this.performTest(() -> {
      Map<String, Object> queryParams = new LinkedHashMap<>();
      if (withInfo != null) queryParams.put("withInfo", withInfo);
      if (withRaw != null) queryParams.put("withRaw", withRaw);
      queryParams.put("entityId", 100L);

      String uriText = this.formatServerUri("reevaluate-entity", queryParams);

      long before = System.currentTimeMillis();
      SzErrorResponse response = this.invokeServerViaHttp(
          POST, uriText, SzErrorResponse.class);
      response.concludeTimers();
      long after = System.currentTimeMillis();

      validateBasics(response,
                     403,
                     POST,
                     uriText,
                     before,
                     after);
    });
  }

  @ParameterizedTest
  @MethodSource("withInfoParams")
  public void reevaluateEntityViaJavaClientTest(Boolean withInfo,
                                                Boolean withRaw)
  {
    this.performTest(() -> {
      final String recordId1 = "ABC123";
      final String recordId2 = "DEF456";
      Map<String, Object> queryParams = new LinkedHashMap<>();
      if (withInfo != null) queryParams.put("withInfo", withInfo);
      if (withRaw != null) queryParams.put("withRaw", withRaw);

      String uriText = this.formatServerUri(
          "data-sources/" + WATCHLIST_DATA_SOURCE + "/records/"
              + recordId1, queryParams);

      Map recordBody = new HashMap();
      recordBody.put("DATA_SOURCE", WATCHLIST_DATA_SOURCE);
      recordBody.put("RECORD_ID", recordId1);
      recordBody.put("NAME_FIRST", "James");
      recordBody.put("NAME_LAST", "Moriarty");
      recordBody.put("PHONE_NUMBER", "702-555-1212");
      recordBody.put("ADDR_FULL", "101 Fifth Ave, Las Vegas, NV 10018");

      long before = System.currentTimeMillis();
      try {
        this.entityDataApi.addRecord(recordBody,
                                     WATCHLIST_DATA_SOURCE,
                                     recordId1,
                                     null,
                                     withInfo,
                                     withRaw);

      } catch (HttpStatusCodeException expected) {
        long after = System.currentTimeMillis();
        com.senzing.gen.api.model.SzErrorResponse clientResponse
            = jsonParse(expected.getResponseBodyAsString(),
                        com.senzing.gen.api.model.SzErrorResponse.class);

        SzErrorResponse response = jsonCopy(clientResponse,
                                            SzErrorResponse.class);

        validateBasics(response,
                       403,
                       PUT,
                       uriText,
                       before,
                       after);
      }

      Long entityId1 = 10L;

      Map<String, Object> queryParams2 = new LinkedHashMap<>();
      queryParams2.put("entityId", entityId1);
      queryParams2.putAll(queryParams);
      uriText = this.formatServerUri("reevaluate-entity", queryParams2);

      before = System.currentTimeMillis();
      try {
        this.entityDataApi.reevaluateEntity(entityId1, withInfo, withRaw);

      } catch (HttpStatusCodeException expected) {
        long after = System.currentTimeMillis();
        com.senzing.gen.api.model.SzErrorResponse clientResponse
            = jsonParse(expected.getResponseBodyAsString(),
                        com.senzing.gen.api.model.SzErrorResponse.class);

        SzErrorResponse response = jsonCopy(clientResponse,
                                            SzErrorResponse.class);

        validateBasics(response,
                       403,
                       POST,
                       uriText,
                       before,
                       after);
      }

      uriText = this.formatServerUri(
          "data-sources/" + CUSTOMER_DATA_SOURCE + "/records/"
              + recordId2, queryParams);

      recordBody = new HashMap();
      recordBody.put("NAME_FIRST", "Joe");
      recordBody.put("NAME_LAST", "Schmoe");
      recordBody.put("PHONE_NUMBER", "702-555-1212");
      recordBody.put("ADDR_FULL", "101 Fifth Ave, Las Vegas, NV 10018");

      before = System.currentTimeMillis();
      try {
        this.entityDataApi.addRecord(recordBody,
                                     CUSTOMER_DATA_SOURCE,
                                     recordId2,
                                     null,
                                     withInfo,
                                     withRaw);

      } catch (HttpStatusCodeException expected) {
        long after = System.currentTimeMillis();
        com.senzing.gen.api.model.SzErrorResponse clientResponse
            = jsonParse(expected.getResponseBodyAsString(),
                        com.senzing.gen.api.model.SzErrorResponse.class);

        SzErrorResponse response = jsonCopy(clientResponse,
                                            SzErrorResponse.class);

        validateBasics(response,
                       403,
                       PUT,
                       uriText,
                       before,
                       after);
      }

      Long entityId2 = 20L;

      queryParams2.clear();
      queryParams2.put("entityId", entityId2);
      queryParams2.putAll(queryParams);
      uriText = this.formatServerUri("reevaluate-entity", queryParams2);

      before = System.currentTimeMillis();
      try {
        this.entityDataApi.reevaluateEntity(entityId2, withInfo, withRaw);
      } catch (HttpStatusCodeException expected) {
        long after = System.currentTimeMillis();
        com.senzing.gen.api.model.SzErrorResponse clientResponse
            = jsonParse(expected.getResponseBodyAsString(),
                        com.senzing.gen.api.model.SzErrorResponse.class);

        SzErrorResponse response = jsonCopy(clientResponse,
                                            SzErrorResponse.class);

        validateBasics(response,
                       403,
                       POST,
                       uriText,
                       before,
                       after);
      }

    });
  }

}
