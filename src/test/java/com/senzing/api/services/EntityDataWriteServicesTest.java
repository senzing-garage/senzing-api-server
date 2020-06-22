package com.senzing.api.services;

import com.senzing.api.model.SzErrorResponse;
import com.senzing.api.model.SzLoadRecordResponse;
import com.senzing.repomgr.RepositoryManager;
import com.senzing.util.JsonUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.json.*;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.core.UriInfo;

import java.util.*;

import static org.junit.jupiter.api.TestInstance.Lifecycle;
import static com.senzing.api.model.SzHttpMethod.*;
import static com.senzing.api.services.ResponseValidators.*;
import static com.senzing.repomgr.RepositoryManager.Configuration;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.junit.jupiter.api.Assertions.*;

@TestInstance(Lifecycle.PER_CLASS)
public class EntityDataWriteServicesTest extends AbstractServiceTest {
  protected static final String CUSTOMER_DATA_SOURCE = "CUSTOMERS";
  protected static final String WATCHLIST_DATA_SOURCE = "WATCHLIST";
  protected static final String WATCHLIST_FLAG = "WATCHLIST-THREAT";

  protected EntityDataServices entityDataServices;

  @BeforeAll public void initializeEnvironment() {
    this.beginTests();
    this.initializeTestEnvironment();
    this.entityDataServices = new EntityDataServices();
  }

  /**
   * Overridden to configure some data sources.
   */
  protected void prepareRepository() {
    Configuration config = RepositoryManager.configSources(
        this.getRepositoryDirectory(),
        Set.of(CUSTOMER_DATA_SOURCE, WATCHLIST_DATA_SOURCE),
        true);

    // get the data source ID for the watch list
    JsonObject g2Config = config.getConfigJson().getJsonObject("G2_CONFIG");
    JsonArray cfgDsrc = g2Config.getJsonArray("CFG_DSRC");
    Integer watchListId = null;
    for (JsonObject dsrcObj : cfgDsrc.getValuesAs(JsonObject.class)) {
      if (dsrcObj.getString("DSRC_CODE").equals(WATCHLIST_DATA_SOURCE)) {
        watchListId = dsrcObj.getInt("DSRC_ID");
        break;
      }
    }

    // configure the flags for interesting entities
    JsonObjectBuilder g2ConfigJob = Json.createObjectBuilder(g2Config);
    g2ConfigJob.remove("CFG_DSRC_INTEREST");
    JsonArrayBuilder  jab = Json.createArrayBuilder();
    JsonObjectBuilder job = Json.createObjectBuilder();
    job.add("DSRC_ID", watchListId);
    job.add("MAX_DEGREE", 2);
    job.add("INTEREST_FLAG", WATCHLIST_FLAG);
    jab.add(job);
    g2ConfigJob.add("CFG_DSRC_INTEREST", jab);
    JsonObjectBuilder configJob = Json.createObjectBuilder();
    configJob.add("G2_CONFIG", g2ConfigJob);
    JsonObject configJson = configJob.build();

    // update the config
    RepositoryManager.updateConfig(this.getRepositoryDirectory(),
                                   configJson,
                                   "Added interesting entity flags",
                                   false);
  }

  @AfterAll public void teardownEnvironment() {
    try {
      this.teardownTestEnvironment();
      this.conditionallyLogCounts(true);
    } finally {
      this.endTests();
    }
  }

  @Test public void postRecordTest() {
    this.performTest(() -> {
      String  uriText = this.formatServerUri(
          "data-sources/" + CUSTOMER_DATA_SOURCE + "/records");
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      JsonObjectBuilder job = Json.createObjectBuilder();
      job.add("NAME_FIRST", "Joe");
      job.add("NAME_LAST", "Schmoe");
      job.add("PHONE_NUMBER", "702-555-1212");
      job.add("ADDR_FULL", "101 Main Street, Las Vegas, NV 89101");
      JsonObject  jsonObject  = job.build();
      String      jsonText    = JsonUtils.toJsonText(jsonObject);

      long before = System.currentTimeMillis();
      SzLoadRecordResponse response = this.entityDataServices.loadRecord(
          CUSTOMER_DATA_SOURCE, null, false, false, uriInfo, jsonText);
      response.concludeTimers();
      long after = System.currentTimeMillis();

      validateLoadRecordResponse(response,
                                 POST,
                                 uriText,
                                 CUSTOMER_DATA_SOURCE,
                                 null,
                                 false,
                                 false,
                                 0,
                                 0,
                                 Collections.emptySet(),
                                 before,
                                 after);
    });
  }

  @Test public void postRecordTestViaHttp() {
    this.performTest(() -> {
      String  uriText = this.formatServerUri(
          "data-sources/" + CUSTOMER_DATA_SOURCE + "/records");
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      Map recordBody = new HashMap();
      recordBody.put("NAME_FIRST", "Joanne");
      recordBody.put("NAME_LAST", "Smith");
      recordBody.put("PHONE_NUMBER", "212-555-1212");
      recordBody.put("ADDR_FULL", "101 Fifth Ave, Las Vegas, NV 10018");

      long before = System.currentTimeMillis();
      SzLoadRecordResponse response = this.invokeServerViaHttp(
          POST, uriText, null, recordBody, SzLoadRecordResponse.class);
      response.concludeTimers();
      long after = System.currentTimeMillis();

      validateLoadRecordResponse(response,
                                 POST,
                                 uriText,
                                 CUSTOMER_DATA_SOURCE,
                                 null,
                                 false,
                                 false,
                                 0,
                                 0,
                                 Collections.emptySet(),
                                 before,
                                 after);
    });
  }

  protected List<Arguments> addRecordWithInfoParams() {
    List<Arguments> argumentsList = new LinkedList<>();
    Boolean[] booleans = { null, Boolean.TRUE, Boolean.FALSE };
    for (Boolean withInfo : booleans) {
      for (Boolean withRaw : booleans) {
        argumentsList.add(arguments(withInfo, withRaw));
      }
    }
    return argumentsList;
  }

  @ParameterizedTest
  @MethodSource("addRecordWithInfoParams")
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
      SzLoadRecordResponse response = this.entityDataServices.loadRecord(
          WATCHLIST_DATA_SOURCE,
          null,
          (withInfo != null ? withInfo : false),
          (withRaw != null ? withRaw : false),
          uriInfo,
          jsonText);
      response.concludeTimers();
      long after = System.currentTimeMillis();

      validateLoadRecordResponse(response,
                                 POST,
                                 uriText,
                                 WATCHLIST_DATA_SOURCE,
                                 null,
                                 (withInfo != null ? withInfo : false),
                                 (withRaw != null ? withRaw : false),
                                 1,
                                 0,
                                 Collections.emptySet(),
                                 before,
                                 after);

      uriText = this.formatServerUri(
          "data-sources/" + CUSTOMER_DATA_SOURCE + "/records",
          queryParams);
      uriInfo = this.newProxyUriInfo(uriText);

      job = Json.createObjectBuilder();
      job.add("NAME_FIRST", "Joe");
      job.add("NAME_LAST", "Schmoe");
      job.add("PHONE_NUMBER", "702-555-1212");
      job.add("ADDR_FULL", "101 Main Street, Las Vegas, NV 89101");
      jsonObject  = job.build();
      jsonText    = JsonUtils.toJsonText(jsonObject);

      before = System.currentTimeMillis();
      response = this.entityDataServices.loadRecord(
          CUSTOMER_DATA_SOURCE,
          null,
          (withInfo != null ? withInfo : false),
          (withRaw != null ? withRaw : false),
          uriInfo,
          jsonText);
      response.concludeTimers();
      after = System.currentTimeMillis();

      validateLoadRecordResponse(response,
                                 POST,
                                 uriText,
                                 CUSTOMER_DATA_SOURCE,
                                 null,
                                 (withInfo != null ? withInfo : false),
                                 (withRaw != null ? withRaw : false),
                                 1,
                                 1,
                                 Set.of(WATCHLIST_FLAG),
                                 before,
                                 after);
    });
  }

  @ParameterizedTest
  @MethodSource("addRecordWithInfoParams")
  public void postRecordWithInfoTestViaHttp(Boolean withInfo,
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
      SzLoadRecordResponse response = this.invokeServerViaHttp(
          POST, uriText, null, recordBody, SzLoadRecordResponse.class);
      response.concludeTimers();
      long after = System.currentTimeMillis();

      validateLoadRecordResponse(response,
                                 POST,
                                 uriText,
                                 WATCHLIST_DATA_SOURCE,
                                 null,
                                 (withInfo != null ? withInfo : false),
                                 (withRaw != null ? withRaw : false),
                                 1,
                                 0,
                                 Collections.emptySet(),
                                 before,
                                 after);

      uriText = this.formatServerUri(
          "data-sources/" + CUSTOMER_DATA_SOURCE + "/records",
          queryParams);

      recordBody = new HashMap();
      recordBody.put("NAME_FIRST", "Joe");
      recordBody.put("NAME_LAST", "Schmoe");
      recordBody.put("PHONE_NUMBER", "702-555-1212");
      recordBody.put("ADDR_FULL", "101 Fifth Ave, Las Vegas, NV 10018");

      before = System.currentTimeMillis();
      response = this.invokeServerViaHttp(
          POST, uriText, null, recordBody, SzLoadRecordResponse.class);
      response.concludeTimers();
      after = System.currentTimeMillis();

      validateLoadRecordResponse(response,
                                 POST,
                                 uriText,
                                 CUSTOMER_DATA_SOURCE,
                                 null,
                                 (withInfo != null ? withInfo : false),
                                 (withRaw != null ? withRaw : false),
                                 1,
                                 1,
                                 Set.of(WATCHLIST_FLAG),
                                 before,
                                 after);
    });
  }

  @Test public void putRecordTest() {
    this.performTest(() -> {
      final String recordId = "ABC123";

      String  uriText = this.formatServerUri(
          "data-sources/" + CUSTOMER_DATA_SOURCE + "/records/" + recordId);
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      JsonObjectBuilder job = Json.createObjectBuilder();
      job.add("NAME_FIRST", "John");
      job.add("NAME_LAST", "Doe");
      job.add("PHONE_NUMBER", "818-555-1313");
      job.add("ADDR_FULL", "100 Main Street, Los Angeles, CA 90012");
      JsonObject  jsonObject  = job.build();
      String      jsonText    = JsonUtils.toJsonText(jsonObject);

      long before = System.currentTimeMillis();
      SzLoadRecordResponse response = this.entityDataServices.loadRecord(
          CUSTOMER_DATA_SOURCE, recordId, null, false, false, uriInfo, jsonText);
      response.concludeTimers();
      long after = System.currentTimeMillis();

      validateLoadRecordResponse(response,
                                 PUT,
                                 uriText,
                                 CUSTOMER_DATA_SOURCE,
                                 recordId,
                                 false,
                                 false,
                                 0,
                                 0,
                                 Collections.emptySet(),
                                 before,
                                 after);
    });
  }

  @Test public void putRecordTestViaHttp() {
    this.performTest(() -> {
      final String recordId = "XYZ456";

      String  uriText = this.formatServerUri(
          "data-sources/" + CUSTOMER_DATA_SOURCE + "/records/" + recordId);
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      Map recordBody = new HashMap();
      recordBody.put("NAME_FIRST", "Jane");
      recordBody.put("NAME_LAST", "Doe");
      recordBody.put("PHONE_NUMBER", "818-555-1212");
      recordBody.put("ADDR_FULL", "500 First Street, Los Angeles, CA 90033");

      long before = System.currentTimeMillis();
      SzLoadRecordResponse response = this.invokeServerViaHttp(
          PUT, uriText, null, recordBody, SzLoadRecordResponse.class);
      response.concludeTimers();
      long after = System.currentTimeMillis();

      validateLoadRecordResponse(response,
                                 PUT,
                                 uriText,
                                 CUSTOMER_DATA_SOURCE,
                                 recordId,
                                 false,
                                 false,
                                 0,
                                 0,
                                 Collections.emptySet(),
                                 before,
                                 after);

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

        fail("Expected BadRequestException for mismatched RECORD_ID");

      } catch (BadRequestException expected) {
        SzErrorResponse response
            = (SzErrorResponse) expected.getResponse().getEntity();
        response.concludeTimers();
        long after = System.currentTimeMillis();
        validateBasics(response, 400, PUT, uriText, before, after);
      }
    });
  }

  @Test public void putMismatchedRecordTestViaHttp() {
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
                     400,
                     PUT,
                     uriText,
                     before,
                     after);
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

        fail("Expected BadRequestException for mismatched DATA_SOURCE");

      } catch (BadRequestException expected) {
        SzErrorResponse response
            = (SzErrorResponse) expected.getResponse().getEntity();
        response.concludeTimers();
        long after = System.currentTimeMillis();
        validateBasics(response, 400, PUT, uriText, before, after);
      }
    });
  }

  @Test public void putMismatchedDataSourceTestViaHttp() {
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
                     400,
                     PUT,
                     uriText,
                     before,
                     after);
    });
  }

  @ParameterizedTest
  @MethodSource("addRecordWithInfoParams")
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
      SzLoadRecordResponse response = this.entityDataServices.loadRecord(
          WATCHLIST_DATA_SOURCE,
          "ABC123",
          null,
          (withInfo != null ? withInfo : false),
          (withRaw != null ? withRaw : false),
          uriInfo,
          jsonText);
      response.concludeTimers();
      long after = System.currentTimeMillis();

      validateLoadRecordResponse(response,
                                 PUT,
                                 uriText,
                                 WATCHLIST_DATA_SOURCE,
                                 "ABC123",
                                 (withInfo != null ? withInfo : false),
                                 (withRaw != null ? withRaw : false),
                                 1,
                                 0,
                                 Collections.emptySet(),
                                 before,
                                 after);

      uriText = this.formatServerUri(
          "data-sources/" + CUSTOMER_DATA_SOURCE + "/records/DEF456",
          queryParams);
      uriInfo = this.newProxyUriInfo(uriText);

      job = Json.createObjectBuilder();
      job.add("NAME_FIRST", "Joe");
      job.add("NAME_LAST", "Schmoe");
      job.add("PHONE_NUMBER", "702-555-1212");
      job.add("ADDR_FULL", "101 Main Street, Las Vegas, NV 89101");
      jsonObject  = job.build();
      jsonText    = JsonUtils.toJsonText(jsonObject);

      before = System.currentTimeMillis();
      response = this.entityDataServices.loadRecord(
          CUSTOMER_DATA_SOURCE,
          "DEF456",
          null,
          (withInfo != null ? withInfo : false),
          (withRaw != null ? withRaw : false),
          uriInfo,
          jsonText);
      response.concludeTimers();
      after = System.currentTimeMillis();

      validateLoadRecordResponse(response,
                                 PUT,
                                 uriText,
                                 CUSTOMER_DATA_SOURCE,
                                 "DEF456",
                                 (withInfo != null ? withInfo : false),
                                 (withRaw != null ? withRaw : false),
                                 1,
                                 1,
                                 Set.of(WATCHLIST_FLAG),
                                 before,
                                 after);
    });
  }

  @ParameterizedTest
  @MethodSource("addRecordWithInfoParams")
  public void putRecordWithInfoTestViaHttp(Boolean withInfo,
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
      SzLoadRecordResponse response = this.invokeServerViaHttp(
          PUT, uriText, null, recordBody, SzLoadRecordResponse.class);
      response.concludeTimers();
      long after = System.currentTimeMillis();

      validateLoadRecordResponse(response,
                                 PUT,
                                 uriText,
                                 WATCHLIST_DATA_SOURCE,
                                 "ABC123",
                                 (withInfo != null ? withInfo : false),
                                 (withRaw != null ? withRaw : false),
                                 1,
                                 0,
                                 Collections.emptySet(),
                                 before,
                                 after);

      uriText = this.formatServerUri(
          "data-sources/" + CUSTOMER_DATA_SOURCE + "/records/DEF456",
          queryParams);

      recordBody = new HashMap();
      recordBody.put("NAME_FIRST", "Joe");
      recordBody.put("NAME_LAST", "Schmoe");
      recordBody.put("PHONE_NUMBER", "702-555-1212");
      recordBody.put("ADDR_FULL", "101 Fifth Ave, Las Vegas, NV 10018");

      before = System.currentTimeMillis();
      response = this.invokeServerViaHttp(
          PUT, uriText, null, recordBody, SzLoadRecordResponse.class);
      response.concludeTimers();
      after = System.currentTimeMillis();

      validateLoadRecordResponse(response,
                                 PUT,
                                 uriText,
                                 CUSTOMER_DATA_SOURCE,
                                 "DEF456",
                                 (withInfo != null ? withInfo : false),
                                 (withRaw != null ? withRaw : false),
                                 1,
                                 1,
                                 Set.of(WATCHLIST_FLAG),
                                 before,
                                 after);
    });
  }

}
