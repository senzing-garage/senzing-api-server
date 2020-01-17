package com.senzing.api.services;

import com.senzing.api.model.SzLoadRecordResponse;
import com.senzing.repomgr.RepositoryManager;
import com.senzing.util.JsonUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.ws.rs.core.UriInfo;

import java.util.*;

import static org.junit.jupiter.api.TestInstance.Lifecycle;
import static com.senzing.api.model.SzHttpMethod.*;
import static com.senzing.api.services.ResponseValidators.*;

@TestInstance(Lifecycle.PER_CLASS)
public class EntityDataWriteServicesTest extends AbstractServiceTest {
  protected static final String TEST_DATA_SOURCE = "CUSTOMERS";

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
    RepositoryManager.configSources(this.getRepositoryDirectory(),
                                    Collections.singleton(TEST_DATA_SOURCE),
                                    true);
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
          "data-sources/" + TEST_DATA_SOURCE + "/records");
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
          TEST_DATA_SOURCE, null, uriInfo, jsonText);
      response.concludeTimers();
      long after = System.currentTimeMillis();

      validateLoadRecordResponse(response,
                                 POST,
                                 uriText,
                                 TEST_DATA_SOURCE,
                                 null,
                                 before,
                                 after);
    });
  }

  @Test public void postRecordTestViaHttp() {
    this.performTest(() -> {
      String  uriText = this.formatServerUri(
          "data-sources/" + TEST_DATA_SOURCE + "/records");
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
                                 TEST_DATA_SOURCE,
                                 null,
                                 before,
                                 after);
    });
  }

  @Test public void putRecordTest() {
    this.performTest(() -> {
      final String recordId = "ABC123";

      String  uriText = this.formatServerUri(
          "data-sources/" + TEST_DATA_SOURCE + "/records/" + recordId);
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
          TEST_DATA_SOURCE, recordId, null, uriInfo, jsonText);
      response.concludeTimers();
      long after = System.currentTimeMillis();

      validateLoadRecordResponse(response,
                                 PUT,
                                 uriText,
                                 TEST_DATA_SOURCE,
                                 recordId,
                                 before,
                                 after);
    });
  }

  @Test public void putRecordTestViaHttp() {
    this.performTest(() -> {
      final String recordId = "XYZ456";

      String  uriText = this.formatServerUri(
          "data-sources/" + TEST_DATA_SOURCE + "/records/" + recordId);
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
                                 TEST_DATA_SOURCE,
                                 recordId,
                                 before,
                                 after);

    });
  }
}
