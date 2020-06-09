package com.senzing.api.services;

import com.senzing.api.model.SzErrorResponse;
import com.senzing.api.server.SzApiServer;
import com.senzing.api.server.SzApiServerOptions;
import com.senzing.util.JsonUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.core.UriInfo;
import java.util.HashMap;
import java.util.Map;

import static com.senzing.api.model.SzHttpMethod.POST;
import static com.senzing.api.model.SzHttpMethod.PUT;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.TestInstance.Lifecycle;
import static com.senzing.api.services.ResponseValidators.*;

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
      try {
        this.entityDataServices.loadRecord(
            TEST_DATA_SOURCE, null, false, false, uriInfo, jsonText);
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
      SzErrorResponse response = this.invokeServerViaHttp(
          POST, uriText, null, recordBody, SzErrorResponse.class);
      response.concludeTimers();
      long after = System.currentTimeMillis();

      validateBasics(response, 403, POST, uriText, before, after);
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
      try {
        this.entityDataServices.loadRecord(
            TEST_DATA_SOURCE, recordId, null, false, false, uriInfo, jsonText);

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
      SzErrorResponse response = this.invokeServerViaHttp(
          PUT, uriText, null, recordBody, SzErrorResponse.class);
      response.concludeTimers();
      long after = System.currentTimeMillis();

      validateBasics(response, 403, PUT, uriText, before, after);
    });
  }
}
