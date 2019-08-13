package com.senzing.api.services;

import com.senzing.api.model.*;
import com.senzing.api.server.SzApiServer;
import com.senzing.g2.engine.*;
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
import java.io.*;
import java.util.*;

import static com.senzing.api.model.SzHttpMethod.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.TestInstance.Lifecycle;
import static com.senzing.io.IOUtilities.*;
import static com.senzing.util.LoggingUtilities.*;

@TestInstance(Lifecycle.PER_CLASS)
public class AutoReinitializeTest extends AbstractServiceTest
{
  private static final Set<String> CUSTOM_DATA_SOURCES;

  private static final Set<String> DEFAULT_DATA_SOURCES;

  private static final Set<String> INITIAL_DATA_SOURCES;

  static {
    Set<String> customSources   = null;
    Set<String> expectedSources = null;
    Set<String> defaultSources  = null;

    try {
      customSources   = new LinkedHashSet<>();
      expectedSources = new LinkedHashSet<>();
      defaultSources  = new LinkedHashSet<>();

      defaultSources.add("TEST");
      defaultSources.add("SEARCH");
      defaultSources = Collections.unmodifiableSet(defaultSources);

      customSources.add("EMPLOYEES");
      customSources.add("CUSTOMERS");
      customSources.add("VENDORS");
      customSources = Collections.unmodifiableSet(customSources);

      expectedSources.addAll(defaultSources);
      expectedSources.addAll(customSources);
      expectedSources = Collections.unmodifiableSet(expectedSources);

    } finally {
      DEFAULT_DATA_SOURCES  = defaultSources;
      CUSTOM_DATA_SOURCES   = customSources;
      INITIAL_DATA_SOURCES = expectedSources;
    }
  }

  private ConfigServices configServices;
  private EntityDataServices entityDataServices;
  private G2ConfigMgr configMgrApi;
  private G2Config configApi;
  private Set<String> expectedDataSources;

  @BeforeAll public void initializeEnvironment() throws IOException {
    this.initializeTestEnvironment();
    this.configServices     = new ConfigServices();
    this.entityDataServices = new EntityDataServices();
    this.configMgrApi       = new G2ConfigMgrJNI();
    this.configApi          = new G2ConfigJNI();
    File initJsonFile       = new File(this.getRepositoryDirectory(),
                                       "g2-init.json");

    String initJson = readTextFileAsString(initJsonFile, "UTF-8");
    this.configMgrApi.initV2(this.getModuleName(),
                             initJson,
                             this.isVerbose());
    this.configApi.initV2(this.getModuleName(),
                          initJson,
                          this.isVerbose());

    this.expectedDataSources = new LinkedHashSet<>();
    this.expectedDataSources.addAll(INITIAL_DATA_SOURCES);
  }

  /**
   * Overridden to configure some data sources.
   */
  protected void prepareRepository() {
    RepositoryManager.configSources(this.getRepositoryDirectory(),
                                    CUSTOM_DATA_SOURCES,
                                    true);
  }

  @AfterAll public void teardownEnvironment() {
    this.teardownTestEnvironment(true);
    if (this.configMgrApi != null) this.configMgrApi.destroy();
    if (this.configApi != null) this.configApi.destroy();
  }

  @Test public void getDataSourcesTest() {
    this.assumeNativeApiAvailable();
    final String newDataSource = "FOO";
    String  uriText = this.formatServerUri("data-sources");
    UriInfo uriInfo = this.newProxyUriInfo(uriText);

    this.addDataSource(newDataSource);

    // now sleep for 1 second longer than the config refresh period
    try {
      Thread.sleep(SzApiServer.CONFIG_REFRESH_PERIOD + 1000L);
    } catch (InterruptedException ignore) {
      fail("Interrupted while sleeping and waiting for config refresh.");
    }

    // now retry the request to get the data sources
    long before = System.currentTimeMillis();
    SzDataSourcesResponse response
        = this.configServices.getDataSources(false, uriInfo);
    response.concludeTimers();
    long after = System.currentTimeMillis();

    synchronized (this.expectedDataSources) {
      this.validateDataSourcesResponse(response,
                                       before,
                                       after,
                                       null,
                                       this.expectedDataSources);
    }
  }

  @Test public void getCurrentConfigTest() {
    final String newDataSource = "PHOO";
    this.assumeNativeApiAvailable();
    String  uriText = this.formatServerUri("config/current");
    UriInfo uriInfo = this.newProxyUriInfo(uriText);

    this.addDataSource(newDataSource);

    // now sleep for 1 second longer than the config refresh period
    try {
      Thread.sleep(SzApiServer.CONFIG_REFRESH_PERIOD + 1000L);
    } catch (InterruptedException ignore) {
      fail("Interrupted while sleeping and waiting for config refresh.");
    }

    long before = System.currentTimeMillis();
    SzConfigResponse response
        = this.configServices.getCurrentConfig(uriInfo);
    response.concludeTimers();
    long after = System.currentTimeMillis();

    synchronized (this.expectedDataSources) {
      this.validateConfigResponse(response,
                                  uriText,
                                  before,
                                  after,
                                  this.expectedDataSources);
    }
  }


  @Test public void postRecordTest() {
    final String newDataSource = "FOOX";
    this.assumeNativeApiAvailable();
    String  uriText = this.formatServerUri(
        "data-sources/" + newDataSource + "/records");
    UriInfo uriInfo = this.newProxyUriInfo(uriText);

    JsonObjectBuilder job = Json.createObjectBuilder();
    job.add("NAME_FIRST", "Joe");
    job.add("NAME_LAST", "Schmoe");
    job.add("PHONE_NUMBER", "702-555-1212");
    job.add("ADDR_FULL", "101 Main Street, Las Vegas, NV 89101");
    JsonObject  jsonObject  = job.build();
    String      jsonText    = JsonUtils.toJsonText(jsonObject);

    // add the data source (so it is there for retry)
    this.addDataSource(newDataSource);

    // now add the record -- this should succeed on retry
    long before = System.currentTimeMillis();
    SzLoadRecordResponse response = this.entityDataServices.loadRecord(
        newDataSource, null, uriInfo, jsonText);
    response.concludeTimers();
    long after = System.currentTimeMillis();

    this.validateLoadRecordResponse(
        response, POST, newDataSource, null, before, after);
  }

  @Test public void putRecordTest() {
    final String recordId = "ABC123";
    final String newDataSource = "PHOOX";

    this.assumeNativeApiAvailable();
    String  uriText = this.formatServerUri(
        "data-sources/" + newDataSource + "/records/" + recordId);
    UriInfo uriInfo = this.newProxyUriInfo(uriText);

    JsonObjectBuilder job = Json.createObjectBuilder();
    job.add("NAME_FIRST", "John");
    job.add("NAME_LAST", "Doe");
    job.add("PHONE_NUMBER", "818-555-1313");
    job.add("ADDR_FULL", "100 Main Street, Los Angeles, CA 90012");
    JsonObject  jsonObject  = job.build();
    String      jsonText    = JsonUtils.toJsonText(jsonObject);

    // add the data source (so it is there for retry)
    this.addDataSource(newDataSource);

    // now add the record -- this should succeed on retry
    long before = System.currentTimeMillis();
    SzLoadRecordResponse response = this.entityDataServices.loadRecord(
        newDataSource, recordId, null, uriInfo, jsonText);
    response.concludeTimers();
    long after = System.currentTimeMillis();

    this.validateLoadRecordResponse(
        response, PUT, newDataSource, recordId, before, after);
  }

  private void addDataSource(String newDataSource) {
    // now get the default config ID
    Result<Long> result = new Result<>();
    int returnCode = this.configMgrApi.getDefaultConfigID(result);
    if (returnCode != 0) {
      String errorMsg = formatError("G2ConfigMgr.getDefaultConfigID",
                                    this.configMgrApi);
      fail("Failure in native API call: " + errorMsg);
    }
    long defaultConfigId = result.getValue();

    // export the config
    StringBuffer sb = new StringBuffer();
    returnCode = this.configMgrApi.getConfig(defaultConfigId, sb);
    if (returnCode != 0) {
      String errorMsg = formatError("G2ConfigMgr.getConfig",
                                    this.configMgrApi);
      fail("Failure in native API call: " + errorMsg);
    }
    String configJsonText = sb.toString();

    // now modify the config
    long configId = this.configApi.load(configJsonText);
    this.configApi.addDataSource(configId, newDataSource);

    // save the new config
    sb.delete(0, sb.length());
    this.configApi.save(configId, sb);
    this.configApi.close(configId);
    returnCode = this.configMgrApi.addConfig(
        sb.toString(), "Added data source FOOBAR", result);
    if (returnCode != 0) {
      String errorMsg = formatError("G2ConfigMgr.addConfig",
                                    this.configMgrApi);
      fail("Failure in native API call: " + errorMsg);
    }

    // set the new config as the default config
    returnCode = this.configMgrApi.setDefaultConfigID(result.getValue());
    if (returnCode != 0) {
      String errorMsg = formatError("G2ConfigMgr.addConfig",
                                    this.configMgrApi);
      fail("Failure in native API call: " + errorMsg);
    }

    synchronized (this.expectedDataSources) {
      this.expectedDataSources.add(newDataSource);
    }
  }
}
