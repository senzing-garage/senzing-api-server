package com.senzing.api.services;

import com.senzing.api.model.*;
import com.senzing.nativeapi.NativeApiFactory;
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
import static com.senzing.api.services.ResponseValidators.*;

@TestInstance(Lifecycle.PER_CLASS)
public class AutoReinitializeTest extends AbstractServiceTest
{
  protected static final Map<String, SzDataSource> CUSTOM_DATA_SOURCES;

  protected static final Map<String, SzDataSource> DEFAULT_DATA_SOURCES;

  protected static final Map<String, SzDataSource> INITIAL_DATA_SOURCES;

  static {
    Map<String, SzDataSource> customSources   = null;
    Map<String, SzDataSource> expectedSources = null;
    Map<String, SzDataSource> defaultSources  = null;

    try {
      customSources   = new LinkedHashMap<>();
      expectedSources = new LinkedHashMap<>();
      defaultSources  = new LinkedHashMap<>();

      List<SzDataSource> sources = new LinkedList<>();
      sources.add(new SzDataSource("TEST", 1));
      sources.add(new SzDataSource("SEARCH", 2));
      for (SzDataSource source : sources) {
        defaultSources.put(source.getDataSourceCode(), source);
      }
      defaultSources = Collections.unmodifiableMap(defaultSources);

      sources.clear();
      sources.add(new SzDataSource("EMPLOYEES", 1001));
      sources.add(new SzDataSource("CUSTOMERS", 1002));
      sources.add(new SzDataSource("VENDORS", 1003));
      for (SzDataSource source: sources) {
        customSources.put(source.getDataSourceCode(), source);
      }
      customSources = Collections.unmodifiableMap(customSources);

      expectedSources.putAll(defaultSources);
      expectedSources.putAll(customSources);
      expectedSources = Collections.unmodifiableMap(expectedSources);

    } catch (Exception e) {
      e.printStackTrace();
      throw new ExceptionInInitializerError(e);

    } finally {
      DEFAULT_DATA_SOURCES  = defaultSources;
      CUSTOM_DATA_SOURCES   = customSources;
      INITIAL_DATA_SOURCES = expectedSources;
    }
  }

  protected ConfigServices configServices;
  protected EntityDataServices entityDataServices;
  protected G2ConfigMgr configMgrApi;
  protected G2Config configApi;
  protected Map<String, SzDataSource> expectedDataSources;

  @BeforeAll public void initializeEnvironment() {
    this.beginTests();
    this.initializeTestEnvironment();
    this.configServices       = new ConfigServices();
    this.entityDataServices   = new EntityDataServices();
    this.expectedDataSources  = new LinkedHashMap<>();
    this.expectedDataSources.putAll(INITIAL_DATA_SOURCES);
  }

  /**
   * Overridden to configure some data sources.
   */
  protected void prepareRepository() {
    RepositoryManager.configSources(this.getRepositoryDirectory(),
                                    CUSTOM_DATA_SOURCES.keySet(),
                                    true);
    if (this.checkNativeApiAvailable()) {
      try {
        this.configMgrApi = NativeApiFactory.createConfigMgrApi();
        this.configApi    = NativeApiFactory.createConfigApi();
        File initJsonFile = new File(this.getRepositoryDirectory(),
                                     "g2-init.json");

        String initJson = readTextFileAsString(initJsonFile, "UTF-8");
        this.configMgrApi.initV2(this.getModuleName("RepoMgr (reconfigure)"),
                                 initJson,
                                 this.isVerbose());
        this.configApi.initV2(this.getModuleName("RepoMgr (reconfigure)"),
                              initJson,
                              this.isVerbose());

      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @AfterAll public void teardownEnvironment() {
    try {
      this.teardownTestEnvironment();
      if (this.configMgrApi != null) this.configMgrApi.destroy();
      if (this.configApi != null) this.configApi.destroy();
      this.conditionallyLogCounts(true);
    } finally {
      this.endTests();
    }
  }

  @Test public void getDataSourcesTest() {
    this.performTest(()-> {
      final String newDataSource = "FOO";
      String  uriText = this.formatServerUri("data-sources");
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      this.addDataSource(newDataSource);

      // now request a config refresh check
      this.requestConfigRefreshCheck();

      // now retry the request to get the data sources
      long before = System.currentTimeMillis();
      SzDataSourcesResponse response
          = this.configServices.getDataSources(false, uriInfo);
      response.concludeTimers();
      long after = System.currentTimeMillis();

      synchronized (this.expectedDataSources) {
        validateDataSourcesResponse(response,
                                    GET,
                                    uriText,
                                    before,
                                    after,
                                    false,
                                    this.expectedDataSources);
      }
    });
  }

  @Test public void getActiveConfigTest() {
    this.performTest(() -> {
      final String newDataSource = "PHOO";
      String  uriText = this.formatServerUri("config/current");
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      this.addDataSource(newDataSource);

      // now request a config refresh check
      this.requestConfigRefreshCheck();

      long before = System.currentTimeMillis();
      SzConfigResponse response
          = this.configServices.getActiveConfig(uriInfo);
      response.concludeTimers();
      long after = System.currentTimeMillis();

      synchronized (this.expectedDataSources) {
        validateConfigResponse(response,
                               uriText,
                               before,
                               after,
                               this.expectedDataSources.keySet());
      }
    });
  }

  @Test public void postRecordTest() {
    this.performTest(() -> {
      final String newDataSource = "FOOX";
      String  uriText = this.formatServerUri(
          "data-sources/" + newDataSource + "/records");
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      JsonObjectBuilder job = Json.createObjectBuilder();
      job.add("NAME_FIRST", "Joe");
      job.add("NAME_LAST", "Schmoe");
      job.add("PHONE_NUMBER", "702-555-1212");
      job.add("ADDR_FULL", "101 Main Street, Las Vegas, NV 89101");
      job.add("ENTITY_TYPE", "GENERIC");
      JsonObject  jsonObject  = job.build();
      String      jsonText    = JsonUtils.toJsonText(jsonObject);

      // add the data source (so it is there for retry)
      this.addDataSource(newDataSource);

      // now add the record -- this should succeed on retry
      long before = System.currentTimeMillis();
      SzLoadRecordResponse response = this.entityDataServices.loadRecord(
          newDataSource, null, false, false, uriInfo, jsonText);
      response.concludeTimers();
      long after = System.currentTimeMillis();

      validateLoadRecordResponse(response,
                                 POST,
                                 uriText,
                                 newDataSource,
                                 null,
                                 null,
                                 null,
                                 null,
                                 null,
                                 null,
                                 before,
                                 after);
    });
  }

  @Test public void putRecordTest() {
    this.performTest(() -> {
      final String recordId = "ABC123";
      final String newDataSource = "PHOOX";

      String  uriText = this.formatServerUri(
          "data-sources/" + newDataSource + "/records/" + recordId);
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      JsonObjectBuilder job = Json.createObjectBuilder();
      job.add("NAME_FIRST", "John");
      job.add("NAME_LAST", "Doe");
      job.add("PHONE_NUMBER", "818-555-1313");
      job.add("ADDR_FULL", "100 Main Street, Los Angeles, CA 90012");
      job.add("ENTITY_TYPE", "GENERIC");
      JsonObject  jsonObject  = job.build();
      String      jsonText    = JsonUtils.toJsonText(jsonObject);

      // add the data source (so it is there for retry)
      this.addDataSource(newDataSource);

      // now add the record -- this should succeed on retry
      long before = System.currentTimeMillis();
      SzLoadRecordResponse response = this.entityDataServices.loadRecord(
          newDataSource, recordId, null, false, false, uriInfo, jsonText);
      response.concludeTimers();
      long after = System.currentTimeMillis();

      validateLoadRecordResponse(response,
                                 PUT,
                                 uriText,
                                 newDataSource,
                                 recordId,
                                 null,
                                 null,
                                 null,
                                 null,
                                 null,
                                 before,
                                 after);
    });
  }

  protected SzDataSource addDataSource(String newDataSource) {
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

    // create the JSON to create the data source
    JsonObjectBuilder job = Json.createObjectBuilder();
    job.add("DSRC_CODE", newDataSource);
    JsonObject jsonObject = job.build();
    String jsonText = JsonUtils.toJsonText(jsonObject);

    // now modify the config
    sb.delete(0, sb.length());
    long configId = this.configApi.load(configJsonText);
    this.configApi.addDataSourceV2(configId, jsonText, sb);

    jsonObject = JsonUtils.parseJsonObject(sb.toString());
    int dataSourceId = jsonObject.getInt("DSRC_ID");

    // save the new config
    sb.delete(0, sb.length());
    this.configApi.save(configId, sb);
    this.configApi.close(configId);
    returnCode = this.configMgrApi.addConfig(
        sb.toString(), "Added data source " + newDataSource, result);
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

    SzDataSource newDS = new SzDataSource(newDataSource, dataSourceId);
    synchronized (this.expectedDataSources) {
      this.expectedDataSources.put(newDS.getDataSourceCode(), newDS);
    }

    return newDS;
  }
}
