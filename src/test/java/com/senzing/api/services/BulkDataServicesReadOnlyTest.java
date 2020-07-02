package com.senzing.api.services;

import com.senzing.api.model.*;
import com.senzing.api.server.SzApiServer;
import com.senzing.api.server.SzApiServerOptions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.senzing.api.services.ResponseValidators.validateBasics;
import static org.junit.jupiter.api.Assertions.fail;
import static com.senzing.api.model.SzHttpMethod.POST;

public class BulkDataServicesReadOnlyTest extends BulkDataServicesTest {
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


  @ParameterizedTest
  @MethodSource("getLoadBulkRecordsParameters")
  public void loadBulkRecordsViaFormTest(
      String              testInfo,
      MediaType mediaType,
      File bulkDataFile,
      SzBulkDataAnalysis analysis,
      Map<String,String> dataSourceMap,
      Map<String,String>  entityTypeMap)
  {
    this.performTest(() -> {
      this.livePurgeRepository();

      String  uriText = this.formatServerUri("bulk-data/load");

      MultivaluedMap queryParams       = new MultivaluedHashMap();
      String          mapDataSources    = null;
      String          mapEntityTypes    = null;
      List<String> mapDataSourceList = new LinkedList<>();
      List<String>    mapEntityTypeList = new LinkedList<>();
      if (dataSourceMap != null) {
        boolean[]         jsonFlag    = { true };
        boolean[]         overlapFlag = { true };
        JsonObjectBuilder builder   = Json.createObjectBuilder();
        dataSourceMap.entrySet().forEach(entry -> {
          String  key   = entry.getKey();
          String  value = entry.getValue();
          if (jsonFlag[0] || overlapFlag[0]) {
            builder.add(key, value);

          } else {
            String mapping = ":" + key + ":" + value;
            mapDataSourceList.add(mapping);
            queryParams.add("mapDataSource", mapping);
            overlapFlag[0] = !overlapFlag[0];
          }
          jsonFlag[0] = !jsonFlag[0];
        });
        JsonObject jsonObject = builder.build();
        if (jsonObject.size() > 0) {
          mapDataSources = jsonObject.toString();
          queryParams.add("mapDataSources", mapDataSources);
        }
      }

      if (entityTypeMap != null) {
        boolean[]         jsonFlag    = { true };
        boolean[]         overlapFlag = { true };
        JsonObjectBuilder builder   = Json.createObjectBuilder();
        entityTypeMap.entrySet().forEach(entry -> {
          String  key   = entry.getKey();
          String  value = entry.getValue();
          if (jsonFlag[0] || overlapFlag[0]) {
            builder.add(key, value);

          } else {
            String mapping = ":" + key + ":" + value;
            mapEntityTypeList.add(mapping);
            queryParams.add("mapEntityType", mapping);
            overlapFlag[0] = !overlapFlag[0];
          }
          jsonFlag[0] = !jsonFlag[0];
        });
        JsonObject jsonObject = builder.build();
        if (jsonObject.size() > 0) {
          mapEntityTypes = jsonObject.toString();
          queryParams.add("mapEntityTypes", mapDataSources);
        }
      }

      UriInfo uriInfo = this.newProxyUriInfo(uriText, queryParams);
      long before = System.currentTimeMillis();

      try (FileInputStream fis = new FileInputStream(bulkDataFile)) {
        SzBulkLoadResponse response
            = this.bulkDataServices.loadBulkRecordsViaForm(
            CONTACTS_DATA_SOURCE,
            mapDataSources,
            mapDataSourceList,
            GENERIC_ENTITY_TYPE,
            mapEntityTypes,
            mapEntityTypeList,
            null,
            0,
            mediaType,
            fis,
            null,
            uriInfo);

        fail("Expected bulk load to be forbidden, but it succeeded.");

      } catch (ForbiddenException expected) {
        SzErrorResponse response
            = (SzErrorResponse) expected.getResponse().getEntity();
        response.concludeTimers();
        long after = System.currentTimeMillis();
        validateBasics(
            testInfo, response, 403, POST, uriText, before, after);

      } catch (Exception e) {
        System.err.println("********** FAILED TEST: " + testInfo);
        e.printStackTrace();
        if (e instanceof RuntimeException) throw ((RuntimeException) e);
        throw new RuntimeException(e);
      }
    });
  }

  @ParameterizedTest
  @MethodSource("getLoadBulkRecordsParameters")
  public void loadBulkRecordsDirectHttpTest(
      String              testInfo,
      MediaType           mediaType,
      File                bulkDataFile,
      SzBulkDataAnalysis  analysis,
      Map<String,String>  dataSourceMap,
      Map<String,String>  entityTypeMap)
  {
    this.performTest(() -> {
      this.livePurgeRepository();

      String uriText = this.formatServerUri(formatLoadURL(
          CONTACTS_DATA_SOURCE, GENERIC_ENTITY_TYPE, null, null,
          dataSourceMap, entityTypeMap, null));

      try (FileInputStream fis = new FileInputStream(bulkDataFile)) {
        long before = System.currentTimeMillis();
        SzErrorResponse response = this.invokeServerViaHttp(
            POST, uriText, null, mediaType.toString(),
            bulkDataFile.length(), new FileInputStream(bulkDataFile),
            SzErrorResponse.class);
        response.concludeTimers();
        long after = System.currentTimeMillis();

        validateBasics(
            testInfo, response, 403, POST, uriText, before, after);

      } catch (Exception e) {
        System.err.println("********** FAILED TEST: " + testInfo);
        e.printStackTrace();
        if (e instanceof RuntimeException) throw ((RuntimeException) e);
        throw new RuntimeException(e);
      }
    });
  }

  @ParameterizedTest
  @MethodSource("getLoadBulkRecordsParameters")
  public void loadBulkRecordsDirectJavaClientTest(
      String              testInfo,
      MediaType           mediaType,
      File                bulkDataFile,
      SzBulkDataAnalysis  analysis,
      Map<String,String>  dataSourceMap,
      Map<String,String>  entityTypeMap)
  {
    this.performTest(() -> {
      this.livePurgeRepository();

      String uriText = this.formatServerUri(formatLoadURL(
          CONTACTS_DATA_SOURCE, GENERIC_ENTITY_TYPE, null, null,
          dataSourceMap, entityTypeMap, null));

      try (FileInputStream fis = new FileInputStream(bulkDataFile)) {
        long before = System.currentTimeMillis();
        com.senzing.gen.api.model.SzErrorResponse clientResponse
            = this.invokeServerViaHttp(
            POST, uriText, null, mediaType.toString(),
            bulkDataFile.length(), new FileInputStream(bulkDataFile),
            com.senzing.gen.api.model.SzErrorResponse.class);
        long after = System.currentTimeMillis();

        SzErrorResponse response = jsonCopy(clientResponse,
                                            SzErrorResponse.class);

        validateBasics(
            testInfo, response, 403, POST, uriText, before, after);

      } catch (Exception e) {
        System.err.println("********** FAILED TEST: " + testInfo);
        e.printStackTrace();
        if (e instanceof RuntimeException) throw ((RuntimeException) e);
        throw new RuntimeException(e);
      }
    });
  }

  @ParameterizedTest
  @MethodSource("getMaxFailureArgs")
  public void testMaxFailuresOnLoad(
      int                   recordCount,
      Integer               maxFailures,
      SzBulkDataStatus expectedStatus,
      Map<String, Integer>  failuresByDataSource,
      Map<String, Integer>  failuresByEntityType,
      File                  dataFile)
  {
    this.performTest(() -> {
      this.livePurgeRepository();

      String testInfo = "recordCount=[ " + recordCount + " ], maxFailures=[ "
          + maxFailures + " ], status=[ "
          + expectedStatus + " ], failuresByDataSource=[ "
          + failuresByDataSource + " ], failuresByEntityType=[ "
          + failuresByEntityType + " ], dataFile=[ " + dataFile + " ]";

      String uriText = this.formatServerUri("bulk-data/load");

      MultivaluedMap queryParams = new MultivaluedHashMap();
      if (maxFailures != null) {
        queryParams.add("maxFailures", String.valueOf(maxFailures));
      }
      UriInfo uriInfo = this.newProxyUriInfo(uriText, queryParams);

      long before = System.currentTimeMillis();
      try (InputStream is = new FileInputStream(dataFile);
           BufferedInputStream bis = new BufferedInputStream(is)) {
        this.bulkDataServices.loadBulkRecordsViaForm(
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            maxFailures == null ? -1 : maxFailures,
            MediaType.valueOf("text/plain"),
            bis,
            null,
            uriInfo);

        fail("Expected bulk load to be forbidden, but it succeeded.");

      } catch (ForbiddenException expected) {
        SzErrorResponse response
            = (SzErrorResponse) expected.getResponse().getEntity();
        response.concludeTimers();
        long after = System.currentTimeMillis();
        validateBasics(
            testInfo, response, 403, POST, uriText, before, after);


      } catch (Exception e) {
        System.err.println("********** FAILED TEST: " + testInfo);
        e.printStackTrace();
        if (e instanceof RuntimeException) throw ((RuntimeException) e);
        throw new RuntimeException(e);
      }
    });
  }

}
