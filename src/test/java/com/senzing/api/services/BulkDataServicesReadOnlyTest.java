package com.senzing.api.services;

import com.senzing.api.model.*;
import com.senzing.api.server.SzApiServer;
import com.senzing.api.server.SzApiServerOptions;
import com.senzing.util.JsonUtilities;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.websocket.ContainerProvider;
import javax.websocket.Session;
import javax.websocket.WebSocketContainer;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;
import java.io.*;
import java.net.URI;
import java.net.URL;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.senzing.api.services.ResponseValidators.validateBasics;
import static com.senzing.io.IOUtilities.UTF_8;
import static com.senzing.io.RecordReader.Format.*;
import static org.junit.jupiter.api.Assertions.fail;
import static com.senzing.api.model.SzHttpMethod.POST;
import static com.senzing.api.model.SzHttpMethod.GET;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
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

  @Test
  public void testCSVWithBadMediaType() {
    this.performTest(() -> {
      String uriText1 = this.formatServerUri("bulk-data/analyze");
      UriInfo uriInfo1 = this.newProxyUriInfo(uriText1);

      String uriText2 = this.formatServerUri("bulk-data/load");
      UriInfo uriInfo2 = this.newProxyUriInfo(uriText2);

      String testInfo = "Test CSV analyze with bad media type";
      File bulkDataFile = null;

      try {
        bulkDataFile = File.createTempFile("bulk-data-", ".csv");

        try (FileOutputStream fos = new FileOutputStream(bulkDataFile);
            OutputStreamWriter osw = new OutputStreamWriter(fos, UTF_8);
            PrintWriter pw = new PrintWriter(new BufferedWriter(osw))) {
          pw.println("RECORD_ID,DATA_SOURCE,NAME_FULL,PHONE_NUMBER");
          pw.println("ABC123,\"" + CUSTOMER_DATA_SOURCE
              + "\"  ,\"JOE SCHMOE\"  ,702-555-1212");
          pw.println("DEF456,   \"" + CUSTOMER_DATA_SOURCE
              + "\",  \"JOHN DOE\",702-555-1313");
          pw.println("GHI789,   \"" + CUSTOMER_DATA_SOURCE
              + "\"  ,  \"JANE SMITH\"  ,702-555-1313");
          pw.flush();
        }

      } catch (IOException e) {
        fail(e);
      }

      MediaType[] badMediaTypes = {
          MediaType.valueOf(JSON.getMediaType()),
          MediaType.valueOf(JSON_LINES.getMediaType()) };

      for (MediaType badMediaType : badMediaTypes) {
        try (FileInputStream fis1 = new FileInputStream(bulkDataFile);
            FileInputStream fis2 = new FileInputStream(bulkDataFile)) {
          try {
            this.bulkDataServices.analyzeBulkRecordsViaForm(badMediaType,
                fis1,
                uriInfo1);

            fail("Unexpectedly analyzed CSV records with wrong media type "
                + "with no error: " + badMediaType);

          } catch (BadRequestException e) {
            // all good -- this is expected

          } catch (Exception e) {
            e.printStackTrace();
            fail("Unexpectedly analyzed CSV records with wrong media type ("
                + badMediaType + ") with an exception other than "
                + "BadRequestException: " + e);

            throw e;
          }

          try {
            this.bulkDataServices.loadBulkRecordsDirect(null,
                null,
                null,
                "FOO",
                -1,
                badMediaType,
                fis2,
                uriInfo2);

            fail("Unexpectedly loaded CSV records with wrong media type "
                + "with no error: " + badMediaType);

          } catch (ForbiddenException e) {
            // all good -- this is expected

          } catch (Exception e) {
            e.printStackTrace();
            fail("Unexpectedly loaded CSV records with wrong media type ("
                + badMediaType + ") with an exception other than "
                + "ForbiddenException: " + e);

            throw e;
          }

        } catch (Exception e) {
          System.err.println("********** FAILED TEST: " + testInfo);
          e.printStackTrace();
          if (e instanceof RuntimeException)
            throw ((RuntimeException) e);
          throw new RuntimeException(e);
        }
      }
    });
  }

  @Test
  public void testJsonWithBadMediaType() {
    this.performTest(() -> {
      String uriText1 = this.formatServerUri("bulk-data/analyze");
      UriInfo uriInfo1 = this.newProxyUriInfo(uriText1);

      String uriText2 = this.formatServerUri("bulk-data/load");
      UriInfo uriInfo2 = this.newProxyUriInfo(uriText2);

      String testInfo = "Test JSON analyze with bad media type";
      File bulkDataFile = null;

      try {
        bulkDataFile = File.createTempFile("bulk-data-", ".csv");

        try (FileOutputStream fos = new FileOutputStream(bulkDataFile);
            OutputStreamWriter osw = new OutputStreamWriter(fos, UTF_8)) {
          JsonArrayBuilder jab = Json.createArrayBuilder();
          JsonObjectBuilder job = Json.createObjectBuilder();
          job.add("RECORD_ID", "ABC123")
              .add("DATA_SOURCE", CUSTOMER_DATA_SOURCE)
              .add("NAME_FULL", "JOE_SCHMOE")
              .add("PHONE_NUMBER", "702-555-1212");

          jab.add(job);
          job = Json.createObjectBuilder();
          job.add("RECORD_ID", "DEF456")
              .add("DATA_SOURCE", CUSTOMER_DATA_SOURCE)
              .add("NAME_FULL", "JOHN DOE")
              .add("PHONE_NUMBER", "702-555-1313");
          jab.add(job);

          job = Json.createObjectBuilder();
          job.add("RECORD_ID", "GHI789")
              .add("DATA_SOURCE", CUSTOMER_DATA_SOURCE)
              .add("NAME_FULL", "JANE SMITH")
              .add("PHONE_NUMBER", "702-555-1313");
          jab.add(job);

          osw.write(JsonUtilities.toJsonText(jab));
          osw.flush();
        }

      } catch (IOException e) {
        fail(e);
      }

      MediaType[] badMediaTypes = {
          MediaType.valueOf(CSV.getMediaType()),
          MediaType.valueOf(JSON_LINES.getMediaType()) };

      for (MediaType badMediaType : badMediaTypes) {
        try (FileInputStream fis1 = new FileInputStream(bulkDataFile);
            FileInputStream fis2 = new FileInputStream(bulkDataFile)) {
          try {
            this.bulkDataServices.analyzeBulkRecordsViaForm(badMediaType,
                fis1,
                uriInfo1);

            fail("Unexpectedly analyzed JSON records with wrong media type "
                + "with no error: " + badMediaType);

          } catch (BadRequestException e) {
            // all good -- this is expected

          } catch (Exception e) {
            e.printStackTrace();
            fail("Unexpectedly analyzed JSON records with wrong media type ("
                + badMediaType + ") with an exception other than "
                + "BadRequestException: " + e);

            throw e;
          }

          try {
            this.bulkDataServices.loadBulkRecordsDirect(null,
                null,
                null,
                "FOO",
                -1,
                badMediaType,
                fis2,
                uriInfo2);

            fail("Unexpectedly loaded JSON records with wrong media type "
                + "with no error: " + badMediaType);

          } catch (ForbiddenException e) {
            // all good -- this is expected

          } catch (Exception e) {
            e.printStackTrace();
            fail("Unexpectedly loaded JSON records with wrong media type ("
                + badMediaType + ") with an exception other than "
                + "ForbiddenException: " + e);

            throw e;
          }

        } catch (Exception e) {
          System.err.println("********** FAILED TEST: " + testInfo);
          e.printStackTrace();
          if (e instanceof RuntimeException)
            throw ((RuntimeException) e);
          throw new RuntimeException(e);
        }
      }
    });
  }

  @Test
  public void testJsonLinesWithBadMediaType() {
    this.performTest(() -> {
      String uriText1 = this.formatServerUri("bulk-data/analyze");
      UriInfo uriInfo1 = this.newProxyUriInfo(uriText1);

      String uriText2 = this.formatServerUri("bulk-data/load");
      UriInfo uriInfo2 = this.newProxyUriInfo(uriText2);

      String testInfo = "Test JSON-Lines analyze with bad media type";
      File bulkDataFile = null;

      try {
        bulkDataFile = File.createTempFile("bulk-data-", ".csv");

        try (FileOutputStream fos = new FileOutputStream(bulkDataFile);
            OutputStreamWriter osw = new OutputStreamWriter(fos, UTF_8);
            PrintWriter pw = new PrintWriter(osw)) {
          JsonObjectBuilder job = Json.createObjectBuilder();
          job.add("RECORD_ID", "ABC123")
              .add("DATA_SOURCE", CUSTOMER_DATA_SOURCE)
              .add("NAME_FULL", "JOE_SCHMOE")
              .add("PHONE_NUMBER", "702-555-1212");

          pw.println(JsonUtilities.toJsonText(job));

          job = Json.createObjectBuilder();
          job.add("RECORD_ID", "DEF456")
              .add("DATA_SOURCE", CUSTOMER_DATA_SOURCE)
              .add("NAME_FULL", "JOHN DOE")
              .add("PHONE_NUMBER", "702-555-1313");
          pw.println(JsonUtilities.toJsonText(job));

          job = Json.createObjectBuilder();
          job.add("RECORD_ID", "GHI789")
              .add("DATA_SOURCE", CUSTOMER_DATA_SOURCE)
              .add("NAME_FULL", "JANE SMITH")
              .add("PHONE_NUMBER", "702-555-1313");
          pw.println(JsonUtilities.toJsonText(job));

          pw.flush();
        }

      } catch (IOException e) {
        fail(e);
      }

      MediaType[] badMediaTypes = {
          MediaType.valueOf(CSV.getMediaType()) };

      for (MediaType badMediaType : badMediaTypes) {
        try (FileInputStream fis1 = new FileInputStream(bulkDataFile);
            FileInputStream fis2 = new FileInputStream(bulkDataFile)) {
          try {
            this.bulkDataServices.analyzeBulkRecordsViaForm(badMediaType,
                fis1,
                uriInfo1);

            fail("Unexpectedly analyzed JSON-lines records with wrong media "
                + "type with no error: " + badMediaType);

          } catch (BadRequestException e) {
            // all good -- this is expected

          } catch (Exception e) {
            e.printStackTrace();
            fail("Unexpectedly analyzed JSON-lines records with wrong media "
                + "type (" + badMediaType + ") with an exception other "
                + "than BadRequestException: " + e);

            throw e;
          }

          try {
            this.bulkDataServices.loadBulkRecordsDirect(null,
                null,
                null,
                "FOO",
                -1,
                badMediaType,
                fis2,
                uriInfo2);

            fail("Unexpectedly loaded JSON records with wrong media type "
                + "with no error: " + badMediaType);

          } catch (ForbiddenException e) {
            // all good -- this is expected

          } catch (Exception e) {
            e.printStackTrace();
            fail("Unexpectedly loaded JSON records with wrong media type ("
                + badMediaType + ") with an exception other than "
                + "ForbiddenException: " + e);

            throw e;
          }

        } catch (Exception e) {
          System.err.println("********** FAILED TEST: " + testInfo);
          e.printStackTrace();
          if (e instanceof RuntimeException)
            throw ((RuntimeException) e);
          throw new RuntimeException(e);
        }
      }
    });
  }

  @ParameterizedTest
  @MethodSource("getLoadBulkRecordsParameters")
  @Override
  public void loadBulkRecordsViaFormTest(
      String testInfo,
      MediaType mediaType,
      File bulkDataFile,
      SzBulkDataAnalysis analysis,
      Map<String, String> dataSourceMap) {
    this.performTest(() -> {
      this.livePurgeRepository();

      String uriText = this.formatServerUri("bulk-data/load");

      MultivaluedMap queryParams = new MultivaluedHashMap();
      String mapDataSources = null;
      List<String> mapDataSourceList = new LinkedList<>();
      if (dataSourceMap != null) {
        boolean[] jsonFlag = { true };
        boolean[] overlapFlag = { true };
        JsonObjectBuilder builder = Json.createObjectBuilder();
        dataSourceMap.entrySet().forEach(entry -> {
          String key = entry.getKey();
          String value = entry.getValue();
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

      UriInfo uriInfo = this.newProxyUriInfo(uriText, queryParams);
      long before = System.nanoTime();

      try (FileInputStream fis = new FileInputStream(bulkDataFile)) {
        SzBulkLoadResponse response = this.bulkDataServices.loadBulkRecordsViaForm(
            CONTACTS_DATA_SOURCE,
            mapDataSources,
            mapDataSourceList,
            null,
            0,
            mediaType,
            fis,
            null,
            uriInfo);

        fail("Expected bulk load to be forbidden, but it succeeded.");

      } catch (ForbiddenException expected) {
        SzErrorResponse response = (SzErrorResponse) expected.getResponse().getEntity();
        response.concludeTimers();
        long after = System.nanoTime();
        validateBasics(
            testInfo, response, 403, POST, uriText, after - before);

      } catch (Exception e) {
        System.err.println("********** FAILED TEST: " + testInfo);
        e.printStackTrace();
        if (e instanceof RuntimeException)
          throw ((RuntimeException) e);
        throw new RuntimeException(e);
      }
    });
  }

  @ParameterizedTest
  @MethodSource("getLoadBulkRecordsParameters")
  @Override
  public void loadBulkRecordsViaDirectHttpTest(
      String testInfo,
      MediaType mediaType,
      File bulkDataFile,
      SzBulkDataAnalysis analysis,
      Map<String, String> dataSourceMap) {
    this.performTest(() -> {
      // check if media type is null
      if (mediaType == null) {
        // we cannot send a null media type via HTTP, so text/plain would be
        // substituted, but we already handled a test with text/plain so just
        // short-circuit here and mark the test as passed
        return;
      }

      this.livePurgeRepository();

      String uriText = this.formatServerUri(formatLoadURL(
          CONTACTS_DATA_SOURCE, null, null,
          dataSourceMap, null));

      try (FileInputStream fis = new FileInputStream(bulkDataFile)) {
        long before = System.nanoTime();
        SzErrorResponse response = this.invokeServerViaHttp(
            POST, uriText, null, String.valueOf(mediaType),
            bulkDataFile.length(), new FileInputStream(bulkDataFile),
            SzErrorResponse.class);
        response.concludeTimers();
        long after = System.nanoTime();

        validateBasics(
            testInfo, response, 403, POST, uriText, after - before);

      } catch (Exception e) {
        System.err.println("********** FAILED TEST: " + testInfo);
        e.printStackTrace();
        if (e instanceof RuntimeException)
          throw ((RuntimeException) e);
        throw new RuntimeException(e);
      }
    });
  }

  @ParameterizedTest
  @MethodSource("getLoadBulkRecordsParameters")
  @Override
  public void loadBulkRecordsDirectJavaClientTest(
      String testInfo,
      MediaType mediaType,
      File bulkDataFile,
      SzBulkDataAnalysis analysis,
      Map<String, String> dataSourceMap) {
    this.performTest(() -> {
      // check if media type is null
      if (mediaType == null) {
        // we cannot send a null media type via HTTP, so text/plain would be
        // substituted, but we already handled a test with text/plain so just
        // short-circuit here and mark the test as passed
        return;
      }

      this.livePurgeRepository();

      String uriText = this.formatServerUri(formatLoadURL(
          CONTACTS_DATA_SOURCE, null, null,
          dataSourceMap, null));

      try (FileInputStream fis = new FileInputStream(bulkDataFile)) {
        long before = System.nanoTime();
        com.senzing.gen.api.model.SzErrorResponse clientResponse = this.invokeServerViaHttp(
            POST, uriText, null, String.valueOf(mediaType),
            bulkDataFile.length(), new FileInputStream(bulkDataFile),
            com.senzing.gen.api.model.SzErrorResponse.class);
        long after = System.nanoTime();

        SzErrorResponse response = jsonCopy(clientResponse,
            SzErrorResponse.class);

        validateBasics(
            testInfo, response, 403, POST, uriText, after - before);

      } catch (Exception e) {
        System.err.println("********** FAILED TEST: " + testInfo);
        e.printStackTrace();
        if (e instanceof RuntimeException)
          throw ((RuntimeException) e);
        throw new RuntimeException(e);
      }
    });
  }

  @ParameterizedTest
  @MethodSource("getLoadBulkRecordsParameters")
  @Override
  public void loadBulkRecordsViaWebSocketsTest(
      String testInfo,
      MediaType mediaType,
      File bulkDataFile,
      SzBulkDataAnalysis analysis,
      Map<String, String> dataSourceMap) {
    this.performTest(() -> {
      this.livePurgeRepository();

      String uriText = this.formatServerUri(formatLoadURL(
          CONTACTS_DATA_SOURCE, null, null,
          dataSourceMap, null));
      uriText = uriText.replaceAll("^http:(.*)", "ws:$1");

      BulkDataWebSocketClient client = null;
      try {
        client = new BulkDataWebSocketClient(bulkDataFile, mediaType);
        WebSocketContainer container = ContainerProvider.getWebSocketContainer();
        container.connectToServer(client, URI.create(uriText));

        fail("Successfully connected to web socket for bulk load when started "
            + "in read-only mode: " + testInfo);

      } catch (Exception expected) {
        if (client != null) {
          Object next = client.getNextResponse();
          if (next != null) {
            if (!(next instanceof Throwable)) {
              fail("Expected failure on Web Socket connection to read-only "
                  + "server, but got a non-failure response instead: " + next);
            }
            Throwable throwable = (Throwable) next;
            String message = throwable.getMessage();
            if (!message.contains("403")) {
              fail("Got an exception on Web Socket connection to "
                  + "read-only server, but it was not a 403 failure",
                  throwable);
            }
          }
        }
      }
    });
  }

  @ParameterizedTest
  @MethodSource("getLoadBulkRecordsParameters")
  @Override
  public void loadBulkRecordsViaSSETest(
      String testInfo,
      MediaType mediaType,
      File bulkDataFile,
      SzBulkDataAnalysis analysis,
      Map<String, String> dataSourceMap) {
    this.performTest(() -> {
      this.livePurgeRepository();

      String uriText = this.formatServerUri(formatLoadURL(
          CONTACTS_DATA_SOURCE, null, null,
          dataSourceMap, null));

      try {
        long before = System.nanoTime();

        URL url = new URL(uriText);

        BulkDataSSEClient client = new BulkDataSSEClient(url,
            bulkDataFile,
            mediaType);

        client.start();

        SzErrorResponse errorResponse = null;
        // grab the results
        for (Object next = client.getNextResponse(); next != null; next = client.getNextResponse()) {
          // check if there was a failure
          if (next instanceof Throwable) {
            ((Throwable) next).printStackTrace();
            fail((Throwable) next);
          }

          // get as a string
          String jsonText = next.toString();
          if (jsonText.matches(".*\"httpStatusCode\":\\s*200.*")) {
            fail("Received 200 response for read-only: " + jsonText);

          } else {
            errorResponse = jsonParse(jsonText, SzErrorResponse.class);
            errorResponse.concludeTimers();
            break;
          }
        }
        long after = System.nanoTime();

        if (errorResponse == null) {
          fail("Did not receive an error response for SSE bulk-load test "
              + "against a read-only API Server: " + testInfo);
        }

        validateBasics(
            testInfo, errorResponse, 403, POST,
            uriText, after - before);

      } catch (Exception e) {
        System.err.println("********** FAILED TEST: " + testInfo);
        e.printStackTrace();
        if (e instanceof RuntimeException)
          throw ((RuntimeException) e);
        throw new RuntimeException(e);
      }
    });
  }

  @ParameterizedTest
  @MethodSource("getMaxFailureArgs")
  @Override
  public void testMaxFailuresOnLoad(
      int recordCount,
      Integer maxFailures,
      SzBulkDataStatus expectedStatus,
      Map<String, Integer> failuresByDataSource,
      File dataFile) {
    this.performTest(() -> {
      this.livePurgeRepository();

      String testInfo = "recordCount=[ " + recordCount + " ], maxFailures=[ "
          + maxFailures + " ], status=[ "
          + expectedStatus + " ], failuresByDataSource=[ "
          + failuresByDataSource + " ], dataFile=[ " + dataFile + " ]";

      String uriText = this.formatServerUri("bulk-data/load");

      MultivaluedMap queryParams = new MultivaluedHashMap();
      if (maxFailures != null) {
        queryParams.add("maxFailures", String.valueOf(maxFailures));
      }
      UriInfo uriInfo = this.newProxyUriInfo(uriText, queryParams);

      long before = System.nanoTime();
      try (InputStream is = new FileInputStream(dataFile);
          BufferedInputStream bis = new BufferedInputStream(is)) {
        this.bulkDataServices.loadBulkRecordsViaForm(
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
        SzErrorResponse response = (SzErrorResponse) expected.getResponse().getEntity();
        response.concludeTimers();
        long after = System.nanoTime();
        validateBasics(
            testInfo, response, 403, POST, uriText, after - before);

      } catch (Exception e) {
        System.err.println("********** FAILED TEST: " + testInfo);
        e.printStackTrace();
        if (e instanceof RuntimeException)
          throw ((RuntimeException) e);
        throw new RuntimeException(e);
      }
    });
  }

}
