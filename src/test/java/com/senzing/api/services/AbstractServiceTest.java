package com.senzing.api.services;

import java.io.*;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.net.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.senzing.api.model.*;
import com.senzing.api.server.SzApiServer;
import com.senzing.api.server.SzApiServerOptions;
import com.senzing.g2.engine.G2Engine;
import com.senzing.g2.engine.G2JNI;
import com.senzing.repomgr.RepositoryManager;
import com.senzing.util.AccessToken;
import com.senzing.util.JsonUtils;

import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.ws.rs.core.UriInfo;

import static com.senzing.api.BuildInfo.MAVEN_VERSION;
import static com.senzing.api.BuildInfo.REST_API_VERSION;
import static com.senzing.api.model.SzFeatureInclusion.NONE;
import static com.senzing.api.model.SzFeatureInclusion.REPRESENTATIVE;
import static com.senzing.api.model.SzHttpMethod.*;
import static com.senzing.io.IOUtilities.readTextFileAsString;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.*;

/**
 * Provides an abstract base class for services tests that will create a
 * Senzing repository and startup the API server configured to use that
 * repository.  It also provides hooks to load the repository with data.
 */
public abstract class AbstractServiceTest {
  /**
   * Whether or not the Senzing native API is available and the G2 native
   * library could be loaded.
   */
  protected static final boolean NATIVE_API_AVAILABLE;

  /**
   * Message to display when the Senzing API is not available and the tests
   * are being skipped.
   */
  protected static final String NATIVE_API_UNAVAILABLE_MESSAGE;

  static {
    G2Engine engineApi = null;
    StringWriter sw = new StringWriter();
    try {
      PrintWriter pw = new PrintWriter(sw);
      pw.println();
      pw.println("Skipping Tests: The Senzing Native API is NOT available.");
      pw.println("Check that SENZING_DIR environment variable is properly defined.");
      pw.println("Alternatively, you can run maven (mvn) with -Dsenzing.dir=[path].");
      pw.println();

      try {
        engineApi = new G2JNI();
      } catch (Throwable ignore) {
        ignore.printStackTrace();
        // do nothing
      }
    } finally {
      NATIVE_API_AVAILABLE = (engineApi != null);
      NATIVE_API_UNAVAILABLE_MESSAGE = sw.toString();
    }
  }

  /**
   * The API Server being used to run the tests.
   */
  private SzApiServer server;

  /**
   * The repository directory used to run the tests.
   */
  private File repoDirectory;

  /**
   * The access token to use for privileged access to created objects.
   */
  private AccessToken accessToken;

  /**
   * The access token to use to unregister the API provider.
   */
  private AccessToken providerToken;

  /**
   * Whether or not the repository has been created.
   */
  private boolean repoCreated = false;

  /**
   * Creates a temp repository directory.
   *
   * @return The {@link File} representing the directory.
   */
  private static File createTempDirectory() {
    try {
      return Files.createTempDirectory("sz-repo-").toFile();
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Protected default constructor.
   */
  protected AbstractServiceTest() {
    this(createTempDirectory());
  }

  /**
   * Protected constructor allowing the derived class to specify the
   * location for the entity respository.
   *
   * @param repoDirectory The directory in which to include the entity
   *                      repository.
   */
  protected AbstractServiceTest(File repoDirectory) {
    this.server = null;
    this.repoDirectory = repoDirectory;
    this.accessToken = new AccessToken();
    this.providerToken = null;
  }

  /**
   * Creates an absolute URI for the relative URI provided.  For example, if
   * <tt>"license"</tt> was passed as the parameter then
   * <tt>"http://localhost:[port]/license"</tt> will be returned where
   * <tt>"[port]"</tt> is the port number of the currently running server, if
   * running, and is <tt>"2080"</tt> (the default port) if not running.
   *
   * @param relativeUri The relative URI to build the absolute URI from.
   * @return The absolute URI for localhost on the current port.
   */
  protected String formatServerUri(String relativeUri) {
    StringBuilder sb = new StringBuilder();
    sb.append("http://localhost:");
    if (this.server != null) {
      sb.append(this.server.getHttpPort());
    } else {
      sb.append("2080");
    }
    if (relativeUri.startsWith(sb.toString())) return relativeUri;
    sb.append("/" + relativeUri);
    return sb.toString();
  }

  /**
   * Checks that the Senzing Native API is available and if not causes the
   * test or tests to be skipped.
   *
   * @return <tt>true</tt> if the native API's are available, otherwise
   * <tt>false</tt>
   */
  protected boolean assumeNativeApiAvailable() {
    assumeTrue(NATIVE_API_AVAILABLE, NATIVE_API_UNAVAILABLE_MESSAGE);
    return NATIVE_API_AVAILABLE;
  }

  /**
   * This method can typically be called from a method annotated with
   * "@BeforeClass".  It will create a Senzing entity repository and
   * initialize and start the Senzing API Server.
   */
  protected void initializeTestEnvironment() {
    try {
      if (!NATIVE_API_AVAILABLE) return;
      RepositoryManager.createRepo(this.getRepositoryDirectory(), true);
      this.repoCreated = true;
      this.prepareRepository();
      RepositoryManager.conclude();
      this.initializeServer();
    } catch (RuntimeException e) {
      e.printStackTrace();
      throw e;
    } catch (Error e) {
      e.printStackTrace();
      throw e;
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  /**
   * This method can typically be called from a method annotated with
   * "@AfterClass".  It will shutdown the server and optionally delete
   * the entity repository that was created for the tests.
   *
   * @param deleteRepository <tt>true</tt> if the test repository should be
   *                         deleted, otherwise <tt>false</tt>
   */
  protected void teardownTestEnvironment(boolean deleteRepository) {
    // destroy the server
    if (this.server != null) this.destroyServer();

    // cleanup the repo directory
    if (this.repoCreated && deleteRepository
        && this.repoDirectory.exists() && this.repoDirectory.isDirectory()) {
      try {
        // delete the repository
        Files.walk(this.repoDirectory.toPath())
            .sorted(Comparator.reverseOrder())
            .map(Path::toFile)
            .forEach(File::delete);

      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    // for good measure
    if (NATIVE_API_AVAILABLE) {
      RepositoryManager.conclude();
    }
  }

  /**
   * Returns the {@link File} identifying the repository directory used for
   * the test.  This can be specified in the constructor, but if not specified
   * is a newly created temporary directory.
   */
  protected File getRepositoryDirectory() {
    return this.repoDirectory;
  }

  /**
   * Override this function to prepare the repository by configuring
   * data sources or loading records.  By default this function does nothing.
   * The repository directory can be obtained via {@link
   * #getRepositoryDirectory()}.
   */
  protected void prepareRepository() {
    // do nothing
  }

  /**
   * Stops the server if it is running and purges the repository.  After
   * purging the server is restarted.
   */
  protected void purgeRepository() {
    this.purgeRepository(true);
  }

  /**
   * Stops the server if it is running and purges the repository.  After
   * purging the server is <b>optionally</b> restarted.  You may not want to
   * restart the server if you intend to load more records into via the
   * {@link RepositoryManager} before restarting.
   *
   * @param restartServer <tt>true</tt> to restart the server and <tt>false</tt>
   *                      if you intend to restart it manually.
   * @see #restartServer()
   */
  protected void purgeRepository(boolean restartServer) {
    boolean running = (this.server != null);
    if (running) this.destroyServer();
    RepositoryManager.purgeRepo(this.repoDirectory);
    if (running && restartServer) this.initializeServer();
  }

  /**
   * Restarts the server.  If the server is already running it is shutdown
   * first and then started.  If not running it is started up.  This cannot
   * be called prior to the repository being created.
   */
  protected void restartServer() {
    if (!this.repoCreated) {
      throw new IllegalStateException(
          "Cannnot restart server prior to calling initializeTestEnvironment()");
    }
    RepositoryManager.conclude();
    this.destroyServer();
    this.initializeServer();
  }

  /**
   * Internal method for shutting down and destroying the server.  This method
   * has no effect if the server is not currently initialized.
   */
  private void destroyServer() {
    if (this.server == null) {
      System.err.println("WARNING: Server was not running at destroy");
      return;
    }
    SzApiProvider.Factory.uninstallProvider(this.providerToken);
    this.server.shutdown(this.accessToken);
    this.server.join();
    this.server = null;
  }

  /**
   * Returns the port that the server should bind to.  By default this returns
   * <tt>null</tt> to indicate that any available port can be used for the
   * server.  Override to use a specific port.
   *
   * @return The port that should be used in initializing the server, or
   * <tt>null</tt> if any available port is fine.
   */
  protected Integer getServerPort() {
    return null;
  }

  /**
   * Retuns the {@link InetAddress} used to initialize the server.  By default
   * this returns the address obtained for <tt>"127.0.0.1"</tt>.  Override this
   * to change the address.  Return <tt>null</tt> if all available interfaces
   * should be bound to.
   *
   * @return The {@link InetAddress} for initializing the server, or
   * <tt>null</tt> if the server should bind to all available network
   * interfaces.
   */
  protected InetAddress getServerAddress() {
    try {
      return InetAddress.getByName("127.0.0.1");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Returns the concurrency with which to initialize the server.  By default
   * this returns one (1).  Override to use a different concurrency.
   *
   * @return The concurrency with which to initialize the server.
   */
  protected int getServerConcurrency() {
    return 1;
  }

  /**
   * Returns the module name with which to initialize the server.  By default
   * this returns <tt>"Test API Server"</tt>.  Override to use a different
   * module name.
   *
   * @return The module name with which to initialize the server.
   */
  protected String getModuleName() {
    return "Test API Server";
  }

  /**
   * Checks whether or not the server should be initialized in verbose mode.
   * By default this <tt>true</tt>.  Override to set to <tt>false</tt>.
   *
   * @return <tt>true</tt> if the server should be initialized in verbose mode,
   * otherwise <tt>false</tt>.
   */
  protected boolean isVerbose() {
    return false;
  }

  /**
   * Sets the desired options for the {@link SzApiServer} during server
   * initialization.
   *
   * @param options The {@link SzApiServerOptions} to initialize.
   */
  protected void initializeServerOptions(SzApiServerOptions options) {
    options.setHttpPort(0);
    options.setBindAddress(this.getServerAddress());
    options.setConcurrency(this.getServerConcurrency());
    options.setModuleName(this.getModuleName());
    options.setVerbose(this.isVerbose());
  }

  /**
   * Internal method for initializing the server.
   */
  private void initializeServer() {
    if (this.server != null) {
      this.destroyServer();
    }
    RepositoryManager.conclude();

    try {
      File        repoDirectory = this.getRepositoryDirectory();
      File        initJsonFile  = new File(repoDirectory, "g2-init.json");
      String      initJsonText  = readTextFileAsString(initJsonFile, "UTF-8");
      JsonObject  initJson      = JsonUtils.parseJsonObject(initJsonText);

      System.err.println("Initializing with initialization file: "
                         + initJsonFile);

      SzApiServerOptions options = new SzApiServerOptions(initJson);
      this.initializeServerOptions(options);

      this.server = new SzApiServer(this.accessToken, options);

      this.providerToken = SzApiProvider.Factory.installProvider(server);

    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   *
   */
  protected UriInfo newProxyUriInfo(String selfLink) {
    try {
      final URI uri = new URI(selfLink);

      InvocationHandler handler = (p, m, a) -> {
        if (m.getName().equals("getRequestUri")) {
          return uri;
        }
        throw new UnsupportedOperationException(
            "Operation not implemented on proxy UriInfo");
      };

      ClassLoader loader = this.getClass().getClassLoader();
      Class[] classes = {UriInfo.class};

      return (UriInfo) Proxy.newProxyInstance(loader, classes, handler);

    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Validates the basic response fields for the specified {@link
   * SzBasicResponse} using the specified self link, timestamp from before
   * calling the service function and timestamp from after calling the
   * service function.
   *
   * @param response        The {@link SzBasicResponse} to validate.
   * @param selfLink        The self link to be expected.
   * @param beforeTimestamp The timestamp from just before calling the service.
   * @param afterTimestamp  The timestamp from just after calling the service.
   */
  protected void validateBasics(SzBasicResponse response,
                                String selfLink,
                                long beforeTimestamp,
                                long afterTimestamp) {
    this.validateBasics(
        null, response, GET, selfLink, beforeTimestamp, afterTimestamp);
  }

  /**
   * Validates the basic response fields for the specified {@link
   * SzBasicResponse} using the specified self link, timestamp from before
   * calling the service function and timestamp from after calling the
   * service function.
   *
   * @param testInfo        Additional test information to be logged with failures.
   * @param response        The {@link SzBasicResponse} to validate.
   * @param selfLink        The self link to be expected.
   * @param beforeTimestamp The timestamp from just before calling the service.
   * @param afterTimestamp  The timestamp from just after calling the service.
   */
  protected void validateBasics(String testInfo,
                                SzBasicResponse response,
                                String selfLink,
                                long beforeTimestamp,
                                long afterTimestamp) {
    this.validateBasics(
        testInfo, response, GET, selfLink, beforeTimestamp, afterTimestamp);
  }

  /**
   * Validates the basic response fields for the specified {@link
   * SzBasicResponse} using the specified self link, timestamp from before
   * calling the service function and timestamp from after calling the
   * service function.
   *
   * @param response           The {@link SzBasicResponse} to validate.
   * @param expectedHttpMethod The {@link SzHttpMethod} that was used.
   * @param selfLink           The self link to be expected.
   * @param beforeTimestamp    The timestamp from just before calling the service.
   * @param afterTimestamp     The timestamp from just after calling the service.
   */
  protected void validateBasics(SzBasicResponse response,
                                SzHttpMethod expectedHttpMethod,
                                String selfLink,
                                long beforeTimestamp,
                                long afterTimestamp)
  {
    this.validateBasics(null,
                        response,
                        expectedHttpMethod,
                        selfLink,
                        beforeTimestamp,
                        afterTimestamp);
  }

  /**
   * Validates the basic response fields for the specified {@link
   * SzBasicResponse} using the specified self link, timestamp from before
   * calling the service function and timestamp from after calling the
   * service function.
   *
   * @param response The {@link SzBasicResponse} to validate.
   * @param expectedResponseCode The expected HTTP response code.
   * @param expectedHttpMethod The {@link SzHttpMethod} that was used.
   * @param selfLink The self link to be expected.
   * @param beforeTimestamp The timestamp from just before calling the service.
   * @param afterTimestamp The timestamp from just after calling the service.
   */
  protected void validateBasics(SzBasicResponse response,
                                int expectedResponseCode,
                                SzHttpMethod expectedHttpMethod,
                                String selfLink,
                                long beforeTimestamp,
                                long afterTimestamp)
  {
    this.validateBasics(null,
                        response,
                        expectedResponseCode,
                        expectedHttpMethod,
                        selfLink,
                        beforeTimestamp,
                        afterTimestamp);
  }

  /**
   * Validates the basic response fields for the specified {@link
   * SzBasicResponse} using the specified self link, timestamp from before
   * calling the service function and timestamp from after calling the
   * service function.
   *
   * @param testInfo           Additional test information to be logged with failures.
   * @param response           The {@link SzBasicResponse} to validate.
   * @param expectedHttpMethod The {@link SzHttpMethod} that was used.
   * @param selfLink           The self link to be expected.
   * @param beforeTimestamp    The timestamp from just before calling the service.
   * @param afterTimestamp     The timestamp from just after calling the service.
   */
  protected void validateBasics(String          testInfo,
                                SzBasicResponse response,
                                SzHttpMethod    expectedHttpMethod,
                                String          selfLink,
                                long            beforeTimestamp,
                                long            afterTimestamp)
  {
    this.validateBasics(testInfo,
                        response,
                        200,
                        expectedHttpMethod,
                        selfLink,
                        beforeTimestamp,
                        afterTimestamp);
  }

  /**
   * Validates the basic response fields for the specified {@link
   * SzBasicResponse} using the specified self link, timestamp from before
   * calling the service function and timestamp from after calling the
   * service function.
   *
   * @param testInfo Additional test information to be logged with failures.
   * @param response The {@link SzBasicResponse} to validate.
   * @param expectedResponseCode The expected HTTP responsec code.
   * @param expectedHttpMethod The {@link SzHttpMethod} that was used.
   * @param selfLink The self link to be expected.
   * @param beforeTimestamp The timestamp from just before calling the service.
   * @param afterTimestamp The timestamp from just after calling the service.
   */
  protected void validateBasics(String          testInfo,
                                SzBasicResponse response,
                                int             expectedResponseCode,
                                SzHttpMethod    expectedHttpMethod,
                                String          selfLink,
                                long            beforeTimestamp,
                                long            afterTimestamp)
  {
    String suffix = (testInfo != null && testInfo.trim().length() > 0)
        ? " ( " + testInfo + " )" : "";

    SzLinks links = response.getLinks();
    SzMeta meta = response.getMeta();
    assertEquals(selfLink, links.getSelf(), "Unexpected self link" + suffix);
    assertEquals(expectedHttpMethod, meta.getHttpMethod(),
                 "Unexpected HTTP method" + suffix);
    assertEquals(expectedResponseCode, meta.getHttpStatusCode(), "Unexpected HTTP status code" + suffix);
    assertEquals(MAVEN_VERSION, meta.getVersion(), "Unexpected server version" + suffix);
    assertEquals(REST_API_VERSION, meta.getRestApiVersion(), "Unexpected REST API version" + suffix);
    assertNotNull(meta.getTimestamp(), "Timestamp unexpectedly null" + suffix);
    long now = meta.getTimestamp().getTime();

    // check the timestamp
    if (now < beforeTimestamp || now > afterTimestamp) {
      fail("Timestamp should be between " + new Date(beforeTimestamp) + " and "
               + new Date(afterTimestamp) + suffix);
    }
    Map<String, Long> timings = meta.getTimings();

    // determine max duration
    long maxDuration = (afterTimestamp - beforeTimestamp);

    timings.entrySet().forEach(entry -> {
      long duration = entry.getValue();
      if (duration > maxDuration) {
        fail("Timing value too large: " + entry.getKey() + " = "
                 + duration + "ms VS " + maxDuration + "ms" + suffix);
      }
    });
  }

  /**
   * Validates the basic response fields for the specified {@link
   * SzResponseWithRawData} using the specified self link, timestamp from before
   * calling the service function, timestamp from after calling the
   * service function, and flag indicating if raw data should be expected.
   *
   * @param response        The {@link SzBasicResponse} to validate.
   * @param selfLink        The self link to be expected.
   * @param beforeTimestamp The timestamp from just before calling the service.
   * @param afterTimestamp  The timestamp from just after calling the service.
   * @param expectRawData   <tt>true</tt> if raw data should be expected,
   *                        otherwise <tt>false</tt>
   */
  protected void validateBasics(SzResponseWithRawData response,
                                String                selfLink,
                                long                  beforeTimestamp,
                                long                  afterTimestamp,
                                boolean               expectRawData)
  {
    this.validateBasics(null,
                        response,
                        selfLink,
                        beforeTimestamp,
                        afterTimestamp,
                        expectRawData);
  }

  /**
   * Validates the basic response fields for the specified {@link
   * SzResponseWithRawData} using the specified self link, timestamp from before
   * calling the service function, timestamp from after calling the
   * service function, and flag indicating if raw data should be expected.
   *
   * @param testInfo        Additional test information to be logged with failures.
   * @param response        The {@link SzBasicResponse} to validate.
   * @param selfLink        The self link to be expected.
   * @param beforeTimestamp The timestamp from just before calling the service.
   * @param afterTimestamp  The timestamp from just after calling the service.
   * @param expectRawData   <tt>true</tt> if raw data should be expected,
   *                        otherwise <tt>false</tt>
   */
  protected void validateBasics(String                testInfo,
                                SzResponseWithRawData response,
                                String                selfLink,
                                long                  beforeTimestamp,
                                long                  afterTimestamp,
                                boolean               expectRawData)
  {
    String suffix = (testInfo != null && testInfo.trim().length() > 0)
        ? " ( " + testInfo + " )" : "";

    this.validateBasics(testInfo, response, selfLink, beforeTimestamp, afterTimestamp);

    Object rawData = response.getRawData();
    if (expectRawData) {
      assertNotNull(rawData, "Raw data unexpectedly non-null" + suffix);
    } else {
      assertNull(rawData, "Raw data unexpectedly null" + suffix);
    }
  }

  /**
   * Invoke an operation on the currently running API server over HTTP.
   *
   * @param httpMethod    The HTTP method to use.
   * @param uri           The relative or absolute URI (optionally including query params)
   * @param responseClass The class of the response.
   * @param <T>           The response type which must extend {@link SzBasicResponse}
   * @return
   */
  protected <T extends SzBasicResponse> T invokeServerViaHttp(
      SzHttpMethod httpMethod,
      String uri,
      Class<T> responseClass) {
    return this.invokeServerViaHttp(
        httpMethod, uri, null, null, responseClass);
  }

  /**
   * Invoke an operation on the currently running API server over HTTP.
   *
   * @param httpMethod    The HTTP method to use.
   * @param uri           The relative or absolute URI (optionally including query params)
   * @param queryParams   The optional map of query parameters.
   * @param bodyContent   The object to be converted to JSON for body content.
   * @param responseClass The class of the response.
   * @param <T>           The response type which must extend {@link SzBasicResponse}
   * @return
   */
  protected <T extends SzBasicResponse> T invokeServerViaHttp(
      SzHttpMethod httpMethod,
      String uri,
      Map<String, ?> queryParams,
      Object bodyContent,
      Class<T> responseClass) {
    ObjectMapper objectMapper = new ObjectMapper();
    try {
      if (!uri.toLowerCase().startsWith("http://")) {
        uri = this.formatServerUri(uri);
      }
      if (queryParams != null && queryParams.size() > 0) {
        String initialPrefix = uri.contains("?") ? "&" : "?";

        StringBuilder sb = new StringBuilder();
        queryParams.entrySet().forEach(entry -> {
          String key = entry.getKey();
          Object value = entry.getValue();
          Collection values = null;
          if (value instanceof Collection) {
            values = (Collection) value;
          } else {
            values = Collections.singletonList(value);
          }
          try {
            key = URLEncoder.encode(key, "UTF-8");
            for (Object val : values) {
              if (val == null) return;
              String textValue = val.toString();
              textValue = URLEncoder.encode(textValue, "UTF-8");
              sb.append((sb.length() == 0) ? initialPrefix : "&");
              sb.append(key).append("=").append(textValue);
            }
          } catch (UnsupportedEncodingException cannotHappen) {
            throw new RuntimeException(cannotHappen);
          }
        });
        uri = uri + sb.toString();
      }

      String jsonContent = null;
      if (bodyContent != null) {
        jsonContent = objectMapper.writeValueAsString(bodyContent);
      }

      URL url = new URL(uri);
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.setRequestMethod(httpMethod.toString());
      conn.setRequestProperty("Accept", "application/json");
      conn.setRequestProperty("Accept-Charset", "utf-8");
      if (jsonContent != null) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        OutputStreamWriter osw = new OutputStreamWriter(baos, "UTF-8");
        osw.write(jsonContent);
        osw.flush();
        byte[] bytes = baos.toByteArray();
        int length = bytes.length;
        conn.addRequestProperty("Content-Length", "" + length);
        conn.addRequestProperty("Content-Type",
                                "application/json; charset=utf-8");
        conn.setDoOutput(true);
        OutputStream os = conn.getOutputStream();
        os.write(bytes);
        os.flush();
      }

      int responseCode = conn.getResponseCode();
      InputStream is = (responseCode >= 200 && responseCode < 300)
          ? conn.getInputStream() : conn.getErrorStream();
      InputStreamReader isr = new InputStreamReader(is, "UTF-8");
      BufferedReader br = new BufferedReader(isr);
      StringBuilder sb = new StringBuilder();
      for (int nextChar = br.read(); nextChar >= 0; nextChar = br.read()) {
        sb.append((char) nextChar);
      }

      String responseJson = sb.toString();

      return objectMapper.readValue(responseJson, responseClass);

    } catch (RuntimeException e) {
      e.printStackTrace();
      throw e;
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  /**
   * Validates the raw data and ensures the expected JSON property keys are
   * present and that no unexpected keys are present.
   *
   * @param rawData      The raw data to validate.
   * @param expectedKeys The zero or more expected property keys.
   */
  protected void validateRawDataMap(Object rawData, String... expectedKeys) {
    this.validateRawDataMap(null,
                            rawData,
                            true,
                            expectedKeys);
  }

  /**
   * Validates the raw data and ensures the expected JSON property keys are
   * present and that no unexpected keys are present.
   *
   * @param testInfo     Additional test information to be logged with failures.
   * @param rawData      The raw data to validate.
   * @param expectedKeys The zero or more expected property keys.
   */
  protected void validateRawDataMap(String    testInfo,
                                    Object    rawData,
                                    String... expectedKeys)
  {
    this.validateRawDataMap(testInfo, rawData, true, expectedKeys);
  }

  /**
   * Validates the raw data and ensures the expected JSON property keys are
   * present and that, optionally, no unexpected keys are present.
   *
   * @param rawData      The raw data to validate.
   * @param strict       Whether or not property keys other than those specified are
   *                     allowed to be present.
   * @param expectedKeys The zero or more expected property keys -- these are
   *                     either a minimum or exact set depending on the
   *                     <tt>strict</tt> parameter.
   */
  protected void validateRawDataMap(Object    rawData,
                                    boolean   strict,
                                    String... expectedKeys)
  {
    this.validateRawDataMap(null, rawData, strict, expectedKeys);
  }

  /**
   * Validates the raw data and ensures the expected JSON property keys are
   * present and that, optionally, no unexpected keys are present.
   *
   *
   * @param testInfo     Additional test information to be logged with failures.
   * @param rawData      The raw data to validate.
   * @param strict       Whether or not property keys other than those specified are
   *                     allowed to be present.
   * @param expectedKeys The zero or more expected property keys -- these are
   *                     either a minimum or exact set depending on the
   *                     <tt>strict</tt> parameter.
   */
  protected void validateRawDataMap(String    testInfo,
                                    Object    rawData,
                                    boolean   strict,
                                    String... expectedKeys)
  {
    String suffix = (testInfo != null && testInfo.trim().length() > 0)
        ? " ( " + testInfo + " )" : "";

    if (rawData == null) {
      fail("Expected raw data but got null value" + suffix);
    }

    if (!(rawData instanceof Map)) {
      fail("Raw data is not a JSON object: " + rawData + suffix);
    }

    Map<String, Object> map = (Map<String, Object>) rawData;
    Set<String> expectedKeySet = new HashSet<>();
    Set<String> actualKeySet = map.keySet();
    for (String key : expectedKeys) {
      expectedKeySet.add(key);
      if (!actualKeySet.contains(key)) {
        fail("JSON property missing from raw data: " + key + " / " + map
             + suffix);
      }
    }
    if (strict && expectedKeySet.size() != actualKeySet.size()) {
      Set<String> extraKeySet = new HashSet<>(actualKeySet);
      extraKeySet.removeAll(expectedKeySet);
      fail("Unexpected JSON properties in raw data: " + extraKeySet + suffix);
    }

  }


  /**
   * Validates the raw data and ensures it is a collection of objects and the
   * expected JSON property keys are present in the array objects and that no
   * unexpected keys are present.
   *
   * @param rawData      The raw data to validate.
   * @param expectedKeys The zero or more expected property keys.
   */
  protected void validateRawDataMapArray(Object rawData, String... expectedKeys)
  {
    this.validateRawDataMapArray(null, rawData, true, expectedKeys);
  }

  /**
   * Validates the raw data and ensures it is a collection of objects and the
   * expected JSON property keys are present in the array objects and that no
   * unexpected keys are present.
   *
   * @param testInfo     Additional test information to be logged with failures.
   * @param rawData      The raw data to validate.
   * @param expectedKeys The zero or more expected property keys.
   */
  protected void validateRawDataMapArray(String     testInfo,
                                         Object     rawData,
                                         String...  expectedKeys)
  {
    this.validateRawDataMapArray(testInfo, rawData, true, expectedKeys);
  }

  /**
   * Validates the raw data and ensures it is a collection of objects and the
   * expected JSON property keys are present in the array objects and that,
   * optionally, no unexpected keys are present.
   *
   * @param rawData      The raw data to validate.
   * @param strict       Whether or not property keys other than those specified are
   *                     allowed to be present.
   * @param expectedKeys The zero or more expected property keys for the array
   *                     objects -- these are either a minimum or exact set
   *                     depending on the <tt>strict</tt> parameter.
   */
  protected void validateRawDataMapArray(Object     rawData,
                                         boolean    strict,
                                         String...  expectedKeys)
  {
    this.validateRawDataMapArray(null, rawData, strict, expectedKeys);
  }

  /**
   * Validates the raw data and ensures it is a collection of objects and the
   * expected JSON property keys are present in the array objects and that,
   * optionally, no unexpected keys are present.
   *
   * @param testInfo     Additional test information to be logged with failures.
   * @param rawData      The raw data to validate.
   * @param strict       Whether or not property keys other than those specified are
   *                     allowed to be present.
   * @param expectedKeys The zero or more expected property keys for the array
   *                     objects -- these are either a minimum or exact set
   *                     depending on the <tt>strict</tt> parameter.
   */
  protected void validateRawDataMapArray(String     testInfo,
                                         Object     rawData,
                                         boolean    strict,
                                         String...  expectedKeys)
  {
    String suffix = (testInfo != null && testInfo.trim().length() > 0)
        ? " ( " + testInfo + " )" : "";

    if (rawData == null) {
      fail("Expected raw data but got null value" + suffix);
    }

    if (!(rawData instanceof Collection)) {
      fail("Raw data is not a JSON array: " + rawData + suffix);
    }

    Collection<Object> collection = (Collection<Object>) rawData;
    Set<String> expectedKeySet = new HashSet<>();
    for (String key : expectedKeys) {
      expectedKeySet.add(key);
    }

    for (Object obj : collection) {
      if (!(obj instanceof Map)) {
        fail("Raw data is not a JSON array of JSON objects: " + rawData + suffix);
      }

      Map<String, Object> map = (Map<String, Object>) obj;

      Set<String> actualKeySet = map.keySet();
      for (String key : expectedKeySet) {
        if (!actualKeySet.contains(key)) {
          fail("JSON property missing from raw data array element: "
                   + key + " / " + map + suffix);
        }
      }
      if (strict && expectedKeySet.size() != actualKeySet.size()) {
        Set<String> extraKeySet = new HashSet<>(actualKeySet);
        extraKeySet.removeAll(expectedKeySet);
        fail("Unexpected JSON properties in raw data: " + extraKeySet + suffix);
      }
    }
  }

  /**
   * Quotes the specified text as a quoted string for a CSV value or header.
   *
   * @param text The text to be quoted.
   * @return The quoted text.
   */
  protected String csvQuote(String text) {
    if (text.indexOf("\"") < 0 && text.indexOf("\\") < 0) {
      return "\"" + text + "\"";
    }
    char[] textChars = text.toCharArray();
    StringBuilder sb = new StringBuilder(text.length() * 2);
    for (char c : textChars) {
      if (c == '"' || c == '\\') {
        sb.append('\\');
      }
      sb.append(c);
    }
    return sb.toString();
  }

  /**
   * Creates a CSV temp file with the specified headers and records.
   *
   * @param filePrefix The prefix for the temp file name.
   * @param headers    The CSV headers.
   * @param records    The one or more records.
   * @return The {@link File} that was created.
   */
  protected File prepareCSVFile(String filePrefix,
                                String[] headers,
                                String[]... records) {
    // check the arguments
    int count = headers.length;
    for (int index = 0; index < records.length; index++) {
      String[] record = records[index];
      if (record.length != count) {
        throw new IllegalArgumentException(
            "The header and records do not all have the same number of "
                + "elements.  expected=[ " + count + " ], received=[ "
                + record.length + " ], index=[ " + index + " ]");
      }
    }

    try {
      File csvFile = File.createTempFile(filePrefix, ".csv");

      // populate the file as a CSV
      try (FileOutputStream fos = new FileOutputStream(csvFile);
           OutputStreamWriter osw = new OutputStreamWriter(fos, "UTF-8");
           PrintWriter pw = new PrintWriter(osw)) {
        String prefix = "";
        for (String header : headers) {
          pw.print(prefix);
          pw.print(csvQuote(header));
          prefix = ",";
        }
        pw.println();
        pw.flush();

        for (String[] record : records) {
          prefix = "";
          for (String value : record) {
            pw.print(prefix);
            pw.print(csvQuote(value));
            prefix = ",";
          }
          pw.println();
          pw.flush();
        }
        pw.flush();

      }

      return csvFile;

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }


  /**
   * Creates a JSON array temp file with the specified headers and records.
   *
   * @param filePrefix The prefix for the temp file name.
   * @param headers    The CSV headers.
   * @param records    The one or more records.
   * @return The {@link File} that was created.
   */
  protected File prepareJsonArrayFile(String filePrefix,
                                      String[] headers,
                                      String[]... records) {
    // check the arguments
    int count = headers.length;
    for (int index = 0; index < records.length; index++) {
      String[] record = records[index];
      if (record.length != count) {
        throw new IllegalArgumentException(
            "The header and records do not all have the same number of "
                + "elements.  expected=[ " + count + " ], received=[ "
                + record.length + " ], index=[ " + index + " ]");
      }
    }

    try {
      File jsonFile = File.createTempFile(filePrefix, ".json");

      // populate the file with a JSON array
      try (FileOutputStream fos = new FileOutputStream(jsonFile);
           OutputStreamWriter osw = new OutputStreamWriter(fos, "UTF-8")) {
        JsonArrayBuilder jab = Json.createArrayBuilder();
        JsonObjectBuilder job = Json.createObjectBuilder();
        for (String[] record : records) {
          for (int index = 0; index < record.length; index++) {
            String key = headers[index];
            String value = record[index];
            job.add(key, value);
          }
          jab.add(job);
        }

        String jsonText = JsonUtils.toJsonText(jab);
        osw.write(jsonText);
        osw.flush();
      }

      return jsonFile;

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Creates a JSON temp file with the specified headers and records.
   *
   * @param filePrefix The prefix for the temp file name.
   * @param headers    The CSV headers.
   * @param records    The one or more records.
   * @return The {@link File} that was created.
   */
  protected File prepareJsonFile(String filePrefix,
                                 String[] headers,
                                 String[]... records) {
    // check the arguments
    int count = headers.length;
    for (int index = 0; index < records.length; index++) {
      String[] record = records[index];
      if (record.length != count) {
        throw new IllegalArgumentException(
            "The header and records do not all have the same number of "
                + "elements.  expected=[ " + count + " ], received=[ "
                + record.length + " ], index=[ " + index + " ]");
      }
    }

    try {
      File jsonFile = File.createTempFile(filePrefix, ".json");

      // populate the file as one JSON record per line
      try (FileOutputStream fos = new FileOutputStream(jsonFile);
           OutputStreamWriter osw = new OutputStreamWriter(fos, "UTF-8");
           PrintWriter pw = new PrintWriter(osw)) {
        for (String[] record : records) {
          JsonObjectBuilder job = Json.createObjectBuilder();
          for (int index = 0; index < record.length; index++) {
            String key = headers[index];
            String value = record[index];
            job.add(key, value);
          }
          String jsonText = JsonUtils.toJsonText(job);
          pw.println(jsonText);
          pw.flush();
        }
        pw.flush();
      }

      return jsonFile;

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Compares two collections to ensure they have the same elements.
   *
   */
  protected void assertSameElements(Collection expected,
                                    Collection actual,
                                    String     description)
  {
    if (expected != null) {
      expected = upperCase(expected);
      actual   = upperCase(actual);
      assertNotNull(actual, "Unexpected null " + description);
      if (!actual.containsAll(expected)) {
        Set missing = new HashSet(expected);
        missing.removeAll(actual);
        fail("Missing one or more expected " + description + ".  missing=[ "
             + missing + " ], actual=[ " + actual + " ]");
      }
      if (!expected.containsAll(actual)) {
        Set extras = new HashSet(actual);
        extras.removeAll(expected);
        fail("One or more extra " + description + ".  extras=[ "
             + extras + " ], actual=[ " + actual + " ]");
      }
    }
  }

  /**
   * Converts the {@link String} elements in the specified {@link Collection}
   * to upper case and returns a {@link Set} contianing all values.
   *
   * @param c The {@link Collection} to process.
   *
   * @return The {@link Set} containing the same elements with the {@link
   *         String} elements converted to upper case.
   */
  protected static Set upperCase(Collection c) {
    Set set = new LinkedHashSet();
    for (Object obj : c) {
      if (obj instanceof String) {
        obj = ((String) obj).toUpperCase();
      }
      set.add(obj);
    }
    return set;
  }

  /**
   * Utility method for creating a {@link Set} to use in validation.
   *
   * @param elements The zero or more elements in the set.
   *
   * @return The {@link Set} of elements.
   */
  protected static <T> Set<T> set(T... elements) {
    Set<T> set = new LinkedHashSet<>();
    for (T element : elements) {
      set.add(element);
    }
    return set;
  }

  /**
   * Utility method for creating a {@link List} to use in validation.
   *
   * @param elements The zero or more elements in the list.
   *
   * @return The {@link Set} of elements.
   */
  protected static <T> List<T> list(T... elements) {
    List<T> list = new ArrayList<>(elements.length);
    for (T element : elements) {
      list.add(element);
    }
    return list;
  }

  /**
   * Validates an entity
   */
  protected void validateEntity(
      String                              testInfo,
      SzResolvedEntity                    entity,
      List<SzRelatedEntity>               relatedEntities,
      Boolean                             forceMinimal,
      SzFeatureInclusion                  featureMode,
      Integer                             expectedRecordCount,
      Set<SzRecordId>                     expectedRecordIds,
      Boolean                             relatedSuppressed,
      Integer                             relatedEntityCount,
      Boolean                             relatedPartial,
      Map<String,Integer>                 expectedFeatureCounts,
      Map<String,Set<String>>             primaryFeatureValues,
      Map<String,Set<String>>             duplicateFeatureValues,
      Map<SzAttributeClass, Set<String>>  expectedDataValues,
      Set<String>                         expectedOtherDataValues)
  {
    if (expectedRecordCount != null) {
      assertEquals(expectedRecordCount, entity.getRecords().size(),
                   "Unexpected number of records: " + testInfo);
    }
    if (expectedRecordIds != null) {
      // get the records and convert to record ID set
      Set<SzRecordId> actualRecordIds = new HashSet<>();
      List<SzMatchedRecord> matchedRecords = entity.getRecords();
      for (SzMatchedRecord record : matchedRecords) {
        SzRecordId recordId = new SzRecordId(record.getDataSource(),
                                             record.getRecordId());
        actualRecordIds.add(recordId);
      }
      this.assertSameElements(expectedRecordIds, actualRecordIds,
                              "Unexpected record IDs: " + testInfo);
    }

    // check the features
    if (forceMinimal != null && forceMinimal) {
      assertEquals(0, entity.getFeatures().size(),
                   "Features included in minimal results: " + testInfo
                       + " / " + entity.getFeatures());
    } else if (featureMode != null && featureMode == NONE) {
      assertEquals(
          0, entity.getFeatures().size(),
          "Features included despite NONE feature mode: " + testInfo
              + " / " + entity.getFeatures());

    } else {
      assertNotEquals(0, entity.getFeatures().size(),
                      "Features not present for entity: " + testInfo);

      // validate representative feature mode
      if (featureMode == REPRESENTATIVE) {
        entity.getFeatures().entrySet().forEach(entry -> {
          String                featureKey    = entry.getKey();
          List<SzEntityFeature> featureValues = entry.getValue();
          featureValues.forEach(featureValue -> {
            if (featureValue.getDuplicateValues().size() != 0) {
              fail("Duplicate feature values present for " + featureKey
                       + " feature despite REPRESENTATIVE feature mode: "
                       + testInfo + " / " + featureValue);
            }
          });
        });
      }

      // validate the feature counts (if any)
      if (expectedFeatureCounts != null) {
        expectedFeatureCounts.entrySet().forEach(entry -> {
          String featureKey = entry.getKey();
          int expectedCount = entry.getValue();
          List<SzEntityFeature> featureValues
              = entity.getFeatures().get(featureKey);
          assertEquals(expectedCount, featureValues.size(),
                       "Unexpected feature count for " + featureKey
                           + " feature: " + testInfo + " / " + featureValues);
        });
      }

      // validate the feature values (if any)
      if (primaryFeatureValues != null) {
        primaryFeatureValues.entrySet().forEach(entry -> {
          String      featureKey    = entry.getKey();
          Set<String> primaryValues = entry.getValue();

          List<SzEntityFeature> featureValues
              = entity.getFeatures().get(featureKey);

          primaryValues.forEach(primaryValue -> {
            boolean found = false;
            for (SzEntityFeature featureValue : featureValues) {
              if (primaryValue.equalsIgnoreCase(featureValue.getPrimaryValue())) {
                found = true;
                break;
              }
            }
            if (!found) {
              fail("Could not find \"" + primaryValue + "\" among the "
                       + featureKey + " primary feature values: " + testInfo
                       + " / " + featureValues);
            }
          });
        });
      }
      if (duplicateFeatureValues != null && (featureMode != REPRESENTATIVE)) {
        duplicateFeatureValues.entrySet().forEach(entry -> {
          String      featureKey      = entry.getKey();
          Set<String> duplicateValues = entry.getValue();

          List<SzEntityFeature> featureValues
              = entity.getFeatures().get(featureKey);

          duplicateValues.forEach(expectedDuplicate -> {
            boolean found = false;
            for (SzEntityFeature featureValue : featureValues) {
              for (String duplicateValue : featureValue.getDuplicateValues()) {
                if (expectedDuplicate.equalsIgnoreCase(duplicateValue)) {
                  found = true;
                  break;
                }
              }
            }
            if (!found) {
              fail("Could not find \"" + expectedDuplicate + "\" among the "
                       + featureKey + " duplicate feature values: " + testInfo
                       + " / " + featureValues);
            }
          });
        });
      }

      // validate the features versus the data elements
      SzApiProvider provider = SzApiProvider.Factory.getProvider();
      entity.getFeatures().entrySet().forEach(entry -> {
        String featureKey = entry.getKey();
        List<SzEntityFeature> featureValues = entry.getValue();

        SzAttributeClass attrClass = SzAttributeClass.parseAttributeClass(
            provider.getAttributeClassForFeature(featureKey));

        List<String> dataSet = this.getDataElements(entity, attrClass);
        if (dataSet == null) return;

        for (SzEntityFeature feature : featureValues) {
          String featureValue = feature.getPrimaryValue().trim().toUpperCase();
          boolean found = false;
          for (String dataValue : dataSet) {
            if (dataValue.toUpperCase().indexOf(featureValue) >= 0) {
              found = true;
              break;
            }
          }
          if (!found) {
            fail(featureKey + " feature value (" + featureValue
                     + ") not found in " + attrClass + " data values: "
                     + dataSet + " (" + testInfo + ")");
          }
        }
      });
    }

    // check if related entities are provided to validate
    if (relatedEntities != null) {
      if (relatedSuppressed == null || !relatedSuppressed) {
        // check if verifying the number of related entities
        if (relatedEntityCount != null) {
          assertEquals(relatedEntityCount, relatedEntities.size(),
                       "Unexpected number of related entities: "
                           + testInfo);
        }

        // check if verifying if related entities are partial
        if (relatedPartial != null || (forceMinimal != null && forceMinimal)) {
          boolean partial = ((relatedPartial != null && relatedPartial)
              || (forceMinimal != null && forceMinimal)
              || (featureMode == NONE));

          for (SzRelatedEntity related : relatedEntities) {
            if (related.isPartial() != partial) {
              if (partial) {
                fail("Entity " + entity.getEntityId() + " has a complete "
                         + "related entity (" + related.getEntityId()
                         + ") where partial entities were expected: " + testInfo);
              } else {
                fail("Entity " + entity.getEntityId() + " has a partial "
                         + "related entity (" + related.getEntityId()
                         + ") where complete entities were expected: " + testInfo);
              }
            }
          }
        }
      }
    }

    if (expectedDataValues != null
        && (forceMinimal == null || !forceMinimal)
        && (featureMode == null || featureMode != NONE))
    {
      expectedDataValues.entrySet().forEach(entry -> {
        SzAttributeClass attrClass      = entry.getKey();
        Set<String>      expectedValues = entry.getValue();
        List<String>     actualValues   = this.getDataElements(entity, attrClass);
        this.assertSameElements(expectedValues,
                                actualValues,
                                attrClass.toString() + " (" + testInfo + ")");
      });
    }
    if (expectedOtherDataValues != null
        && (forceMinimal == null || !forceMinimal) )
    {
      List<String> actualValues = entity.getOtherData();
      this.assertSameElements(expectedOtherDataValues, actualValues,
                              "OTHER DATA (" + testInfo + ")");
    }
  }

  /**
   * Gets the data elements from the specified entity for the given attribute
   * class.
   *
   * @param entity The entity to get the data from.
   * @param attrClass The attribute class identifying the type of data
   * @return The {@link List} of data elements.
   */
  protected List<String> getDataElements(SzResolvedEntity  entity,
                                         SzAttributeClass  attrClass)
  {
    switch (attrClass) {
      case NAME:
        return entity.getNameData();
      case CHARACTERISTIC:
        return entity.getAttributeData();
      case PHONE:
        return entity.getPhoneData();
      case IDENTIFIER:
        return entity.getIdentifierData();
      case ADDRESS:
        return entity.getAddressData();
      case RELATIONSHIP:
        return entity.getRelationshipData();
      default:
        return null;
    }
  }

  /**
   * Returns the contents of the JSON init file as a {@link String}.
   *
   * @return The contents of the JSON init file as a {@link String}.
   */
  protected String readInitJsonFile() {
    try {
      File    repoDir       = this.getRepositoryDirectory();
      File    initJsonFile  = new File(repoDir, "g2-init.json");

      return readTextFileAsString(initJsonFile, "UTF-8");

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Validates an {@link SzDataSourcesResponse} instance.
   *
   * @param response The response to validate.
   * @param beforeTimestamp The timestamp before executing the request.
   * @param afterTimestamp The timestamp after executing the request and
   *                       concluding timers on the response.
   * @param expectRawData Whether or not to expect raw data.
   * @param expectedDataSources The expected data sources.
   */
  protected void validateDataSourcesResponse(
      SzDataSourcesResponse   response,
      long                    beforeTimestamp,
      long                    afterTimestamp,
      Boolean                 expectRawData,
      Set<String>             expectedDataSources)
  {
    String selfLink = this.formatServerUri("data-sources");
    if (expectRawData != null) {
      selfLink += "?withRaw=" + expectRawData;
    } else {
      expectRawData = false;
    }

    this.validateBasics(
        response, selfLink, beforeTimestamp, afterTimestamp, expectRawData);

    SzDataSourcesResponse.Data data = response.getData();

    assertNotNull(data, "Response data is null");

    Set<String> sources = data.getDataSources();

    assertNotNull(sources, "Data sources set is null");

    assertEquals(expectedDataSources, sources,
                 "Unexpected or missing data sources in set");

    if (expectRawData) {
      this.validateRawDataMap(response.getRawData(), "DSRC_CODE");
    }
  }

  /**
   * Validates an {@link SzAttributeTypesResponse} instance.
   *
   * @param response The response to validate.
   * @param selfLink The expected meta data self link.
   * @param beforeTimestamp The timestamp before executing the request.
   * @param afterTimestamp The timestamp after executing the request and
   *                       concluding timers on the response.
   * @param expectRawData Whether or not to expect raw data.
   * @param expectedAttrTypeCodes The expected attribute type codes.
   */
  protected void validateAttributeTypesResponse(
      SzAttributeTypesResponse  response,
      String                    selfLink,
      long                      beforeTimestamp,
      long                      afterTimestamp,
      Boolean                   expectRawData,
      Set<String>               expectedAttrTypeCodes)
  {
    selfLink = this.formatServerUri(selfLink);
    if (expectRawData == null) {
      expectRawData = false;
    }

    this.validateBasics(
        response, selfLink, beforeTimestamp, afterTimestamp, expectRawData);

    SzAttributeTypesResponse.Data data = response.getData();

    assertNotNull(data, "Response data is null");

    List<SzAttributeType> attrTypes = data.getAttributeTypes();

    assertNotNull(attrTypes, "List of attribute types is null");

    Map<String, SzAttributeType> map = new LinkedHashMap<>();
    for (SzAttributeType attrType : attrTypes) {
      map.put(attrType.getAttributeCode(), attrType);
    }

    assertEquals(expectedAttrTypeCodes, map.keySet(),
                 "Unexpected or missing attribute types");

    if (expectRawData) {
      this.validateRawDataMapArray(response.getRawData(),
                                   false,
                                   "DEFAULT_VALUE",
                                   "ATTR_CODE",
                                   "FELEM_REQ",
                                   "ATTR_CLASS",
                                   "INTERNAL",
                                   "ATTR_ID",
                                   "FTYPE_CODE",
                                   "FELEM_CODE",
                                   "ADVANCED");
    }
  }

  /**
   /**
   * Validates an {@link SzAttributeTypeResponse} instance.
   *
   * @param response The response to validate.
   * @param attributeCode The requested attribute code.
   * @param beforeTimestamp The timestamp before executing the request.
   * @param afterTimestamp The timestamp after executing the request and
   *                       concluding timers on the response.
   * @param expectRawData Whether or not to expect raw data.
   */
  protected void validateAttributeTypeResponse(
      SzAttributeTypeResponse response,
      String                  attributeCode,
      long                    beforeTimestamp,
      long                    afterTimestamp,
      Boolean                 expectRawData)
  {
    String selfLink = this.formatServerUri(
        "attribute-types/" + attributeCode);
    if (expectRawData != null) {
      selfLink += "?withRaw=" + expectRawData;
    } else {
      expectRawData = false;
    }

    this.validateBasics(
        response, selfLink, beforeTimestamp, afterTimestamp, expectRawData);

    SzAttributeTypeResponse.Data data = response.getData();

    assertNotNull(data, "Response data is null");

    SzAttributeType attrType = data.getAttributeType();

    assertNotNull(attrType, "Attribute Type is null");

    assertEquals(attributeCode, attrType.getAttributeCode(),
                 "Unexpected attribute type code");

    if (expectRawData) {
      this.validateRawDataMap(response.getRawData(),
                              "DEFAULT_VALUE",
                              "ATTR_CODE",
                              "FELEM_REQ",
                              "ATTR_CLASS",
                              "INTERNAL",
                              "ATTR_ID",
                              "FTYPE_CODE",
                              "FELEM_CODE",
                              "ADVANCED");
    }
  }

  /**
   * Validates an {@link SzConfigResponse} instance.
   *
   * @param response The response to validate.
   * @param selfLink The expected meta data self link.
   * @param beforeTimestamp The timestamp before executing the request.
   * @param afterTimestamp The timestamp after executing the request and
   *                       concluding timers on the response.
   * @param expectedDataSources The expected data sources.
   */
  protected void validateConfigResponse(
      SzConfigResponse        response,
      String                  selfLink,
      long                    beforeTimestamp,
      long                    afterTimestamp,
      Set<String>             expectedDataSources)
  {
    selfLink = this.formatServerUri(selfLink);

    this.validateBasics(
        response, selfLink, beforeTimestamp, afterTimestamp, true);

    Object rawData = response.getRawData();

    this.validateRawDataMap(rawData, true, "G2_CONFIG");

    Object g2Config = ((Map) rawData).get("G2_CONFIG");

    this.validateRawDataMap(g2Config,
                            false,
                            "CFG_ATTR",
                            "CFG_FELEM",
                            "CFG_DSRC");

    Object cfgDsrc = ((Map) g2Config).get("CFG_DSRC");

    this.validateRawDataMapArray(cfgDsrc,
                                 false,
                                 "DSRC_ID",
                                 "DSRC_DESC",
                                 "DSRC_CODE");

    Set<String> actualDataSources = new LinkedHashSet<>();
    for (Object dsrc : ((Collection) cfgDsrc)) {
      Map dsrcMap = (Map) dsrc;
      String dsrcCode = (String) dsrcMap.get("DSRC_CODE");
      actualDataSources.add(dsrcCode);
    }

    assertEquals(expectedDataSources, actualDataSources,
                 "Unexpected set of data sources in config.");
  }

  /**
   * Validates an {@link SzRecordResponse} instance.
   *
   * @param response The response to validate.
   * @param dataSourceCode The data source code for the requested record.
   * @param expectedRecordId The record ID for the requested record.
   * @param expectedNameData The expected name data or <tt>null</tt> if not
   *                         validating the name data.
   * @param expectedAddressData The expected address data or <tt>null</tt> if
   *                            not validating the address data.
   * @param expectedPhoneData The expected phone data or <tt>null</tt> if not
   *                          validating the phone data.
   * @param expectedIdentifierData The expected identifier data or <tt>null</tt>
   *                               if not validating the identifier data.
   * @param expectedAttributeData The expected attribute data or <tt>null</tt>
   *                              if not validating the attribute data.
   * @param expectedOtherData The expected other data or <tt>null</tt>
   *                          if not validating the other data.
   * @param beforeTimestamp The timestamp before executing the request.
   * @param afterTimestamp The timestamp after executing the request and
   *                       concluding timers on the response.
   * @param expectRawData Whether or not to expect raw data.
   */
  protected void validateRecordResponse(SzRecordResponse response,
                                        String dataSourceCode,
                                        String expectedRecordId,
                                        Set<String> expectedNameData,
                                        Set<String> expectedAddressData,
                                        Set<String> expectedPhoneData,
                                        Set<String> expectedIdentifierData,
                                        Set<String> expectedAttributeData,
                                        Set<String> expectedRelationshipData,
                                        Set<String> expectedOtherData,
                                        long beforeTimestamp,
                                        long afterTimestamp,
                                        Boolean expectRawData)
  {
    String selfLink = this.formatServerUri(
        "data-sources/" + dataSourceCode
            + "/records/" + expectedRecordId);
    if (expectRawData != null) {
      selfLink += "?withRaw=" + expectRawData;
    } else {
      expectRawData = false;
    }

    this.validateBasics(
        response, selfLink, beforeTimestamp, afterTimestamp);

    SzEntityRecord record = response.getData();

    assertNotNull(record, "Response data is null");

    String dataSource = record.getDataSource();
    assertNotNull(dataSource, "Data source is null");
    assertEquals(dataSourceCode, dataSource, "Unexpected data source value");

    String recordId = record.getRecordId();
    assertNotNull(recordId, "Record ID is null");
    assertEquals(expectedRecordId, recordId, "Unexpected record ID value");

    this.assertSameElements(
        expectedNameData, record.getNameData(), "names");
    this.assertSameElements(
        expectedAddressData, record.getAddressData(), "addresses");
    this.assertSameElements(
        expectedPhoneData, record.getPhoneData(), "phone numbers");
    this.assertSameElements(
        expectedIdentifierData, record.getIdentifierData(), "identifiers");
    this.assertSameElements(
        expectedAttributeData, record.getAttributeData(), "attributes");
    this.assertSameElements(
        expectedRelationshipData, record.getRelationshipData(), "relationships");
    this.assertSameElements(
        expectedOtherData, record.getOtherData(), "other");

    if (expectRawData) {
      this.validateRawDataMap(response.getRawData(),
                              false,
                              "JSON_DATA",
                              "NAME_DATA",
                              "ATTRIBUTE_DATA",
                              "IDENTIFIER_DATA",
                              "ADDRESS_DATA",
                              "PHONE_DATA",
                              "RELATIONSHIP_DATA",
                              "ENTITY_DATA",
                              "OTHER_DATA",
                              "DATA_SOURCE",
                              "RECORD_ID");
    }

  }

  /**
   * Validates an {@link SzEntityResponse} instance.
   *
   * @param testInfo The test information describing the test.
   * @param response The response to validate.
   * @param selfLink The expected meta data self link.
   * @param withRaw <tt>true</tt> if requested with raw data, <tt>false</tt>
   *                if requested without raw data and <tt>null</tt> if this is
   *                not being validated.
   * @param withRelated <tt>true</tt> if requested with first-degree relations,
   *                    <tt>false</tt> if requested without them and
   *                    <tt>null</tt> if this aspect is not being validated.
   * @param forceMinimal <tt>true</tt> if requested with minimal data,
   *                     <tt>false</tt> if requested with standard data and
   *                     <tt>null</tt> if this aspect is not being validated.
   * @param featureMode The {@link SzFeatureInclusion} requested or
   *                    <tt>null</tt> if this is not being validated.
   * @param expectedRecordCount The number of expected records for the entity,
   *                            or <tt>null</tt> if this is not being validated.
   * @param expectedRecordIds The expected record IDs for the entity to have or
   *                          <tt>null</tt> if this is not being validated.
   * @param relatedEntityCount The expected number of related entities or
   *                           <tt>null</tt> if this is not being validated.
   * @param expectedFeatureCounts The expected number of features by feature
   *                              type, or <tt>null</tt> if this is not being
   *                              validated.
   * @param primaryFeatureValues The expected primary feature values by feature
   *                             type, or <tt>null</tt> if this is not being
   *                             validated.
   * @param duplicateFeatureValues The expected duplicate fature values by
   *                               feature type, or <tt>null</tt> if this is not
   *                               being validated.
   * @param expectedDataValues The expected data values by attribute class, or
   *                           <tt>null</tt> if this is not being validated.
   * @param beforeTimestamp The timestamp before executing the request.
   * @param afterTimestamp The timestamp after executing the request and
   *                       concluding timers on the response.
   */
  protected void validateEntityResponse(
      String                              testInfo,
      SzEntityResponse                    response,
      String                              selfLink,
      Boolean                             withRaw,
      Boolean                             withRelated,
      Boolean                             forceMinimal,
      SzFeatureInclusion                  featureMode,
      Integer                             expectedRecordCount,
      Set<SzRecordId>                     expectedRecordIds,
      Integer                             relatedEntityCount,
      Map<String,Integer>                 expectedFeatureCounts,
      Map<String,Set<String>>             primaryFeatureValues,
      Map<String,Set<String>>             duplicateFeatureValues,
      Map<SzAttributeClass, Set<String>>  expectedDataValues,
      Set<String>                         expectedOtherDataValues,
      long                                beforeTimestamp,
      long                                afterTimestamp)
  {
    selfLink = this.formatServerUri(selfLink);

    this.validateBasics(
        testInfo, response, selfLink, beforeTimestamp, afterTimestamp);

    SzEntityData entityData = response.getData();

    assertNotNull(entityData, "Response data is null: " + testInfo);

    SzResolvedEntity resolvedEntity = entityData.getResolvedEntity();

    assertNotNull(resolvedEntity, "Resolved entity is null: " + testInfo);

    List<SzRelatedEntity> relatedEntities = entityData.getRelatedEntities();

    assertNotNull(relatedEntities,
                  "Related entities list is null: " + testInfo);

    this.validateEntity(testInfo,
                        resolvedEntity,
                        relatedEntities,
                        forceMinimal,
                        featureMode,
                        expectedRecordCount,
                        expectedRecordIds,
                        false,
                        relatedEntityCount,
                        (withRelated == null || !withRelated),
                        expectedFeatureCounts,
                        primaryFeatureValues,
                        duplicateFeatureValues,
                        expectedDataValues,
                        expectedOtherDataValues);

    if (withRaw != null && withRaw) {
      if (withRelated != null && withRelated
          && (forceMinimal == null || !forceMinimal))
      {
        this.validateRawDataMap(testInfo,
                                response.getRawData(),
                                true,
                                "ENTITY_PATHS", "ENTITIES");

        Object entities = ((Map) response.getRawData()).get("ENTITIES");
        this.validateRawDataMapArray(testInfo,
                                     entities,
                                     false,
                                     "RESOLVED_ENTITY",
                                     "RELATED_ENTITIES");


        if (featureMode == NONE || (forceMinimal != null && forceMinimal)) {
          for (Object entity : ((Collection) entities)) {
            this.validateRawDataMap(testInfo,
                                    ((Map) entity).get("RESOLVED_ENTITY"),
                                    false,
                                    "ENTITY_ID",
                                    "RECORDS");
          }

        } else {
          for (Object entity : ((Collection) entities)) {
            this.validateRawDataMap(testInfo,
                                    ((Map) entity).get("RESOLVED_ENTITY"),
                                    false,
                                    "ENTITY_ID",
                                    "FEATURES",
                                    "RECORD_SUMMARY",
                                    "RECORDS");
          }
        }


      } else {
        this.validateRawDataMap(testInfo,
                                response.getRawData(),
                                false,
                                "RESOLVED_ENTITY",
                                "RELATED_ENTITIES");

        Object entity = ((Map) response.getRawData()).get("RESOLVED_ENTITY");
        if (featureMode == NONE || (forceMinimal != null && forceMinimal)) {
          this.validateRawDataMap(testInfo,
                                  entity,
                                  false,
                                  "ENTITY_ID",
                                  "RECORDS");

        } else {
          this.validateRawDataMap(testInfo,
                                  entity,
                                  false,
                                  "ENTITY_ID",
                                  "FEATURES",
                                  "RECORD_SUMMARY",
                                  "RECORDS");
        }
      }
    }
  }

  /**
   * Validate an {@link SzAttributeSearchResponse} instance.
   *
   * @param testInfo The test information describing the test.
   * @param response The response to validate.
   * @param selfLink The expected meta data self link.
   * @param expectedCount The number of expected matching entities for the
   *                      search, or <tt>null</tt> if this is not being
   *                      validated.
   * @param withRelationships <tt>true</tt> if requested with relationship
   *                          information should be included with the entity
   *                          results, <tt>false</tt> if the relationship
   *                          information should be excluded and <tt>null</tt>
   *                          if this aspect is not being validated.
   * @param forceMinimal <tt>true</tt> if requested with minimal data,
   *                     <tt>false</tt> if requested with standard data and
   *                     <tt>null</tt> if this aspect is not being validated.
   * @param featureInclusion The {@link SzFeatureInclusion} requested or
   *                         <tt>null</tt> if this is not being validated.
   * @param beforeTimestamp The timestamp before executing the request.
   * @param afterTimestamp The timestamp after executing the request and
   *                       concluding timers on the response.
   * @param expectRawData Whether or not to expect raw data.
   */
  protected void validateSearchResponse(String testInfo,
                                        SzAttributeSearchResponse response,
                                        String selfLink,
                                        Integer expectedCount,
                                        Boolean withRelationships,
                                        Boolean forceMinimal,
                                        SzFeatureInclusion featureInclusion,
                                        long beforeTimestamp,
                                        long afterTimestamp,
                                        Boolean expectRawData)
  {
    selfLink = this.formatServerUri(selfLink);
    if (expectRawData == null) {
      expectRawData = false;
    }

    this.validateBasics(
        testInfo, response, selfLink, beforeTimestamp, afterTimestamp);

    SzAttributeSearchResponse.Data data = response.getData();

    assertNotNull(data, "Response data is null: " + testInfo);

    List<SzAttributeSearchResult> results = data.getSearchResults();

    assertNotNull(results, "Result list is null: " + testInfo);

    if (expectedCount != null) {
      assertEquals(expectedCount, results.size(),
                   "Unexpected number of results: " + testInfo);
    }

    for (SzAttributeSearchResult result : results) {

      this.validateEntity(testInfo,
                          result,
                          result.getRelatedEntities(),
                          forceMinimal,
                          featureInclusion,
                          null,
                          null,
                          (withRelationships == null || !withRelationships),
                          null,
                          true,
                          null,
                          null,
                          null,
                          null,
                          null);
    }

    if (expectRawData) {
      this.validateRawDataMap(testInfo,
                              response.getRawData(),
                              false,
                              "RESOLVED_ENTITIES");

      Object entities = ((Map) response.getRawData()).get("RESOLVED_ENTITIES");
      this.validateRawDataMapArray(testInfo,
                                   entities,
                                   false,
                                   "MATCH_INFO", "ENTITY");
      for (Object obj : ((Collection) entities)) {
        Object matchInfo = ((Map) obj).get("MATCH_INFO");
        this.validateRawDataMap(testInfo,
                                matchInfo,
                                false,
                                "MATCH_LEVEL",
                                "MATCH_KEY",
                                "MATCH_SCORE",
                                "ERRULE_CODE",
                                "REF_SCORE",
                                "FEATURE_SCORES");
        Object entity = ((Map) obj).get("ENTITY");
        Object resolvedEntity = ((Map) entity).get("RESOLVED_ENTITY");
        if (featureInclusion == NONE || (forceMinimal != null && forceMinimal)) {
          this.validateRawDataMap(testInfo,
                                  resolvedEntity,
                                  false,
                                  "ENTITY_ID",
                                  "RECORDS");

        } else {
          this.validateRawDataMap(testInfo,
                                  resolvedEntity,
                                  false,
                                  "ENTITY_ID",
                                  "FEATURES",
                                  "RECORD_SUMMARY",
                                  "RECORDS");

        }
      }

    }
  }


  /**
   * Validates an {@Link SzLoadRecordResponse} instance.
   *
   * @param response The response to validate.
   * @param httpMethod The HTTP method used to load the record.
   * @param dataSourceCode The data source code fo the loaded record.
   * @param expectedRecordId The record ID of the loaded record.
   * @param beforeTimestamp The timestamp before executing the request.
   * @param afterTimestamp The timestamp after executing the request and
   *                       concluding timers on the response.
   */
  protected void validateLoadRecordResponse(
      SzLoadRecordResponse  response,
      SzHttpMethod          httpMethod,
      String                dataSourceCode,
      String                expectedRecordId,
      long                  beforeTimestamp,
      long                  afterTimestamp)
  {
    String selfLink = this.formatServerUri(
        "data-sources/" + dataSourceCode + "/records");

    if (expectedRecordId != null) {
      selfLink += ("/" + expectedRecordId);
    }

    this.validateBasics(
        response, httpMethod, selfLink, beforeTimestamp, afterTimestamp);

    SzLoadRecordResponse.Data data = response.getData();

    assertNotNull(data, "Response data is null");

    String recordId = data.getRecordId();

    assertNotNull(recordId, "Record ID is null");

    if (expectedRecordId != null) {
      assertEquals(expectedRecordId, recordId,
                   "Unexpected record ID value");
    }
  }

}
