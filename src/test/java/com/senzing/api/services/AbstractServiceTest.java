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
import com.senzing.g2.engine.G2Engine;
import com.senzing.g2.engine.G2JNI;
import com.senzing.repomgr.RepositoryManager;
import com.senzing.util.AccessToken;

import javax.ws.rs.core.UriInfo;

import static com.senzing.api.BuildInfo.MAVEN_VERSION;
import static com.senzing.api.model.SzHttpMethod.*;
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
    G2Engine      engineApi = null;
    StringWriter  sw        = new StringWriter();
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
    } catch(Exception e) {
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
    this.server         = null;
    this.repoDirectory  = repoDirectory;
    this.accessToken    = new AccessToken();
    this.providerToken  = null;
  }

  /**
   * Creates an absolute URI for the relative URI provided.  For example, if
   * <tt>"license"</tt> was passed as the parameter then
   * <tt>"http://localhost:[port]/license"</tt> will be returned where
   * <tt>"[port]"</tt> is the port number of the currently running server, if
   * running, and is <tt>"2080"</tt> (the default port) if not running.
   *
   * @param relativeUri The relative URI to build the absolute URI from.
   *
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
    sb.append("/" + relativeUri);
    return sb.toString();
  }

  /**
   * Checks that the Senzing Native API is available and if not causes the
   * test or tests to be skipped.
   *
   * @return <tt>true</tt> if the native API's are available, otherwise
   *         <tt>false</tt>
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
    if (!NATIVE_API_AVAILABLE) return;
    RepositoryManager.createRepo(this.getRepositoryDirectory());
    this.repoCreated = true;
    this.prepareRepository();
    RepositoryManager.conclude();
    this.initializeServer();
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
        && this.repoDirectory.exists() && this.repoDirectory.isDirectory())
    {
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
   *
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
   *
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
   *
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
   *         <tt>null</tt> if any available port is fine.
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
   *         <tt>null</tt> if the server should bind to all available network
   *         interfaces.
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
   *         otherwise <tt>false</tt>.
   *
   */
  protected boolean isVerbose() {
    return false;
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
      File iniFile = new File(this.getRepositoryDirectory(), "g2.ini");

      System.err.println("Initializing with INI file: " + iniFile);

      this.server = new SzApiServer(this.accessToken,
                                    0,
                                    getServerAddress(),
                                    this.getServerConcurrency(),
                                    this.getModuleName(),
                                    iniFile,
                                    this.isVerbose());

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

      InvocationHandler handler = (p,m,a) -> {
        if (m.getName().equals("getRequestUri")) {
          return uri;
        }
        throw new UnsupportedOperationException(
            "Operation not implemented on proxy UriInfo");
      };

      ClassLoader loader  = this.getClass().getClassLoader();
      Class[]     classes = {UriInfo.class};

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
   * @param response The {@link SzBasicResponse} to validate.
   * @param selfLink The self link to be expected.
   * @param beforeTimestamp The timestamp from just before calling the service.
   * @param afterTimestamp The timestamp from just after calling the service.
   */
  protected void validateBasics(SzBasicResponse  response,
                                String           selfLink,
                                long             beforeTimestamp,
                                long             afterTimestamp)
  {

    SzLinks links = response.getLinks();
    SzMeta meta  = response.getMeta();
    assertEquals(selfLink, links.getSelf(), "Unexpected self link");
    assertEquals(GET, meta.getHttpMethod(), "Unexpected HTTP method");
    assertEquals(200, meta.getHttpStatusCode(), "Unexpected HTTP status code");
    assertEquals(MAVEN_VERSION, meta.getVersion(), "Unexpected server version");
    assertNotNull(meta.getTimestamp(), "Timestamp unexpectedly null");
    long now = meta.getTimestamp().getTime();

    // check the timestamp
    if (now < beforeTimestamp || now > afterTimestamp) {
      fail("Timestamp should be between " + new Date(beforeTimestamp) + " and "
               + new Date(afterTimestamp));
    }
    Map<String, Long> timings = meta.getTimings();

    // determine max duration
    long maxDuration = (afterTimestamp - beforeTimestamp);

    timings.entrySet().forEach(entry -> {
      long duration = entry.getValue();
      if (duration > maxDuration) {
        fail("Timing value too large: " + entry.getKey() + " = "
             + duration + "ms VS " + maxDuration + "ms");
      }
    });
  }

  /**
   * Validates the basic response fields for the specified {@link
   * SzResponseWithRawData} using the specified self link, timestamp from before
   * calling the service function, timestamp from after calling the
   * service function, and flag indicating if raw data should be expected.
   *
   * @param response The {@link SzBasicResponse} to validate.
   * @param selfLink The self link to be expected.
   * @param beforeTimestamp The timestamp from just before calling the service.
   * @param afterTimestamp The timestamp from just after calling the service.
   * @param expectRawData <tt>true</tt> if raw data should be expected,
   *                      otherwise <tt>false</tt>
   */
  protected void validateBasics(SzResponseWithRawData response,
                                String                selfLink,
                                long                  beforeTimestamp,
                                long                  afterTimestamp,
                                boolean               expectRawData)
  {
    this.validateBasics(response, selfLink, beforeTimestamp, afterTimestamp);

    Object rawData = response.getRawData();
    if (expectRawData) {
      assertNotNull(rawData, "Raw data unexpectedly non-null");
    } else {
      assertNull(rawData, "Raw data unexpectedly null");
    }
  }

  /**
   * Invoke an operation on the currently running API server over HTTP.
   *
   * @param httpMethod The HTTP method to use.
   * @param uri The relative or absolute URI (optionally including query params)
   * @param responseClass The class of the response.
   * @param <T> The response type which must extend {@link SzBasicResponse}
   * @return
   */
  protected <T extends SzBasicResponse> T invokeServerViaHttp(
      SzHttpMethod    httpMethod,
      String          uri,
      Class<T>        responseClass)
  {
    return this.invokeServerViaHttp(
        httpMethod, uri, null, null, responseClass);
  }

  /**
   * Invoke an operation on the currently running API server over HTTP.
   *
   * @param httpMethod The HTTP method to use.
   * @param uri The relative or absolute URI (optionally including query params)
   * @param queryParams The optional map of query parameters.
   * @param bodyContent The object to be converted to JSON for body content.
   * @param responseClass The class of the response.
   * @param <T> The response type which must extend {@link SzBasicResponse}
   * @return
   */
  protected <T extends SzBasicResponse> T invokeServerViaHttp(
      SzHttpMethod    httpMethod,
      String          uri,
      Map<String, ?>  queryParams,
      Object          bodyContent,
      Class<T>        responseClass)
  {
    ObjectMapper objectMapper = new ObjectMapper();
    try {
      if (!uri.toLowerCase().startsWith("http://")) {
        uri = this.formatServerUri(uri);
      }
      if (queryParams != null && queryParams.size() > 0) {
        String initialPrefix = uri.contains("?") ? "&" : "?";

        StringBuilder sb = new StringBuilder();
        queryParams.entrySet().forEach(entry -> {
          String      key    = entry.getKey();
          Object      value  = entry.getValue();
          Collection  values = null;
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
        byte[] bytes = baos.toByteArray();
        int length = bytes.length;
        conn.addRequestProperty("Content-Length", "" + length);
        conn.addRequestProperty("Content-Type",
                                "application/json; charset=utf-8");
        OutputStream os = conn.getOutputStream();
        os.write(bytes);
        os.flush();
      }

      InputStream is = conn.getInputStream();
      InputStreamReader isr = new InputStreamReader(is, "UTF-8");
      BufferedReader br = new BufferedReader(isr);
      StringBuilder sb = new StringBuilder();
      for (int nextChar = br.read(); nextChar >= 0; nextChar = br.read()) {
        sb.append((char) nextChar);
      }

      String responseJson = sb.toString();

      return objectMapper.readValue(responseJson, responseClass);

    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }


}
