package com.senzing.api.server;

import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.*;

import com.senzing.g2.engine.*;
import com.senzing.util.JsonUtils;
import com.senzing.util.WorkerThreadPool;
import org.eclipse.jetty.server.ServerConnector;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.rewrite.handler.RewriteHandler;
import org.eclipse.jetty.rewrite.handler.TerminatingRegexRule;
import org.eclipse.jetty.rewrite.handler.RewriteRegexRule;
import org.eclipse.jetty.servlets.CrossOriginFilter;

import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonValue;
import javax.servlet.DispatcherType;

import static com.senzing.util.WorkerThreadPool.Task;

/**
 *
 *
 */
public class SzApiServer {
  private static final String JAR_FILE_NAME;

  private static final String JAR_BASE_URL;

  private static final String PATH_TO_JAR;

  private static final String DEFAULT_MODULE_NAME = "ApiServer";

  private static SzApiServer INSTANCE = null;

  private static enum Option {
    HELP,
    VERSION,
    HTTP_PORT,
    BIND_ADDRESS,
    MODULE_NAME,
    INI_FILE,
    VERBOSE,
    MONITOR_FILE;
  }

  ;

  public static final class AccessToken {
  }

  static {
    System.out.println();

    String url = SzApiServer.class.getResource(
        SzApiServer.class.getSimpleName() + ".class").toString();

    int index = url.lastIndexOf(
        SzApiServer.class.getName().replace(".", "/") + ".class");
    JAR_BASE_URL = url.substring(0, index);

    index = JAR_BASE_URL.lastIndexOf("!");
    url = url.substring(0, index);
    index = url.lastIndexOf("/");
    JAR_FILE_NAME = url.substring(index + 1);
    url = url.substring(0, index);
    index = url.indexOf("/");
    PATH_TO_JAR = url.substring(index);
  }

  public static synchronized SzApiServer getInstance() {
    if (INSTANCE == null) {
      throw new IllegalStateException("SzApiServer instance not initialized");
    }
    return INSTANCE;
  }

  /**
   * The access token to use call privileged functions on the server.
   * This may be <tt>null</tt> if no privileged functions may be performed.
   */
  private AccessToken accessToken = null;

  /**
   * The base URL for the server.
   */
  private String baseUrl;

  /**
   * The HTTP port for the server.
   */
  private int httpPort;

  /**
   * The {@link InetAddress} for the server.
   */
  private InetAddress ipAddr;

  /**
   * The {@link G2Config} config API.
   */
  private G2Config g2Config;

  /**
   * The {@link G2Product} product API.
   */
  private G2Product g2Product;

  /**
   * The {@link G2Engine} engine API.
   */
  private G2Engine g2Engine;

  /**
   * The set of configured data sources.
   */
  private Set<String> dataSources;

  /**
   * The {@link Map} of FTYPE_CODE values to ATTR_CLASS values from the config.
   */
  private Map<String, String> featureToAttrClassMap;

  /**
   * The {@link Map} of ATTR_CODE values to ATTR_CLASS values from the config.
   */
  private Map<String, String> attrCodeToAttrClassMap;

  /**
   * The Jetty Server.
   */
  private Server jettyServer;

  /**
   * The {@link FileMonitor} thread that monitors when to shutdown.
   */
  private FileMonitor fileMonitor;

  /**
   * The module name for initializing the API.
   */
  private String moduleName;

  /**
   * Whether or not the API should be initialized in verbose mode.
   */
  private boolean verbose;

  /**
   * The INI {@link File} for initializing the API.
   */
  private File iniFile;

  /**
   * The {@link WorkerThreadPool} for executing Senzing API calls.
   */
  private WorkerThreadPool workerThreadPool;

  /**
   * The monitor object to use while waiting for the server to shutdown.
   */
  private final Object joinMonitor = new Object();

  /**
   * Flag indicating if the server has been shutdown.
   */
  private boolean completed = false;

  /**
   * Returns the base URL for this instance.
   *
   * @return The base URL for this instance.
   */
  public String getBaseUrl() {
    this.assertNotShutdown();
    return this.baseUrl;
  }

  /**
   * Returns the HTTP port for this instance.
   *
   * @return The HTTP port for this instance.
   */
  public int getHttpPort() {
    this.assertNotShutdown();
    return this.httpPort;
  }

  /**
   * Returns the {@link InetAddress} for the IP address interface to which
   * the server is bound.
   *
   * @return The {@link InetAddress} for the IP address interface to which
   * the server is bound.
   */
  public InetAddress getIPAddress() {
    this.assertNotShutdown();
    return this.ipAddr;
  }

  /**
   * Returns the configured API module name.
   *
   * @return The configured API module name.
   */
  public String getApiModuleName() {
    this.assertNotShutdown();
    return this.moduleName;
  }

  /**
   * Returns the configured verbose flag for the API initialization.
   *
   * @return The configured verbose flag for the API initialization.
   */
  public boolean isApiVerbose() {
    this.assertNotShutdown();
    return this.verbose;
  }

  /**
   * Returns the API INI file used to initialize.
   *
   * @return The API INI file used to initialize.
   */
  public File getApiIniFile() {
    this.assertNotShutdown();
    return this.iniFile;
  }

  /**
   * Returns the initialized {@link G2Product} API interface.
   *
   * @return The initialized {@link G2Product} API interface.
   */
  public G2Product getProductApi() {
    this.assertNotShutdown();
    return this.g2Product;
  }

  /**
   * Returns the initialized {@link G2Config} API interface.
   *
   * @return The initialized {@link G2Config} API interface.
   */
  public G2Config getConfigApi() {
    this.assertNotShutdown();
    return this.g2Config;
  }

  /**
   * Returns the initialized {@link G2Engine} API interface.
   *
   * @return The initialized {@link G2Engine} API interface.
   */
  public G2Engine getEngineApi() {
    this.assertNotShutdown();
    return this.g2Engine;
  }

  /**
   * Evaluates the configuration and populates the {@link Set} of
   * data sources and maps mapping f-type code to attribute class and
   * attribute code to attribute class.
   *
   * @param config       The {@link JsonObject} describing the config.
   * @param dataSources  The {@link Set} of data sources to populate.
   * @param ftypeCodeMap The {@link Map} of f-type codes to attribute classes
   *                     to populate.
   * @param attrCodeMap  The {@link Map} of attribute code to attribute classes
   *                     to populate.
   */
  private static void evaluateConfig(JsonObject config,
                                     Set<String> dataSources,
                                     Map<String, String> ftypeCodeMap,
                                     Map<String, String> attrCodeMap) {
    JsonValue jsonValue = config.getValue("/G2_CONFIG/CFG_DSRC");
    JsonArray jsonArray = jsonValue.asJsonArray();

    for (JsonValue val : jsonArray) {
      JsonObject dataSource = val.asJsonObject();
      String dsrcCode = dataSource.getString("DSRC_CODE").toUpperCase();
      dataSources.add(dsrcCode);
    }

    jsonValue = config.getValue("/G2_CONFIG/CFG_ATTR");
    jsonArray = jsonValue.asJsonArray();

    for (JsonValue val : jsonArray) {
      JsonObject cfgAttr = val.asJsonObject();
      String attrCode = cfgAttr.getString("ATTR_CODE").toUpperCase();
      String ftypeCode = cfgAttr.getString("FTYPE_CODE").toUpperCase();
      String attrClass = cfgAttr.getString("ATTR_CLASS").toUpperCase();

      String ac = attrCodeMap.get(attrCode);
      if (ac != null && !ac.equals(attrClass)) {
        System.err.println(
            "*** WARNING : Multiple attribute classes for ATTR_CODE: "
                + attrCode + " ( " + ac + " / " + attrClass + " )");
      } else {
        attrCodeMap.put(attrCode, attrClass);
      }

      ac = ftypeCodeMap.get(ftypeCode);
      if (ac != null && !ac.equals(attrClass)) {
        System.err.println(
            "*** WARNING : Multiple attribute classes for FTYPE_CODE: "
                + ftypeCode + " ( " + ac + " / " + attrClass + " )");
      } else {
        ftypeCodeMap.put(ftypeCode, attrClass);
      }
    }
  }

  /**
   * Returns the unmodifiable {@link Set} of configured data source codes.
   *
   * @return The unmodifiable {@link Set} of configured data source codes.
   */
  public Set<String> getDataSources() {
    this.assertNotShutdown();
    return this.dataSources;
  }

  /**
   * Returns the attribute class (<tt>ATTR_CLASS</tt>) associated with the
   * specified feature name (<tt>FTYPE_CODE</tt>).
   *
   * @param featureName The feature name from the configuration to lookup the
   *                    attribute class.
   * @return The attribute class associated with the specified f-type code.
   */
  public String getAttributeClassForFeature(String featureName) {
    this.assertNotShutdown();
    return this.featureToAttrClassMap.get(featureName);
  }

  /**
   * Returns the attribute class (<tt>ATTR_CLASS</tt>) associated with the
   * specified attribute code (<tt>ATTR_CODE</tt>).
   *
   * @param attrCode The attribute code from the configuration to lookup the
   *                 attribute class.
   * @return The attribute class associated with the specified attribute code.
   */
  public String getAttributeClassForAttributeCode(String attrCode) {
    this.assertNotShutdown();
    return this.attrCodeToAttrClassMap.get(attrCode);
  }

  /**
   * Uses the {@linkplain #getBaseUrl() base URL} to build a link with the
   * specified path.
   *
   * @param path The path to use for building the path.
   * @return The URL link for the path with the base URL.
   */
  public String makeLink(String path) {
    this.assertNotShutdown();
    String sep = "";
    if (path.startsWith("/")) path = path.substring(1);
    if (!this.baseUrl.endsWith("/")) sep = "/";
    return this.baseUrl + sep + path;
  }

  /**
   * Utility method to ensure a command line argument with the specified index
   * exists and if not then throws an exception.
   *
   * @param args  The array of command line arguments.
   * @param index The index to check.
   * @throws IllegalArgumentException If the argument does not exist.
   */
  private static void ensureArgument(String[] args, int index) {
    if (index >= args.length) {
      System.err.println();

      System.err.println(
          "Missing expected argument following " + args[index - 1]);

      throw new IllegalArgumentException(
          "Missing expected argument following: " + args[index - 1]);
    }
    return;
  }

  /**
   *
   */
  private static String buildErrorMessage(String heading,
                                          Integer errorCode,
                                          String errorMessage) {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    if (heading != null) pw.println(heading);
    if (errorCode != null) {
      pw.println("ERROR CODE: " + errorCode);
      pw.println();
    }
    if (errorMessage != null) {
      pw.println(errorMessage);
      pw.println();
    }
    pw.flush();
    return sw.toString();
  }

  /**
   *
   */
  private static void exitOnError(Throwable t) {
    System.err.println(t.getMessage());
    System.exit(1);
  }

  /**
   * @param args
   * @return
   */
  private static Map<Option, ?> parseCommandLine(String[] args) {
    Map<Option, Object> result = new LinkedHashMap<>();
    for (int index = 0; index < args.length; index++) {
      switch (args[index]) {
        case "-help":
          if (args.length > 1) {
            System.err.println();

            System.err.println(
                "Help option should be only option when provided.");

            throw new IllegalArgumentException("Extra options with help");
          }
          result.put(Option.HELP, Boolean.TRUE);
          return result;

        case "-version":
          if (args.length > 1) {
            System.err.println();

            System.err.println(
                "Version option should be only option when provided.");

            throw new IllegalArgumentException("Extra options with version");
          }

          result.put(Option.VERSION, Boolean.TRUE);
          return result;

        case "-monitorFile":
          index++;
          ensureArgument(args, index);
          File f = new File(args[index]);
          FileMonitor fileMonitor = new FileMonitor(f);
          result.put(Option.MONITOR_FILE, fileMonitor);
          break;

        case "-httpPort":
          index++;
          ensureArgument(args, index);
          result.put(Option.HTTP_PORT, Integer.parseInt(args[index]));
          break;

        case "-bindAddr":
          index++;
          ensureArgument(args, index);
          String addrArg = args[index];
          InetAddress addr = null;
          try {
            if ("all".equals(addrArg)) {
              addr = InetAddress.getByName("0.0.0.0");
            } else if ("loopback".equals(addrArg)) {
              addr = InetAddress.getLoopbackAddress();
            } else {
              addr = InetAddress.getByName(addrArg);
            }
          } catch (RuntimeException e) {
            throw e;
          } catch (Exception e) {
            throw new IllegalArgumentException(e);
          }
          result.put(Option.BIND_ADDRESS, addr);
          break;

        case "-iniFile":
          index++;
          ensureArgument(args, index);
          File iniFile = new File(args[index]);
          if (!iniFile.exists()) {
            String msg = "Specified INI file does not exist: " + iniFile;

            System.err.println(msg);

            throw new IllegalArgumentException(msg);
          }
          result.put(Option.INI_FILE, iniFile);
          break;

        case "-verbose":
          result.put(Option.VERBOSE, Boolean.TRUE);
          break;

        case "-moduleName":
          index++;
          ensureArgument(args, index);
          String moduleName = args[index];
          result.put(Option.MODULE_NAME, moduleName);
          break;

        default:
          System.err.println();

          System.err.println("Unrecognized option: " + args[index]);

          throw new IllegalArgumentException(
              "Bad command line option: " + args[index]);
      }
    }
    return result;
  }

  /**
   * @return
   */
  private static String getVersionString() {
    // use G2Product API without "init()" for now
    G2Product productApi = new G2ProductJNI();
    return JAR_FILE_NAME + " version " + productApi.version();
  }

  /**
   * @return
   */
  private static String getUsageString() {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);

    pw.println();
    pw.println("java -jar " + JAR_FILE_NAME + " <options>");
    pw.println();
    pw.println("<options> includes: ");
    pw.println("   -help");
    pw.println("        Should be the first and only option if provided.");
    pw.println("        Causes this help messasge to be displayed.");
    pw.println("        NOTE: If this option is provided, the server will not start.");
    pw.println();
    pw.println("   -version");
    pw.println("        Should be the first and only option if provided.");
    pw.println("        Causes the version of the G2 Web Server to be displayed.");
    pw.println("        NOTE: If this option is provided, the server will not start.");
    pw.println();
    pw.println("   -httpPort [port-number]");
    pw.println("        Sets the port for HTTP communication.  Defaults to 2080.");
    pw.println("        Specify 0 for a randomly selected port number.");
    pw.println();
    pw.println("   -bindAddr [ip-address|loopback|all]");
    pw.println("        Sets the port for HTTP bind address communication.");
    pw.println("        Defaults to loopback.");
    pw.println();
    pw.println("   -moduleName [module-name]");
    pw.println("        The module name to initialize with.  Defaults to '"
                   + DEFAULT_MODULE_NAME + "'.");
    pw.println();
    pw.println("   -iniFile [ini-file-path]");
    pw.println("        The path to the Senzing INI file to with which to initialize.");
    pw.println();
    pw.println("   -verbose If specified then initialize in verbose mode.");
    pw.println();
    pw.println("   -monitorFile [filePath]");
    pw.println("        Specifies a file whose timestamp is monitored to determine");
    pw.println("        when to shutdown.");
    pw.println();
    pw.flush();
    sw.flush();

    return sw.toString();
  }

  /**
   * @param context
   * @param packageName
   * @param path
   * @param initOrder
   */
  private static void addJerseyServlet(ServletContextHandler context,
                                       String packageName,
                                       String path,
                                       int initOrder) {
    ServletHolder jerseyServlet = context.addServlet(
        org.glassfish.jersey.servlet.ServletContainer.class, path);

    jerseyServlet.setInitOrder(initOrder);
    System.out.println("PACKAGE NAME: " + packageName);

    jerseyServlet.setInitParameter(
        "jersey.config.server.provider.packages",
        packageName + ".services;"
            + packageName + ".providers;"
            + "org.codehaus.jackson.jaxrs;"
            + "org.glassfish.jersey.media.multipart");

    jerseyServlet.setInitParameter(
        "jersey.config.server.provider.classnames",
        "org.glassfish.jersey.media.multipart.MultiPartFeature");

    jerseyServlet.setInitParameter(
        "jersey.api.json.POJOMappingFeature", "true");
  }

  /**
   * @param context
   * @param path
   * @param viaHost
   * @param preserveHost
   * @param hostHeader
   * @param initOrder
   */
  private static void addProxyServlet(ServletContextHandler context,
                                      String path,
                                      String viaHost,
                                      boolean preserveHost,
                                      String hostHeader,
                                      int initOrder) {
    ServletHolder proxyServlet = context.addServlet(
        org.eclipse.jetty.proxy.ProxyServlet.class, path);

    proxyServlet.setInitOrder(initOrder);

    proxyServlet.setInitParameter("viaHost", viaHost);

    proxyServlet.setInitParameter("preserveHost", "" + preserveHost);

    if (hostHeader != null) {
      proxyServlet.setInitParameter("hostHeader", hostHeader);
    }
  }

  /**
   * @param args
   * @throws Exception
   */
  public static void main(String[] args)
      throws Exception {

    Map<Option, ?> options = null;
    try {
      options = parseCommandLine(args);
    } catch (Exception e) {
      System.err.println(SzApiServer.getUsageString());
      System.exit(1);
    }

    if (options.containsKey(Option.HELP)) {
      System.out.println(SzApiServer.getUsageString());
      System.exit(0);
    }
    if (options.containsKey(Option.VERSION)) {
      System.out.println(SzApiServer.getVersionString());
      System.exit(0);
    }
    if (!options.containsKey(Option.INI_FILE)) {
      System.err.println("The INI file must be specified.");
      System.err.println(SzApiServer.getUsageString());
      System.exit(1);
    }

    System.out.println("os.arch        = " + System.getProperty("os.arch"));
    System.out.println("os.name        = " + System.getProperty("os.name"));
    System.out.println("user.dir       = " + System.getProperty("user.dir"));
    System.out.println("user.home      = " + System.getProperty("user.home"));
    System.out.println("java.io.tmpdir = " + System.getProperty("java.io.tmpdir"));

    try {
      if (!SzApiServer.initialize(options)) {
        System.err.println("FAILED TO INITIALIZE");
        System.exit(0);
      }
    } catch (Exception e) {
      e.printStackTrace();
      exitOnError(e);
    }

    SzApiServer server = SzApiServer.getInstance();
    server.join();
  }

  /**
   * @param options
   * @return
   */
  private static boolean initialize(Map<Option, ?> options) {
    if (options.containsKey(Option.HELP)) {
      System.out.println(SzApiServer.getUsageString());
      return false;
    }
    if (options.containsKey(Option.VERSION)) {
      System.out.println(SzApiServer.getVersionString());
      return false;
    }
    if (!options.containsKey(Option.INI_FILE)) {
      throw new IllegalArgumentException("The INI file must be specified.");
    }

    synchronized (SzApiServer.class) {
      if (SzApiServer.INSTANCE != null) {
        throw new IllegalStateException("Server already initialized!");
      }

      try {
        SzApiServer.INSTANCE = new SzApiServer(options);

      } catch (RuntimeException e) {
        e.printStackTrace();
        throw e;

      } catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }
    return true;
  }

  /**
   * Constructs with the specified parameters.
   *
   * @param httpPort The HTTP port to bind to.  Use zero to bind to a random
   *                 port and <tt>null</tt> to bind to the default port.
   *
   * @param bindAddress The {@link InetAddress} for the address to bind to.
   *                    If <tt>null</tt> then the loopback address is used.
   *
   * @param moduleName The module name to bind to.  If <tt>null</tt> then
   *                   the {@link #DEFAULT_MODULE_NAME} is used.
   *
   * @param iniFile The non-null {@link File} with which to initialize.
   *
   * @param verbose Whether or not to initialize as verbose or not.  If
   *                <tt>null</tt> then the default setting is invoked.
   *
   * @throws Exception If a failure occurs.
   */
  public SzApiServer(Integer      httpPort,
                     InetAddress  bindAddress,
                     String       moduleName,
                     File         iniFile,
                     Boolean      verbose)
    throws Exception
  {
    this(null, httpPort, bindAddress, moduleName, iniFile, verbose);
  }

  /**
   * Constructs with the specified parameters.
   *
   * @param accessToken The {@link AccessToken} for later accessing privileged
   *                    functions.
   *
   * @param httpPort The HTTP port to bind to.  Use zero to bind to a random
   *                 port and <tt>null</tt> to bind to the default port.
   *
   * @param bindAddress The {@link InetAddress} for the address to bind to.
   *                    If <tt>null</tt> then the loopback address is used.
   *
   * @param moduleName The module name to bind to.  If <tt>null</tt> then
   *                   the {@link #DEFAULT_MODULE_NAME} is used.
   *
   * @param iniFile The non-null {@link File} with which to initialize.
   *
   * @param verbose Whether or not to initialize as verbose or not.  If
   *                <tt>null</tt> then the default setting is invoked.
   *
   * @throws Exception If a failure occurs.
   */
  public SzApiServer(AccessToken  accessToken,
                     Integer      httpPort,
                     InetAddress  bindAddress,
                     String       moduleName,
                     File         iniFile,
                     Boolean      verbose)
    throws Exception
  {
    this(accessToken,
         buildOptionsMap(httpPort, bindAddress, moduleName, iniFile, verbose));
  }

  /**
   * Internal method to build an options map.
   */
  private static Map<Option, ?> buildOptionsMap(Integer      httpPort,
                                                InetAddress  bindAddress,
                                                String       moduleName,
                                                File         iniFile,
                                                Boolean      verbose)
  {
    Map<Option, Object> map = new HashMap<>();
    if (httpPort != null)     map.put(Option.HTTP_PORT, httpPort);
    if (bindAddress != null)  map.put(Option.BIND_ADDRESS, bindAddress);
    if (moduleName != null)   map.put(Option.MODULE_NAME, moduleName);
    if (iniFile != null)      map.put(Option.INI_FILE, iniFile);
    if (verbose != null)      map.put(Option.VERBOSE, verbose);
    return map;
  }

  /**
   * Constructs with the specified parameters.
   *
   * @param options The options with which to initialize.
   * @throws Exception If a failure occurs.
   */
  private SzApiServer(Map<Option, ?> options)
      throws Exception {

    this(null, options);
  }

  /**
   * Constructs with the specified parameters.
   *
   * @param token The {@link AccessToken} for later accessing privileged
   *              functions.
   *
   * @param options The options with which to initialize.
   *
   * @throws Exception If a failure occurs.
   */
  private SzApiServer(AccessToken token, Map<Option, ?> options)
      throws Exception
    {
    this.accessToken = token;

    this.httpPort = 2080;
    if (options.containsKey(Option.HTTP_PORT)) {
      this.httpPort = (Integer) options.get(Option.HTTP_PORT);
    }
    this.ipAddr = InetAddress.getLoopbackAddress();
    if (options.containsKey(Option.BIND_ADDRESS)) {
      this.ipAddr = (InetAddress) options.get(Option.BIND_ADDRESS);
    }
    this.moduleName = DEFAULT_MODULE_NAME;
    if (options.containsKey(Option.MODULE_NAME)) {
      this.moduleName = (String) options.get(Option.MODULE_NAME);
    }
    this.verbose = false;
    if (options.containsKey(Option.VERBOSE)) {
      this.verbose = (Boolean) options.get(Option.VERBOSE);
    }
    this.iniFile = (File) options.get(Option.INI_FILE);
    String ini = iniFile.getCanonicalPath();

    this.baseUrl = "http://" + ipAddr + ":" + httpPort + "/";
    this.g2Product = new G2ProductJNI();
    int initResult = this.g2Product.init(moduleName, ini, verbose);
    if (initResult < 0) {
      throw new RuntimeException(buildErrorMessage(
          "Failed to initialize G2Product API.",
          this.g2Product.getLastExceptionCode(),
          this.g2Product.getLastException()));
    }

    this.g2Config = new G2ConfigJNI();
    initResult = this.g2Config.init(moduleName, ini, verbose);
    if (initResult < 0) {
      throw new RuntimeException(buildErrorMessage(
          "Failed to initialize G2Config API.",
          this.g2Config.getLastExceptionCode(),
          this.g2Config.getLastException()));
    }

    this.g2Engine = new G2JNI();
    this.g2Engine.init(moduleName, ini, verbose);
    if (initResult < 0) {
      throw new RuntimeException(buildErrorMessage(
          "Failed to initialize G2Engine API",
          g2Engine.getLastExceptionCode(),
          g2Engine.getLastException()));
    }

    this.workerThreadPool
        = new WorkerThreadPool(this.getClass().getName(), 8);

    StringBuffer sb = new StringBuffer();
    this.g2Engine.exportConfig(sb);
    JsonObject config = JsonUtils.parseJsonObject(sb.toString());

    Set<String>         dataSourceSet = new LinkedHashSet<>();
    Map<String,String>  ftypeCodeMap  = new LinkedHashMap<>();
    Map<String,String>  attrCodeMap   = new LinkedHashMap<>();

    this.attrCodeToAttrClassMap   = new LinkedHashMap<>();

    this.evaluateConfig(config, dataSourceSet, ftypeCodeMap, attrCodeMap);

    this.dataSources              = Collections.unmodifiableSet(dataSourceSet);
    this.featureToAttrClassMap = Collections.unmodifiableMap(ftypeCodeMap);
    this.attrCodeToAttrClassMap   = Collections.unmodifiableMap(attrCodeMap);

    // setup a servlet context handler
    ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
    context.setContextPath("/");
    FilterHolder filterHolder = context.addFilter(CrossOriginFilter.class, "/*", EnumSet.of(DispatcherType.REQUEST));
    filterHolder.setInitParameter("allowedOrigins", "http://localhost:*");

    // find how this class was loaded so we can find the path to the static content
    ClassLoader loader = SzApiServer.class.getClassLoader();
    String url = JAR_BASE_URL + "com/senzing/webapps/";
    context.setResourceBase(url);

    RewriteHandler rewriteHandler = new RewriteHandler();
    rewriteHandler.setRewritePathInfo(true);
    rewriteHandler.setRewriteRequestURI(true);
    RewriteRegexRule rewiteRule = null;
    TerminatingRegexRule terminatingRule = null;
    terminatingRule = new TerminatingRegexRule("/sz/api/(.*)");
    rewriteHandler.addRule(terminatingRule);
    rewiteRule = new RewriteRegexRule();
    rewiteRule.setRegex("/sz/[^\\.]+$");
    rewiteRule.setReplacement("/sz/");
    rewriteHandler.addRule(rewiteRule);

    rewriteHandler.setHandler(context);

    // create our server (TODO: add connectors for HTTP + HTTPS)
    this.jettyServer = new Server(new InetSocketAddress(ipAddr, httpPort));
    this.fileMonitor = null;
    if (options.containsKey(Option.MONITOR_FILE)) {
      this.fileMonitor = (FileMonitor) options.get(Option.MONITOR_FILE);
    }
    this.jettyServer.setHandler(rewriteHandler);
    LifeCycleListener lifeCycleListener
        = new LifeCycleListener(this.jettyServer, httpPort, ipAddr, this.fileMonitor);
    this.jettyServer.addLifeCycleListener(lifeCycleListener);
    int initOrder = 0;
    String packageName = SzApiServer.class.getPackage().getName();
    String apiPath = "/*";

    //addProxyServlet(context, "/www/*", "https://www.senzing.com", false,
    //                "www.senzing.com", initOrder++);

    addJerseyServlet(context, packageName, apiPath, initOrder++);

    ServletHolder rootHolder = new ServletHolder("default", DefaultServlet.class);
    rootHolder.setInitParameter("dirAllowed", "false");
    context.addServlet(rootHolder, "/");

    // System.out.println("INITIALIZING SENZING ENGINE....");
    try {
      this.jettyServer.start();
      int actualPort = this.httpPort
          = ((ServerConnector) (this.jettyServer.getConnectors()[0])).getLocalPort();

      if (options.containsKey(Option.MONITOR_FILE)) {
        this.fileMonitor.initialize(actualPort);
        this.fileMonitor.start();
      }
    } catch (Exception e) {
      this.shutdown();
      throw e;
    }

    // create a thread to monitor for server termination
    Thread thread = new Thread(() -> {
      try {
        if (this.fileMonitor != null) {
          System.out.println("********************************************** ");
          System.out.println("    MONITORING FILE FOR SHUTDOWN SIGNAL");
          System.out.println("********************************************** ");
          fileMonitor.join();
          System.out.println("********************************************** ");
          System.out.println("    RECEIVED SHUTDOWN SIGNAL");
          System.out.println("********************************************** ");
          try {
            context.stop();
            this.jettyServer.stop();
            this.jettyServer.join();

          } catch (Exception e) {
            e.printStackTrace();
          }

        } else {
          this.jettyServer.join();
        }
      } catch (Exception e) {
        throw new RuntimeException(e);

      } finally {
        this.shutdown();
      }
    });
    thread.start();
  }

  /**
   *
   */
  private void assertNotShutdown() {
    if (this.isShutdown()) {
      throw new IllegalStateException(
          "This API server instance is already shutdown.");
    }
  }

  /**
   * Checks if this instance has been shutdown.
   *
   * @return <tt>true</tt> if this instance has been shutdown, otherwise
   *         <tt>false</tt>.
   */
  public boolean isShutdown() {
    synchronized (this.joinMonitor) {
      return this.completed;
    }
  }

  /**
   * Shuts down this instance if the specified {@link AccessToken} matches
   * the token with which this instance was constructed.
   *
   * @param token The {@link AccessToken} for privileged access.
   *
   * @throws UnsupportedOperationException If this instance was not constructed
   *                                       with an {@link AccessToken}.
   *
   * @throws IllegalArgumentException If the specified {@link AccessToken} does
   *                                  not match the one with which this instance
   *                                  was constructed.
   */
  public void shutdown(AccessToken token) {
    if (this.accessToken == null) {
      throw new UnsupportedOperationException(
          "This operation is not supported for this instance.");
    }
    if (this.accessToken != token) {
      throw new IllegalArgumentException(
          "The specified AccessToken does not provide access to shutdown.");
    }
    this.shutdown();
  }

  /**
   * Internal method for handling cleanup on shutdown.
   */
  private void shutdown() {
    if (this.jettyServer != null) {
      synchronized (this.jettyServer) {
        try {
          this.jettyServer.destroy();
        } catch (Exception e) {
          // ignore
        }
      }
    }
    if (this.fileMonitor != null) {
      this.fileMonitor.signalShutdown();
    }

    // uninitialize
    synchronized (SzApiServer.class) {
      if (INSTANCE == this) {
        INSTANCE = null;
      }
    }
    synchronized (this.joinMonitor) {
      this.completed = true;
      this.joinMonitor.notifyAll();
    }
  }

  /**
   * Executes the specified task within a thread pool managed by the
   * {@link SzApiServer} instance.
   *
   * @param task The task to execute.
   *
   * @return The result from the specified {@link Task}.
   *
   * @throws Exception If the task has a failure.
   */
  public <T, E extends Exception> T executeInThread(Task<T, E> task)
    throws E
  {
    return this.workerThreadPool.execute(task);
  }

  /**
   * Waits for the API server to complete.
   *
   */
  public void join() {
    synchronized (this.joinMonitor) {
      while (!this.completed) {
        try {
          this.joinMonitor.wait(20000L);
        } catch (InterruptedException ignore) {
          // ignore the exception
        }
      }
    }
  }
}
