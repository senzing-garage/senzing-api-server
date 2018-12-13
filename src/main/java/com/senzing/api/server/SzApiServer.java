package com.senzing.api.server;

import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.LinkedHashMap;

import com.senzing.g2.engine.*;
import org.eclipse.jetty.server.ServerConnector;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.rewrite.handler.RewriteHandler;
import org.eclipse.jetty.rewrite.handler.TerminatingRegexRule;
import org.eclipse.jetty.rewrite.handler.RewriteRegexRule;

/**
 *
 *
 */
public class SzApiServer
{
  private static final String JAR_FILE_NAME;

  private static final String JAR_BASE_URL;

  private static final String PATH_TO_JAR;

  private static final String DEFAULT_MODULE_NAME = "ApiServer";

  private static String base_url;

  /**
   *
   */
  private static G2Config g2_config;

  /**
   *
   */
  private static G2Product g2_product;

  /**
   *
   */
  private static G2Engine g2_engine;

  private static enum Option {
      HELP,
      VERSION,
      HTTP_PORT,
      BIND_ADDRESS,
      MODULE_NAME,
      INI_FILE,
      VERBOSE,
      MONITOR_FILE;
    };

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
    JAR_FILE_NAME = url.substring(index+1);
    url = url.substring(0, index);
    index = url.indexOf("/");
    PATH_TO_JAR = url.substring(index);
  }

  public static String getBaseUrl() {
      return base_url;
    }

  public static G2Product getProductApi() {
    return g2_product;
  }

  public static G2Config getConfigApi() {
    return g2_config;
  }

  public static G2Engine getEngineApi() {
    return g2_engine;
  }

  public static String makeLink(String path) {
    String sep = "";
    if (path.startsWith("/")) path = path.substring(1);
    if (!base_url.endsWith("/")) sep = "/";
    return base_url + sep + path;
  }

  private static void ensureArgument(String[] args, int index) {
    if (index >= args.length) {
      System.err.println();

      System.err.println(
        "Missing expected argument following " + args[index-1]);

      throw new IllegalArgumentException(
          "Missing expected argument following: " + args[index-1]);
    }
    return;
  }

  private static void exitOnError(int errorCode, String errorMessage) {
    System.err.println("ERROR CODE: " + errorCode);
    System.err.println();
    System.err.println(errorMessage);
    System.err.println();
    System.exit(1);
  }

  private static Map<Option,?> parseCommandLine(String[] args) {
    Map<Option,Object> result = new LinkedHashMap<Option,Object>();

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

    private static String getVersionString() {
      return JAR_FILE_NAME + " version 1.0-SNAPSHOT-1";
    }

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

    private static void addJerseyServlet(ServletContextHandler  context,
                                         String                 packageName,
                                         String                 path,
                                         int                    initOrder) {
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

    private static void addProxyServlet(ServletContextHandler context,
                                        String                path,
                                        String                viaHost,
                                        boolean               preserveHost,
                                        String                hostHeader,
                                        int                   initOrder)
    {
      ServletHolder proxyServlet = context.addServlet(
        org.eclipse.jetty.proxy.ProxyServlet.class, path);

      proxyServlet.setInitOrder(initOrder);

      proxyServlet.setInitParameter("viaHost", viaHost);

      proxyServlet.setInitParameter("preserveHost", "" + preserveHost);

      if (hostHeader != null) {
        proxyServlet.setInitParameter("hostHeader", hostHeader);
      }
    }


    public static void main( String[] args )
      throws Exception {

      Map<Option,?> options = null;
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
      Integer httpPort = 2080;
      if (options.containsKey(Option.HTTP_PORT)) {
        httpPort = (Integer) options.get(Option.HTTP_PORT);
      }
      InetAddress ipAddr = InetAddress.getLoopbackAddress();
      if (options.containsKey(Option.BIND_ADDRESS)) {
        ipAddr = (InetAddress) options.get(Option.BIND_ADDRESS);
      }
      String moduleName = DEFAULT_MODULE_NAME;
      if (options.containsKey(Option.MODULE_NAME)) {
        moduleName = (String) options.get(Option.MODULE_NAME);
      }
      boolean verbose = false;
      if (options.containsKey(Option.VERBOSE)) {
        verbose = (Boolean) options.get(Option.VERBOSE);
      }
      File iniFile = (File) options.get(Option.INI_FILE);
      String ini = iniFile.getCanonicalPath();

      SzApiServer.base_url = "http://" + ipAddr + ":" + httpPort + "/";
      G2Product g2Product = SzApiServer.g2_product = new G2ProductJNI();
      int initResult = g2Product.init(moduleName, ini, verbose);
      if (initResult < 0) {
        System.err.println("Failed to initialize G2Product API.");
        exitOnError(g2Product.getLastExceptionCode(),
                    g2Product.getLastException());
      }

      G2Config g2Config = SzApiServer.g2_config  = new G2ConfigJNI();
      initResult = g2Config.init(moduleName, ini, verbose);
      if (initResult < 0) {
        System.err.println("Failed to initialize G2Config API.");
        exitOnError(g2Config.getLastExceptionCode(),
                    g2Config.getLastException());
      }

      G2Engine g2Engine = SzApiServer.g2_engine = new G2JNI();
      initResult = g2Engine.init(moduleName, ini, verbose);
      if (initResult < 0) {
        System.err.println("Failed to initialize G2Engine API");
        exitOnError(g2Engine.getLastExceptionCode(),
                    g2Engine.getLastException());
      }

      // setup a servlet context handler
      ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
      context.setContextPath("/");

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
      Server jettyServer = new Server(new InetSocketAddress(ipAddr, httpPort));
      FileMonitor fileMonitor = null;
      if (options.containsKey(Option.MONITOR_FILE)) {
        fileMonitor = (FileMonitor) options.get(Option.MONITOR_FILE);
      }
      jettyServer.setHandler(rewriteHandler);
      LifeCycleListener lifeCycleListener
          = new LifeCycleListener(jettyServer, httpPort, ipAddr, fileMonitor);
      jettyServer.addLifeCycleListener(lifeCycleListener);
      int initOrder = 0;
      String packageName = SzApiServer.class.getPackage().getName();
      String apiPath = "/*";

      //addProxyServlet(context, "/www/*", "https://www.senzing.com", false,
      //                "www.senzing.com", initOrder++);

      addJerseyServlet(context, packageName, apiPath, initOrder++);

      ServletHolder rootHolder = new ServletHolder("default", DefaultServlet.class);
      rootHolder.setInitParameter("dirAllowed", "false");
      context.addServlet(rootHolder, "/");

      System.out.println("os.arch        = " + System.getProperty("os.arch"));
      System.out.println("os.name        = " + System.getProperty("os.name"));
      System.out.println("user.dir       = " + System.getProperty("user.dir"));
      System.out.println("user.home      = " + System.getProperty("user.home"));
      System.out.println("java.io.tmpdir = " + System.getProperty("java.io.tmpdir"));

      // System.out.println("INITIALIZING SENZING ENGINE....");

      try {
        jettyServer.start();
        int actualPort = ((ServerConnector)(jettyServer.getConnectors()[0])).getLocalPort();

        if (options.containsKey(Option.MONITOR_FILE)) {
          fileMonitor.initialize(actualPort);
          fileMonitor.start();
        }

        if (fileMonitor != null) {
          System.out.println("********************************************** ");
          System.out.println("    MONITORING FILE FOR SHUTDOWN SIGNAL");
          System.out.println("********************************************** ");
          fileMonitor.join();
          System.out.println("********************************************** ");
          System.out.println("    RECEIVED SHUTDOWN SIGNAL");
          System.out.println("********************************************** ");
          try {
            context.stop();
            jettyServer.stop();
            jettyServer.join();

          } catch (Exception e) {
            e.printStackTrace();
          }

        } else {
          jettyServer.join();
        }

      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        jettyServer.destroy();
        if (fileMonitor != null) {
          System.exit(0);
        }
      }
    }

}
