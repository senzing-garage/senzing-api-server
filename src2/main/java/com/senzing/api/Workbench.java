package com.senzing.api;

import java.io.*;
import java.net.URLDecoder;
import java.nio.file.Files;
import java.util.Properties;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.Collections;
import java.time.LocalDateTime;
import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonValue;

import com.senzing.io.IOUtilities;
import com.senzing.util.JsonUtils;
import com.senzing.api.server.AppLifeCycle;
import com.senzing.util.OperatingSystemFamily;
import com.senzing.api.util.StreamLogger;
import com.senzing.api.util.SubProcessDisposer;
import com.senzing.api.util.WorkbenchLogging;

import static com.senzing.util.Closer.*;
import static com.senzing.io.IOUtilities.*;
import static com.senzing.util.OperatingSystemFamily.*;
import static java.nio.file.StandardCopyOption.*;

public class Workbench implements AppLifeCycle {
  public static final File USER_DATA_DIR;

  public static final File USER_TEMP_DIR;

  public static final File VERSION_FILE;

  public static final String JAR_PATH;

  public static final String LIB_PATH;

  public static final String JAVA_LIBRARY_PATH;

  public static final String BASE_JRE_PATH;

  public static final String JRE_PATH;

  public static final String BIN_PATH;

  public static final String DATA_PATH;

  public static final File DATA_DIR;

  public static final File LICENSE_DIR;

  public static final File LICENSE_FILE;

  public static final String PYTHON_PATH;

  public static final String TEMPLATE_CONFIG_PATH;

  public static final String TEMPLATE_DB_PATH;

  public static final String TEMPLATE_REPORT_DB_PATH;

  public static final String SENZING_ROOT;

  public static final String ELASTIC_SEARCH_ROOT;

  public static final String ELASTIC_SEARCH_SCRIPT;

  public static final String ELASTIC_SEARCH_CLUSTER_NAME
    = "senzing-g2-api";

  public static final String PATH_SEP;

  public static final String FILE_SEP;

  public static final String ELASTIC_SEARCH_HOST = "127.0.0.1";

  public static final int ELASTIC_SEARCH_PORT = 9200;

  public static final int ELASTIC_SEARCH_CLIENT_PORT = 9300;

  private static final PrintWriter LOG_WRITER;

  private static final Map<String,String> ATTR_CODE_TO_ATTR_CLASS;

  private static final Map<String,String> FTYPE_CODE_TO_ATTR_CLASS;

  public static final Map<String,Map<String,String>> VARIANT_TOKEN_MAPS;

  public static final OperatingSystemFamily OS_FAMILY;

  private static final InheritableThreadLocal<PrintWriter> THREAD_LOCAL_LOG
    = new InheritableThreadLocal<PrintWriter>();

  public interface ShutdownHook {
    void execute();
  }

  private static List<ShutdownHook> SHUTDOWN_HOOKS = new LinkedList<>();

  static {
    Map<String,String> attrCode2AttrClass = new LinkedHashMap<String,String>();
    Map<String,String> ftypeCode2AttrClass = new LinkedHashMap<String,String>();
    Map<String,Map<String,String>> tokenMaps = new LinkedHashMap<>();

    try {
      Class.forName("org.sqlite.JDBC");

      final String baseDir = System.getProperty("user.home");
      final String osName = System.getProperty("os.name");
      final String linuxPath = ".senzing/api";
      final String windowsPath = "AppData/Local/Senzing/Workbench";
      final String macPath = "Library/Application Support/com.senzing.api";

      boolean windows = false;
      boolean macOS = false;
      OperatingSystemFamily osFamily = null;

      String lowerOSName = osName.toLowerCase().trim();
      if (lowerOSName.startsWith("windows")) {
        osFamily = WINDOWS;
        windows = true;
      } else if (lowerOSName.startsWith("mac")
                 || lowerOSName.indexOf("darwin") >= 0)
      {
        osFamily = MAC_OS;
        macOS = true;
      }

      PATH_SEP    = System.getProperty("path.separator");
      FILE_SEP    = System.getProperty("file.separator");
      OS_FAMILY   = osFamily;

      Class<Workbench> c = Workbench.class;

      String simpleName = c.getSimpleName();

      String resourceName = simpleName + ".class";

      String url = c.getResource(resourceName).toString();
      url = URLDecoder.decode(url, "UTF-8");

      System.out.println("URL : " + url);
      String jarPath = url.replaceAll(
        "jar:file:(.*\\.jar)\\!/.*\\.class", "$1");
      if (windows && jarPath.startsWith("/")) {
        jarPath = jarPath.replaceAll("[/]+([^/].*)", "$1");
      }
      System.out.println("JAR PATH: " + jarPath);
      if (windows && jarPath.startsWith("/")) {
        jarPath = jarPath.substring(1);
      }

      int lastSlash = jarPath.lastIndexOf("/");
      String baseJarPath = jarPath.substring(0, lastSlash+1);
      File jarDir = new File(baseJarPath);
      File propsFile = new File(jarDir, "api.properties");

      if (propsFile.exists()) {
        System.out.println("WORKBENCH PROPERTIES FILE FOUND: " + propsFile);
        try (FileInputStream fis = new FileInputStream(propsFile);
             InputStreamReader isr = new InputStreamReader(fis, "UTF-8")) {

          Properties props = new Properties();
          props.load(isr);

          for (Map.Entry entry : props.entrySet()) {
            String key = "" + entry.getKey();
            String val = "" + entry.getValue();
            System.setProperty(key, val);
            System.out.println("OVERRIDING " + key + " = " + val);
          }

        } catch (Exception e) {
          System.err.println("****** FAILED TO LOAD PROPERTIES FILE: " + propsFile);
          e.printStackTrace();
        }
      } else {
        System.out.println("NO WORKBENCH PROPERTIES FILE FOUND.  USING DEFAULTS");
      }

      String userSubdir = null;
      if (osName.toLowerCase().trim().startsWith("mac")) {
        userSubdir = macPath;
      } else if (osName.toLowerCase().trim().startsWith("windows")) {
        userSubdir = windowsPath;
      } else {
        userSubdir = linuxPath;
      }

      String overrideDir = System.getProperty("USER_DATA_DIR");
      System.out.println("OVERRIDE DIR: " + overrideDir);
      USER_DATA_DIR = (overrideDir != null && overrideDir.length() > 0)
                      ? (new File(overrideDir)).getCanonicalFile()
                      : (new File(baseDir+FILE_SEP+userSubdir)).getCanonicalFile();

      if (!USER_DATA_DIR.exists()) {
        boolean created = USER_DATA_DIR.mkdirs();
        if (!created) {
          throw new ExceptionInInitializerError(
            "Failed to create user directory: " + USER_DATA_DIR);
        }
      }

      System.out.println("USER_DATA_DIR = " + USER_DATA_DIR);

      LICENSE_DIR = new File(USER_DATA_DIR, "license-keys");
      if (! LICENSE_DIR.exists()) {
        boolean created = LICENSE_DIR.mkdirs();
        if (!created) {
          throw new ExceptionInInitializerError(
              "Failed to create license directory: " + LICENSE_DIR);
        }
      }

      LICENSE_FILE = new File(LICENSE_DIR, "g2.lic");

      String libPath = null;
      System.out.println("JAR URL: " + url);
      if (url.startsWith("jar:file:") && url.indexOf("/g2/api/") >= 0)
      {
        // assume default install path
        libPath = url.replaceAll(
          "jar:file:(.*/g2)/workbench.*", "$1/lib");

        if (windows && libPath.startsWith("/")) {
          libPath = libPath.replaceAll("[/]+([^/].+)", "$1");
        }

        File libFile = new File(libPath);
        if (!libFile.exists()) {
          libPath = null;
        }
      } else if (macOS && url.startsWith("jar:file:")
                 && url.indexOf(".app/Contents/Resources/app/dist/") >= 0)
      {
        System.out.println("************** GOT HERE: " + url);
        // assume the default install path for mac
        libPath = url.replaceAll(
            "jar:file:(.*/Contents/Resources/app)/dist/.*", "$1/g2/lib");
        System.out.println("************** TRYING LIB PATH: " + libPath);
        File libFile = new File(libPath);
        if (!libFile.exists()) {
          libPath = null;
        }
      }

      if (libPath == null) {
        System.out.println("******** WARNING: LIB PATH NOT FOUND... USING DEFAULT.");
        if (windows) {
          File installerPath = new File("C:\\Program Files\\Senzing\\g2\\lib");
          File unzipPath = new File("C:\\senzing\\g2\\lib");
          if (installerPath.exists() && !unzipPath.exists()) {
            libPath = "C:/Program Files/Senzing/g2/lib";
          } else if (unzipPath.exists() && !installerPath.exists()) {
            libPath = "C:/senzing/g2/lib";
          } else if (installerPath.exists() && unzipPath.exists()) {
            System.err.println("******* FOUND MULTIPLE VERSIONS OF SENZING:");
            System.err.println("C:\\Program Files\\Senzing\\g2");
            System.err.println("C:\\senzing\\g2");

            File installerDLL = new File(installerPath, "G2.dll");
            File unzipDLL = new File(unzipPath, "G2.dll");
            if (installerDLL.lastModified() > unzipDLL.lastModified()) {
              libPath = "C:/Program Files/Senzing/g2/lib";
              System.err.println("---> C:\\Program File\\Senzing\\g2 IS NEWER");
              System.err.println("******* USING: C:\\Program Files\\Senzing\\g2");
            } else if (unzipDLL.lastModified() > installerDLL.lastModified()) {
              libPath = "C:/senzing/g2/lib";
              System.err.println("---> C:\\\\senzing\\g2 IS NEWER");
              System.err.println("******* USING: C:\\senzing\\g2");
            } else {
              libPath = "C:/senzing/g2/lib";
              System.err.println("---> BOTH APPEAR TO BE THE SAME");
              System.err.println("******* USING: C:\\senzing\\g2");
            }
          } else {
            System.err.println("********** FAILED TO FIND SENZING INSTALLATION");
            System.exit(1);
          }

        } else if (macOS) {
          File appDir = new File("/Applications/Senzing.app");
          if (!appDir.exists()) {
            appDir = new File("/Applications/Senzing Workbench.app");
          }
          if (appDir.exists()) {
            File subDir = new File(appDir + "/Contents/Resources/app/g2/lib");
            if (subDir.exists()) {
              libPath = subDir.getCanonicalPath();
            }
          }
          if (libPath == null) {
            libPath = baseDir + "/senzing/g2/lib";
          }
        } else {
          libPath = "/opt/senzing/g2/lib";
        }
        File libFile = new File(libPath);
        if (!libFile.exists()) {
          System.err.println("WARNING: Cannot find G2 library location.");
        } else {
          System.err.println("DEFAULT LIB PATH: " + libPath);
        }
      }
      String senzingRoot = libPath.replaceAll("(.*)/g2/lib", "$1");
      String binPath = senzingRoot + "/g2/bin";
      String pythonPath = senzingRoot + "/g2/python";
      String dataPath = senzingRoot + "/g2/data";
      String templateConfigPath = dataPath + "/g2config.json";
      String templateDBPath = dataPath + "/G2C.db";
      String templateReportDBPath = dataPath + "/G2_REPORT.db";
      String elasticSearchRoot = senzingRoot + "/g2/elasticsearch";

      // determine the location of the "app dir"
      File appDir = null;
      if (windows) {
        File senzingDir = new File(senzingRoot);
        File g2Dir = new File(senzingRoot, "g2");
        File workbenchDir = new File(g2Dir, "api");
        File resourceDir = new File(workbenchDir, "resources");
        appDir = new File(resourceDir, "app");
      } else if (macOS) {
        appDir = new File(senzingRoot);
      }

      // find the version file
      String osType = null;
      if (windows) {
        osType = "Win64";
      } else if (macOS) {
        osType = "macOS";
      } else {
        osType = "Linux";
      }
      VERSION_FILE = new File(appDir, "Senzing-" + osType + "-Version.json");

      System.out.println("VERSION FILE LOCATED AT: " + VERSION_FILE);

      String jrePath = null;
      String baseJrePath = null;
      if (macOS && jarPath.indexOf("/Contents/Resources/app/dist") > 0) {
        baseJrePath = jarPath.replaceAll("(.*)/dist/.*\\.jar", "$1/jre/Contents/Home/bin");
        jrePath = baseJrePath + "/java";

        File jreFile = new File(jrePath);

        if (!jreFile.exists()) jrePath = null;

      } else if (jarPath.indexOf("/resources/app/dist/") > 0) {
        baseJrePath = jarPath.replaceAll("(.*)/dist/.*\\.jar", "$1/jre/bin");
        jrePath = baseJrePath + "/java";
        if (windows) jrePath = jrePath + "w.exe";

        File jreFile = new File(jrePath);

        if (!jreFile.exists()) jrePath = null;
      }
      if (jrePath == null) jrePath = (windows ? "java.exe" : "java");
      if (baseJrePath == null) baseJrePath = "";

      String logPath = USER_DATA_DIR + "/api.log";

      File logFile = new File(logPath);

      LOG_WRITER = new PrintWriter(new OutputStreamWriter(new FileOutputStream(logFile), "UTF-8"));

      JAR_PATH = windows ? jarPath.replaceAll("/", "\\\\") : jarPath;

      LIB_PATH = windows ? libPath.replaceAll("/", "\\\\") : libPath;

      BIN_PATH = windows ? binPath.replaceAll("/", "\\\\") : binPath;

      PYTHON_PATH = windows ? pythonPath.replaceAll("/", "\\\\") : pythonPath;

      BASE_JRE_PATH = windows ? baseJrePath.replaceAll("/", "\\\\") : baseJrePath;

      JRE_PATH = windows ? jrePath.replaceAll("/", "\\\\") : jrePath;

      DATA_PATH = windows ? dataPath.replaceAll("/", "\\\\") : dataPath;

      DATA_DIR = new File(DATA_PATH);

      TEMPLATE_CONFIG_PATH = windows ? templateConfigPath.replaceAll("/", "\\\\") : templateConfigPath;

      TEMPLATE_DB_PATH = windows ? templateDBPath.replaceAll("/", "\\\\") : templateDBPath;

      TEMPLATE_REPORT_DB_PATH = windows ? templateReportDBPath.replaceAll("/", "\\\\") : templateReportDBPath;

      SENZING_ROOT = windows ? senzingRoot.replaceAll("/", "\\\\") : senzingRoot;

      ELASTIC_SEARCH_ROOT = windows ? elasticSearchRoot.replaceAll("/", "\\\\") : elasticSearchRoot;

      ELASTIC_SEARCH_SCRIPT = "elasticsearch" + (windows ? ".bat" : "");

      String libraryPath = System.getProperty("java.library.path");
      if (libraryPath.indexOf(LIB_PATH) < 0) {
        if (macOS) {
          libraryPath = LIB_PATH + FILE_SEP + "macos" + PATH_SEP + libraryPath;
        }
        libraryPath = LIB_PATH + PATH_SEP + libraryPath;
        System.setProperty("java.library.path", libraryPath);
      }

      JAVA_LIBRARY_PATH = libraryPath;

      USER_TEMP_DIR = new File(USER_DATA_DIR, "tmp");

      if (!USER_TEMP_DIR.exists()) USER_TEMP_DIR.mkdirs();

      File dataDir = new File(DATA_PATH);

      // copy the base G2C.db entity repository
      File sourceDB = new File(DATA_DIR, "G2C.db");
      File targetDB = new File(USER_DATA_DIR, "G2C.db");
      try {
        if (IOUtilities.checkFilesDiffer(sourceDB, targetDB)) {
          File shmFile = new File(USER_DATA_DIR, "G2C.db-shm");
          File walFile = new File(USER_DATA_DIR, "G2C.db-wal");
          if (shmFile.exists()) shmFile.delete();
          if (walFile.exists()) walFile.delete();
          Files.copy(sourceDB.toPath(), targetDB.toPath(), REPLACE_EXISTING);
        }
      } catch (IOException e) {
        throw new ExceptionInInitializerError(e);
      }

      File configFile = new File(dataDir, "g2config.json");
      List<Object[]> defaultData = new LinkedList<Object[]>();
      try (FileInputStream fis = new FileInputStream(configFile);
           InputStreamReader isr = new InputStreamReader(fis, "UTF-8")) {

        JsonReader jsonReader = Json.createReader(isr);
        JsonObject jsonObject = jsonReader.readObject();
        JsonValue  jsonValue  = jsonObject.getValue("/G2_CONFIG/CFG_ATTR");
        JsonArray  jsonArray  = jsonValue.asJsonArray();

        for (JsonValue val : jsonArray) {
          JsonObject cfgAttr = val.asJsonObject();
          String attrCode   = cfgAttr.getString("ATTR_CODE").toUpperCase();
          String ftypeCode  = cfgAttr.getString("FTYPE_CODE").toUpperCase();
          String attrClass  = cfgAttr.getString("ATTR_CLASS").toUpperCase();

          String ac = attrCode2AttrClass.get(attrCode);
          if (ac != null && !ac.equals(attrClass)) {
            System.err.println(
              "*** WARNING : Multiple attribute classes for FTYPE_CODE: "
              + attrCode + " ( " + ac + " / " + attrClass + " )");
          } else {
            attrCode2AttrClass.put(attrCode, attrClass);
          }

          ac = ftypeCode2AttrClass.get(attrCode);
          if (ac != null && !ac.equals(attrClass)) {
            System.err.println(
              "*** WARNING : Multiple attribute classes for FTYPE_CODE: "
              + ftypeCode + " ( " + ac + " / " + attrClass + " )");
          } else {
            ftypeCode2AttrClass.put(ftypeCode, attrClass);
          }
        }
      }

      File variantFile = new File(dataDir, "cfgVariant.json");
      try (FileInputStream fis = new FileInputStream(variantFile);
           InputStreamReader isr = new InputStreamReader(fis, "UTF-8")) {

        JsonReader jsonReader = Json.createReader(isr);
        JsonObject jsonObject = jsonReader.readObject();
        JsonValue  jsonValue  = jsonObject.getValue("/TOKEN_LIBRARY/TOKEN");
        JsonArray  jsonArray  = jsonValue.asJsonArray();

        for (JsonValue val : jsonArray) {
          JsonObject mapping = val.asJsonObject();
          String tokenType  = JsonUtils.getString(mapping, "TOKEN_TYPE");
          String tokenValue = JsonUtils.getString(mapping, "TOKEN_VALUE");
          String stdValue   = JsonUtils.getString(mapping, "STD_VALUE");

          Map<String,String> tokenMap = tokenMaps.get(tokenType);
          if (tokenMap == null) {
            tokenMap = new LinkedHashMap<String,String>();
            tokenMaps.put(tokenType, tokenMap);
          }
          tokenMap.put(tokenValue, stdValue);
        }
      }
      Iterator<Map.Entry<String,Map<String,String>>> iter
        = tokenMaps.entrySet().iterator();
      while (iter.hasNext()) {
        Map.Entry<String,Map<String,String>> entry = iter.next();
        Map<String,String> map = entry.getValue();
        map = Collections.unmodifiableMap(map);
        entry.setValue(map);
      }

      WorkbenchLogging.claimControllerStatus();
      WorkbenchLogging.initialize(USER_DATA_DIR);

      // disable elastic search for now
      //startElasticSearch();

      cleanupDeletedProjects();

    } catch (Exception e) {
      throw new ExceptionInInitializerError(e);
    } finally {
      ATTR_CODE_TO_ATTR_CLASS
        = Collections.unmodifiableMap(attrCode2AttrClass);

      FTYPE_CODE_TO_ATTR_CLASS
        = Collections.unmodifiableMap(ftypeCode2AttrClass);

      VARIANT_TOKEN_MAPS = Collections.unmodifiableMap(tokenMaps);
    }
  }

  static {
    try {
      if (USER_TEMP_DIR.exists()) {
        // cleanup the temp mapdb files
        File[] tmpFiles = USER_TEMP_DIR.listFiles();
        for (File file : tmpFiles) {
          file.delete();
        }
      }
    } catch (Exception ignore) {
      ignore.printStackTrace();
      log(ignore);
    }
  }

  public void init() throws Exception {
    log("ENSURING SCHEMA EXIST...");
    WorkbenchSchema.createSchema();
    log("ENSURED SCHEMA EXISTS.");
  }

  public static File getProjectDirectory(long projectId) throws IOException {
    File projectDir = new File(USER_DATA_DIR, "project_" + projectId);
    // create the project directory if it does not exist
    createDirectoryIfMissing(projectDir);
    return projectDir.getCanonicalFile();
  }

  public static void registerShutdownHook(ShutdownHook hook) {
    synchronized (SHUTDOWN_HOOKS) {
      if (hook != null) SHUTDOWN_HOOKS.add(hook);
    }
  }

  public void shutdown() {
    synchronized (SHUTDOWN_HOOKS) {
      for (ShutdownHook hook : SHUTDOWN_HOOKS) {
        try {
          hook.execute();
        } catch (Exception e) {
          log("SHUTDOWN HOOK FAILED:");
          log(e);
        }
      }
    }
  }

  public static void openThreadLocalLog(File file) throws IOException {
    closeThreadLocalLog();
    THREAD_LOCAL_LOG.set(
      new PrintWriter(
        new OutputStreamWriter(
          new FileOutputStream(file))));
  }

  public static void closeThreadLocalLog() {
    PrintWriter pw = THREAD_LOCAL_LOG.get();
    close(pw);
    THREAD_LOCAL_LOG.set(null);
  }

  public static void log() {
    log("", true);
  }

  public static void log(String msg) {
    log(msg, true);
  }

  public static void log(String msg, boolean newline) {
    msg = LocalDateTime.now().toString()
          + " (" + Thread.currentThread().getName()
          + ") : " + msg;
    try {
      if (newline) {
        LOG_WRITER.println(msg);
        LOG_WRITER.flush();
        PrintWriter pw = THREAD_LOCAL_LOG.get();
        if (pw != null) {
          synchronized (pw) {
            pw.println(msg);
            pw.flush();
          }
        }
        System.out.println(msg);
      } else {
        LOG_WRITER.print(msg);
        LOG_WRITER.flush();
        PrintWriter pw = THREAD_LOCAL_LOG.get();
        if (pw != null) {
          synchronized (pw) {
            pw.print(msg);
            pw.flush();
          }
        }
        System.out.print(msg);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static void log(Throwable t) {
    try {
      String timestamp = LocalDateTime.now().toString();
      LOG_WRITER.println(timestamp + ": " + t.toString());
      t.printStackTrace(LOG_WRITER);
      LOG_WRITER.flush();
      PrintWriter pw = THREAD_LOCAL_LOG.get();
      if (pw != null) {
        pw.println(t.toString());
        t.printStackTrace(pw);
        pw.flush();
      }
      System.out.println(timestamp + ": " + t.toString());
      t.printStackTrace(System.out);

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static String getAttributeClassForFeatureTypeCode(String code) {
    if (code == null) return null;
    return FTYPE_CODE_TO_ATTR_CLASS.get(code.toUpperCase());
  }

  public static String getAttributeClassForAttributeTypeCode(String code) {
    if (code == null) return null;
    return ATTR_CODE_TO_ATTR_CLASS.get(code.toUpperCase());
  }

  public static Map<String,String> getVariantTokens(String tokenType) {
    return VARIANT_TOKEN_MAPS.get(tokenType);
  }

  private static void cleanupDeletedProjects() {
    try {
      File[] files = USER_DATA_DIR.listFiles(f -> {
        return (f.getName().startsWith("project_")
                && f.getName().endsWith(".del"));
      });
      if (files.length == 0) return;

      File indexFile = new File(USER_DATA_DIR, "project-index.txt");
      Set<String> existing = new HashSet<>();
      if (!indexFile.exists()) return;

      try (FileInputStream    fis = new FileInputStream(indexFile);
           InputStreamReader  isr = new InputStreamReader(fis, "UTF-8");
           BufferedReader     br  = new BufferedReader(isr))
      {
        for (String line = br.readLine(); line != null; line = br.readLine()) {
          if (line.startsWith("#")) continue;
          line = line.replaceAll("^(.*--> )(project_\\d+)$", "$2");
          existing.add(line);
        }
      }

      for (File file : files) {
        String name       = file.getName();
        String prefix     = name.substring(0, name.length()-4);
        if (existing.contains(prefix)) {
          file.delete();
          continue;
        }

        File projectDir = new File(USER_DATA_DIR, prefix);
        if (!projectDir.exists()) continue;

        boolean success = false;
        try {
          int count = recursiveDeleteDirectory(projectDir);
          if (count == 0 && !projectDir.exists()) success = true;
        } catch (Exception ignore) {
          ignore.printStackTrace();
        } finally {
          if (success) {
            file.delete();
          }
        }
      }

    } catch (IOException ignore) {
      log(ignore);
    }
  }
  private static void startElasticSearch() throws IOException {
    Process       elasticSearchProcess    = null;
    Integer       elasticSearchProcessId  = null;
    Thread        elasticSearchDisposer   = null;
    boolean       elasticSearchRunning    = false;
    StreamLogger  elasticOutLogger        = null;
    StreamLogger  elasticErrLogger        = null;

    File elasticSearchRoot = new File(ELASTIC_SEARCH_ROOT);
    File elasticSearchBin = new File(elasticSearchRoot, "bin");
    File elasticSearch = new File(elasticSearchBin, ELASTIC_SEARCH_SCRIPT);

    File elasticDataDir = new File(USER_DATA_DIR, "elastic-search-data");
    if (!elasticDataDir.exists()) {
      elasticDataDir.mkdir();
    }

    File elasticLogsDir = new File(USER_DATA_DIR, "elastic-search-logs");
    if (!elasticLogsDir.exists()) {
      elasticLogsDir.mkdir();
    }

    File elasticPIDFile = new File(USER_DATA_DIR, "elastic-search.pid");
    if (elasticPIDFile.exists()) {
      elasticPIDFile.delete();
    }

    Runtime runtime = Runtime.getRuntime();

    String[] cmdArray = new String[] {
      elasticSearch.toString(),
      "-p",
      elasticPIDFile.toString(),
      "-Epath.data=" + elasticDataDir.toString(),
      "-Epath.logs=" + elasticLogsDir.toString(),
      "-Ehttp.port=" + ELASTIC_SEARCH_PORT,
      "-Ecluster.name=" + ELASTIC_SEARCH_CLUSTER_NAME
    };

    StringBuilder sb = new StringBuilder();
    for (String token : cmdArray) {
      sb.append(token + " ");
    }
    log(sb.toString());

    Map<String,String> origEnv = System.getenv();
    List<String> envList = new ArrayList<String>(origEnv.size()+2);
    for (Map.Entry<String,String> entry : origEnv.entrySet()) {
      String envKey = entry.getKey();
      String envVal = entry.getValue();
      if (envKey.equalsIgnoreCase("PATH") && BASE_JRE_PATH.length() > 0) {
        envVal = BASE_JRE_PATH + PATH_SEP + envVal;
      }
      envList.add(envKey + "=" + envVal);
    }
    String[] env = envList.toArray(new String[envList.size()]);

    log("STARTING ELASTIC SEARCH SUB-PROCESS....");
    elasticSearchProcess = runtime.exec(cmdArray, env, elasticSearchBin);

    if (!elasticSearchProcess.isAlive()) {
      throw new IllegalStateException("Elastic search process exited with code: "
                                      + elasticSearchProcess.exitValue());
    }
    elasticSearchRunning = true;

    elasticOutLogger = new StreamLogger(elasticSearchProcess.getInputStream());

    elasticErrLogger = new StreamLogger(elasticSearchProcess.getErrorStream());

    long now = System.currentTimeMillis();
    long maxWait = now + 120000L;
    while (!elasticPIDFile.exists() && System.currentTimeMillis() < maxWait) {
      try {
        Thread.sleep(100L);
      } catch (InterruptedException ignore) {
        log(ignore);
      }
    }
    try (FileInputStream    fis = new FileInputStream(elasticPIDFile);
         InputStreamReader  isr = new InputStreamReader(fis);
         BufferedReader     br  = new BufferedReader(isr))
    {
      String line = br.readLine();
      elasticSearchProcessId = Integer.parseInt(line);
    }

    elasticSearchDisposer = new SubProcessDisposer(
      "ELASTIC-SEARCH",
      elasticSearchProcess,
      elasticSearchProcessId,
      false,
      elasticOutLogger,
      elasticErrLogger);

    runtime.addShutdownHook(elasticSearchDisposer);

    log("STARTED ELASTIC SEARCH SUB-PROCESS.");
  }

}
