package com.senzing.repomgr;

import com.senzing.io.IOUtilities;
import com.senzing.nativeapi.NativeApiFactory;
import com.senzing.cmdline.CommandLineUtilities;
import com.senzing.g2.engine.*;
import com.senzing.nativeapi.InstallLocations;
import com.senzing.io.RecordReader;
import com.senzing.util.JsonUtils;

import javax.json.*;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static com.senzing.util.LoggingUtilities.*;
import static com.senzing.util.OperatingSystemFamily.*;
import static java.nio.file.StandardCopyOption.*;
import static com.senzing.io.IOUtilities.*;
import static com.senzing.cmdline.CommandLineUtilities.*;
import static com.senzing.repomgr.RepositoryManagerOption.*;
import static com.senzing.io.RecordReader.Format.*;

public class RepositoryManager {
  private static final File INSTALL_DIR;

  private static final File CONFIG_DIR;

  private static final File RESOURCE_DIR;

  private static final File SUPPORT_DIR;

  private static final File TEMPLATES_DIR;

  private static final G2Engine ENGINE_API;

  private static final G2Config CONFIG_API;

  private static final G2ConfigMgr CONFIG_MGR_API;

  private static final Set<String> EXCLUDED_TEMPLATE_FILES;

  private static final ThreadLocal<String> THREAD_MODULE_NAME
      = new ThreadLocal<>();

  private static String baseInitializedWith = null;
  private static String engineInitializedWith = null;

  static {
    Set<String> set = new LinkedHashSet<>();
    set.add("G2Module.ini.template".toLowerCase());
    set.add("G2Project.ini.template".toLowerCase());
    set.add("G2C.db.template".toLowerCase());
    set.add("g2config.json.template".toLowerCase());
    EXCLUDED_TEMPLATE_FILES = Collections.unmodifiableSet(set);
  }

  static {
    InstallLocations locations = InstallLocations.findLocations();

    if (locations != null) {
      INSTALL_DIR   = locations.getInstallDirectory();
      CONFIG_DIR    = locations.getConfigDirectory();
      SUPPORT_DIR   = locations.getSupportDirectory();
      RESOURCE_DIR  = locations.getResourceDirectory();
      TEMPLATES_DIR = locations.getTemplatesDirectory();
    } else {
      INSTALL_DIR   = null;
      CONFIG_DIR    = null;
      SUPPORT_DIR   = null;
      RESOURCE_DIR  = null;
      TEMPLATES_DIR = null;
    }

    G2Engine    engineApi     = null;
    G2Config    configApi     = null;
    G2ConfigMgr configMgrApi  = null;
    try {
      engineApi     = NativeApiFactory.createEngineApi();
      configApi     = NativeApiFactory.createConfigApi();
      configMgrApi  = NativeApiFactory.createConfigMgrApi();

    } catch (Exception e) {
      File libPath = new File(INSTALL_DIR, "lib");
      e.printStackTrace();
      System.err.println();
      switch (RUNTIME_OS_FAMILY) {
        case WINDOWS:
          System.err.println("Failed to load native G2.dll library.");
          System.err.println(
              "Check PATH environment variable for " + libPath);
          break;
        case MAC_OS:
          System.err.println("Failed to load native libG2.so library");
          System.err.println(
              "Check DYLD_LIBRARY_PATH environment variable for: ");
          System.err.println("     - " + libPath);
          System.err.println("     - " + (new File(libPath, "macos")));
          break;
        case UNIX:
          System.err.println("Failed to load native libG2.so library");
          System.err.println(
              "Check LD_LIBRARY_PATH environment variable for: ");
          System.err.println("     - " + libPath);
          System.err.println("     - " + (new File(libPath, "debian")));
          break;
        default:
          // do nothing
      }
      throw new ExceptionInInitializerError(e);

    } finally {
      ENGINE_API      = engineApi;
      CONFIG_API      = configApi;
      CONFIG_MGR_API  = configMgrApi;
    }
  }

  private static final String JAR_FILE_NAME;

  private static final String JAR_BASE_URL;

  // private static final String PATH_TO_JAR;

  static {
    String jarBaseUrl   = null;
    String jarFileName  = null;

    try {
      Class<RepositoryManager> cls = RepositoryManager.class;

      String url = cls.getResource(
          cls.getSimpleName() + ".class").toString();

      if (url.indexOf(".jar") >= 0) {
        int index = url.lastIndexOf(
            cls.getName().replace(".", "/") + ".class");
        jarBaseUrl = url.substring(0, index);

        index = jarBaseUrl.lastIndexOf("!");
        if (index >= 0) {
          url = url.substring(0, index);
          index = url.lastIndexOf("/");

          if (index >= 0) {
            jarFileName = url.substring(index + 1);
          }

          // url = url.substring(0, index);
          // index = url.indexOf("/");
          // PATH_TO_JAR = url.substring(index);
        }
      }

    } finally {
      JAR_BASE_URL  = jarBaseUrl;
      JAR_FILE_NAME = jarFileName;
    }
  }

  /**
   * Describes a repository configuration.
   *
   */
  public static class Configuration {
    private long configId;
    private JsonObject configJson;
    public Configuration(long configId, JsonObject configJson) {
      this.configId   = configId;
      this.configJson = configJson;
    }

    /**
     * Returns the configuration ID.
     * @return The configuration ID.
     */
    public long getConfigId() {
      return this.configId;
    }

    /**
     * Returns the configuration JSON as a {@link JsonObject}.
     * @return The {@link JsonObject} describing the configuration.
     */
    public JsonObject getConfigJson() {
      return this.configJson;
    }
  }

  /**
   * Validates a repository directory specified in the command-line arguments.
   *
   */
  private static void validateRepositoryDirectory(File directory) {
    if (!directory.exists()) {
      String msg = "Specified repository directory path does not exist: "
                 + directory;

      System.err.println();
      System.err.println(msg);
      throw new IllegalArgumentException(msg);
    }
    if (!directory.isDirectory()) {
      String msg = "Specified repository directory path exists, but is not a "
                 + "directory: " + directory;

      System.err.println();
      System.err.println(msg);
      throw new IllegalArgumentException(msg);
    }

    File iniFile = new File(directory, "g2-init.json");
    if (!iniFile.exists() || iniFile.isDirectory()) {
      String msg = "Specified repository directory path exists, but does not "
                 + "contain a g2-init.json file: " + directory;

      System.err.println();
      System.err.println(msg);
      throw new IllegalArgumentException(msg);
    }
  }

  /**
   * Validates that the specified file exists, is not a directory and appears
   * to be a CSV or a JSON file for loading.
   */
  private static void validateSourceFile(File sourceFile) {
    if (!sourceFile.exists()) {
      String msg = "Specified file does not exist: " + sourceFile;
      System.err.println();
      System.err.println(msg);
      throw new IllegalArgumentException(msg);
    }
    if (sourceFile.isDirectory()) {
      String msg = "Specified file exists, but is a directory: " + sourceFile;
      System.err.println();
      System.err.println(msg);
      throw new IllegalArgumentException(msg);
    }
    String fileName = sourceFile.toString().toUpperCase();
    if (!fileName.endsWith(".JSON") && !fileName.endsWith(".CSV")) {
      String msg = "Specified file must be CSV or JSON: " + fileName;
      System.err.println();
      System.err.println(msg);
      throw new IllegalArgumentException(msg);
    }
  }

  /**
   * Validates the specified JSON for loading.
   */
  private static void validateJsonRecord(String jsonRecord) {
    JsonObject jsonObject;
    try {
      jsonObject = JsonUtils.parseJsonObject(jsonRecord);

    } catch (Exception e) {
      String msg = "The provided JSON record is invalid for loading: "
                 + jsonRecord;
      System.err.println();
      System.err.println(msg);
      throw new IllegalArgumentException(msg);
    }

    if (jsonObject.size() == 0) {
      String msg = "The provided JSON record has no properties: " + jsonRecord;
      System.err.println();
      System.err.println(msg);
      throw new IllegalArgumentException(msg);
    }
  }

  /**
   * Parses the command line arguments and returns a {@link Map} of those
   * arguments.
   *
   * @param args
   * @return
   */
  private static Map<RepositoryManagerOption, Object> parseCommandLine(String[] args) {
    return CommandLineUtilities.parseCommandLine(
        RepositoryManagerOption.class,
        args,
        (option, params) -> {
          switch (option) {
            case HELP:
              if (args.length > 1) {
                throw new IllegalArgumentException(
                    "Help option should be only option when provided.");
              }
              return Boolean.TRUE;

            case VERBOSE:
              return Boolean.TRUE;

            case CREATE_REPO: {
              File repoDirectory = new File(params.get(0));
              if (repoDirectory.exists()) {
                throw new IllegalArgumentException(
                    "Specified repository directory file path "
                        + "already exists: " + repoDirectory);
              }
              return repoDirectory;
            }
            case REPOSITORY: {
              File repoDirectory = new File(params.get(0));
              validateRepositoryDirectory(repoDirectory);
              return repoDirectory;
            }
            case PURGE_REPO:
              return Boolean.TRUE;

            case LOAD_FILE:
              File sourceFile = new File(params.get(0));
              validateSourceFile(sourceFile);
              return sourceFile;

            case ADD_RECORD:
              String jsonRecord = params.get(0);
              validateJsonRecord(jsonRecord);
              return jsonRecord;

            case CONFIG_SOURCES:
              Set<String> sources = new LinkedHashSet<>(params);
              if (sources.size() == 0) {
                throw new IllegalArgumentException(
                    "No data source names were provided for the "
                    + option.getCommandLineFlag() + " option");
              }
              return sources;

            case DATA_SOURCE:
              String dataSource = params.get(0);
              return dataSource;

            default:
              throw new IllegalArgumentException(
                  "Unhandled command line option: "
                      + option.getCommandLineFlag()
                      + " / " + option);
      }
    });
  }

  /**
   * Exits and prints the message associated with the specified exception.
   */
  private static void exitOnError(Throwable t) {
    System.err.println(t.getMessage());
    System.exit(1);
  }

  /**
   * Use this method in conjunction with {@link #clearThreadModuleName()} to
   * provide a specific module name for the repository manager to use when
   * initializing the G2 API's.
   *
   * @param moduleName The module name to initialize with, or <tt>null</tt>
   *                   to do the equivalent of clearing the name.
   *
   */
  public static void setThreadModuleName(String moduleName) {
    RepositoryManager.THREAD_MODULE_NAME.set(moduleName);
  }

  /**
   * Clears any previously set thread module name.  This method should be called
   * in a "finally" block.
   */
  public static void clearThreadModuleName() {
    RepositoryManager.setThreadModuleName(null);
  }

  /**
   * @return
   */
  public static String getUsageString(boolean full) {
    // check if called from the RepositoryManager.main() directly
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    pw.println();
    Class<RepositoryManager> cls = RepositoryManager.class;
    if (checkClassIsMain(cls)) {
      pw.println("USAGE: java -cp " + JAR_FILE_NAME + " "
                 + cls.getName() + " <options>");
    } else {
      pw.println("USAGE: java -jar " + JAR_FILE_NAME + " --repomgr <options>");
    }
    pw.println();
    if (!full) {
      pw.flush();
      return sw.toString();
    }
    pw.print(multilineFormat(
        "<options> includes: ",
        "   --help",
        "        Should be the first and only option if provided.",
        "        Displays a complete usage message describing all options.",
        "",
        "   --createRepo <repository-directory-path>",
        "        Creates a new Senzing repository at the specified path.",
        "",
        "   --purgeRepo",
        "        Purges the Senzing repository at the specified path.",
        "",
        "   --configSources <data-source-1> [data-source-2 ... data-source-n]",
        "        Configures the specified data sources for the repository",
        "        specified by the -repo option.",
        "",
        "   --loadFile <source-file>",
        "        Loads the records in the specified source CSV or JSON file.",
        "        Records are loaded to the repository specified by the -repo option.",
        "        Use the -dataSource option to specify or override a data source for",
        "        the records.",
        "",
        "   --addRecord <json-record>",
        "        Loads the specified JSON record provided on the command line.",
        "        The record is loaded to the repository specified by the -repo option.",
        "        Use the -dataSource option to specify or override the data source for ",
        "        the record.",
        "",
        "   -repo <repository-directory-path>",
        "        Specifies the directory path to the repository to use when performing",
        "        other operations such as:",
        formatUsageOptionsList(
            "           ".length(),
            PURGE_REPO, CONFIG_SOURCES, LOAD_FILE, ADD_RECORD),
        "   -dataSource <data-source>",
        "        Specifies a data source to use when loading records.  If the records",
        "        already have a DATA_SOURCE property then this will override that value.",
        "",
        "   -verbose",
        "        If provided then Senzing will be initialized in verbose mode"));
    pw.flush();
    sw.flush();

    return sw.toString();
  }

  /**
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    Map<RepositoryManagerOption, Object> options = null;
    try {
      options = parseCommandLine(args);
    } catch (Exception e) {
      if (!isLastLoggedException(e)) e.printStackTrace();
      System.out.println(RepositoryManager.getUsageString(false));
      System.exit(1);
    }

    if (options.containsKey(RepositoryManagerOption.HELP)) {
      System.out.println(RepositoryManager.getUsageString(true));
      System.exit(0);
    }

    File repository = (File) options.get(RepositoryManagerOption.REPOSITORY);
    String dataSource = (String) options.get(RepositoryManagerOption.DATA_SOURCE);
    Boolean verbose = (Boolean) options.get(RepositoryManagerOption.VERBOSE);
    if (verbose == null) verbose = Boolean.FALSE;

    try {
      // check if we are creating a repo
      if (options.containsKey(RepositoryManagerOption.CREATE_REPO)) {
        File directory = (File) options.get(RepositoryManagerOption.CREATE_REPO);
        createRepo(directory);
      } else if (options.containsKey(RepositoryManagerOption.PURGE_REPO)) {
        try {
          purgeRepo(repository, verbose);
        } finally {
          destroyApis();
        }

      } else if (options.containsKey(RepositoryManagerOption.LOAD_FILE)) {
        File sourceFile = (File) options.get(RepositoryManagerOption.LOAD_FILE);
        try {
          loadFile(repository, verbose, sourceFile, dataSource);
        } finally {
          destroyApis();
        }

      } else if (options.containsKey(RepositoryManagerOption.ADD_RECORD)) {
        String jsonRecord = (String) options.get(RepositoryManagerOption.ADD_RECORD);
        try {
          addRecord(repository, verbose, jsonRecord, dataSource);
        } finally {
          destroyApis();
        }

      } else if (options.containsKey(RepositoryManagerOption.CONFIG_SOURCES)) {
        Set<String> dataSources = (Set<String>) options.get(RepositoryManagerOption.CONFIG_SOURCES);
        try {
          configSources(repository, verbose, dataSources);
        } finally {
          destroyApis();
        }
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Creates a new Senzing SQLite repository from the default repository data.
   *
   * @param directory The directory at which to create the repository.
   *
   * @return The {@link Configuration} describing the initial configuration.
   */
  public static Configuration createRepo(File directory) {
    return createRepo(directory, false);
  }

  /**
   * Creates a new Senzing SQLite repository from the default repository data.
   *
   * @param directory The directory at which to create the repository.
   *
   * @return The {@link Configuration} describing the initial configuration.
   */
  public static Configuration createRepo(File directory, boolean silent) {
    JsonObject resultConfig = null;
    Long resultConfigId = null;
    try {
      directory = directory.getCanonicalFile();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    if (directory.exists()) {
      if (!directory.isDirectory()) {
        throw new IllegalArgumentException(
            "Repository directory exists and is not a directory: " + directory);
      }
      if (directory.listFiles().length > 0) {
        throw new IllegalArgumentException(
            "Repository directory exists and is not empty: "
                + directory + " / " + directory.listFiles()[0]);
      }
    }
    try {
      directory.mkdirs();
      File repoConfigDir = null;
      if (TEMPLATES_DIR != null) {
        File[] templateFiles = TEMPLATES_DIR.listFiles(
            f -> f.getName().endsWith(".template") && !f.isDirectory()
             && (!EXCLUDED_TEMPLATE_FILES.contains(f.getName().toLowerCase())));

        if (templateFiles.length > 0)
        {
          repoConfigDir = new File(directory, "etc");
          repoConfigDir.mkdirs();
          for (File templateFile : templateFiles) {
            String  templateName  = templateFile.getName();
            int     nameLength    = templateName.length();
            int     targetLength  = nameLength - ".template".length();
            String  targetName    = templateName.substring(0, targetLength);
            File    targetFile    = new File(repoConfigDir, targetName);
            copyFile(templateFile, targetFile);
          }
        }
      }

      // find the template DB file
      File templateDB = (TEMPLATES_DIR != null)
          ? new File(TEMPLATES_DIR, "G2C.db.template")
          : new File(SUPPORT_DIR, "G2C.db.template");
      if (!templateDB.exists()) {
        templateDB = new File(SUPPORT_DIR, "G2C.db");
      }

      if (templateDB.exists()) {
        // copy the file
        copyFile(templateDB, new File(directory, "G2C.db"));
        copyFile(templateDB, new File(directory, "G2_RES.db"));
        copyFile(templateDB, new File(directory, "G2_LIB_FEAT.db"));
      } else {
        // handle running in mock replay mode (no installation)
        touchFile(new File(directory, "G2C.db"));
        touchFile(new File(directory, "G2_RES.db"));
        touchFile(new File(directory, "G2_LIB_FEAT.db"));
      }
      File licensePath    = new File(directory, "g2.lic");

      String fileSep = System.getProperty("file.separator");
      String sqlitePrefix = "sqlite3://na:na@" + directory.toString() + fileSep;

      File jsonInitFile = new File(directory, "g2-init.json");
      JsonObjectBuilder builder = Json.createObjectBuilder();
      JsonObjectBuilder subBuilder = Json.createObjectBuilder();
      if (SUPPORT_DIR != null) {
        subBuilder.add("SUPPORTPATH", SUPPORT_DIR.toString());
      }
      if (RESOURCE_DIR != null) {
        subBuilder.add("RESOURCEPATH", RESOURCE_DIR.toString());
      }
      if (repoConfigDir != null) {
        subBuilder.add("CONFIGPATH", repoConfigDir.toString());
      } else if (CONFIG_DIR != null) {
        subBuilder.add("CONFIGPATH", CONFIG_DIR.toString());
      }
      subBuilder.add("LICENSEFILE", licensePath.getCanonicalPath());
      builder.add("PIPELINE", subBuilder);

      subBuilder = Json.createObjectBuilder();
      subBuilder.add("BACKEND", "HYBRID");
      subBuilder.add("CONNECTION", sqlitePrefix + "G2C.db");
      builder.add("SQL", subBuilder);

      subBuilder = Json.createObjectBuilder();
      subBuilder.add("RES_FEAT", "C1");
      subBuilder.add("RES_FEAT_EKEY", "C1");
      subBuilder.add("RES_FEAT_LKEY", "C1");
      subBuilder.add("RES_FEAT_STAT", "C1");
      subBuilder.add("LIB_FEAT", "C2");
      subBuilder.add("LIB_FEAT_HKEY", "C2");
      builder.add("HYBRID", subBuilder);

      subBuilder = Json.createObjectBuilder();
      subBuilder.add("CLUSTER_SIZE", "1");
      subBuilder.add("DB_1", sqlitePrefix + "G2_RES.db");
      builder.add("C1", subBuilder);

      subBuilder = Json.createObjectBuilder();
      subBuilder.add("CLUSTER_SIZE", "1");
      subBuilder.add("DB_1", sqlitePrefix + "G2_LIB_FEAT.db");
      builder.add("C2", subBuilder);

      JsonObject  initJson      = builder.build();
      String      initJsonText  = JsonUtils.toJsonText(initJson, true);

      try (FileOutputStream   fos = new FileOutputStream(jsonInitFile);
           OutputStreamWriter osw = new OutputStreamWriter(fos, "UTF-8"))
      {
        osw.write(initJsonText);
        osw.flush();
      }

      // setup the initial configuration
      initBaseApis(directory, false);
      try {
        long configId = CONFIG_API.create();
        if (configId <= 0) {
          String msg = logError("G2Config.create()", CONFIG_API);
          throw new IllegalStateException(msg);
        }
        StringBuffer sb = new StringBuffer();
        int returnCode = CONFIG_API.save(configId, sb);
        if (returnCode != 0) {
          String msg = logError("G2Config.save()", CONFIG_API);
          throw new IllegalStateException(msg);
        }
        CONFIG_API.close(configId);

        String configJsonText = sb.toString();

        resultConfig = JsonUtils.parseJsonObject(configJsonText);

        Result<Long> result = new Result<>();
        returnCode = CONFIG_MGR_API.addConfig(configJsonText,
                                              "Initial Config",
                                              result);
        if (returnCode != 0) {
          String msg = logError("G2ConfigMgr.addConfig()",
                                CONFIG_MGR_API);
          throw new IllegalStateException(msg);
        }

        resultConfigId = result.getValue();
        returnCode = CONFIG_MGR_API.setDefaultConfigID(resultConfigId);
        if (returnCode != 0) {
          String msg = logError("G2ConfigMgr.setDefaultConfigID()",
                                CONFIG_MGR_API);
          throw new IllegalStateException(msg);
        }

      } finally {
        destroyBaseApis();
      }

      if (!silent) {
        System.out.println("Entity repository created at: " + directory);
      }

    } catch (IOException e) {
      e.printStackTrace();
      deleteRecursively(directory);
      throw new RuntimeException(e);
    }
    return new Configuration(resultConfigId, resultConfig);
  }

  private static void copyFile(File source, File target)
    throws IOException
  {
    Files.copy(source.toPath(), target.toPath(), COPY_ATTRIBUTES);
  }

  private static void deleteRecursively(File directory) {
    try {
      Files.walk(directory.toPath())
          .sorted(Comparator.reverseOrder())
          .map(Path::toFile)
          .forEach(File::delete);

    } catch (IOException e) {
      System.err.println("Failed to delete directory: " + directory);
    }

  }

  private static synchronized void initBaseApis(File repository, boolean verbose)
  {
    try {
      String moduleName = THREAD_MODULE_NAME.get();
      if (moduleName == null) moduleName = "G2 Repository Manager";
      String initializer = verbose + ":" + repository.getCanonicalPath();
      if (baseInitializedWith == null || !baseInitializedWith.equals(initializer))
      {
        if (baseInitializedWith != null) {
          destroyBaseApis();
        }
        File iniJsonFile = new File(repository, "g2-init.json");
        String initJsonText = readTextFileAsString(iniJsonFile, "UTF-8");
        int returnCode = CONFIG_API.initV2(moduleName, initJsonText, verbose);
        if (returnCode != 0) {
          logError("G2Config.init()", CONFIG_API);
          return;
        }
        returnCode = CONFIG_MGR_API.initV2(moduleName, initJsonText, verbose);
        if (returnCode != 0) {
          CONFIG_API.destroy();
          logError("G2ConfigMgr.init()", CONFIG_MGR_API);
          return;
        }
        baseInitializedWith = initializer;
      }

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static synchronized void initApis(File repository, boolean verbose) {
    try {
      String moduleName = THREAD_MODULE_NAME.get();
      if (moduleName == null) moduleName = "G2 Repository Manager";

      initBaseApis(repository, verbose);

      String initializer = verbose + ":" + repository.getCanonicalPath();
      if (engineInitializedWith == null
          || !engineInitializedWith.equals(initializer))
      {
        if (engineInitializedWith != null) {
          destroyApis();
        }
        File iniJsonFile = new File(repository, "g2-init.json");
        String initJsonText = readTextFileAsString(iniJsonFile, "UTF-8");
        int returnCode = ENGINE_API.initV2(moduleName, initJsonText, verbose);
        if (returnCode != 0) {
          destroyBaseApis();
          logError("G2Engine.init()", ENGINE_API);
          return;
        }
        engineInitializedWith = initializer;
      }

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static synchronized void destroyBaseApis() {
    if (baseInitializedWith != null) {
      CONFIG_API.destroy();
      CONFIG_MGR_API.destroy();
      baseInitializedWith = null;
    }
  }


  private static synchronized void destroyApis() {
    if (engineInitializedWith != null) {
      ENGINE_API.destroy();
      engineInitializedWith = null;
    }
    destroyBaseApis();
  }

  /**
   * Shuts down the repository manager after use to ensure the native
   * Senzing API destroy() functions are called.
   */
  public static void conclude() {
    destroyApis();
  }

  private static String logError(String operation, G2Fallible fallible) {
    String errorMsg = formatError(operation, fallible, true);
    System.err.println();
    System.err.println(errorMsg);
    System.err.println();
    return errorMsg;
  }

  private static Set<String> getDataSources() {
    Result<Long> configId = new Result<>();
    try {
      return getDataSources(configId);

    } finally {
      if (configId.getValue() != null) {
        CONFIG_API.close(configId.getValue());
      }
    }
  }

  private static Set<String> getDataSources(Result<Long> configId) {
    StringBuffer sb = new StringBuffer();
    int returnCode = ENGINE_API.exportConfig(sb);
    if (returnCode != 0) {
      logError("G2Engine.exportConfig()", ENGINE_API);
      return null;
    }
    long handle = CONFIG_API.load(sb.toString());
    configId.setValue(handle);
    return getDataSources(handle);
  }

  private static Set<String> getDataSources(long configId) {
    StringBuffer sb = new StringBuffer();
    int returnCode = CONFIG_API.listDataSources(configId, sb);
    if (returnCode != 0) {
      logError("G2Config.listDataSources()", CONFIG_API);
      return null;
    }

    Set<String> existingSet = new LinkedHashSet<>();

    // parse the raw data
    JsonObject jsonObject = JsonUtils.parseJsonObject(sb.toString());
    JsonArray jsonArray = jsonObject.getJsonArray("DSRC_CODE");
    for (JsonString jsonString : jsonArray.getValuesAs(JsonString.class)) {
      existingSet.add(jsonString.getString());
    }

    return existingSet;
  }

  /**
   * Purges the repository that resides at the specified repository directory.
   *
   * @param repository The directory for the repository.
   */
  public static void purgeRepo(File repository) {
    purgeRepo(repository, false);
  }

  /**
   * Purges the repository that resides at the specified repository directory.
   *
   * @param repository The directory for the repository.
   *
   * @param verbose <tt>true</tt> for verbose API logging, otherwise
   *                <tt>false</tt>
   */
  public static void purgeRepo(File repository, boolean verbose) {
    purgeRepo(repository, verbose, false);
  }

  /**
   * Purges the repository that resides at the specified repository directory.
   *
   * @param repository The directory for the repository.
   *
   * @param verbose <tt>true</tt> for verbose API logging, otherwise
   *                <tt>false</tt>
   *
   * @param silent <tt>true</tt> if no feedback should be given to the user
   *               upon completion, otherwise <tt>false</tt>
   */
  public static void purgeRepo(File repository, boolean verbose, boolean silent)
  {
    initApis(repository, verbose);
    int result = ENGINE_API.purgeRepository();
    if (result != 0) {
      logError("G2Engine.purgeRepository()", ENGINE_API);
    } else if (!silent) {
      System.out.println();
      System.out.println("Repository purged: " + repository);
      System.out.println();
    }
  }

  /**
   * Loads a single CSV or JSON file to the repository -- optionally setting
   * the data source for all the records.  NOTE: if the records in the file do
   * not have a defined DATA_SOURCE then the specified data source is required.
   *
   * @param repository The directory for the repository.
   * @param sourceFile The source file to load (JSON or CSV).
   * @param dataSource The data source to use for loading the records.
   * @return <tt>true</tt> if successful, otherwise <tt>false</tt>
   */
  public static boolean loadFile(File             repository,
                                 File             sourceFile,
                                 String           dataSource)
  {
    return loadFile(repository,
                    false,
                    sourceFile,
                    dataSource,
                    null,
                    null,
                    false);
  }

  /**
   * Loads a single CSV or JSON file to the repository -- optionally setting
   * the data source for all the records.  NOTE: if the records in the file do
   * not have a defined DATA_SOURCE then the specified data source is required.
   *
   * @param repository The directory for the repository.
   * @param sourceFile The source file to load (JSON or CSV).
   * @param dataSource The data source to use for loading the records.
   * @param silent <tt>true</tt> if no feedback should be given to the user
   *               upon completion, otherwise <tt>false</tt>
   * @return <tt>true</tt> if successful, otherwise <tt>false</tt>
   */
  public static boolean loadFile(File             repository,
                                 File             sourceFile,
                                 String           dataSource,
                                 boolean          silent)
  {
    return loadFile(repository,
                    false,
                    sourceFile,
                    dataSource,
                    null,
                    null,
                    silent);
  }

  /**
   * Loads a single CSV or JSON file to the repository -- optionally setting
   * the data source for all the records.  NOTE: if the records in the file do
   * not have a defined DATA_SOURCE then the specified data source is required.
   *
   * @param repository The directory for the repository.
   * @param sourceFile The source file to load (JSON or CSV).
   * @param dataSource The data source to use for loading the records.
   * @param loadedCount The output parameter for the number successfully loaded.
   * @param failedCount The output parameter for the number that failed to load.
   * @return <tt>true</tt> if successful, otherwise <tt>false</tt>
   */
  public static boolean loadFile(File             repository,
                                 File             sourceFile,
                                 String           dataSource,
                                 Result<Integer>  loadedCount,
                                 Result<Integer>  failedCount)
  {
    return loadFile(repository,
                    false,
                    sourceFile,
                    dataSource,
                    loadedCount,
                    failedCount,
                    false);
  }

  /**
   * Loads a single CSV or JSON file to the repository -- optionally setting
   * the data source for all the records.  NOTE: if the records in the file do
   * not have a defined DATA_SOURCE then the specified data source is required.
   *
   * @param repository The directory for the repository.
   * @param sourceFile The source file to load (JSON or CSV).
   * @param dataSource The data source to use for loading the records.
   * @param loadedCount The output parameter for the number successfully loaded.
   * @param failedCount The output parameter for the number that failed to load.
   * @param silent <tt>true</tt> if no feedback should be given to the user
   *               upon completion, otherwise <tt>false</tt>
   * @return <tt>true</tt> if successful, otherwise <tt>false</tt>
   */
  public static boolean loadFile(File             repository,
                                 File             sourceFile,
                                 String           dataSource,
                                 Result<Integer>  loadedCount,
                                 Result<Integer>  failedCount,
                                 boolean          silent)
  {
    return loadFile(repository,
                    false,
                    sourceFile,
                    dataSource,
                    loadedCount,
                    failedCount,
                    silent);
  }

  /**
   * Loads a single CSV or JSON file to the repository -- optionally setting
   * the data source for all the records.  NOTE: if the records in the file do
   * not have a defined DATA_SOURCE then the specified data source is required.
   *
   * @param repository The directory for the repository.
   * @param verbose <tt>true</tt> for verbose API logging, otherwise
   *                <tt>false</tt>
   * @param sourceFile The source file to load (JSON or CSV).
   * @param dataSource The data source to use for loading the records.
   *
   * @return <tt>true</tt> if successful, otherwise <tt>false</tt>
   */
  public static boolean loadFile(File             repository,
                                 boolean          verbose,
                                 File             sourceFile,
                                 String           dataSource)
  {
    return loadFile(repository,
                    verbose,
                    sourceFile,
                    dataSource,
                    null,
                    null,
                    false);
  }

  /**
   * Loads a single CSV or JSON file to the repository -- optionally setting
   * the data source for all the records.  NOTE: if the records in the file do
   * not have a defined DATA_SOURCE then the specified data source is required.
   *
   * @param repository The directory for the repository.
   * @param verbose <tt>true</tt> for verbose API logging, otherwise
   *                <tt>false</tt>
   * @param sourceFile The source file to load (JSON or CSV).
   * @param dataSource The data source to use for loading the records.
   * @param silent <tt>true</tt> if no feedback should be given to the user
   *               upon completion, otherwise <tt>false</tt>
   *
   * @return <tt>true</tt> if successful, otherwise <tt>false</tt>
   */
  public static boolean loadFile(File             repository,
                                 boolean          verbose,
                                 File             sourceFile,
                                 String           dataSource,
                                 boolean          silent)
  {
    return loadFile(repository,
                    verbose,
                    sourceFile,
                    dataSource,
                    null,
                    null,
                    silent);
  }

  /**
   * Loads a single CSV or JSON file to the repository -- optionally setting
   * the data source for all the records.  NOTE: if the records in the file do
   * not have a defined DATA_SOURCE then the specified data source is required.
   *
   * @param repository The directory for the repository.
   * @param verbose <tt>true</tt> for verbose API logging, otherwise
   *                <tt>false</tt>
   * @param sourceFile The source file to load (JSON or CSV).
   * @param dataSource The data source to use for loading the records.
   * @param loadedCount The output parameter for the number successfully loaded.
   * @param failedCount The output parameter for the number that failed to load.
   *
   * @return <tt>true</tt> if successful, otherwise <tt>false</tt>
   */
  public static boolean loadFile(File             repository,
                                 boolean          verbose,
                                 File             sourceFile,
                                 String           dataSource,
                                 Result<Integer>  loadedCount,
                                 Result<Integer>  failedCount)
  {
    return loadFile(repository,
                    verbose,
                    sourceFile,
                    dataSource,
                    loadedCount,
                    failedCount,
                    false);
  }

  /**
   * Loads a single CSV or JSON file to the repository -- optionally setting
   * the data source for all the records.  NOTE: if the records in the file do
   * not have a defined DATA_SOURCE then the specified data source is required.
   *
   * @param repository The directory for the repository.
   * @param verbose <tt>true</tt> for verbose API logging, otherwise
   *                <tt>false</tt>
   * @param sourceFile The source file to load (JSON or CSV).
   * @param dataSource The data source to use for loading the records.
   * @param loadedCount The output parameter for the number successfully loaded.
   * @param failedCount The output parameter for the number that failed to load.
   *
   * @return <tt>true</tt> if successful, otherwise <tt>false</tt>
   */
  public static boolean loadFile(File             repository,
                                 boolean          verbose,
                                 File             sourceFile,
                                 String           dataSource,
                                 Result<Integer>  loadedCount,
                                 Result<Integer>  failedCount,
                                 boolean          silent)
  {
    String normalizedFileName = sourceFile.toString().toUpperCase();
    if ((!normalizedFileName.endsWith(".JSON"))
        && (!normalizedFileName.endsWith(".CSV"))) {
      throw new IllegalArgumentException(
          "File must be a CSV or JSON file: " + sourceFile);
    }

    initApis(repository, verbose);

    final Integer ZERO = 0;
    if (loadedCount != null) loadedCount.setValue(ZERO);
    if (failedCount != null) failedCount.setValue(ZERO);

    if (dataSource != null) dataSource = dataSource.toUpperCase();

    Set<String> dataSources = getDataSources();
    // check if the data source is configured
    if (dataSource != null && !dataSources.contains(dataSource)) {
      if (!addDataSource(repository, dataSource, verbose)) return false;
      dataSources.add(dataSource);
    }

    RecordReader recordReader = null;
    // check the file type
    if (normalizedFileName.endsWith(".JSON")) {
      recordReader = provideJsonRecords(sourceFile, dataSource);
    } else if (normalizedFileName.endsWith(".CSV")) {
      recordReader = provideCsvRecords(sourceFile, dataSource);
    }
    if (recordReader == null) {
      return false;
    }

    String loadId = (new Date()).toString();
    int loaded = 0;
    int failed = 0;
    int loadedInterval = 100;
    int failedInterval = 100;
    PrintStream printStream = System.err;
    try {
      for (JsonObject record = recordReader.readRecord();
           (record != null);
           record = recordReader.readRecord())
      {
        String recordId = JsonUtils.getString(record, "RECORD_ID");
        String recordSource = JsonUtils.getString(record, "DATA_SOURCE");
        if (recordSource == null) {
          System.err.println();
          System.err.println(
              "If records in the file do not have a DATA_SOURCE then "
                  + RepositoryManagerOption.DATA_SOURCE.getCommandLineFlag() + " is required.");
          return false;
        }

        if (!dataSources.contains(recordSource)) {
          if (!addDataSource(repository, recordSource, verbose)) return false;
          dataSources.add(recordSource);
        }

        StringBuffer sb = new StringBuffer();
        String jsonRecord = JsonUtils.toJsonText(record);

        int returnCode
            = (recordId != null)
            ? ENGINE_API.addRecord(dataSource, recordId, jsonRecord, loadId)
            : ENGINE_API.addRecordWithReturnedRecordID(dataSource,
                                                       sb,
                                                       jsonRecord,
                                                       loadId);
        if (returnCode == 0) {
          loaded++;
          loadedInterval = doLoadFeedback(
              "Loaded so far", loaded, loadedInterval, loaded, failed, silent);

        } else {
          failed++;
          if (failed == 1 || ((failed % failedInterval) == 0)) {
            logError("G2Engine.addRecord()", ENGINE_API);
          }
          failedInterval = doLoadFeedback(
              "Loaded so far", failed, failedInterval, loaded, failed, silent);
        }
      }
      doLoadFeedback(
          "Loaded all records", loaded, 0, loaded, failed, silent);
      processRedos(silent);
      printStream = (silent) ? null : System.out;

      return true;

    } finally {
      if (loaded > 0 || failed > 0) {
        if (printStream != null) {
          printStream.println();
          printStream.println("Loaded records from file:");
          printStream.println("     Repository  : " + repository);
          printStream.println("     File        : " + sourceFile);
          if (dataSource != null) {
            printStream.println("     Data Source : " + dataSource);
          }
          printStream.println("     Load Count  : " + loaded);
          printStream.println("     Fail Count  : " + failed);
          printStream.println();
        }
      }
      // set the counts
      if (failedCount != null) failedCount.setValue(failed);
      if (loadedCount != null) loadedCount.setValue(loaded);
    }
  }

  private static int processRedos(boolean silent) {
    int loaded = 0;
    int failed = 0;
    try {
      // process redos
      int loadedInterval = 100;
      int failedInterval = 100;
      long originalCount = ENGINE_API.countRedoRecords();
      if (originalCount == 0) return 0;
      if (originalCount > 0) {
        System.out.println();
        System.out.println("Found redos to process: " + originalCount);
        System.out.println();
      }
      for (int count = 0; ENGINE_API.countRedoRecords() > 0; count++) {
        StringBuffer sb = new StringBuffer();
        int returnCode = ENGINE_API.processRedoRecord(sb);
        if (returnCode != 0) {
          logError("G2Engine.processRedoRecord()", ENGINE_API);
          failed++;
          failedInterval = doLoadFeedback(
              "Redo's so far", failed, failedInterval, loaded, failed, silent);
        } else {
          loaded++;
          loadedInterval = doLoadFeedback(
              "Redo's so far", loaded, loadedInterval, loaded, failed, silent);
        }
        if (count > (originalCount*5)) {
          System.err.println();
          System.err.println("Processing redo's not converging -- giving up.");
          System.err.println();
          return count;
        }
      }
      System.out.println();
      System.out.println("Processed all redos (succeeded / failed): "
                         + loaded + " / " + failed);
      System.out.println();

      return loaded;

    } catch (Exception ignore) {
      System.err.println();
      System.err.println("IGNORING EXCEPTION DURING REDOS:");
      ignore.printStackTrace();
      System.err.println();
      return loaded;
    }

  }

  private static int doLoadFeedback(String  prefix,
                                    int     count,
                                    int     interval,
                                    int     loaded,
                                    int     failed,
                                    boolean silent)
  {
    if (count > (interval * 10)) {
      interval *= 10;
    }
    if ((count > 0) && ((interval == 0) || (count % interval) == 0)) {
      if (!silent) {
        System.out.println(prefix + " (succeeded / failed): "
                           + loaded + " / " + failed);
      }
    }
    return interval;
  }

  private static boolean addDataSource(File     repository,
                                       String   dataSource,
                                       boolean  verbose)
  {
    // add the data source and reinitialize
    Configuration config = configSources(repository,
                                         Collections.singleton(dataSource),
                                         verbose);
    if (config == null) return false;
    destroyApis();
    initApis(repository, verbose);
    return true;
  }

  private static RecordReader provideJsonRecords(File    sourceFile,
                                                 String  dataSource)
  {
    RecordReader recordReader = null;
    // check if we have a real JSON array
    try {
      FileInputStream    fis = new FileInputStream(sourceFile);
      InputStreamReader  isr = new InputStreamReader(fis, "UTF-8");
      BufferedReader     br  = new BufferedReader(isr);

      recordReader = new RecordReader(br, dataSource);

      RecordReader.Format format = recordReader.getFormat();
      if (format != JSON && format != JSON_LINES) {
        System.err.println();
        System.err.println(
            "JSON file does not contain JSON or JSON-lines formatted records");
        System.err.println();
        return null;
      }

    } catch (IOException e) {
      e.printStackTrace();
      System.err.println();
      System.err.println("Failed to read file: " + sourceFile);
      System.err.println();
      return null;
    }

    // return the record reader
    return recordReader;
  }

  private static RecordReader provideCsvRecords(File   sourceFile,
                                                String dataSource)
  {
    RecordReader recordReader = null;
    // check if we have a real JSON array
    try {
      FileInputStream    fis = new FileInputStream(sourceFile);
      InputStreamReader  isr = new InputStreamReader(fis, "UTF-8");
      BufferedReader     br  = new BufferedReader(isr);

      recordReader = new RecordReader(CSV, br, dataSource);

    } catch (IOException e) {
      e.printStackTrace();
      System.err.println();
      System.err.println("Failed to read file: " + sourceFile);
      System.err.println();
      return null;
    }

    // return the record reader
    return recordReader;
  }

  /**
   * Loads a single JSON record to the repository -- optionally setting
   * the data source for the record.  NOTE: if the specified record does not
   * have a DATA_SOURCE property then the specified data source is required.
   *
   * @param repository The directory for the repository.
   * @param jsonRecord The JSON record to load.
   * @param dataSource The data source to use for loading the records.
   *
   * @return <tt>true</tt> if successful, otherwise <tt>false</tt>
   */
  public static boolean addRecord(File   repository,
                                  String jsonRecord,
                                  String dataSource)
  {
    return addRecord(repository, false, jsonRecord, dataSource, false);
  }

  /**
   * Loads a single JSON record to the repository -- optionally setting
   * the data source for the record.  NOTE: if the specified record does not
   * have a DATA_SOURCE property then the specified data source is required.
   *
   * @param repository The directory for the repository.
   * @param jsonRecord The JSON record to load.
   * @param dataSource The data source to use for loading the records.
   *
   * @return <tt>true</tt> if successful, otherwise <tt>false</tt>
   */
  public static boolean addRecord(File    repository,
                                  boolean verbose,
                                  String  jsonRecord,
                                  String  dataSource)
  {
    return addRecord(repository, verbose, jsonRecord, dataSource, false);
  }

  /**
   * Loads a single JSON record to the repository -- optionally setting
   * the data source for the record.  NOTE: if the specified record does not
   * have a DATA_SOURCE property then the specified data source is required.
   *
   * @param repository The directory for the repository.
   * @param jsonRecord The JSON record to load.
   * @param dataSource The data source to use for loading the records.
   *
   * @return <tt>true</tt> if successful, otherwise <tt>false</tt>
   */
  public static boolean addRecord(File    repository,
                                  String  jsonRecord,
                                  String  dataSource,
                                  boolean silent)
  {
    return addRecord(repository, false, jsonRecord, dataSource, silent);
  }


  /**
   * Loads a single JSON record to the repository -- optionally setting
   * the data source for the record.  NOTE: if the specified record does not
   * have a DATA_SOURCE property then the specified data source is required.
   *
   * @param repository The directory for the repository.
   * @param jsonRecord The JSON record to load.
   * @param dataSource The data source to use for loading the records.
   * @param verbose <tt>true</tt> for verbose API logging, otherwise
   *                <tt>false</tt>
   *
   * @return <tt>true</tt> if successful, otherwise <tt>false</tt>
   */
  public static boolean addRecord(File     repository,
                                  boolean  verbose,
                                  String   jsonRecord,
                                  String   dataSource,
                                  boolean  silent)
  {
    initApis(repository, verbose);

    Set<String> dataSources = getDataSources();
    JsonObject jsonObject = JsonUtils.parseJsonObject(jsonRecord);
    if (dataSource == null) {
      dataSource = JsonUtils.getString(jsonObject, "DATA_SOURCE");
    }
    if (dataSource == null) {
      System.err.println();
      System.err.println("ERROR: Could not determine data source for record.");
      System.err.println();
      return false;
    }

    // check if the data source is configured
    dataSource = dataSource.toUpperCase();
    if (!dataSources.contains(dataSource)) {
      if (!addDataSource(repository, dataSource, verbose)) return false;
      dataSources.add(dataSource);
    }
    String recordId = JsonUtils.getString(jsonObject, "RECORD_ID");
    StringBuffer sb = new StringBuffer();
    String loadId = (new Date()).toString();
    int returnCode
        = (recordId != null)
        ? ENGINE_API.addRecord(dataSource, recordId, jsonRecord, loadId)
        : ENGINE_API.addRecordWithReturnedRecordID(dataSource,
                                                   sb,
                                                   jsonRecord,
                                                   loadId);
    if (returnCode != 0) {
      logError("G2Engine.addRecord()"
                   + ((recordId == null) ? "WithReturnedRecordId" : ""),
               ENGINE_API);
      return false;
    }

    processRedos(silent);

    if (!silent) {
      System.out.println();
      System.out.println("Added record to " + dataSource + " data source: ");
      System.out.println(jsonRecord);
      System.out.println();
    }

    return true;
  }

  /**
   * Configures the specified data sources for the specified repository
   * if not already configured.
   *
   * @param repository The directory for the repository.
   * @param dataSources The {@link List} of data source names.
   *
   * @return The {@link Configuration} describing the new configuration or
   *         <tt>null</tt> if the operation failed.
   */
  public static Configuration configSources(File         repository,
                                            Set<String>  dataSources)
  {
    return configSources(repository, dataSources, false);
  }

  /**
   * Configures the specified data sources for the specified repository
   * if not already configured.
   *
   * @param repository The directory for the repository.
   * @param dataSources The {@link List} of data source names.
   * @param verbose <tt>true</tt> for verbose API logging, otherwise
   *                <tt>false</tt>
   *
   * @return The {@link Configuration} describing the new configuration or
   *         <tt>null</tt> if the operation failed.
   */
  public static Configuration configSources(File         repository,
                                            boolean      verbose,
                                            Set<String>  dataSources)
  {
    return configSources(repository, verbose, dataSources, false);
  }

  /**
   * Configures the specified data sources for the specified repository
   * if not already configured.
   *
   * @param repository The directory for the repository.
   * @param dataSources The {@link List} of data source names.
   * @param silent <tt>true</tt> if no feedback should be given to the user
   *               upon completion, otherwise <tt>false</tt>
   *
   * @return The {@link Configuration} describing the new configuration or
   *         <tt>null</tt> if the operation failed.
   */
  public static Configuration configSources(File         repository,
                                            Set<String>  dataSources,
                                            boolean      silent)
  {
    return configSources(repository, false, dataSources, silent);
  }

  /**
   * Configures the specified data sources for the specified repository
   * if not already configured.
   *
   * @param repository The directory for the repository.
   * @param verbose <tt>true</tt> for verbose API logging, otherwise
   *                <tt>false</tt>
   * @param dataSources The {@link List} of data source names.
   * @param silent <tt>true</tt> if no feedback should be given to the user
   *               upon completion, otherwise <tt>false</tt>
   *
   * @return The {@link Configuration} describing the new configuration or
   *         <tt>null</tt> if the operation failed.
   */
  public static Configuration configSources(File         repository,
                                            boolean      verbose,
                                            Set<String>  dataSources,
                                            boolean      silent)
  {
    initApis(repository, verbose);
    Long        resultConfigId  = null;
    JsonObject  resultConfig    = null;

    Result<Long> configId = new Result<>();
    int returnCode = 0;
    try {
      Set<String> existingSet = getDataSources(configId);

      Map<String, Boolean> dataSourceActions = new LinkedHashMap<>();
      Set<String> addedDataSources = new LinkedHashSet<>();
      int addedCount = 0;
      for (String dataSource : dataSources) {
        if (existingSet.contains(dataSource)) {
          dataSourceActions.put(dataSource, false);
          continue;
        }
        returnCode = CONFIG_API.addDataSource(configId.getValue(), dataSource);
        if (returnCode != 0) {
          logError("G2Config.addDataSource()", CONFIG_API);
          return null;
        }
        dataSourceActions.put(dataSource, true);
        addedDataSources.add(dataSource);
        addedCount++;
      }

      if (addedCount > 0) {
        // write the modified config to a string buffer
        StringBuffer sb = new StringBuffer();
        returnCode = CONFIG_API.save(configId.getValue(), sb);
        if (returnCode != 0) {
          logError("G2Config.save()", CONFIG_API);
          return null;
        }

        String comment;
        if (addedCount == 1) {
          comment = "Added data source: " + addedDataSources.iterator().next();
        } else {
          StringBuilder commentSB = new StringBuilder();
          commentSB.append("Added data sources: ");
          Iterator<String> iter = addedDataSources.iterator();
          String prefix = "";
          while (iter.hasNext()) {
            String dataSource = iter.next();
            commentSB.append(prefix).append(dataSource);
            prefix = iter.hasNext() ? ", " : " and ";
          }
          comment = commentSB.toString();
        }

        Result<Long> result = new Result<>();
        String configJsonText = sb.toString();
        returnCode = CONFIG_MGR_API.addConfig(configJsonText, comment, result);
        if (returnCode != 0) {
          logError("G2ConfigMgr.addConfig()", CONFIG_MGR_API);
          return null;
        }

        returnCode = CONFIG_MGR_API.setDefaultConfigID(result.getValue());
        if (returnCode != 0) {
          logError("G2ConfigMgr.setDefaultConfigID()", CONFIG_MGR_API);
          return null;
        }

        // get the result config and its ID for the result
        resultConfig    = JsonUtils.parseJsonObject(configJsonText);
        resultConfigId  = result.getValue();
      }

      if (!silent) {
        System.out.println();
        System.out.println("Ensured specified data sources are configured.");
        System.out.println("     Repository   : " + repository);
        System.out.println("     Data Sources : ");
        dataSourceActions.entrySet().forEach(entry -> {
          System.out.println(
              "          - " + entry.getKey()
              + " (" + ((entry.getValue()) ? "added" : "preconfigured") + ")");
        });
        System.out.println();
      }

      if (addedCount > 0) {
        destroyApis();
        initApis(repository, verbose);
      }

    } finally {
      if (configId.getValue() != null) {
        CONFIG_API.close(configId.getValue());
      }
    }

    // check if the result config ID is not set (usually means that all the
    // data sources to be added already existed)
    if (resultConfigId == null) {
      Result<Long> result = new Result<>();
      returnCode = CONFIG_MGR_API.getDefaultConfigID(result);
      if (returnCode != 0) {
        logError("G2ConfigMgr.getDefaultConfigID()", CONFIG_MGR_API);
        return null;
      }
      resultConfigId = result.getValue();
      StringBuffer sb = new StringBuffer();
      returnCode = CONFIG_MGR_API.getConfig(resultConfigId, sb);
      if (returnCode != 0) {
        logError("G2ConfigMgr.getConfig()", CONFIG_MGR_API);
        return null;
      }
      resultConfig = JsonUtils.parseJsonObject(sb.toString());
    }

    return new Configuration(resultConfigId, resultConfig);
  }

}
