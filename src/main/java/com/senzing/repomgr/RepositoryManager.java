package com.senzing.repomgr;

import com.senzing.g2.engine.G2Config;
import com.senzing.g2.engine.G2Engine;
import com.senzing.g2.engine.G2Fallible;
import com.senzing.g2.engine.Result;
import com.senzing.util.JsonUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import javax.json.*;
import javax.json.stream.JsonParserFactory;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static java.util.EnumSet.*;
import static com.senzing.util.OperatingSystemFamily.*;
import static java.nio.file.StandardCopyOption.*;

public class RepositoryManager {
  public static final File SENZING_DIR;

  private static final G2Engine ENGINE_API;

  private static final G2Config CONFIG_API;

  private static boolean initialized = false;

  static {
    String defaultDir = null;
    switch (RUNTIME_OS_FAMILY) {
      case WINDOWS:
        defaultDir = "C:\\Program Files\\Senzing\\g2";
        break;
      case MAC_OS:
        defaultDir = "/Applications/Senzing.app/Contents/Resources/app/g2";
        break;
      case UNIX:
        defaultDir = "/opt/senzing/g2";
        break;
      default:
        throw new ExceptionInInitializerError(
            "Unrecognized Operating System: " + RUNTIME_OS_FAMILY);
    }

    // check environment for SENZING_DIR
    String envVariable = "SENZING_DIR";
    String senzingPath = System.getenv(envVariable);
    if (senzingPath == null) {
      // check environment for SENZING_ROOT
      envVariable = "SENZING_ROOT";
      senzingPath = System.getenv(envVariable);
    }
    if (senzingPath == null) {
      envVariable = null;
      senzingPath = defaultDir;
    }

    // check the senzing path
    File senzingDir = new File(senzingPath);
    if (!senzingDir.exists()) {
      System.err.println("Could not find Senzing installation directory:");
      System.err.println("     " + senzingPath);
      System.err.println();
      if (envVariable != null) {
        System.err.println(
            "Check the " + envVariable + " environment variable");
      }

      throw new ExceptionInInitializerError(
          "Could not find Senzing installation directory: " + senzingPath);
    }

    if (!senzingDir.isDirectory()
        || (!(new File(senzingDir, "data")).exists())
        || (!(new File(senzingDir, "data")).isDirectory()))
    {
      System.err.println("Senzing installation directory appears invalid:");
      System.err.println("     " + senzingPath);
      System.err.println();
      if (envVariable != null) {
        System.err.println(
            "Check the " + envVariable + " environment variable");
      }

      throw new ExceptionInInitializerError(
          "Could not find Senzing installation directory: " + senzingPath);
    }

    SENZING_DIR = senzingDir;

    G2Engine engineApi = null;
    G2Config configApi = null;
    try {
      Class engineApiClass = Class.forName("com.senzing.g2.engine.G2JNI");
      Class configApiClass = Class.forName("com.senzing.g2.engine.G2ConfigJNI");

      engineApi = (G2Engine) (engineApiClass.newInstance());
      configApi = (G2Config) (configApiClass.newInstance());

    } catch (Exception e) {
      File libPath = new File(SENZING_DIR, "lib");
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
      ENGINE_API = engineApi;
      CONFIG_API = configApi;
    }
  }

  private static final String JAR_FILE_NAME;

  private static final String JAR_BASE_URL;

  private static final String PATH_TO_JAR;

  static {
    System.out.println();
    Class<RepositoryManager> cls = RepositoryManager.class;

    String url = cls.getResource(
        cls.getSimpleName() + ".class").toString();

    int index = url.lastIndexOf(
        cls.getName().replace(".", "/") + ".class");
    JAR_BASE_URL = url.substring(0, index);

    index = JAR_BASE_URL.lastIndexOf("!");
    url = url.substring(0, index);
    index = url.lastIndexOf("/");
    JAR_FILE_NAME = url.substring(index + 1);
    url = url.substring(0, index);
    index = url.indexOf("/");
    PATH_TO_JAR = url.substring(index);
  }

  private enum Option {
    HELP("-help"),
    CREATE_REPO("-createRepo"),
    PURGE_REPO("-purgeRepo"),
    LOAD_FILE("-loadFile"),
    ADD_RECORD("-addRecord"),
    CONFIG_SOURCES("-configSources"),
    DATA_SOURCE("-dataSource"),
    REPOSITORY("-repo"),
    VERBOSE("-verbose");

    Option(String commandLineFlag) {
      this.commandLineFlag = commandLineFlag;
      this.conflicts       = null;
      this.dependencies    = null;
    }

    private String commandLineFlag;
    private EnumSet<Option> conflicts;
    private EnumSet<Option> dependencies;

    public static final EnumSet<Option> PRIMARY_OPTIONS
        = complementOf(of(DATA_SOURCE, REPOSITORY, VERBOSE));

    String getCommandLineFlag() {
      return this.commandLineFlag;
    }

    public EnumSet<Option> getConflicts() {
      return this.conflicts;
    }

    public EnumSet<Option> getDependencies() {
      return this.dependencies;
    }

    static {
      HELP.conflicts = complementOf(EnumSet.of(HELP));
      HELP.dependencies = noneOf(Option.class);
      CREATE_REPO.conflicts = complementOf(of(CREATE_REPO, VERBOSE));
      CREATE_REPO.dependencies = noneOf(Option.class);
      PURGE_REPO.conflicts = complementOf(of(PURGE_REPO, REPOSITORY, VERBOSE));
      PURGE_REPO.dependencies = of(REPOSITORY);
      LOAD_FILE.conflicts
          = complementOf(of(LOAD_FILE, REPOSITORY, DATA_SOURCE, VERBOSE));
      LOAD_FILE.dependencies = of(REPOSITORY);
      ADD_RECORD.conflicts
          = complementOf(of(ADD_RECORD, REPOSITORY, DATA_SOURCE, VERBOSE));
      ADD_RECORD.dependencies = of(REPOSITORY);
      CONFIG_SOURCES.conflicts
          = complementOf(of(CONFIG_SOURCES, REPOSITORY, VERBOSE));
      CONFIG_SOURCES.dependencies = of(REPOSITORY);
      DATA_SOURCE.conflicts
          = complementOf(of(DATA_SOURCE, LOAD_FILE, ADD_RECORD, VERBOSE, REPOSITORY));
      DATA_SOURCE.dependencies = noneOf(Option.class);
      REPOSITORY.conflicts = of(HELP, CREATE_REPO);
      REPOSITORY.dependencies = noneOf(Option.class);
    }
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
      String msg = "Missing expected argument following " + args[index - 1];

      System.err.println();
      System.err.println(msg);

      throw new IllegalArgumentException(msg);
    }
    return;
  }

  /**
   * Stores the option and its value in the specified option map, first
   * checking to ensure that the option is NOT already specified.
   *
   */
  private static void putOption(Map<Option, Object> optionMap,
                                Option              option,
                                Object              value)
  {
    if (optionMap.containsKey(option)) {
      String msg = "Cannot specify command-line option more than once: "
                 + option.getCommandLineFlag();

      System.err.println();
      System.err.println(msg);

      throw new IllegalArgumentException(msg);
    }

    // put it in the option map
    optionMap.put(option, value);
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

    File iniFile = new File(directory, "g2.ini");
    if (!iniFile.exists() || iniFile.isDirectory()) {
      String msg = "Specified repository directory path exists, but does not "
                 + "contain a g2.ini file: " + directory;

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
    JsonObject jsonObject = null;
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
  private static Map<Option, Object> parseCommandLine(String[] args) {
    Map<Option, Object> result = new LinkedHashMap<>();
    File repoDirectory = null;
    for (int index = 0; index < args.length; index++) {
      switch (args[index]) {
        case "-help":
          if (args.length > 1) {
            System.err.println();

            System.err.println(
                "Help option should be only option when provided.");

            throw new IllegalArgumentException("Extra options with help");
          }
          putOption(result, Option.HELP, Boolean.TRUE);
          return result;

        case "-verbose":
          putOption(result, Option.VERBOSE, Boolean.TRUE);
          return result;

        case "-createRepo":
          index++;
          ensureArgument(args, index);
          repoDirectory = new File(args[index]);
          if (repoDirectory.exists()) {
            String msg = "Specified repository directory file path "
                       + "already exists: " + repoDirectory;

            System.err.println(msg);

            throw new IllegalArgumentException(msg);
          }
          putOption(result, Option.CREATE_REPO, repoDirectory);
          break;

        case "-repo":
          index++;
          ensureArgument(args, index);
          repoDirectory = new File(args[index]);
          validateRepositoryDirectory(repoDirectory);
          putOption(result, Option.REPOSITORY, repoDirectory);
          break;

        case "-purgeRepo":
          putOption(result, Option.PURGE_REPO, Boolean.TRUE);
          break;

        case "-loadFile":
          index++;
          ensureArgument(args, index);
          File sourceFile = new File(args[index]);
          validateSourceFile(sourceFile);
          putOption(result, Option.LOAD_FILE, sourceFile);
          break;

        case "-addRecord":
          index++;
          ensureArgument(args, index);
          String jsonRecord = args[index];
          validateJsonRecord(jsonRecord);
          putOption(result, Option.ADD_RECORD, jsonRecord);
          break;

        case "-configSources":
          Set<String> sources = new LinkedHashSet<>();
          while ((index + 1) < args.length && !args[index + 1].startsWith("-"))
          {
            index++;
            sources.add(args[index]);
          }
          if (sources.size() == 0) {
            String msg = "No data source names were provided for "
                       + "-configSources";
            System.err.println();
            System.err.println(msg);
            throw new IllegalArgumentException(msg);
          }
          putOption(result, Option.CONFIG_SOURCES, sources);
          break;

        case "-dataSource":
          index++;
          ensureArgument(args, index);
          String dataSource = args[index];
          putOption(result, Option.DATA_SOURCE, dataSource);
          break;

        default:
          System.err.println();

          System.err.println("Unrecognized option: " + args[index]);

          throw new IllegalArgumentException(
              "Bad command line option: " + args[index]);
      }
    }

    // check for problems
    Set<Option> options = result.keySet();

    // check if we have one primary option
    int primaryCount = 0;
    for (Option option : options) {
      if (Option.PRIMARY_OPTIONS.contains(option)) {
        primaryCount++;
      }
    }
    if (primaryCount == 0) {
      System.err.println();
      System.err.println("Must specify at least one of the following:");
      for (Option option: Option.PRIMARY_OPTIONS) {
        System.err.println("     " + option.getCommandLineFlag());
      }
      throw new IllegalArgumentException(
          "Must specify at least one primary option.");
    }

    if (primaryCount > 1) {
      System.err.println();
      System.err.println("Only one of the following options can be specified:");
      for (Option option: Option.PRIMARY_OPTIONS) {
        if (options.contains(option)) {
          System.err.println("     " + option.getCommandLineFlag());
        }
      }
      throw new IllegalArgumentException(
          "Cannot specify more than one primary option.");
    }

    // check for conflicts and dependencies
    for (Option option : options) {
      EnumSet<Option> conflicts     = option.getConflicts();
      EnumSet<Option> dependencies  = option.getDependencies();
      for (Option conflict : conflicts) {
        if (options.contains(conflict)) {
          String msg = "Cannot specify both " + option.getCommandLineFlag()
              + " and " + conflict.getCommandLineFlag();

          System.err.println();
          System.err.println(msg);

          throw new IllegalArgumentException(msg);
        }
      }
      if (!options.containsAll(dependencies)) {
        System.err.println();
        System.err.println(
            "The " + option.getCommandLineFlag() + " option also requires:");
        for (Option dependency : dependencies) {
          if (!options.contains(dependency)) {
            System.err.println("     " + dependency.getCommandLineFlag());
          }
        }
        throw new IllegalArgumentException(
            "Missing dependencies for " + option.getCommandLineFlag());
      }
    }
    return result;
  }

  /**
   * Exits and prints the message associated with the specified exception.
   */
  private static void exitOnError(Throwable t) {
    System.err.println(t.getMessage());
    System.exit(1);
  }

  /**
   * @return
   */
  public static String getUsageString(boolean full) {
    boolean jarMain = false;

    // check if called from the RepositoryManager.main() directly
    Throwable t = new Throwable();
    StackTraceElement[] trace = t.getStackTrace();
    StackTraceElement lastStackFrame = trace[trace.length-1];
    String className = lastStackFrame.getClassName();
    String methodName = lastStackFrame.getMethodName();
    Class<RepositoryManager> cls = RepositoryManager.class;
    if ("main".equals(methodName) && cls.getName().equals(className)) {
      jarMain = true;
    }

    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    pw.println();
    if (jarMain) {
      pw.println("USAGE: java -cp " + JAR_FILE_NAME + " "
                 + cls.getName() + " <options>");
    } else {
      pw.println("USAGE: java -jar " + JAR_FILE_NAME + " --repomgr <options>");
    }
    pw.println();
    if (!full) {
      pw.flush();
      sw.flush();
      return sw.toString();
    }
    pw.println("<options> includes: ");
    pw.println("   -help");
    pw.println("        Should be the first and only option if provided.");
    pw.println("        Displays a complete usage message describing all options.");
    pw.println();
    pw.println("   -createRepo [repository-directory-path]");
    pw.println("        Creates a new Senzing repository at the specified path.");
    pw.println();
    pw.println("   -purgeRepo");
    pw.println("        Purges the Senzing repository at the specified path.");
    pw.println();
    pw.println("   -configSources [data-source, ...]");
    pw.println("        Configures the specified data sources for the repository");
    pw.println("        specified by the -repo option.");
    pw.println();
    pw.println("   -loadFile [source-file]");
    pw.println("        Loads the records in the specified source CSV or JSON file.");
    pw.println("        Records are loaded to the repository specified by the -repo option.");
    pw.println("        Use the -dataSource option to specify or override a data source for");
    pw.println("        the records.");
    pw.println();
    pw.println("   -addRecord [json-record]");
    pw.println("        Loads the specified JSON record provided on the command line.");
    pw.println("        The record is loaded to the repository specified by the -repo option.");
    pw.println("        Use the -dataSource option to specify or override the data source for ");
    pw.println("        the record.");
    pw.println();
    pw.println("   -repo [repository-directory-path]");
    pw.println("        Specifies the directory path to the repository to use when performing");
    pw.println("        other operations such as:");
    pw.println("           o -purgeRepo");
    pw.println("           o -configSources");
    pw.println("           o -loadFile");
    pw.println("           o -addRecord");
    pw.println();
    pw.println("   -dataSource [data-source]");
    pw.println("        Specifies a data source to use when loading records.  If the records");
    pw.println("        already have a DATA_SOURCE property then this will override that value.");
    pw.println();
    pw.println("   -verbose");
    pw.println("        Provide this switch to initialize the native Senzing API's in verbose mode");
    pw.println();
    pw.flush();
    sw.flush();

    return sw.toString();
  }

  /**
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    Map<Option, Object> options = null;
    try {
      options = parseCommandLine(args);
    } catch (Exception e) {
      System.out.println(RepositoryManager.getUsageString(false));
      System.exit(1);
    }

    if (options.containsKey(Option.HELP)) {
      System.out.println(RepositoryManager.getUsageString(true));
      System.exit(0);
    }

    File repository = (File) options.get(Option.REPOSITORY);
    String dataSource = (String) options.get(Option.DATA_SOURCE);
    Boolean verbose = (Boolean) options.get(Option.VERBOSE);
    if (verbose == null) verbose = Boolean.FALSE;

    try {
      // check if we are creating a repo
      if (options.containsKey(Option.CREATE_REPO)) {
        File directory = (File) options.get(Option.CREATE_REPO);
        createRepo(directory);
      } else if (options.containsKey(Option.PURGE_REPO)) {
        try {
          purgeRepo(repository, verbose);
        } finally {
          destroyApis();
        }

      } else if (options.containsKey(Option.LOAD_FILE)) {
        File sourceFile = (File) options.get(Option.LOAD_FILE);
        try {
          loadFile(repository, sourceFile, dataSource, verbose);
        } finally {
          destroyApis();
        }

      } else if (options.containsKey(Option.ADD_RECORD)) {
        String jsonRecord = (String) options.get(Option.ADD_RECORD);
        try {
          addRecord(repository, jsonRecord, dataSource, verbose);
        } finally {
          destroyApis();
        }

      } else if (options.containsKey(Option.CONFIG_SOURCES)) {
        Set<String> dataSources = (Set<String>) options.get(Option.CONFIG_SOURCES);
        try {
          configSources(repository, dataSources, verbose);
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
   */
  public static void createRepo(File directory) {
    if (directory.exists()) {
      throw new IllegalArgumentException(
          "Senzing repository directory already exists: " + directory);
    }
    try {
      directory.mkdirs();
      File dataDir    = new File(SENZING_DIR, "data");
      File templateDB = new File(dataDir, "G2C.db");

      copyFile(templateDB, new File(directory, "G2C.db"));
      copyFile(templateDB, new File(directory, "G2_RES.db"));
      copyFile(templateDB, new File(directory, "G2_LIB_FEAT.db"));

      File templateConfig = new File(dataDir, "g2config.json");
      File configFile = new File(directory, "g2config.json");
      copyFile(templateConfig, configFile);

      String fileSep = System.getProperty("file.separator");
      String sqlitePrefix = "sqlite3://na:na@" + directory.toString() + fileSep;

      File iniFile = new File(directory, "g2.ini");
      try (FileOutputStream fos = new FileOutputStream(iniFile);
           PrintWriter pw = new PrintWriter(new OutputStreamWriter(fos))) {
        pw.println("[PIPELINE]");
        pw.println(" SUPPORTPATH=" + dataDir.getCanonicalPath());
        pw.println();
        pw.println("[SQL]");
        pw.println(" BACKEND=HYBRID");
        pw.println(" CONNECTION=" + sqlitePrefix + "G2C.db");
        pw.println(" G2CONFIGFILE=" + configFile.getCanonicalPath());
        pw.println();
        pw.println("[HYBRID]");
        pw.println(" RES_FEAT=C1");
        pw.println(" RES_FEAT_EKEY=C1");
        pw.println(" RES_FEAT_LKEY=C1");
        pw.println(" RES_FEAT_STAT=C1");
        pw.println();
        pw.println(" LIB_FEAT=C2");
        pw.println(" LIB_FEAT_HKEY=C2");
        pw.println();
        pw.println("[C1]");
        pw.println(" CLUSTER_SIZE=1");
        pw.println(" DB_1=" + sqlitePrefix + "G2_RES.db");
        pw.println();
        pw.println("[C2]");
        pw.println(" CLUSTER_SIZE=1");
        pw.println(" DB_1=" + sqlitePrefix + "G2_LIB_FEAT.db");
        pw.flush();
      }

    } catch (IOException e) {
      e.printStackTrace();
      deleteRecursively(directory);
      throw new RuntimeException(e);
    }
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

  private static synchronized void initApis(File repository, boolean verbose) {
    if (!initialized) {
      File iniFile = new File(repository, "g2.ini");
      int returnCode = CONFIG_API.init("G2", iniFile.toString(), verbose);
      if (returnCode != 0) {
        logError("G2Config.init()", CONFIG_API);
        return;
      }
      returnCode = ENGINE_API.init("G2", iniFile.toString(), verbose);
      if (returnCode != 0) {
        CONFIG_API.destroy();
        logError("G2Engine.init()", ENGINE_API);
        return;
      }
      initialized = true;
    }
  }

  private static synchronized void destroyApis() {
    if (initialized) {
      ENGINE_API.destroy();
      CONFIG_API.destroy();
      initialized = false;
    }
  }

  /**
   * Shuts down the repository manager after use to ensure the native
   * Senzing API destroy() functions are called.
   */
  public static void conclude() {
    destroyApis();
  }

  private static void logError(String operation, G2Fallible fallible) {
    int errorCode = fallible.getLastExceptionCode();
    String message = fallible.getLastException();
    System.err.println();
    System.err.println("Operation Failed : " + operation);
    System.err.println("Error Code       : " + errorCode);
    System.err.println("Reason           : " + message);
    System.err.println();
  }

  private static Set<String> getDataSources() {
    Result<Long> configId = new Result<Long>();
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
    initApis(repository, verbose);
    int result = ENGINE_API.purgeRepository();
    if (result != 0) {
      logError("G2Engine.purgeRepository()", ENGINE_API);
    } else {
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
    return loadFile(repository, sourceFile, dataSource, null, null);
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
                    sourceFile,
                    dataSource,
                    false,
                    loadedCount,
                    failedCount);
  }
  /**
   * Loads a single CSV or JSON file to the repository -- optionally setting
   * the data source for all the records.  NOTE: if the records in the file do
   * not have a defined DATA_SOURCE then the specified data source is required.
   *
   * @param repository The directory for the repository.
   * @param sourceFile The source file to load (JSON or CSV).
   * @param dataSource The data source to use for loading the records.
   * @param verbose <tt>true</tt> for verbose API logging, otherwise
   *                <tt>false</tt>
   *
   * @return <tt>true</tt> if successful, otherwise <tt>false</tt>
   */
  public static boolean loadFile(File             repository,
                                 File             sourceFile,
                                 String           dataSource,
                                 boolean          verbose)
  {
    return loadFile(repository, sourceFile, dataSource, verbose, null, null);
  }

  /**
   * Loads a single CSV or JSON file to the repository -- optionally setting
   * the data source for all the records.  NOTE: if the records in the file do
   * not have a defined DATA_SOURCE then the specified data source is required.
   *
   * @param repository The directory for the repository.
   * @param sourceFile The source file to load (JSON or CSV).
   * @param dataSource The data source to use for loading the records.
   * @param verbose <tt>true</tt> for verbose API logging, otherwise
   *                <tt>false</tt>
   * @param loadedCount The output parameter for the number successfully loaded.
   * @param failedCount The output parameter for the number that failed to load.
   *
   * @return <tt>true</tt> if successful, otherwise <tt>false</tt>
   */
  public static boolean loadFile(File             repository,
                                 File             sourceFile,
                                 String           dataSource,
                                 boolean          verbose,
                                 Result<Integer>  loadedCount,
                                 Result<Integer>  failedCount)
  {
    String normalizedFileName = sourceFile.toString().toUpperCase();
    if ((!normalizedFileName.endsWith(".JSON"))
        && (!normalizedFileName.endsWith(".CSV"))) {
      throw new IllegalArgumentException(
          "File must be a CSV or JSON file: " + sourceFile);
    }

    initApis(repository, verbose);

    final Integer ZERO = new Integer(0);
    if (loadedCount != null) loadedCount.setValue(ZERO);
    if (failedCount != null) failedCount.setValue(ZERO);

    if (dataSource != null) dataSource = dataSource.toUpperCase();

    Set<String> dataSources = getDataSources();
    // check if the data source is configured
    if (dataSource != null && !dataSources.contains(dataSource)) {
      if (!addDataSource(repository, dataSource, verbose)) return false;
      dataSources.add(dataSource);
    }

    RecordProvider provider = null;
    // check the file type
    if (normalizedFileName.endsWith(".JSON")) {
      provider = provideJsonRecords(sourceFile, dataSource);
    } else if (normalizedFileName.endsWith(".CSV")) {
      provider = provideCsvRecords(sourceFile, dataSource);
    }
    if (provider == null) {
      return false;
    }

    String loadId = (new Date()).toString();
    int loaded = 0;
    int failed = 0;
    int loadedInterval = 100;
    int failedInterval = 100;
    PrintStream printStream = System.err;
    try {
      for (JsonObject record = provider.getNextRecord();
           (record != null);
           record = provider.getNextRecord())
      {
        String recordId = JsonUtils.getString(record, "RECORD_ID");
        String recordSource = JsonUtils.getString(record, "DATA_SOURCE");
        if (recordSource == null) {
          System.err.println();
          System.err.println(
              "If records in the file do not have a DATA_SOURCE then "
                  + Option.DATA_SOURCE.getCommandLineFlag() + " is required.");
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
          loadedInterval
              = doLoadFeedback(
                  "Loaded so far", loaded, loadedInterval, loaded, failed);

        } else {
          failed++;
          failedInterval
              = doLoadFeedback(
                  "Loaded so far", failed, failedInterval, loaded, failed);
        }
      }
      doLoadFeedback(
          "Loaded all records", loaded, 0, loaded, failed);
      processRedos();
      printStream = System.out;

      return true;

    } finally {
      if (loaded > 0 || failed > 0) {
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
      // set the counts
      if (failedCount != null) failedCount.setValue(failed);
      if (loadedCount != null) loadedCount.setValue(loaded);
    }
  }

  private static int processRedos() {
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
              "Redo's so far", failed, failedInterval, loaded, failed);
        } else {
          loaded++;
          loadedInterval = doLoadFeedback(
              "Redo's so far", loaded, loadedInterval, loaded, failed);
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

  private static int doLoadFeedback(String prefix,
                                    int count,
                                    int interval,
                                    int loaded,
                                    int failed)
  {
    if (count > (interval * 10)) {
      interval *= 10;
    }
    if ((count > 0) && ((interval == 0) || (count % interval) == 0)) {
      System.out.println(prefix + " (succeeded / failed): "
                         + loaded + " / " + failed);
    }
    return interval;
  }

  private static boolean addDataSource(File     repository,
                                       String   dataSource,
                                       boolean  verbose)
  {
    // add the data source and reinitialize
    boolean success = configSources(repository,
                                    Collections.singleton(dataSource),
                                    verbose);
    if (!success) return false;
    destroyApis();
    initApis(repository, verbose);
    return true;
  }

  private interface RecordProvider {
    JsonObject getNextRecord();
  }

  private static JsonObject augmentRecord(JsonObject   record,
                                          String       dataSource,
                                          File         sourceFile)
  {
    JsonObjectBuilder job = Json.createObjectBuilder(record);
    if (dataSource != null) {
      if (record.containsKey("DATA_SOURCE")) {
        job.remove("DATA_SOURCE");
      }
      if (record.containsKey("ENTITY_TYPE")) {
        job.remove("ENTITY_TYPE");
      }
      job.add("DATA_SOURCE", dataSource);
      job.add("ENTITY_TYPE", dataSource);
    }
    job.add("SOURCE_ID", sourceFile.toString());
    return job.build();
  }

  private static class JsonArrayRecordProvider implements RecordProvider {
    private Iterator<JsonObject> recordIter;
    private String dataSource;
    private File sourceFile;

    public JsonArrayRecordProvider(File sourceFile, String dataSource) {
      this.sourceFile = sourceFile;
      JsonParserFactory jpf = Json.createParserFactory(Collections.emptyMap());
      try {
        FileInputStream    fis = new FileInputStream(sourceFile);
        InputStreamReader  isr = new InputStreamReader(fis, "UTF-8");
        BufferedReader     br  = new BufferedReader(isr);

       this.recordIter = jpf.createParser(br).getArrayStream()
                            .map(jv -> (JsonObject) jv).iterator();

       this.dataSource = dataSource;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    public JsonObject getNextRecord() {
      if (this.recordIter.hasNext()) {
        return augmentRecord(this.recordIter.next(), this.dataSource, this.sourceFile);
      } else {
        return null;
      }
    }
  }

  private static class JsonRecordProvider implements RecordProvider {
    private BufferedReader reader;
    private String dataSource;
    private File sourceFile;

    public JsonRecordProvider(File sourceFile, String dataSource) {
      JsonParserFactory jpf = Json.createParserFactory(Collections.emptyMap());
      try {
        FileInputStream    fis = new FileInputStream(sourceFile);
        InputStreamReader  isr = new InputStreamReader(fis, "UTF-8");
        this.reader = new BufferedReader(isr);
        this.dataSource = dataSource;

      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    public JsonObject getNextRecord() {
      try {
        JsonObject record = null;
        while (this.reader != null && record == null) {
          // read the next line and check for EOF
          String line = this.reader.readLine();
          if (line == null) {
            this.reader.close();
            this.reader = null;
            continue;
          }

          // trim the line of extra whitespace
          line = line.trim();

          // check for blank lines and skip them
          if (line.length() == 0) continue;

          // check if the line begins with a "#" for a comment lines
          if (line.startsWith("#")) continue;

          // check if the line does NOT start with "{"
          if (!line.startsWith("{")) {
            throw new IllegalStateException(
                "Line does not appear to be JSON record: " + line);
          }

          // parse the line
          record = JsonUtils.parseJsonObject(line);
        }

        return augmentRecord(record, this.dataSource, this.sourceFile);

      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static class CsvRecordProvider implements RecordProvider {
    private Iterator<CSVRecord> recordIter;
    private String dataSource;
    private File sourceFile;

    public CsvRecordProvider(File sourceFile, String dataSource) {
      this.dataSource = dataSource;
      this.sourceFile = sourceFile;
      CSVFormat csvFormat = CSVFormat.DEFAULT
          .withFirstRecordAsHeader().withIgnoreEmptyLines(true).withTrim(true);

      try {
        FileInputStream    fis = new FileInputStream(sourceFile);
        InputStreamReader  isr = new InputStreamReader(fis, "UTF-8");
        BufferedReader     br  = new BufferedReader(isr);

        CSVParser parser = new CSVParser(br, csvFormat);
        Map<String, Integer> headerMap = parser.getHeaderMap();
        Set<String> headers = new HashSet<>();
        headerMap.keySet().forEach(h -> {
          headers.add(h.toUpperCase());
        });
        if (dataSource == null && !headers.contains("DATA_SOURCE")) {
          throw new IllegalStateException(
              "The " + Option.DATA_SOURCE.getCommandLineFlag() + " option is "
              + "required if DATA_SOURCE missing from CSV");
        }
        this.recordIter = parser.iterator();

      } catch (IOException e) {
        throw new RuntimeException(e);
      }

    }
    public JsonObject getNextRecord() {
      try {
        if (!this.recordIter.hasNext()) return null;
        CSVRecord record = this.recordIter.next();
        Map<String,String> recordMap = record.toMap();
        Iterator<Map.Entry<String,String>> entryIter
            = recordMap.entrySet().iterator();
        while (entryIter.hasNext()) {
          Map.Entry<String,String> entry = entryIter.next();
          String value = entry.getValue();
          if (value == null || value.trim().length() == 0) {
            entryIter.remove();
          }
        }
        if (this.dataSource != null) {
          recordMap.put("DATA_SOURCE", this.dataSource);
          recordMap.put("ENTITY_TYPE", this.dataSource);
        }
        recordMap.put("SOURCE_ID", this.sourceFile.toString());

        Map<String,Object> map = (Map) recordMap;
        return Json.createObjectBuilder(map).build();

      } catch (RuntimeException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

  }

  private static RecordProvider provideJsonRecords(File    sourceFile,
                                                   String  dataSource)
  {
    // check if we have a real JSON array
    boolean array = false;
    try (FileInputStream    fis = new FileInputStream(sourceFile);
         InputStreamReader  isr = new InputStreamReader(fis, "UTF-8");
         BufferedReader     br  = new BufferedReader(isr))
    {
      for (int nextChar = br.read(); nextChar >= 0; nextChar = br.read()) {
        if (!Character.isWhitespace((char) nextChar)) {
          if (((char) nextChar) == '[') {
            array = true;
          }
          break;
        }
      }

    } catch (IOException e) {
      e.printStackTrace();
      System.err.println();
      System.err.println("Failed to read file: " + sourceFile);
      System.err.println();
      return null;
    }

    if (array) {
      return new JsonArrayRecordProvider(sourceFile, dataSource);

    } else {
      return new JsonRecordProvider(sourceFile, dataSource);
    }
  }

  private static RecordProvider provideCsvRecords(File   sourceFile,
                                                  String dataSource)
  {
    return new CsvRecordProvider(sourceFile, dataSource);
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
    return addRecord(repository, jsonRecord, dataSource, false);
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
                                  String   jsonRecord,
                                  String   dataSource,
                                  boolean  verbose)
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

    processRedos();

    System.out.println();
    System.out.println("Added record to " + dataSource + " data source: ");
    System.out.println(jsonRecord);
    System.out.println();

    return true;
  }

  /**
   * Configures the specified data sources for the specified repository
   * if not already configured.
   *
   * @param repository The directory for the repository.
   * @param dataSources The {@link List} of data source names.
   *
   * @return <tt>true</tt> if successful, otherwise <tt>false</tt>
   */
  public static boolean configSources(File         repository,
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
   * @return <tt>true</tt> if successful, otherwise <tt>false</tt>
   */
  public static boolean configSources(File         repository,
                                      Set<String>  dataSources,
                                      boolean      verbose)
  {
    initApis(repository, verbose);
    Result<Long> configId = new Result<Long>();
    int returnCode = 0;
    try {
      Set<String> existingSet = getDataSources(configId);

      Map<String, Boolean> dataSourceActions = new LinkedHashMap<>();
      for (String dataSource : dataSources) {
        if (existingSet.contains(dataSource)) {
          dataSourceActions.put(dataSource, false);
          continue;
        }
        returnCode = CONFIG_API.addDataSource(configId.getValue(),
                                                  dataSource);
        if (returnCode != 0) {
          logError("G2Config.addDataSource()", CONFIG_API);
          return false;
        }
        dataSourceActions.put(dataSource, true);
      }
      StringBuffer sb = new StringBuffer();
      returnCode = CONFIG_API.save(configId.getValue(), sb);
      if (returnCode != 0) {
        logError("G2Config.save()", CONFIG_API);
        return false;
      }

      File configFile = new File(repository, "g2config.json");
      try (FileOutputStream fos = new FileOutputStream(configFile);
           OutputStreamWriter osw = new OutputStreamWriter(fos, "UTF-8"))
      {
        osw.write(sb.toString());
        osw.flush();

      } catch (IOException e) {
        e.printStackTrace();
        System.err.println();
        System.err.println("Failed to save updated config: " + configFile);
        System.err.println();
        return false;
      }

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

    } finally {
      if (configId.getValue() != null) {
        CONFIG_API.close(configId.getValue());
      }
    }

    return true;
  }

}
