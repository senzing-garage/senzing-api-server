package com.senzing.configmgr;

import com.senzing.cmdline.CommandLineOption;

import java.util.*;

import static java.util.EnumSet.*;
import static java.util.EnumSet.of;

enum ConfigManagerOption implements CommandLineOption<ConfigManagerOption>
{
  /**
   * <p>
   * Option for displaying help/usage for the configuration manager.  This
   * option can only be provided by itself and has no parameters.
   * </p>
   * <p>
   * This option can be specified in the following ways:
   * <ul>
   *   <li>Command Line: <tt>--help</tt></li>
   * </ul>
   * </p>
   */
  HELP("--help", null, true, 0),

  /**
   * <p>
   * This presence of this option causes the Senzing API's to be initialized in
   * verbose mode, but its absence causes the Senzing API's in standard mode
   * (the default).  A single parameter may optionally be specified as
   * <tt>true</tt> or <tt>false</tt> with <tt>false</tt> simulating the absence
   * of the option.
   * </p>
   * <p>
   * This option can be specified in the following ways:
   * <ul>
   *   <li>Command Line: <tt>--verbose [true|false]</tt></li>
   *   <li>Command Line: <tt>-verbose [true|false]</tt></li>
   * </ul>
   * </p>
   */
  VERBOSE("--verbose", Set.of("-verbose"), 0),

  /**
   * <p>
   * Option for specifying the JSON init file to initialize the Senzing API's
   * with.  The parameter to this option should be a file path to a JSON init
   * file.  Alternatively, one can specify {@link #INIT_JSON} or
   * {@link #INIT_ENV_VAR}.
   * </p>
   * <p>
   * This option can be specified in the following ways:
   * <ul>
   *   <li>Command Line: <tt>--init-file {file-path}</tt></li>
   *   <li>Command Line: <tt>-initFile {file-path}</tt></li>
   * </ul>
   * </p>
   */
  INIT_FILE("--init-file", Set.of("-initFile"), 1),

  /**
   * <p>
   * Option for specifying the JSON text to initialize the Senzing API's
   * with.  The parameter to this option should be the actual JSON text with
   * which to initialize.  Alternatively, one can specify {@link #INIT_FILE} or
   * {@link #INIT_ENV_VAR}.
   * </p>
   * <p>
   * This option can be specified in the following ways:
   * <ul>
   *   <li>Command Line: <tt>--init-json {json-text}</tt></li>
   *   <li>Command Line: <tt>-initJson {json-text}</tt></li>
   * </ul>
   * </p>
   */
  INIT_JSON("--init-json", Set.of("-initJson"), 1),

  /**
   * <p>
   * Option for specifying the name of an environment variable which contains
   * the JSON text to initialize the Senzing API's with.  The parameter to this
   * option should be the name of the environment variable which contians the
   * actual JSON text with which to initialize.  Alternatively, one can specify
   * {@link #INIT_FILE} or {@link #INIT_JSON}.
   * </p>
   * <p>
   * This option can be specified in the following ways:
   * <ul>
   *   <li>Command Line: <tt>--init-env-var {env-var-name}</tt></li>
   *   <li>Command Line: <tt>-initEnvVar {env-var-name}</tt></li>
   * </ul>
   * </p>
   */
  INIT_ENV_VAR("--init-env-var", Set.of("-initEnvVar"), 1),

  /**
   * <p>
   * Option used with the {@link #EXPORT_CONFIG} and
   * {@link #SET_DEFAULT_CONFIG_ID} options to specify the ID of the
   * configuration to use.
   * </p>
   * <p>
   * This option can be specified in the following ways:
   * <ul>
   *   <li>Command Line: <tt>--config-id {config-id}</tt></li>
   *   <li>Command Line: <tt>-configId {config-id}</tt></li>
   * </ul>
   * </p>
   */
  CONFIG_ID("--config-id", Set.of("-configId"), 1),

  /**
   * <p>
   * Option used to list the available configurations and their IDs.  This
   * requires one of the following options:
   * <ul>
   *   <li>{@link #INIT_FILE}</li>
   *   <li>{@link #INIT_JSON}</li>
   *   <li>{@link #INIT_ENV_VAR}</li>
   * </ul>
   * </p>
   * <p>
   * This option can be specified in the following ways:
   * <ul>
   *   <li>Command Line: <tt>--list-configs</tt></li>
   *   <li>Command Line: <tt>--listConfigs</tt></li>
   * </ul>
   * </p>
   */
  LIST_CONFIGS("--list-configs",
               Set.of("--listConfigs"),true, 0),

  /**
   * <p>
   * Option used to get the default configuration ID from the repository and
   * prints it.  This requires one of the following options:
   * <ul>
   *   <li>{@link #INIT_FILE}</li>
   *   <li>{@link #INIT_JSON}</li>
   *   <li>{@link #INIT_ENV_VAR}</li>
   * </ul>
   * </p>
   * <p>
   * This option can be specified in the following ways:
   * <ul>
   *   <li>Command Line: <tt>--get-default-config</tt></li>
   *   <li>Command Line: <tt>--getDefaultConfig</tt></li>
   * </ul>
   * </p>
   */
  GET_DEFAULT_CONFIG_ID("--get-default-config",
                        Set.of("--getDefaultConfig"),
                        true, 0),

  /**
   * <p>
   * Option used to set the default configuration ID.  This requires the
   * {@link #CONFIG_ID} and one of the following options:
   * <ul>
   *   <li>{@link #INIT_FILE}</li>
   *   <li>{@link #INIT_JSON}</li>
   *   <li>{@link #INIT_ENV_VAR}</li>
   * </ul>
   * </p>
   * <p>
   * This option can be specified in the following ways:
   * <ul>
   *   <li>Command Line: <tt>--set-default-config</tt></li>
   *   <li>Command Line: <tt>--setDefaultConfig</tt></li>
   * </ul>
   * </p>
   */
  SET_DEFAULT_CONFIG_ID("--set-default-config",
                        Set.of("--setDefaultConfig"),
                        true, 0),

  /**
   * <p>
   * Option to export the configuration specified by the {@link #CONFIG_ID}
   * option to the specified file.  If the {@link #CONFIG_ID} option is not
   * specified then the current default configuration is exported.  If no
   * default configuration then an error message will be displayed.  This
   * accepts an optional parameter representing the file to export the config
   * to, if not provided the configuration is written to stdout.  This respects
   * the {@link #CONFIG_ID} and requires one of the following options:
   * <ul>
   *   <li>{@link #INIT_FILE}</li>
   *   <li>{@link #INIT_JSON}</li>
   *   <li>{@link #INIT_ENV_VAR}</li>
   * </ul>
   * </p>
   * <p>
   * This option can be specified in the following ways:
   * <ul>
   *   <li>Command Line: <tt>--export-config</tt></li>
   *   <li>Command Line: <tt>--exportConfig</tt></li>
   * </ul>
   * </p>
   */
  EXPORT_CONFIG("--export-config",
                Set.of("--exportConfig"),
                true, 0, 1),

  /**
   * <p>
   * Option to import the JSON configuration contained in the file specified by
   * the provided file path using the optional description.  The configuration
   * ID for the imported configuration will be output to stdout.  This requires
   * one of the following options:
   * <ul>
   *   <li>{@link #INIT_FILE}</li>
   *   <li>{@link #INIT_JSON}</li>
   *   <li>{@link #INIT_ENV_VAR}</li>
   * </ul>
   * </p>
   * <p>
   * This option can be specified in the following ways:
   * <ul>
   *   <li>Command Line: <tt>--import-config {config-file-path} [description]</tt></li>
   *   <li>Command Line: <tt>--importConfig {config-file-path} [description]</tt></li>
   * </ul>
   * </p>
   */
  IMPORT_CONFIG("--import-config",
                Set.of("--importConfig"),
                true, 1, 2),

  /**
   * <p>
   * Option to migrate the specified INI file to the JSON initialization
   * parameters and imports any refrenced configuration file and sets it as the
   * default configuration.  If a different configuration is already configured
   * as the default then it is left in place and a warning is displayed.  The
   * first parameter is the path to the INI file and the second optional
   * parameter specifies the file path to write the JSON initialization
   * parameters to.  If the second parameter is not provided then the JSON
   * initialization parameters are written to stdout.
   * </p>
   * <p>
   * This option can be specified in the following ways:
   * <ul>
   *   <li>Command Line: <tt>--migrate-ini {ini-file-path} [init-json-file]</tt></li>
   *   <li>Command Line: <tt>--migrateIni {ini-file-path} [init-json-file]</tt></li>
   * </ul>
   * </p>
   */
  MIGRATE_INI_FILE("--migrate-ini",
                   Set.of("--migrateIni"),
                   true, 1, 2);

  ConfigManagerOption(String      commandLineFlag,
                      Set<String> synonymFlags,
                      int         parameterCount)
  {
    this(commandLineFlag, synonymFlags, false, parameterCount);
  }

  ConfigManagerOption(String      commandLineFlag,
                      Set<String> synonymFlags,
                      boolean     primary,
                      int         parameterCount)
  {
    this(commandLineFlag,
         synonymFlags,
         primary,
         (parameterCount < 0) ? 0 : parameterCount,
         parameterCount);
  }

  ConfigManagerOption(String      commandLineFlag,
                      Set<String> synonymFlags,
                      boolean     primary,
                      int         minParameterCount,
                      int         maxParameterCount)
  {
    this.commandLineFlag = commandLineFlag;
    this.primary         = primary;
    this.minParamCount   = minParameterCount;
    this.maxParamCount   = maxParameterCount;
    this.conflicts       = null;
    this.dependencies    = null;
    this.synonymFlags    = (synonymFlags == null)
        ? Collections.emptySet() : Set.copyOf(synonymFlags);
  }

  private static Map<String, ConfigManagerOption> OPTIONS_BY_FLAG;

  private String commandLineFlag;
  private Set<String> synonymFlags;
  private int minParamCount;
  private int maxParamCount;
  private boolean primary;
  private EnumSet<ConfigManagerOption> conflicts;
  private Set<Set<ConfigManagerOption>> dependencies;

  public static final EnumSet<ConfigManagerOption> PRIMARY_OPTIONS
      = complementOf(EnumSet.of(LIST_CONFIGS,
                                GET_DEFAULT_CONFIG_ID,
                                SET_DEFAULT_CONFIG_ID,
                                EXPORT_CONFIG,
                                IMPORT_CONFIG,
                                MIGRATE_INI_FILE));

  @Override
  public String getCommandLineFlag() {
    return this.commandLineFlag;
  }

  @Override
  public Set<String> getSynonymFlags() {
    return this.synonymFlags;
  }

  @Override
  public int getMinimumParameterCount() { return this.minParamCount; }

  @Override
  public int getMaximumParameterCount() { return this.maxParamCount; }

  @Override
  public boolean isPrimary() { return this.primary; }

  @Override
  public boolean isDeprecated() { return false; };

  @Override
  public Set<ConfigManagerOption> getConflicts() {
    return this.conflicts;
  }

  @Override
  public Set<Set<ConfigManagerOption>> getDependencies() {
    return this.dependencies;
  }

  public static ConfigManagerOption lookup(String commandLineFlag) {
    return OPTIONS_BY_FLAG.get(commandLineFlag.toLowerCase());
  }

  static {
    try {
      Map<String, ConfigManagerOption> lookupMap = new LinkedHashMap<>();
      for (ConfigManagerOption opt: values()) {
        lookupMap.put(opt.getCommandLineFlag(), opt);
      }
      OPTIONS_BY_FLAG = Collections.unmodifiableMap(lookupMap);

      Set<Set<ConfigManagerOption>> nodeps = Collections.singleton(noneOf(ConfigManagerOption.class));

      HELP.conflicts = complementOf(EnumSet.of(HELP));
      HELP.dependencies = nodeps;

      INIT_FILE.conflicts = of(INIT_JSON, INIT_ENV_VAR, MIGRATE_INI_FILE);
      INIT_FILE.dependencies = nodeps;

      INIT_JSON.conflicts = of(INIT_FILE, INIT_ENV_VAR, MIGRATE_INI_FILE);
      INIT_JSON.dependencies = nodeps;

      INIT_ENV_VAR.conflicts = of(INIT_FILE, INIT_JSON, MIGRATE_INI_FILE);
      INIT_ENV_VAR.dependencies = nodeps;

      Set<Set<ConfigManagerOption>> initDeps = new LinkedHashSet<>();
      initDeps.add(of(INIT_FILE));
      initDeps.add(of(INIT_ENV_VAR));
      initDeps.add(of(INIT_JSON));
      initDeps = Collections.unmodifiableSet(initDeps);

      LIST_CONFIGS.conflicts = complementOf(
          of(INIT_FILE, INIT_JSON, INIT_ENV_VAR, VERBOSE));
      LIST_CONFIGS.dependencies = initDeps;

      GET_DEFAULT_CONFIG_ID.conflicts = complementOf(
          of(INIT_FILE, INIT_JSON, INIT_ENV_VAR, VERBOSE));
      GET_DEFAULT_CONFIG_ID.dependencies = initDeps;

      IMPORT_CONFIG.conflicts = complementOf(
          of(INIT_FILE, INIT_JSON, INIT_ENV_VAR, VERBOSE));
      IMPORT_CONFIG.dependencies = initDeps;

      Set<Set<ConfigManagerOption>> initConfigDeps = new LinkedHashSet<>();
      initConfigDeps.add(of(INIT_FILE, CONFIG_ID));
      initConfigDeps.add(of(INIT_ENV_VAR, CONFIG_ID));
      initConfigDeps.add(of(INIT_JSON, CONFIG_ID));
      initConfigDeps = Collections.unmodifiableSet(initConfigDeps);

      SET_DEFAULT_CONFIG_ID.conflicts = complementOf(
          of(INIT_FILE, INIT_JSON, INIT_ENV_VAR, VERBOSE, CONFIG_ID));
      SET_DEFAULT_CONFIG_ID.dependencies = initConfigDeps;

      EXPORT_CONFIG.conflicts = complementOf(
          of(INIT_FILE, INIT_JSON, INIT_ENV_VAR, VERBOSE, CONFIG_ID));
      EXPORT_CONFIG.dependencies = initDeps;

      MIGRATE_INI_FILE.conflicts = complementOf(of(VERBOSE));
      MIGRATE_INI_FILE.dependencies = nodeps;

    } catch (Exception e) {
      e.printStackTrace();
      throw new ExceptionInInitializerError(e);
    }
  }
}
