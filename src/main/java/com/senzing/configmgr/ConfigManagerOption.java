package com.senzing.configmgr;

import com.senzing.cmdline.CommandLineOption;

import java.util.*;

import static java.util.EnumSet.*;
import static java.util.EnumSet.of;

enum ConfigManagerOption implements CommandLineOption<ConfigManagerOption>
{
  HELP("--help", true, 0),
  VERBOSE("-verbose", 0),
  INIT_FILE("-initFile", 1),
  INIT_JSON("-initJson", 1),
  INIT_ENV_VAR("-initEnvVar", 1),
  CONFIG_ID("-configId", 1),
  LIST_CONFIGS("--listConfigs", true, 0),
  GET_DEFAULT_CONFIG_ID("--getDefaultConfig", true, 0),
  SET_DEFAULT_CONFIG_ID("--setDefaultConfig", true, 0),
  EXPORT_CONFIG("--exportConfig", true, 0, 1),
  IMPORT_CONFIG("--importConfig", true, 1, 2),
  MIGRATE_INI_FILE("--migrateIni", true, 1, 2);

  ConfigManagerOption(String commandLineFlag) {
    this(commandLineFlag, false, -1);
  }
  ConfigManagerOption(String commandLineFlag, boolean primary) {
    this(commandLineFlag, primary, -1);
  }
  ConfigManagerOption(String commandLineFlag, int parameterCount) {
    this(commandLineFlag, false, parameterCount);
  }
  ConfigManagerOption(String   commandLineFlag,
                      boolean  primary,
                      int      parameterCount)
  {
    this(commandLineFlag,
         primary,
         (parameterCount < 0) ? 0 : parameterCount,
         parameterCount);
  }
  ConfigManagerOption(String   commandLineFlag,
                      boolean  primary,
                      int      minParameterCount,
                      int      maxParameterCount)
  {
    this.commandLineFlag = commandLineFlag;
    this.primary         = primary;
    this.minParamCount   = minParameterCount;
    this.maxParamCount   = maxParameterCount;
    this.conflicts       = null;
    this.dependencies    = null;
  }

  private static Map<String, ConfigManagerOption> OPTIONS_BY_FLAG;

  private String commandLineFlag;
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

  public String getCommandLineFlag() {
    return this.commandLineFlag;
  }

  public int getMinimumParameterCount() { return this.minParamCount; }

  public int getMaximumParameterCount() { return this.maxParamCount; }

  public boolean isPrimary() { return this.primary; }

  public boolean isDeprecated() { return false; };

  public Set<ConfigManagerOption> getConflicts() {
    return this.conflicts;
  }

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
