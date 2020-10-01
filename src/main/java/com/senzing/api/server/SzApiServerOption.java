package com.senzing.api.server;

import com.senzing.cmdline.CommandLineOption;

import java.util.*;

import static com.senzing.util.CollectionUtilities.recursivelyUnmodifiableMap;

/**
 * Describes the command-line options for {@link SzApiServer}.
 */
enum SzApiServerOption implements CommandLineOption<SzApiServerOption> {
  HELP("-help", true, 0),
  VERSION("-version", true, 0),
  HTTP_PORT("-httpPort", 1),
  BIND_ADDRESS("-bindAddr", 1),
  MODULE_NAME("-moduleName", 1),
  INI_FILE("-iniFile", true, 1),
  INIT_FILE("-initFile", true, 1),
  INIT_JSON("-initJson", true, 1),
  INIT_ENV_VAR("-initEnvVar", true, 1),
  CONFIG_ID("-configId", 1),
  READ_ONLY("-readOnly", 0),
  ENABLE_ADMIN("-enableAdmin", 0),
  VERBOSE("-verbose", 0),
  QUIET("-quiet", 0),
  MONITOR_FILE("-monitorFile", 1),
  CONCURRENCY("-concurrency", 1),
  AUTO_REFRESH_PERIOD("-autoRefreshPeriod", 1),
  ALLOWED_ORIGINS("-allowedOrigins", 1),
  STATS_INTERVAL("-statsInterval", 1),
  SKIP_STARTUP_PERF("-skipStartupPerf", 0);

  private static Map<SzApiServerOption, Set<SzApiServerOption>> CONFLICTING_OPTIONS;

  private static Map<SzApiServerOption, Set<SzApiServerOption>> ALTERNATIVE_OPTIONS;

  private static Map<String, SzApiServerOption> OPTIONS_BY_FLAG;

  private boolean primary;

  private boolean deprecated;

  private String cmdLineFlag;

  private int minParamCount;

  private int maxParamCount;

  SzApiServerOption(String cmdLineFlag) {
    this(cmdLineFlag, false);
  }
  SzApiServerOption(String cmdLineFlag, boolean deprecated) {
    this(cmdLineFlag, false, -1, deprecated);
  }
  SzApiServerOption(String cmdLineFlag, int parameterCount) {
    this(cmdLineFlag, false, parameterCount, false);
  }
  SzApiServerOption(String cmdLineFlag, boolean primary, int parameterCount) {
    this(cmdLineFlag, primary, parameterCount, false);
  }
  SzApiServerOption(String   cmdLineFlag,
                    boolean  primary,
                    int      parameterCount,
                    boolean  deprecated)
  {
    this(cmdLineFlag,
         primary,
         parameterCount < 0 ? 0 : parameterCount,
         parameterCount,
         deprecated);
  }

  SzApiServerOption(String   cmdLineFlag,
                    boolean  primary,
                    int      minParameterCount,
                    int      maxParameterCount,
                    boolean  deprecated)
  {
    this.cmdLineFlag    = cmdLineFlag;
    this.primary        = primary;
    this.minParamCount  = minParameterCount;
    this.maxParamCount  = maxParameterCount;
    this.deprecated     = deprecated;
  }

  public int getMinimumParameterCount() { return this.minParamCount; }

  public int getMaximumParameterCount() { return this.maxParamCount; }

  public boolean isPrimary() {
    return this.primary;
  }

  public boolean isDeprecated() {
    return this.deprecated;
  }

  public String getCommandLineFlag() {
    return this.cmdLineFlag;
  }

  public Set<SzApiServerOption> getConflictingOptions() {
    return CONFLICTING_OPTIONS.get(this);
  }

  public Set<SzApiServerOption> getAlternatives() {
    return ALTERNATIVE_OPTIONS.get(this);
  }

  public static SzApiServerOption lookup(String commandLineFlag) {
    return OPTIONS_BY_FLAG.get(commandLineFlag.toLowerCase());
  }

  static {
    try {
      Map<SzApiServerOption,Set<SzApiServerOption>> conflictMap = new LinkedHashMap<>();
      Map<SzApiServerOption,Set<SzApiServerOption>> altMap = new LinkedHashMap<>();
      Map<String, SzApiServerOption> lookupMap = new LinkedHashMap<>();

      for (SzApiServerOption option : SzApiServerOption.values()) {
        conflictMap.put(option, new LinkedHashSet<>());
        altMap.put(option, new LinkedHashSet<>());
        lookupMap.put(option.getCommandLineFlag().toLowerCase(), option);
      }
      SzApiServerOption[] exclusiveOptions = { HELP, VERSION };
      for (SzApiServerOption option : SzApiServerOption.values()) {
        for (SzApiServerOption exclOption : exclusiveOptions) {
          Set<SzApiServerOption> set = conflictMap.get(exclOption);
          set.add(option);
          set = conflictMap.get(option);
          set.add(exclOption);
        }
      }
      SzApiServerOption[] initOptions = { INI_FILE, INIT_ENV_VAR, INIT_FILE, INIT_JSON };
      for (SzApiServerOption option1 : initOptions) {
        for (SzApiServerOption option2 : initOptions) {
          if (option1 != option2) {
            Set<SzApiServerOption> set = conflictMap.get(option1);
            set.add(option2);
          }
        }
      }

      Set<SzApiServerOption> iniAlts = altMap.get(INI_FILE);
      iniAlts.add(INIT_ENV_VAR);
      iniAlts.add(INIT_FILE);
      iniAlts.add(INIT_JSON);

      CONFLICTING_OPTIONS = recursivelyUnmodifiableMap(conflictMap);
      ALTERNATIVE_OPTIONS = recursivelyUnmodifiableMap(altMap);
      OPTIONS_BY_FLAG = Collections.unmodifiableMap(lookupMap);

    } catch (Exception e) {
      e.printStackTrace();
      throw new ExceptionInInitializerError(e);
    }
  }
}
