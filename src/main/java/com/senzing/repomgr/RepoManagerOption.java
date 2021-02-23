package com.senzing.repomgr;

import com.senzing.cmdline.CommandLineOption;

import java.util.*;

import static java.util.EnumSet.*;

enum RepoManagerOption implements CommandLineOption<RepoManagerOption> {
  HELP("--help", true, 0),
  CREATE_REPO("--createRepo", true, 1),
  PURGE_REPO("--purgeRepo", true, 0),
  LOAD_FILE("--loadFile", true, 1),
  ADD_RECORD("--addRecord", true, 1),
  CONFIG_SOURCES("--configSources", true),
  CONFIG_ENTITY_TYPES("--configEntityTypes", true),
  DATA_SOURCE("-dataSource", false, 1),
  ENTITY_TYPE("-entityType", false, 1),
  REPOSITORY("-repo", false, 1),
  VERBOSE("-verbose", false, 0);

  RepoManagerOption(String commandLineFlag) {
    this(commandLineFlag, false, -1);
  }
  RepoManagerOption(String commandLineFlag, boolean primary) {
    this(commandLineFlag, primary, -1);
  }
  RepoManagerOption(String commandLineFlag, int parameterCount) {
    this(commandLineFlag, false, parameterCount);
  }
  RepoManagerOption(String  commandLineFlag,
                    boolean primary,
                    int     parameterCount)
  {
    this(commandLineFlag,
         primary,
         parameterCount < 0 ? 0 : parameterCount,
         parameterCount);
  }
  RepoManagerOption(String  commandLineFlag,
                    boolean primary,
                    int     minParameterCount,
                    int     maxParameterCount)
  {
    this.commandLineFlag = commandLineFlag;
    this.primary         = primary;
    this.minParamCount   = minParameterCount;
    this.maxParamCount   = maxParameterCount;
    this.conflicts       = null;
    this.dependencies    = null;
  }

  private static Map<String, RepoManagerOption> OPTIONS_BY_FLAG;

  private String commandLineFlag;
  private boolean primary;
  private int minParamCount;
  private int maxParamCount;
  private Set<RepoManagerOption> conflicts;
  private Set<Set<RepoManagerOption>>  dependencies;

  public static final EnumSet<RepoManagerOption> PRIMARY_OPTIONS
      = complementOf(of(DATA_SOURCE, REPOSITORY, VERBOSE));

  public String getCommandLineFlag() {
    return this.commandLineFlag;
  }

  public int getMinimumParameterCount() { return this.minParamCount; }

  public int getMaximumParameterCount() { return this.maxParamCount; }

  public boolean isPrimary() { return this.primary; }

  public boolean isDeprecated() { return false; };

  public Set<RepoManagerOption> getConflicts() {
    return this.conflicts;
  }

  public Set<Set<RepoManagerOption>> getDependencies() {
    return this.dependencies;
  }

  public static RepoManagerOption lookup(String commandLineFlag) {
    return OPTIONS_BY_FLAG.get(commandLineFlag.toLowerCase());
  }

  static {
    Map<String, RepoManagerOption> lookupMap = new LinkedHashMap<>();
    for (RepoManagerOption opt: values()) {
      lookupMap.put(opt.getCommandLineFlag(), opt);
    }
    OPTIONS_BY_FLAG = Collections.unmodifiableMap(lookupMap);

    HELP.conflicts = complementOf(EnumSet.of(HELP));
    HELP.dependencies = Collections.singleton(noneOf(RepoManagerOption.class));
    CREATE_REPO.conflicts = complementOf(of(CREATE_REPO, VERBOSE));
    CREATE_REPO.dependencies = Collections.singleton(noneOf(RepoManagerOption.class));
    PURGE_REPO.conflicts = complementOf(of(PURGE_REPO, REPOSITORY, VERBOSE));
    PURGE_REPO.dependencies = Collections.singleton(of(REPOSITORY));
    LOAD_FILE.conflicts
        = complementOf(of(LOAD_FILE, REPOSITORY, DATA_SOURCE, ENTITY_TYPE, VERBOSE));
    LOAD_FILE.dependencies = Collections.singleton(of(REPOSITORY));
    ADD_RECORD.conflicts
        = complementOf(of(ADD_RECORD, REPOSITORY, DATA_SOURCE, ENTITY_TYPE, VERBOSE));
    ADD_RECORD.dependencies = Collections.singleton(of(REPOSITORY));
    CONFIG_SOURCES.conflicts
        = complementOf(of(CONFIG_SOURCES, REPOSITORY, VERBOSE));
    CONFIG_SOURCES.dependencies = Collections.singleton(of(REPOSITORY));
    CONFIG_ENTITY_TYPES.conflicts
        = complementOf(of(CONFIG_ENTITY_TYPES, REPOSITORY, VERBOSE));
    CONFIG_ENTITY_TYPES.dependencies = Collections.singleton(of(REPOSITORY));
    DATA_SOURCE.conflicts
        = complementOf(of(DATA_SOURCE, LOAD_FILE, ADD_RECORD, VERBOSE, REPOSITORY));
    DATA_SOURCE.dependencies = Collections.singleton(noneOf(RepoManagerOption.class));
    ENTITY_TYPE.conflicts
        = complementOf(of(ENTITY_TYPE, LOAD_FILE, ADD_RECORD, VERBOSE, REPOSITORY));
    ENTITY_TYPE.dependencies = Collections.singleton(noneOf(RepoManagerOption.class));
    REPOSITORY.conflicts = of(HELP, CREATE_REPO);
    REPOSITORY.dependencies = Collections.singleton(noneOf(RepoManagerOption.class));
  }
}
