package com.senzing.repomgr;

import com.senzing.cmdline.CommandLineOption;

import java.util.*;

import static java.util.EnumSet.*;

enum RepoManagerOption implements CommandLineOption<RepoManagerOption> {
  HELP("--help", true, 0),
  CREATE_REPO("--create-repo", true, 1),
  PURGE_REPO("--purge-repo", true, 0),
  LOAD_FILE("--load-file", true, 1),
  ADD_RECORD("--add-record", true, 1),
  CONFIG_SOURCES("--config-sources", true),
  CONFIG_ENTITY_TYPES("--config-entity-types", true),
  DATA_SOURCE("--data-source", false, 1),
  ENTITY_TYPE("--entity-type", false, 1),
  REPOSITORY("--repo", false, 1),
  VERBOSE("--verbose", false,
          0, "false");

  RepoManagerOption(String commandLineFlag, String... defaultParameters) {
    this(commandLineFlag, false, -1, defaultParameters);
  }

  RepoManagerOption(String    commandLineFlag,
                    boolean   primary,
                    String... defaultParameters)
  {
    this(commandLineFlag, primary, -1, defaultParameters);
  }

  RepoManagerOption(String    commandLineFlag,
                    int       parameterCount,
                    String... defaultParameters)
  {
    this(commandLineFlag, false, parameterCount, defaultParameters);
  }

  RepoManagerOption(String    commandLineFlag,
                    boolean   primary,
                    int       parameterCount,
                    String... defaultParameters)
  {
    this(commandLineFlag,
         primary,
         parameterCount < 0 ? 0 : parameterCount,
         parameterCount,
         defaultParameters);
  }

  RepoManagerOption(String    commandLineFlag,
                    boolean   primary,
                    int       minParameterCount,
                    int       maxParameterCount,
                    String... defaultParameters)
  {
    this.commandLineFlag    = commandLineFlag;
    this.primary            = primary;
    this.minParamCount      = minParameterCount;
    this.maxParamCount      = maxParameterCount;
    this.conflicts          = null;
    this.dependencies       = null;
    this.defaultParameters  = (defaultParameters == null)
        ? Collections.emptyList() : Arrays.asList(defaultParameters);
  }

  private static Map<String, RepoManagerOption> OPTIONS_BY_FLAG;

  private String commandLineFlag;
  private boolean primary;
  private int minParamCount;
  private int maxParamCount;
  private Set<RepoManagerOption> conflicts;
  private Set<Set<RepoManagerOption>>  dependencies;
  private List<String> defaultParameters;

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
