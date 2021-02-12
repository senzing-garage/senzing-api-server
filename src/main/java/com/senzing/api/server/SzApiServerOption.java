package com.senzing.api.server;

import com.senzing.cmdline.CommandLineOption;

import java.util.*;

import static com.senzing.util.CollectionUtilities.recursivelyUnmodifiableMap;
import static com.senzing.api.server.mq.KafkaEndpoint.*;
import static com.senzing.api.server.mq.SqsEndpoint.*;
import static com.senzing.api.server.mq.RabbitEndpoint.*;
import static com.senzing.api.server.SzApiServerUtilities.*;

/**
 * Describes the command-line options for {@link SzApiServer}.
 */
enum SzApiServerOption implements CommandLineOption<SzApiServerOption>
{
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
  SKIP_STARTUP_PERF("-skipStartupPerf", 0),
  SQS_INFO_URL(
      "-sqsInfoUrl", 1,
      SQS_INFO_QUEUE_GROUP, URL_PROPERTY_KEY, false),
  RABBIT_INFO_USER(
      "-rabbitInfoUser", 1,
      RABBITMQ_INFO_QUEUE_GROUP, USER_PROPERTY_KEY, false),
  RABBIT_INFO_PASSWORD(
      "-rabbitInfoPassword", 1,
      RABBITMQ_INFO_QUEUE_GROUP, PASSWORD_PROPERTY_KEY, false),
  RABBIT_INFO_HOST(
      "-rabbitInfoHost", 1,
      RABBITMQ_INFO_QUEUE_GROUP, HOST_PROPERTY_KEY, false),
  RABBIT_INFO_PORT(
      "-rabbitInfoPort", 1,
      RABBITMQ_INFO_QUEUE_GROUP, PORT_PROPERTY_KEY, false),
  RABBIT_INFO_VIRTUAL_HOST(
      "-rabbitInfoVirtualHost", 1,
      RABBITMQ_INFO_QUEUE_GROUP, VIRTUAL_HOST_PROPERTY_KEY, false),
  RABBIT_INFO_EXCHANGE(
      "-rabbitInfoExchange", 1,
      RABBITMQ_INFO_QUEUE_GROUP, EXCHANGE_PROPERTY_KEY, false),
  RABBIT_INFO_ROUTING_KEY(
      "-rabbitInfoRoutingKey", 1,
      RABBITMQ_INFO_QUEUE_GROUP, ROUTING_KEY_PROPERTY_KEY, false),
  KAFKA_INFO_BOOTSTRAP_SERVERS(
      "-kafkaInfoBootstrapServers", 1,
      KAFKA_INFO_QUEUE_GROUP, BOOTSTRAP_SERVERS_PROPERTY_KEY, false),
  KAFKA_INFO_GROUP_ID(
      "-kafkaInfoGroupId", 1,
      KAFKA_INFO_QUEUE_GROUP, GROUP_ID_PROPERTY_KEY, true),
  KAFKA_INFO_TOPIC(
      "-kafkaInfoTopic", 1,
      KAFKA_INFO_QUEUE_GROUP, TOPIC_PROPERTY_KEY, false);

  private static Map<SzApiServerOption, Set<SzApiServerOption>> CONFLICTING_OPTIONS;

  private static Map<SzApiServerOption, Set<SzApiServerOption>> ALTERNATIVE_OPTIONS;

  private static Map<String, SzApiServerOption> OPTIONS_BY_FLAG;

  private static Map<SzApiServerOption, Set<Set<SzApiServerOption>>> DEPENDENCIES;

  private boolean primary;

  private boolean deprecated;

  private String cmdLineFlag;

  private int minParamCount;

  private int maxParamCount;

  private String groupName;

  private String groupPropertyKey;

  private boolean groupOptional;

  SzApiServerOption(String cmdLineFlag) {
    this(cmdLineFlag, false);
  }

  SzApiServerOption(String cmdLineFlag, boolean deprecated) {
    this(cmdLineFlag, false, -1, deprecated);
  }

  SzApiServerOption(String cmdLineFlag, int parameterCount) {
    this(cmdLineFlag,
         false,
         parameterCount,
         false,
         null,
         null,
         true);
  }

  SzApiServerOption(String  cmdLineFlag,
                    int     parameterCount,
                    String  groupName,
                    String  groupPropertyKey,
                    boolean groupOptional)
  {
    this(cmdLineFlag,
         false,
         parameterCount,
         false,
         groupName,
         groupPropertyKey,
         groupOptional);
  }

  SzApiServerOption(String cmdLineFlag, boolean primary, int parameterCount) {
    this(cmdLineFlag,
         primary,
         parameterCount,
         false,
         null,
         null,
         true);
  }

  SzApiServerOption(String  cmdLineFlag,
                    boolean primary,
                    int     parameterCount,
                    String  groupName,
                    String  groupPropertyKey,
                    boolean groupOptional)
  {
    this(cmdLineFlag,
         primary,
         parameterCount,
         false,
         groupName,
         groupPropertyKey,
         groupOptional);
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
         deprecated,
         null,
         null,
         true);
  }

  SzApiServerOption(String  cmdLineFlag,
                    boolean primary,
                    int     parameterCount,
                    boolean deprecated,
                    String  groupName,
                    String  groupPropertyKey,
                    boolean groupOptional)
  {
    this(cmdLineFlag,
         primary,
         parameterCount < 0 ? 0 : parameterCount,
         parameterCount,
         deprecated,
         groupName,
         groupPropertyKey,
         groupOptional);
  }

  SzApiServerOption(String   cmdLineFlag,
                    boolean  primary,
                    int      minParameterCount,
                    int      maxParameterCount,
                    boolean  deprecated)
  {
    this(cmdLineFlag,
         primary,
         minParameterCount,
         maxParameterCount,
         deprecated,
         null,
         null,
         true);
  }

  SzApiServerOption(String   cmdLineFlag,
                    boolean  primary,
                    int      minParameterCount,
                    int      maxParameterCount,
                    boolean  deprecated,
                    String   groupName,
                    String   groupPropertyKey,
                    boolean  groupOptional)
  {
    this.cmdLineFlag      = cmdLineFlag;
    this.primary          = primary;
    this.minParamCount    = minParameterCount;
    this.maxParamCount    = maxParameterCount;
    this.deprecated       = deprecated;
    this.groupName        = groupName;
    this.groupPropertyKey = groupPropertyKey;
    this.groupOptional    = groupOptional;
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

  public String getGroupName() {
    return this.groupName;
  }

  public String getGroupPropertyKey() {
    return this.groupPropertyKey;
  }

  public boolean isGroupOptional() {
    return this.groupOptional;
  }

  @Override
  public Set<SzApiServerOption> getConflicts() {
    return CONFLICTING_OPTIONS.get(this);
  }

  public Set<SzApiServerOption> getAlternatives() {
    return ALTERNATIVE_OPTIONS.get(this);
  }

  public Set<Set<SzApiServerOption>> getDependencies() {
    Set<Set<SzApiServerOption>> set = DEPENDENCIES.get(this);
    return (set == null) ? Collections.emptySet() : set;
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

      SzApiServerOption[] initOptions
          = { INI_FILE, INIT_ENV_VAR, INIT_FILE, INIT_JSON };
      for (SzApiServerOption option1 : initOptions) {
        for (SzApiServerOption option2 : initOptions) {
          if (option1 != option2) {
            Set<SzApiServerOption> set = conflictMap.get(option1);
            set.add(option2);
          }
        }
      }

      Set<SzApiServerOption> kafkaInfoOptions = Set.of(
          KAFKA_INFO_BOOTSTRAP_SERVERS,
          KAFKA_INFO_GROUP_ID,
          KAFKA_INFO_TOPIC);

      Set<SzApiServerOption> rabbitInfoOptions = Set.of(
          RABBIT_INFO_USER,
          RABBIT_INFO_PASSWORD,
          RABBIT_INFO_HOST,
          RABBIT_INFO_PORT,
          RABBIT_INFO_VIRTUAL_HOST,
          RABBIT_INFO_EXCHANGE,
          RABBIT_INFO_ROUTING_KEY);

      Set<SzApiServerOption> sqsInfoOptions = Set.of(SQS_INFO_URL);

      // enforce that we only have one info queue
      for (SzApiServerOption option: kafkaInfoOptions) {
        Set<SzApiServerOption> conflictSet = conflictMap.get(option);
        conflictSet.addAll(rabbitInfoOptions);
        conflictSet.addAll(sqsInfoOptions);
      }
      for (SzApiServerOption option: rabbitInfoOptions) {
        Set<SzApiServerOption> conflictSet = conflictMap.get(option);
        conflictSet.addAll(kafkaInfoOptions);
        conflictSet.addAll(sqsInfoOptions);
      }
      for (SzApiServerOption option: sqsInfoOptions) {
        Set<SzApiServerOption> conflictSet = conflictMap.get(option);
        conflictSet.addAll(kafkaInfoOptions);
        conflictSet.addAll(rabbitInfoOptions);
      }

      Set<SzApiServerOption> readOnlyConflicts = conflictMap.get(READ_ONLY);
      readOnlyConflicts.addAll(kafkaInfoOptions);
      readOnlyConflicts.addAll(rabbitInfoOptions);
      readOnlyConflicts.addAll(sqsInfoOptions);

      Set<SzApiServerOption> iniAlts = altMap.get(INI_FILE);
      iniAlts.add(INIT_ENV_VAR);
      iniAlts.add(INIT_FILE);
      iniAlts.add(INIT_JSON);

      Map<SzApiServerOption, Set<Set<SzApiServerOption>>> dependencyMap
          = new LinkedHashMap<>();

      // handle dependencies for groups of options that go together
      Map<String, Set<SzApiServerOption>> groups = new LinkedHashMap<>();
      for (SzApiServerOption option: SzApiServerOption.values()) {
        String groupName = option.getGroupName();
        if (groupName == null) continue;
        Set<SzApiServerOption> set = groups.get(groupName);
        if (set == null) {
          set = new LinkedHashSet<>();
          groups.put(groupName, set);
        }
        set.add(option);
      }

      // create the dependencies using the groupings
      groups.forEach((groupName, group) -> {
        for (SzApiServerOption option : group) {
          Set<SzApiServerOption> others = new LinkedHashSet<>(group);

          // remove self from the group (can't depend on itself)
          others.remove(option);

          // remove any options that are not required
          for (SzApiServerOption opt: group) {
            if (opt.isGroupOptional()) others.remove(opt);
          }

          // make the others set unmodifiable
          others = Collections.unmodifiableSet(others);

          // add the dependency
          dependencyMap.put(option, Set.of(others));
        }
      });

      CONFLICTING_OPTIONS = recursivelyUnmodifiableMap(conflictMap);
      ALTERNATIVE_OPTIONS = recursivelyUnmodifiableMap(altMap);
      OPTIONS_BY_FLAG = Collections.unmodifiableMap(lookupMap);
      DEPENDENCIES = Collections.unmodifiableMap(dependencyMap);

    } catch (Exception e) {
      e.printStackTrace();
      throw new ExceptionInInitializerError(e);
    }
  }
}
