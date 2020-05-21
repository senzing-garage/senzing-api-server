package com.senzing.datagen;

import com.senzing.cmdline.CommandLineOption;

import java.util.*;

import static java.util.EnumSet.*;

enum DataGeneratorOption
    implements CommandLineOption<DataGeneratorOption>
{
  HELP("-help",0),
  PERSON_COUNT("-personCount", 1),
  ORGANIZATION_COUNT("-orgCount", 1),
  BUSINESS_COUNT("-bizCount", 1),
  PERSON_SOURCES("-personSources", 1, -1),
  ORGANIZATION_SOURCES("-orgSources", 1, -1),
  BUSINESS_SOURCES("-bizSources", 1, -1),
  ENTITY_TYPE_STRATEGY("-entityTypeStrategy", 1),
  DEFAULT_NO_FEATURES("-defaultNoFeatures"),
  MAX_NAME_COUNT("-maxNames", 1),
  MAX_BIRTH_DATE_COUNT("-maxBirthDates", 1),
  MAX_ADDRESS_COUNT("-maxAddresses", 1),
  MAX_PHONE_COUNT("-maxPhones", 1),
  MAX_EMAIL_COUNT("-maxEmails", 1),
  NAME_DENSITY("-nameDensity", 1),
  BIRTH_DATE_DENSITY("-birthDateDensity", 1),
  ADDRESS_DENSITY("-addressDensity", 1),
  PHONE_DENSITY("-phoneDensity", 1),
  EMAIL_DENSITY("-emailDensity", 1),
  WITH_RECORD_IDS("-withRecordIds", 0),
  FULL_VALUES("-fullValues", 0),
  FLATTEN("-flatten", 0),
  SEED("-seed", 1),
  CSV_FILE("-csvFile", 1),
  JSON_FILE("-jsonFile", 1),
  JSON_LINES_FILE("-jsonLinesFile", 1),
  OVERWRITE("-overwrite", 0),
  PRETTY_PRINT("-prettyPrint", 0);

  DataGeneratorOption(String commandLineFlag) {
    this(commandLineFlag, false, -1);
  }

  DataGeneratorOption(String commandLineFlag, boolean primary) {
    this(commandLineFlag, primary, -1);
  }

  DataGeneratorOption(String commandLineFlag, int parameterCount) {
    this(commandLineFlag, false, parameterCount);
  }

  DataGeneratorOption(String   commandLineFlag,
                      boolean  primary,
                      int      parameterCount)
  {
    this(commandLineFlag,
         primary,
         (parameterCount < 0) ? 0 : parameterCount,
         parameterCount);
  }

  DataGeneratorOption(String   commandLineFlag,
                      int      minParameterCount,
                      int      maxParameterCount)
  {
    this(commandLineFlag, false, minParameterCount, maxParameterCount);
  }

  DataGeneratorOption(String   commandLineFlag,
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

  private static Map<String, DataGeneratorOption> OPTIONS_BY_FLAG;

  private String commandLineFlag;
  private int minParamCount;
  private int maxParamCount;
  private boolean primary;
  private EnumSet<DataGeneratorOption> conflicts;
  private Set<Set<DataGeneratorOption>> dependencies;

  public String getCommandLineFlag() {
    return this.commandLineFlag;
  }

  public int getMinimumParameterCount() { return this.minParamCount; }

  public int getMaximumParameterCount() { return this.maxParamCount; }

  public boolean isPrimary() { return this.primary; }

  public boolean isDeprecated() { return false; };

  public Set<DataGeneratorOption> getConflicts() {
    return this.conflicts;
  }

  public Set<Set<DataGeneratorOption>> getDependencies() {
    return this.dependencies;
  }

  public static DataGeneratorOption lookup(String commandLineFlag) {
    return OPTIONS_BY_FLAG.get(commandLineFlag.toLowerCase());
  }

  static {
    try {
      Map<String, DataGeneratorOption> lookupMap = new LinkedHashMap<>();
      for (DataGeneratorOption opt: values()) {
        lookupMap.put(opt.getCommandLineFlag(), opt);
      }
      OPTIONS_BY_FLAG = Collections.unmodifiableMap(lookupMap);

      Set<Set<DataGeneratorOption>> nodeps
          = Collections.singleton(noneOf(DataGeneratorOption.class));

      HELP.conflicts = complementOf(EnumSet.of(HELP));
      HELP.dependencies = nodeps;

      Set<Set<DataGeneratorOption>> outputFileDependencies
          = new LinkedHashSet<>();
      outputFileDependencies.add(EnumSet.of(CSV_FILE));
      outputFileDependencies.add(EnumSet.of(JSON_FILE));
      outputFileDependencies.add(EnumSet.of(JSON_LINES_FILE));
      outputFileDependencies
          = Collections.unmodifiableSet(outputFileDependencies);

      OVERWRITE.dependencies = outputFileDependencies;
      PRETTY_PRINT.dependencies = Collections.singleton(EnumSet.of(JSON_FILE));
      ORGANIZATION_SOURCES.dependencies
          = Collections.singleton(EnumSet.of(ORGANIZATION_COUNT));
      BUSINESS_SOURCES.dependencies
          = Collections.singleton(EnumSet.of(BUSINESS_COUNT));
      PERSON_SOURCES.dependencies
          = Collections.singleton(EnumSet.of(PERSON_COUNT));

    } catch (Exception e) {
      e.printStackTrace();
      throw new ExceptionInInitializerError(e);
    }
  }
}
