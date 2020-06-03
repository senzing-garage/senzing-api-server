package com.senzing.api.services;

import com.senzing.api.model.*;
import com.senzing.datagen.*;
import com.senzing.repomgr.RepositoryManager;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;
import java.io.*;
import java.net.URLEncoder;
import java.util.*;

import static com.senzing.io.IOUtilities.*;
import static com.senzing.datagen.RecordType.*;
import static com.senzing.datagen.FeatureType.*;
import static com.senzing.datagen.UsageType.usageTypesFor;
import static com.senzing.datagen.FeatureDensity.*;
import static com.senzing.api.model.SzHttpMethod.POST;
import static com.senzing.api.services.ResponseValidators.*;
import static org.junit.jupiter.api.TestInstance.Lifecycle;
import static org.junit.jupiter.api.Assertions.*;
import static com.senzing.api.model.SzBulkDataStatus.*;

@TestInstance(Lifecycle.PER_CLASS)
public class BulkDataServicesTest extends AbstractServiceTest {
  protected static final String CUSTOMER_DATA_SOURCE      = "CUSTOMER";
  protected static final String SUBSCRIBER_DATA_SOURCE    = "SUBSCRIBER";
  protected static final String EMPLOYEE_DATA_SOURCE      = "EMPLOYEE";
  protected static final String VENDOR_DATA_SOURCE        = "VENDOR";
  protected static final String PARTNER_DATA_SOURCE       = "PARTNER";
  protected static final String STORE_DATA_SOURCE         = "STORE";
  protected static final String CUSTOMERS_DATA_SOURCE     = "CUSTOMERS";
  protected static final String SUBSCRIBERS_DATA_SOURCE   = "SUBSCRIBERS";
  protected static final String EMPLOYEES_DATA_SOURCE     = "EMPLOYEES";
  protected static final String VENDORS_DATA_SOURCE       = "VENDORS";
  protected static final String PARTNERS_DATA_SOURCE      = "PARTNERS";
  protected static final String STORES_DATA_SOURCE        = "STORES";
  protected static final String CONTACTS_DATA_SOURCE      = "CONTACTS";

  protected static final String CUSTOMER_ENTITY_TYPE      = "CUSTOMER";
  protected static final String EMPLOYEE_ENTITY_TYPE      = "EMPLOYEE";
  protected static final String SUBSCRIBER_ENTITY_TYPE    = "SUBSCRIBER";
  protected static final String VENDOR_ENTITY_TYPE        = "VENDOR";
  protected static final String PARTNER_ENTITY_TYPE       = "PARTNER";
  protected static final String STORE_ENTITY_TYPE         = "STORE";
  protected static final String PERSON_ENTITY_TYPE        = "PERSON";
  protected static final String ORGANIZATION_ENTITY_TYPE  = "ORGANIZATION";

  protected static final Map<String, String> DATA_SOURCE_MAP;
  protected static final Map<String, String> ENTITY_TYPE_MAP;

  protected static final Map<String, RecordType> SOURCE_RECORD_TYPE_MAP;

  protected static final Map<String, String> SOURCE_ENTITY_TYPE_MAP;

  protected final long SEED = 8736123213L;

  private enum FlagValue {
    YES, NO, MIXED;
    public boolean toBoolean(int iteration) {
      switch (this) {
        case YES:
          return true;
        case NO:
          return false;
        case MIXED:
          return ((iteration % 2) == 0);
        default:
          throw new IllegalStateException(
              "Unhandled FlagValue: " + this);
      }
    }
    public FlagValue next() {
      switch (this) {
        case YES:
          return NO;
        case NO:
          return MIXED;
        case MIXED:
          return YES;
        default:
          throw new IllegalStateException(
              "Unhandled FlagValue: " + this);
      }
    }
  };

  static {
    try {
      Map<String, String> dataSourceMap = new LinkedHashMap<>();
      Map<String, String> entityTypeMap = new LinkedHashMap<>();

      dataSourceMap.put(CUSTOMER_DATA_SOURCE, CUSTOMERS_DATA_SOURCE);
      dataSourceMap.put(SUBSCRIBER_DATA_SOURCE, SUBSCRIBERS_DATA_SOURCE);
      dataSourceMap.put(EMPLOYEE_DATA_SOURCE, EMPLOYEES_DATA_SOURCE);
      dataSourceMap.put(VENDOR_DATA_SOURCE, VENDORS_DATA_SOURCE);
      dataSourceMap.put(PARTNER_DATA_SOURCE, PARTNERS_DATA_SOURCE);
      dataSourceMap.put(STORE_DATA_SOURCE, STORES_DATA_SOURCE);

      entityTypeMap.put(CUSTOMER_ENTITY_TYPE, PERSON_ENTITY_TYPE);
      entityTypeMap.put(EMPLOYEE_ENTITY_TYPE, PERSON_ENTITY_TYPE);
      entityTypeMap.put(SUBSCRIBER_ENTITY_TYPE, PERSON_ENTITY_TYPE);
      entityTypeMap.put(VENDOR_ENTITY_TYPE, ORGANIZATION_ENTITY_TYPE);
      entityTypeMap.put(PARTNER_ENTITY_TYPE, ORGANIZATION_ENTITY_TYPE);
      entityTypeMap.put(STORE_ENTITY_TYPE, ORGANIZATION_ENTITY_TYPE);

      DATA_SOURCE_MAP = Collections.unmodifiableMap(dataSourceMap);
      ENTITY_TYPE_MAP = Collections.unmodifiableMap(entityTypeMap);

      Map<String, RecordType> sourceRecordTypeMap = new LinkedHashMap<>();

      sourceRecordTypeMap.put(CUSTOMER_DATA_SOURCE, PERSON);
      sourceRecordTypeMap.put(EMPLOYEE_DATA_SOURCE, PERSON);
      sourceRecordTypeMap.put(SUBSCRIBER_DATA_SOURCE, PERSON);
      sourceRecordTypeMap.put(VENDOR_DATA_SOURCE, ORGANIZATION);
      sourceRecordTypeMap.put(PARTNER_ENTITY_TYPE, ORGANIZATION);
      sourceRecordTypeMap.put(STORE_ENTITY_TYPE, BUSINESS);

      SOURCE_RECORD_TYPE_MAP = Collections.unmodifiableMap(sourceRecordTypeMap);

      Map<String, String> sourceEntityTypeMap = new LinkedHashMap<>();

      sourceEntityTypeMap.put(CUSTOMER_DATA_SOURCE, CUSTOMER_ENTITY_TYPE);
      sourceEntityTypeMap.put(EMPLOYEE_DATA_SOURCE, EMPLOYEE_ENTITY_TYPE);
      sourceEntityTypeMap.put(SUBSCRIBER_DATA_SOURCE, SUBSCRIBER_ENTITY_TYPE);
      sourceEntityTypeMap.put(VENDOR_DATA_SOURCE, VENDOR_ENTITY_TYPE);
      sourceEntityTypeMap.put(PARTNER_ENTITY_TYPE, PARTNER_ENTITY_TYPE);
      sourceEntityTypeMap.put(STORE_ENTITY_TYPE, STORE_ENTITY_TYPE);

      SOURCE_ENTITY_TYPE_MAP = Collections.unmodifiableMap(sourceEntityTypeMap);

    } catch (Exception e) {
      e.printStackTrace();
      throw new ExceptionInInitializerError(e);
    }
  }

  protected BulkDataServices bulkDataServices;
  protected DataGenerator dataGenerator;
  protected Random prng;

  @BeforeAll public void initializeEnvironment() {
    this.beginTests();
    this.initializeTestEnvironment();
    this.bulkDataServices = new BulkDataServices();
    this.dataGenerator    = new DataGenerator(SEED);
    this.prng             = new Random(SEED);
  }

  /**
   * Overridden to configure some data sources.
   */
  protected void prepareRepository() {
    Set<String> dataSourceSet = new LinkedHashSet<>(DATA_SOURCE_MAP.values());
    dataSourceSet.add(CONTACTS_DATA_SOURCE);
    Set<String> entityTypeSet = new LinkedHashSet<>(ENTITY_TYPE_MAP.values());

    RepositoryManager.configSources(this.getRepositoryDirectory(),
                                    dataSourceSet,
                                    true);
    RepositoryManager.configEntityTypes(this.getRepositoryDirectory(),
                                        entityTypeSet,
                                        true);
  }

  @AfterAll public void teardownEnvironment() {
    try {
      this.teardownTestEnvironment();
      this.conditionallyLogCounts(true);
    } finally {
      this.endTests();
    }
  }

  /**
   * Override to return 8 threads.
   * @return The number of server threads (8).
   */
  protected int getServerConcurrency() {
    return 8;
  }

  private Map<FeatureType, Set<UsageType>> featureGenMap(RecordType recordType)
  {
    Map<FeatureType, Set<UsageType>> map = new LinkedHashMap<>();

    // determine the default feature counts
    int maxNames       = prng.nextInt(2) + 1;
    int maxAddresses   = prng.nextInt(3) + 1;
    int maxPhones      = prng.nextInt(3) + 1;
    int maxEmails      = prng.nextInt(3) + 1;

    map.put(NAME, UsageType.usageTypesFor(NAME, recordType, maxNames, true));
    if (recordType == PERSON) map.put(BIRTH_DATE, null);
    map.put(ADDRESS, usageTypesFor(ADDRESS, recordType, maxAddresses, true));
    map.put(PHONE, usageTypesFor(PHONE, recordType, maxPhones, true));
    map.put(EMAIL, usageTypesFor(EMAIL, recordType, maxEmails, true));

    return map;
  }

  private Map<FeatureType, FeatureDensity> featureDensityMap() {
    Map<FeatureType, FeatureDensity> map = new LinkedHashMap<>();
    map.put(NAME, FIRST_THEN_SPARSE);
    map.put(BIRTH_DATE, COMMON);
    map.put(ADDRESS, COMMON);
    map.put(PHONE, COMMON);
    map.put(EMAIL, COMMON);
    return map;
  }

  private List<Arguments> getLoadBulkRecordsParameters() {
    List<Arguments> analyzeParams = this.getAnalyzeBulkRecordsParameters();
    List<Arguments> result = new LinkedList<>();

    boolean evenOdd = true;
    for (Arguments args : analyzeParams) {
      Object[]            argArray  = args.get();
      String              testInfo  = (String) argArray[0];
      MediaType           mediaType = (MediaType) argArray[1];
      File                dataFile  = (File) argArray[2];
      SzBulkDataAnalysis  analysis  = (SzBulkDataAnalysis) argArray[3];

      Set<String> dataSources     = new LinkedHashSet<>();
      Set<String> entityTypes     = new LinkedHashSet<>();

      analysis.getAnalysisByDataSource().forEach(abds -> {
        dataSources.add(abds.getDataSource());
      });

      analysis.getAnalysisByEntityType().forEach(abet -> {
        entityTypes.add(abet.getEntityType());
      });

      Map<String, String> dataSourceMap = new LinkedHashMap<>();
      Map<String, String> entityTypeMap = new LinkedHashMap<>();

      for (String dataSource: dataSources) {
        if (dataSource != null) {
          dataSourceMap.put(dataSource, DATA_SOURCE_MAP.get(dataSource));
        }
      }

      for (String entityType: entityTypes) {
        if (entityType != null) {
          entityTypeMap.put(entityType, ENTITY_TYPE_MAP.get(entityType));
        }
      }

      testInfo = testInfo + ", dataSourceMap=[ " + dataSourceMap
          + " ], entityTypeMap=[ " + entityTypeMap + " ]";

      if (evenOdd) {
        result.add(Arguments.of(
            testInfo + ", mapping=[ GENERIC ]",
            mediaType,
            dataFile,
            analysis,
            null,
            null));
      } else {
        result.add(Arguments.of(
            testInfo + ", mapping=[ SPECIFIC ]",
            mediaType,
            dataFile,
            analysis,
            dataSourceMap,
            entityTypeMap));
      }
      evenOdd = !evenOdd;
    }

    return result;
  }

  private List<Arguments> getAnalyzeBulkRecordsParameters() {
    Set<String> dataSources = DATA_SOURCE_MAP.keySet();

    String UTF8_SUFFIX          = "; charset=UTF-8";
    String CSV_SPEC             = "text/csv";
    String CSV_UTF8_SPEC        = CSV_SPEC + UTF8_SUFFIX;
    String JSON_SPEC            = "application/json";
    String JSON_UTF8_SPEC       = JSON_SPEC + UTF8_SUFFIX;
    String JSON_LINES_SPEC      = "application/x-jsonlines";
    String JSON_LINES_UTF8_SPEC = JSON_LINES_SPEC + UTF8_SUFFIX;
    String TEXT_SPEC            = "text/plain";
    String TEXT_UTF8_SPEC       = TEXT_SPEC + UTF8_SUFFIX;

    MediaType CSV             = MediaType.valueOf(CSV_SPEC);
    MediaType CSV_UTF8        = MediaType.valueOf(CSV_UTF8_SPEC);
    MediaType JSON            = MediaType.valueOf(JSON_SPEC);
    MediaType JSON_UTF8       = MediaType.valueOf(JSON_UTF8_SPEC);
    MediaType JSON_LINES      = MediaType.valueOf(JSON_LINES_SPEC);
    MediaType JSON_LINES_UTF8 = MediaType.valueOf(JSON_LINES_UTF8_SPEC);
    MediaType TEXT            = MediaType.valueOf(TEXT_SPEC);
    MediaType TEXT_UTF8       = MediaType.valueOf(TEXT_UTF8_SPEC);

    Map<MediaType, List<Arguments>> bulkDataMap = new LinkedHashMap<>();

    List<MediaType> csvMediaTypes = Arrays.asList(
        CSV, TEXT_UTF8);
    List<MediaType> jsonMediaTypes = Arrays.asList(
        JSON_UTF8, TEXT);
    List<MediaType> jsonLinesMediaTypes = Arrays.asList(
        JSON_LINES, TEXT);

    Map<String, List<MediaType>> mediaTypesMap = new LinkedHashMap<>();
    mediaTypesMap.put(CSV_SPEC, csvMediaTypes);
    mediaTypesMap.put(JSON_SPEC, jsonMediaTypes);
    mediaTypesMap.put(JSON_LINES_SPEC, jsonLinesMediaTypes);

    boolean[] booleans = { true, false };

    // setup the feature-gen maps by record type
    Map<RecordType, Map<FeatureType, Set<UsageType>>> featureGenMaps
        = new LinkedHashMap<>();

    // aggregate all features into a single map for CSV record handler
    for (RecordType recordType: RecordType.values()) {
      Map<FeatureType, Set<UsageType>> map = featureGenMap(recordType);
      featureGenMaps.put(recordType, map);
    }

    int dataFileIndex = 0;

    // iterate over the entity types
    FlagValue withEntityTypes = FlagValue.MIXED;
    FlagValue withRecordIds   = FlagValue.MIXED;
    boolean   fullValues      = false;
    boolean   flatten         = false;

    // iterate over the data sources
    for (int dataSourceCount = 1; dataSourceCount < dataSources.size();
         dataSourceCount+=2)
    {
      // get the data source list
      List<String> dataSourceList = new ArrayList<>(dataSources);
      int start = (dataSourceCount > 3) ? 2 : 0;
      dataSourceList = dataSourceList.subList(start, dataSourceCount);

      // find the set of record types
      Set<RecordType> recordTypes = new LinkedHashSet<>();
      for (String dataSource: dataSourceList) {
        RecordType recordType = SOURCE_RECORD_TYPE_MAP.get(dataSource);
        recordTypes.add(recordType);
      }

      // create the aggregate feature gen map
      Map<FeatureType, Set<UsageType>> allFeatureGenMap = new LinkedHashMap<>();
      for (RecordType recordType: recordTypes) {
        Map<FeatureType,Set<UsageType>> map = featureGenMaps.get(recordType);
        map.entrySet().forEach(entry -> {
          FeatureType     featureType = entry.getKey();
          Set<UsageType>  usageTypes  = entry.getValue();
          Set<UsageType>  set         = allFeatureGenMap.get(featureType);

          if (set == null) {
            set = new LinkedHashSet<>();
            allFeatureGenMap.put(featureType, set);
          }
          if (usageTypes != null) set.addAll(usageTypes);
          else set.add(null);
        });
      }

      // get the feature density map
      Map<FeatureType, FeatureDensity> featureDensityMap = featureDensityMap();

      // set the boolean/flag values
      withEntityTypes = withEntityTypes.next();
      withRecordIds   = withRecordIds.next();
      fullValues      = !fullValues;
      flatten         = !flatten;

      String testInfo = "dataSources=[ " + dataSourceList
          + " ], withEntityTypes=[ " + withEntityTypes
          + " ], withRecordIds=[ " + withRecordIds
          + " ], fullValues=[ " + fullValues
          + " ], flatten=[ " + flatten + " ]";

      File dataDir = new File(this.getRepositoryDirectory(), "data");
      dataDir.mkdirs();

      RecordHandler recordHandler = null;
      File          csvFile       = null;
      File          jsonFile      = null;
      File          jsonLinesFile = null;

      Map<String, File> dataFileMap = new LinkedHashMap<>();

      SzBulkDataAnalysis csvAnalysis        = new SzBulkDataAnalysis();
      SzBulkDataAnalysis jsonAnalysis       = new SzBulkDataAnalysis();
      SzBulkDataAnalysis jsonLinesAnalysis  = new SzBulkDataAnalysis();

      SzBulkDataAnalysis[] analyses = {
          csvAnalysis, jsonAnalysis, jsonLinesAnalysis
      };
      for (SzBulkDataAnalysis analysis : analyses) {
        analysis.setCharacterEncoding("UTF-8");
      }
      csvAnalysis.setMediaType(CSV_SPEC);
      jsonAnalysis.setMediaType(JSON_SPEC);
      jsonLinesAnalysis.setMediaType(JSON_LINES_SPEC);

      try {
        dataFileIndex++;
        String prefix = "data-" + ((dataFileIndex<1000)?"0":"")
            + ((dataFileIndex<100)?"0":"") + ((dataFileIndex<10)?"0":"")
            + dataFileIndex + "-";
        csvFile = File.createTempFile(prefix, ".csv", dataDir);
        jsonFile = new File(
            csvFile.toString().replaceAll("\\.csv$", ".json"));
        jsonLinesFile = new File(
            csvFile.toString().replaceAll("\\.csv$", ".jsonl"));

        dataFileMap.put(CSV_SPEC, csvFile);
        dataFileMap.put(JSON_SPEC, jsonFile);
        dataFileMap.put(JSON_LINES_SPEC, jsonLinesFile);

        Writer csvWriter = new BufferedWriter(
            new OutputStreamWriter(
                new FileOutputStream(csvFile), UTF_8));

        Writer jsonWriter = new BufferedWriter(
            new OutputStreamWriter(
                new FileOutputStream(jsonFile), UTF_8));

        Writer jsonLinesWriter = new BufferedWriter(
            new OutputStreamWriter(
                new FileOutputStream(jsonLinesFile), UTF_8));

        CSVRecordHandler csvHandler = new CSVRecordHandler(
            csvWriter,
            (withRecordIds != FlagValue.NO),
            (dataSourceList.size() > 0),
            (withEntityTypes != FlagValue.NO),
            allFeatureGenMap,
            recordTypes,
            fullValues);
        JsonArrayRecordHandler jsonHandler
            = new JsonArrayRecordHandler(jsonWriter);
        JsonLinesRecordHandler jsonLinesHandler
            = new JsonLinesRecordHandler(jsonLinesWriter);

        recordHandler = new CompoundRecordHandler(csvHandler,
                                                  jsonHandler,
                                                  jsonLinesHandler);

        int iteration = 0;
        for (String dataSource: dataSourceList) {
          RecordType recordType = SOURCE_RECORD_TYPE_MAP.get(dataSource);
          String entityType = SOURCE_ENTITY_TYPE_MAP.get(dataSource);

          if (!withEntityTypes.toBoolean(iteration)) {
            entityType = null;
          }

          int recordCount = (dataSourceList.size() == 1) ? 500
              : Math.max(500, ((iteration + 1) * 2000) % 2500);

          Map<FeatureType, Set<UsageType>> featureGenMap
              = featureGenMaps.get(recordType);

          boolean includeRecordIds = withRecordIds.toBoolean(iteration);

          this.dataGenerator.generateRecords(recordHandler,
                                             recordType,
                                             recordCount,
                                             includeRecordIds,
                                             dataSource,
                                             entityType,
                                             featureGenMap,
                                             featureDensityMap,
                                             fullValues,
                                             flatten);

          for (SzBulkDataAnalysis analysis : analyses) {
            analysis.trackRecords(recordCount,
                                  dataSource,
                                  entityType,
                                  includeRecordIds);
          }
        }
      } catch (IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      } finally {
        if (recordHandler != null) recordHandler.close();
      }

      Map<String, SzBulkDataAnalysis> analysisMap
          = new LinkedHashMap<>();
      for (SzBulkDataAnalysis analysis : analyses) {
        analysis.setStatus(SzBulkDataStatus.COMPLETED);
        analysisMap.put(analysis.getMediaType(), analysis);
      }

      mediaTypesMap.entrySet().forEach(entry -> {
        String mediaTypeSpec = entry.getKey();
        List<MediaType> mediaTypes = entry.getValue();
        SzBulkDataAnalysis analysis = analysisMap.get(mediaTypeSpec);
        File dataFile = dataFileMap.get(mediaTypeSpec);

        for (MediaType mediaType : mediaTypes) {
          String fullTestInfo = "recordCount[ " + analysis.getRecordCount()
              + " ], " + testInfo + ", mediaType=[ " + mediaType
              + " ], format=[ " + mediaTypeSpec + " ], dataFile=[ "
              + dataFile + " ]";

          List<Arguments> list = bulkDataMap.get(mediaType);
          if (list == null) {
            list = new LinkedList<>();
            bulkDataMap.put(mediaType, list);
          }
          list.add(Arguments.arguments(
              fullTestInfo,
              mediaType,
              dataFile,
              analysis
          ));
        }
      });
    }
    List<Arguments> result = new LinkedList<>();
    boolean[] firstLast = { true };
    bulkDataMap.values().forEach(argsList -> {
      if (argsList != null) {
        Arguments args = (firstLast[0])
            ? argsList.get(0) : argsList.get(argsList.size() - 1);
        result.add(args);
        firstLast[0] = !firstLast[0];
      }
    });
    return result;
  }

  @ParameterizedTest
  @MethodSource("getAnalyzeBulkRecordsParameters")
  public void analyzeBulkRecordsViaFormTest(
      String              testInfo,
      MediaType           mediaType,
      File                bulkDataFile,
      SzBulkDataAnalysis  expected)
  {
    this.performTest(() -> {
      String  uriText = this.formatServerUri("bulk-data/analyze");
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      try (FileInputStream fis = new FileInputStream(bulkDataFile)) {
        long before = System.currentTimeMillis();
        SzBulkDataAnalysisResponse response
            = this.bulkDataServices.analyzeBulkRecordsViaForm(
            mediaType, fis, uriInfo);
        response.concludeTimers();
        long after = System.currentTimeMillis();

        validateAnalyzeResponse(testInfo,
                                response,
                                POST,
                                uriText,
                                mediaType,
                                bulkDataFile,
                                expected,
                                before,
                                after);

      } catch (Exception e) {
        System.err.println("********** FAILED TEST: " + testInfo);
        e.printStackTrace();
        if (e instanceof RuntimeException) throw ((RuntimeException) e);
        throw new RuntimeException(e);
      }
    });
  }

  @ParameterizedTest
  @MethodSource("getAnalyzeBulkRecordsParameters")
  public void analyzeBulkRecordsViaFormHttpTest(
      String              testInfo,
      MediaType           mediaType,
      File                bulkDataFile,
      SzBulkDataAnalysis  expected)
  {
    this.performTest(() -> {
      String uriText = this.formatServerUri("bulk-data/analyze");

      try (FileInputStream fis = new FileInputStream(bulkDataFile)) {
        long before = System.currentTimeMillis();
        SzBulkDataAnalysisResponse response = this.invokeServerViaHttp(
            POST, uriText, null, mediaType.toString(),
            bulkDataFile.length(), new FileInputStream(bulkDataFile),
            SzBulkDataAnalysisResponse.class);
        response.concludeTimers();
        long after = System.currentTimeMillis();

        validateAnalyzeResponse(testInfo,
                                response,
                                POST,
                                uriText,
                                mediaType,
                                bulkDataFile,
                                expected,
                                before,
                                after);

      } catch (Exception e) {
        System.err.println("********** FAILED TEST: " + testInfo);
        e.printStackTrace();
        if (e instanceof RuntimeException) throw ((RuntimeException) e);
        throw new RuntimeException(e);
      }
    });
  }

  private String formatLoadURL(String               defaultDataSource,
                               String               defaultEntityType,
                               String               loadId,
                               Integer              maxFailures,
                               Map<String, String>  dataSourceMap,
                               Map<String, String>  entityTypeMap,
                               String               progressPeriod)
  {
    try {
      StringBuilder sb = new StringBuilder();
      sb.append("bulk-data/load");
      String prefix = "?";
      if (defaultDataSource != null) {
        sb.append(prefix).append("dataSource=").append(
            URLEncoder.encode(defaultDataSource, UTF_8));
        prefix = "&";
      }
      if (defaultEntityType != null) {
        sb.append(prefix).append("entityType=").append(
            URLEncoder.encode(defaultEntityType, UTF_8));
        prefix = "&";
      }
      if (loadId != null) {
        sb.append(prefix).append("loadId=").append(
            URLEncoder.encode(loadId, UTF_8));
        prefix = "&";
      }
      if (maxFailures != null) {
        sb.append(prefix).append("maxFailures=").append(maxFailures);
        prefix = "&";
      }

      if (dataSourceMap != null) {
        String[]          prefixArr   = { prefix };
        boolean[]         jsonFlag    = { true };
        boolean[]         overlapFlag = { true };
        JsonObjectBuilder builder   = Json.createObjectBuilder();
        dataSourceMap.entrySet().forEach(entry -> {
          String  key   = entry.getKey();
          String  value = entry.getValue();
          if (jsonFlag[0] || overlapFlag[0]) {
            builder.add(key, value);

          } else {
            String mapping = ":" + key + ":" + value;
            try {
              sb.append(prefixArr[0]).append("mapDataSource=").append(
                  URLEncoder.encode(mapping, UTF_8));
              prefixArr[0] = "&";
            } catch (UnsupportedEncodingException cannotHappen) {
              throw new IllegalStateException("UTF-8 encoding not supported");
            }
            overlapFlag[0] = !overlapFlag[0];
          }
          jsonFlag[0] = !jsonFlag[0];
        });
        JsonObject jsonObject = builder.build();
        if (jsonObject.size() > 0) {
          String mapDataSources = jsonObject.toString();
          try {
            sb.append(prefixArr[0]).append("mapDataSources=").append(
                URLEncoder.encode(mapDataSources, UTF_8));

            prefixArr[0] = "&";

          } catch (UnsupportedEncodingException cannotHappen) {
            throw new IllegalStateException("UTF-8 encoding not supported");
          }
        }

        // update the prefix
        prefix = prefixArr[0];
      }

      if (entityTypeMap != null) {
        String[]          prefixArr   = { prefix };
        boolean[]         jsonFlag    = { true };
        boolean[]         overlapFlag = { true };
        JsonObjectBuilder builder   = Json.createObjectBuilder();
        entityTypeMap.entrySet().forEach(entry -> {
          String  key   = entry.getKey();
          String  value = entry.getValue();
          if (jsonFlag[0] || overlapFlag[0]) {
            builder.add(key, value);

          } else {
            String mapping = ":" + key + ":" + value;
            try {
              sb.append(prefixArr[0]).append("mapEntityType=").append(
                  URLEncoder.encode(mapping, UTF_8));
              prefixArr[0] = "&";
            } catch (UnsupportedEncodingException cannotHappen) {
              throw new IllegalStateException("UTF-8 encoding not supported");
            }
            overlapFlag[0] = !overlapFlag[0];
          }
          jsonFlag[0] = !jsonFlag[0];
        });
        JsonObject jsonObject = builder.build();
        if (jsonObject.size() > 0) {
          String mapEntityTypes = jsonObject.toString();
          try {
            sb.append(prefixArr[0]).append("mapEntityTypes=").append(
                URLEncoder.encode(mapEntityTypes, UTF_8));

            prefixArr[0] = "&";

          } catch (UnsupportedEncodingException cannotHappen) {
            throw new IllegalStateException("UTF-8 encoding not supported");
          }
        }

        // update the prefix
        prefix = prefixArr[0];
      }

      if (progressPeriod != null) {
        sb.append(prefix).append("progressPeriod=").append(progressPeriod);
      }

      return sb.toString();

    } catch (UnsupportedEncodingException cannotHappen) {
      throw new IllegalStateException("UTF-8 encoding not supported");
    }
  }

  @ParameterizedTest
  @MethodSource("getLoadBulkRecordsParameters")
  public void loadBulkRecordsViaFormTest(
      String              testInfo,
      MediaType           mediaType,
      File                bulkDataFile,
      SzBulkDataAnalysis  analysis,
      Map<String,String>  dataSourceMap,
      Map<String,String>  entityTypeMap)
  {
    this.performTest(() -> {
      this.livePurgeRepository();

      String  uriText = this.formatServerUri("bulk-data/load");

      MultivaluedMap  queryParams       = new MultivaluedHashMap();
      String          mapDataSources    = null;
      String          mapEntityTypes    = null;
      List<String>    mapDataSourceList = new LinkedList<>();
      List<String>    mapEntityTypeList = new LinkedList<>();
      if (dataSourceMap != null) {
        boolean[]         jsonFlag    = { true };
        boolean[]         overlapFlag = { true };
        JsonObjectBuilder builder   = Json.createObjectBuilder();
        dataSourceMap.entrySet().forEach(entry -> {
          String  key   = entry.getKey();
          String  value = entry.getValue();
          if (jsonFlag[0] || overlapFlag[0]) {
            builder.add(key, value);

          } else {
            String mapping = ":" + key + ":" + value;
            mapDataSourceList.add(mapping);
            queryParams.add("mapDataSource", mapping);
            overlapFlag[0] = !overlapFlag[0];
          }
          jsonFlag[0] = !jsonFlag[0];
        });
        JsonObject jsonObject = builder.build();
        if (jsonObject.size() > 0) {
          mapDataSources = jsonObject.toString();
          queryParams.add("mapDataSources", mapDataSources);
        }
      }

      if (entityTypeMap != null) {
        boolean[]         jsonFlag    = { true };
        boolean[]         overlapFlag = { true };
        JsonObjectBuilder builder   = Json.createObjectBuilder();
        entityTypeMap.entrySet().forEach(entry -> {
          String  key   = entry.getKey();
          String  value = entry.getValue();
          if (jsonFlag[0] || overlapFlag[0]) {
            builder.add(key, value);

          } else {
            String mapping = ":" + key + ":" + value;
            mapEntityTypeList.add(mapping);
            queryParams.add("mapEntityType", mapping);
            overlapFlag[0] = !overlapFlag[0];
          }
          jsonFlag[0] = !jsonFlag[0];
        });
        JsonObject jsonObject = builder.build();
        if (jsonObject.size() > 0) {
          mapEntityTypes = jsonObject.toString();
          queryParams.add("mapEntityTypes", mapDataSources);
        }
      }

      UriInfo uriInfo = this.newProxyUriInfo(uriText, queryParams);

      try (FileInputStream fis = new FileInputStream(bulkDataFile)) {
        long before = System.currentTimeMillis();
        SzBulkLoadResponse response
            = this.bulkDataServices.loadBulkRecordsViaForm(
                CONTACTS_DATA_SOURCE,
                mapDataSources,
                mapDataSourceList,
                GENERIC_ENTITY_TYPE,
                mapEntityTypes,
                mapEntityTypeList,
                null,
                0,
                mediaType,
                fis,
                null,
                uriInfo);
        response.concludeTimers();
        long after = System.currentTimeMillis();

        Map<String,String> allDataSourceMap = new LinkedHashMap<>();
        allDataSourceMap.put(null, CONTACTS_DATA_SOURCE);
        if (dataSourceMap != null) allDataSourceMap.putAll(dataSourceMap);

        Map<String,String> allEntityTypeMap = new LinkedHashMap<>();
        allEntityTypeMap.put(null, GENERIC_ENTITY_TYPE);
        if (entityTypeMap != null) allEntityTypeMap.putAll(entityTypeMap);

        validateLoadResponse(testInfo,
                             response,
                             POST,
                             uriText,
                             COMPLETED,
                             mediaType,
                             bulkDataFile,
                             analysis,
                             analysis.getRecordCount(),
                             allDataSourceMap,
                             allEntityTypeMap,
                             null,
                             null,
                             before,
                             after);

      } catch (Exception e) {
        System.err.println("********** FAILED TEST: " + testInfo);
        e.printStackTrace();
        if (e instanceof RuntimeException) throw ((RuntimeException) e);
        throw new RuntimeException(e);
      }
    });
  }

  @ParameterizedTest
  @MethodSource("getLoadBulkRecordsParameters")
  public void loadBulkRecordsDirectHttpTest(
      String              testInfo,
      MediaType           mediaType,
      File                bulkDataFile,
      SzBulkDataAnalysis  analysis,
      Map<String,String>  dataSourceMap,
      Map<String,String>  entityTypeMap)
  {
    this.performTest(() -> {
      this.livePurgeRepository();

      String uriText = this.formatServerUri(formatLoadURL(
          CONTACTS_DATA_SOURCE, GENERIC_ENTITY_TYPE, null, null,
          dataSourceMap, entityTypeMap, null));

      try (FileInputStream fis = new FileInputStream(bulkDataFile)) {
        long before = System.currentTimeMillis();
        SzBulkLoadResponse response = this.invokeServerViaHttp(
            POST, uriText, null, mediaType.toString(),
            bulkDataFile.length(), new FileInputStream(bulkDataFile),
            SzBulkLoadResponse.class);
        response.concludeTimers();
        long after = System.currentTimeMillis();

        Map<String,String> allDataSourceMap = new LinkedHashMap<>();
        allDataSourceMap.put(null, CONTACTS_DATA_SOURCE);
        if (dataSourceMap != null) allDataSourceMap.putAll(dataSourceMap);

        Map<String,String> allEntityTypeMap = new LinkedHashMap<>();
        allEntityTypeMap.put(null, GENERIC_ENTITY_TYPE);
        if (entityTypeMap != null) allEntityTypeMap.putAll(entityTypeMap);

        validateLoadResponse(testInfo,
                             response,
                             POST,
                             uriText,
                             COMPLETED,
                             mediaType,
                             bulkDataFile,
                             analysis,
                             analysis.getRecordCount(),
                             allDataSourceMap,
                             allEntityTypeMap,
                             null,
                             null,
                             before,
                             after);

      } catch (Exception e) {
        System.err.println("********** FAILED TEST: " + testInfo);
        e.printStackTrace();
        if (e instanceof RuntimeException) throw ((RuntimeException) e);
        throw new RuntimeException(e);
      }
    });
  }

  private List<Arguments> getMaxFailureArgs() {
    try {
      File[] tempFiles = {
          File.createTempFile("failures-test", ".jsonl"),
          File.createTempFile("failures-test", ".jsonl"),
          File.createTempFile("failures-test", ".jsonl")
      };

      List<RecordHandler> handlers = new ArrayList<>(tempFiles.length);
      for (File tempFile : tempFiles) {
        FileOutputStream      fos     = new FileOutputStream(tempFile);
        BufferedOutputStream  bos     = new BufferedOutputStream(fos);
        OutputStreamWriter    osw     = new OutputStreamWriter(bos, UTF_8);
        RecordHandler         handler = new JsonLinesRecordHandler(osw);
        handlers.add(handler);
      }

      Map<FeatureType, Set<UsageType>> featureGenMap = featureGenMap(PERSON);
      Map<FeatureType, FeatureDensity> featDensityMap = featureDensityMap();

      // add 12 bad records mixed in with 10 good ones
      RecordHandler handler = new CompoundRecordHandler(handlers);
      this.dataGenerator.generateRecords(handler,
                                         PERSON,
                                         2,
                                         true,
                                         CUSTOMER_DATA_SOURCE,
                                         GENERIC_ENTITY_TYPE,
                                         featureGenMap,
                                         featDensityMap,
                                         true,
                                         true);

      this.dataGenerator.generateRecords(handler,
                                         PERSON,
                                         10,
                                         true,
                                         CUSTOMERS_DATA_SOURCE,
                                         PERSON_ENTITY_TYPE,
                                         featureGenMap,
                                         featDensityMap,
                                         true,
                                         true);

      this.dataGenerator.generateRecords(handler,
                                         PERSON,
                                         10,
                                         true,
                                         CUSTOMER_DATA_SOURCE,
                                         PERSON_ENTITY_TYPE,
                                         featureGenMap,
                                         featDensityMap,
                                         true,
                                         true);

      // the first handler gets 978 additional good records to make an even 1000
      // (this is the maximum that should be handled in a single thread)
      this.dataGenerator.generateRecords(handlers.get(0),
                                         PERSON,
                                         978,
                                         true,
                                         CUSTOMERS_DATA_SOURCE,
                                         PERSON_ENTITY_TYPE,
                                         featureGenMap,
                                         featDensityMap,
                                         true,
                                         true);

      // the second handler gets 979 additional good records to make for 1001
      // (this is the minimum to trigger concurrent handling)
      this.dataGenerator.generateRecords(handlers.get(1),
                                         PERSON,
                                         979,
                                         true,
                                         CUSTOMERS_DATA_SOURCE,
                                         PERSON_ENTITY_TYPE,
                                         featureGenMap,
                                         featDensityMap,
                                         true,
                                         true);

      List<Arguments> result = new LinkedList<>();

      Map<String, Integer> entityTypeFailures12 = new LinkedHashMap<>();
      entityTypeFailures12.put(GENERIC_ENTITY_TYPE, 2);
      entityTypeFailures12.put(PERSON_ENTITY_TYPE, 10);

      Map<String, Integer> entityTypeFailures5 = new LinkedHashMap<>();
      entityTypeFailures5.put(GENERIC_ENTITY_TYPE, 2);
      entityTypeFailures5.put(PERSON_ENTITY_TYPE, 3);

      for (int index = 0; index < tempFiles.length; index++) {
        File tempFile = tempFiles[index];
        int recordCount = (index == 2) ? 22 : (1000 + index);
        result.add(Arguments.of(
            recordCount,
            null,
            COMPLETED,
            Collections.singletonMap(CUSTOMER_DATA_SOURCE, 12),
            entityTypeFailures12,
            tempFile));

        result.add(Arguments.of(
            recordCount,
            -1,
            COMPLETED,
            Collections.singletonMap(CUSTOMER_DATA_SOURCE, 12),
            entityTypeFailures12,
            tempFile));

        result.add(Arguments.of(
            recordCount,
            5,
            ABORTED,
            Collections.singletonMap(CUSTOMER_DATA_SOURCE, 5),
            entityTypeFailures5,
            tempFile));
      }
      return result;

    } catch (RuntimeException e) {
      e.printStackTrace();
      throw e;
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @ParameterizedTest
  @MethodSource("getMaxFailureArgs")
  public void testMaxFailuresOnLoad(
      int                   recordCount,
      Integer               maxFailures,
      SzBulkDataStatus      expectedStatus,
      Map<String, Integer>  failuresByDataSource,
      Map<String, Integer>  failuresByEntityType,
      File                  dataFile)
  {
    this.performTest(() -> {
      this.livePurgeRepository();

      String testInfo = "recordCount=[ " + recordCount + " ], maxFailures=[ "
          + maxFailures + " ], status=[ "
          + expectedStatus + " ], failuresByDataSource=[ "
          + failuresByDataSource + " ], failuresByEntityType=[ "
          + failuresByEntityType + " ], dataFile=[ " + dataFile + " ]";

      String uriText = this.formatServerUri("bulk-data/load");

      MultivaluedMap queryParams = new MultivaluedHashMap();
      if (maxFailures != null) {
        queryParams.add("maxFailures", String.valueOf(maxFailures));
      }
      UriInfo uriInfo = this.newProxyUriInfo(uriText, queryParams);

      SzBulkLoadResponse response = null;
      try (InputStream is = new FileInputStream(dataFile);
           BufferedInputStream bis = new BufferedInputStream(is)) {
        long before = System.currentTimeMillis();
        response = this.bulkDataServices.loadBulkRecordsViaForm(
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            maxFailures == null ? -1 : maxFailures,
            MediaType.valueOf("text/plain"),
            bis,
            null,
            uriInfo);

        response.concludeTimers();
        long after = System.currentTimeMillis();

        this.validateLoadResponse(testInfo,
                                  response,
                                  POST,
                                  uriText,
                                  expectedStatus,
                                  MediaType.valueOf("text/plain"),
                                  dataFile,
                                  null,
                                  recordCount,
                                  null,
                                  null,
                                  failuresByDataSource,
                                  failuresByEntityType,
                                  before,
                                  after);

      } catch (Exception e) {
        System.err.println("********** FAILED TEST: " + testInfo);
        e.printStackTrace();
        if (e instanceof RuntimeException) throw ((RuntimeException) e);
        throw new RuntimeException(e);
      }
    });
  }

  /**
   *
   */
  private void validateAnalyzeResponse(String                     testInfo,
                                       SzBulkDataAnalysisResponse response,
                                       SzHttpMethod               httpMethod,
                                       String                     selfLink,
                                       MediaType                  mediaType,
                                       File                       bulkDataFile,
                                       SzBulkDataAnalysis         expected,
                                       long                       startTime,
                                       long                       endTime)
  {
    validateBasics(response, httpMethod, selfLink, startTime, endTime);

    SzBulkDataAnalysis actual = response.getData();

    assertNotNull(actual, "Response analysis is null: " + testInfo);

    assertEquals(expected.getCharacterEncoding(), actual.getCharacterEncoding(),
        "Character encoding in analysis not as expected: " + testInfo);

    assertEquals(expected.getMediaType(), actual.getMediaType(),
                 "Media type in analysis not as expected: " + testInfo);

    assertEquals(expected.getRecordCount(), actual.getRecordCount(),
                 "Total record count not as expected: " + testInfo);

    assertEquals(expected.getRecordsWithRecordIdCount(),
                 actual.getRecordsWithRecordIdCount(),
                 "Records with record ID count not as expected: "
                 + testInfo);

    assertEquals(expected.getRecordsWithDataSourceCount(),
                 actual.getRecordsWithDataSourceCount(),
                 "Records with data source count not as expected: "
                 + testInfo);

    assertEquals(expected.getRecordsWithEntityTypeCount(),
                 actual.getRecordsWithEntityTypeCount(),
                 "Records with entity type count not as expected: "
                 + testInfo);

    // validate the analysis by data source fields
    List<SzDataSourceRecordAnalysis> actualSourceList
        = actual.getAnalysisByDataSource();

    List<SzDataSourceRecordAnalysis> expectedSourceList
        = expected.getAnalysisByDataSource();

    int actualCount   = actualSourceList.size();
    int expectedCount = expectedSourceList.size();

    assertEquals(expectedCount, actualCount,
                 "The number of items in the analysis-by-data-source "
                  + "list is not as expected: " + testInfo);

    for (int index = 0; index < actualCount; index++) {
      SzDataSourceRecordAnalysis actualSourceAnalysis
          = actualSourceList.get(index);
      SzDataSourceRecordAnalysis expectedSourceAnalysis
          = expectedSourceList.get(index);

      String expectedSource = expectedSourceAnalysis.getDataSource();
      String actualSource   = actualSourceAnalysis.getDataSource();

      assertEquals(expectedSource, actualSource,
                   "The data sources do not match: " + testInfo);

      assertEquals(expectedSourceAnalysis.getRecordCount(),
                   actualSourceAnalysis.getRecordCount(),
                   "The record counts for the " + actualSource
                       + " data source do not match: " + testInfo);

      assertEquals(expectedSourceAnalysis.getRecordsWithRecordIdCount(),
                   actualSourceAnalysis.getRecordsWithRecordIdCount(),
                   "The records with record ID counts for the "
                       + actualSource + " data source do not match: "
                       + testInfo);

      assertEquals(expectedSourceAnalysis.getRecordsWithEntityTypeCount(),
                   actualSourceAnalysis.getRecordsWithEntityTypeCount(),
                   "The records with entity type counts for the "
                       + actualSource + " data source do not match: "
                       + testInfo);
    }


    // validate the analysis by entity type fields
    List<SzEntityTypeRecordAnalysis> actualTypeList
        = actual.getAnalysisByEntityType();

    List<SzEntityTypeRecordAnalysis> expectedTypeList
        = expected.getAnalysisByEntityType();

    actualCount   = actualTypeList.size();
    expectedCount = expectedTypeList.size();

    assertEquals(expectedCount, actualCount,
                 "The number of items in the analysis-by-entity-type "
                 + "list is not as expected: " + testInfo);

    for (int index = 0; index < actualCount; index++) {
      SzEntityTypeRecordAnalysis actualTypeAnalysis
          = actualTypeList.get(index);
      SzEntityTypeRecordAnalysis expectedTypeAnalysis
          = expectedTypeList.get(index);

      String expectedEntityType = expectedTypeAnalysis.getEntityType();
      String actualEntityType   = actualTypeAnalysis.getEntityType();

      assertEquals(expectedEntityType, actualEntityType,
                   "The entity types do not match: " + testInfo);

      assertEquals(expectedTypeAnalysis.getRecordCount(),
                   actualTypeAnalysis.getRecordCount(),
                   "The record counts for the " + actualEntityType
                       + " entity type do not match: " + testInfo);

      assertEquals(expectedTypeAnalysis.getRecordsWithRecordIdCount(),
                   actualTypeAnalysis.getRecordsWithRecordIdCount(),
                   "The records with record ID counts for the "
                       + actualEntityType + " entity type do not match: "
                       + testInfo);

      assertEquals(expectedTypeAnalysis.getRecordsWithDataSourceCount(),
                   actualTypeAnalysis.getRecordsWithDataSourceCount(),
                   "The records with data source counts for the "
                       + actualEntityType + " entity type do not match: "
                       + testInfo);
    }
  }

  /**
   *
   */
  private void validateLoadResponse(String                     testInfo,
                                    SzBulkLoadResponse         response,
                                    SzHttpMethod               httpMethod,
                                    String                     selfLink,
                                    SzBulkDataStatus           expectedStatus,
                                    MediaType                  mediaType,
                                    File                       bulkDataFile,
                                    SzBulkDataAnalysis         analysis,
                                    Integer                    totalRecordCount,
                                    Map<String, String>        dataSourceMap,
                                    Map<String, String>        entityTypeMap,
                                    Map<String, Integer>       failuresBySource,
                                    Map<String, Integer>       failuresByEType,
                                    long                       startTime,
                                    long                       endTime)
  {
    validateBasics(testInfo,
                   response,
                   200,
                   httpMethod,
                   selfLink,
                   startTime,
                   endTime,
                   this.getServerConcurrency());

    final Integer ZERO = new Integer(0);

    SzBulkLoadResult actual = response.getData();

    assertNotNull(actual, "Response result is null: " + testInfo);

    if (analysis != null) {
      assertEquals(analysis.getCharacterEncoding(), actual.getCharacterEncoding(),
                   "Character encoding in result not as expected: " + testInfo);

      assertEquals(analysis.getMediaType(), actual.getMediaType(),
                   "Media type in result not as expected: " + testInfo);

      assertEquals(analysis.getRecordCount(), totalRecordCount,
                   "Unexpected number of total records: " + testInfo);
    }

    if (expectedStatus != null) {
      assertEquals(
          expectedStatus, actual.getStatus(),
          "Unexpected status for bulk load result: " + testInfo);
    }

    // determine how many failures are expected
    int expectedFailures = 0;
    if (failuresBySource != null) {
      for (Integer count : failuresBySource.values()) {
        expectedFailures += count;
      }
    }

    int concurrency         = this.getServerConcurrency();
    int minExpectedFailures = expectedFailures;
    int maxExpectedFailures = totalRecordCount <= 1000
        ? expectedFailures : (expectedFailures + concurrency - 1);

    if (maxExpectedFailures < actual.getFailedRecordCount()) {
      if (analysis != null) {
        System.out.println("----------------------------------");
        System.out.println("RECORD COUNT: " + analysis.getRecordCount());
      }
      for (SzBulkLoadError error : actual.getTopErrors()) {
        System.out.println(error);
        System.out.println();
      }
    }

    if (minExpectedFailures == maxExpectedFailures) {
      assertEquals(expectedFailures, actual.getFailedRecordCount(),
                   "Unexpected number of failed records: " + testInfo);
    } else {
      assertTrue(
          minExpectedFailures <= actual.getFailedRecordCount(),
          "Actual number of failures (" + actual.getFailedRecordCount()
              + ") is fewer than expected (" + minExpectedFailures + "): "
              + testInfo);
      assertTrue(
          maxExpectedFailures >= actual.getFailedRecordCount(),
          "Actual number of failures (" + actual.getFailedRecordCount()
              + ") is more than expected (" + maxExpectedFailures + "): "
              + testInfo);
    }

    // check if nothing more to validate
    if (analysis == null) return;

    // get the analyses for missing data source and missing entity type
    SzDataSourceRecordAnalysis missingSourceAnalysis = null;
    for (SzDataSourceRecordAnalysis item: analysis.getAnalysisByDataSource()) {
      if (item.getDataSource() == null) {
        missingSourceAnalysis = item;
        break;
      }
    }

    // determine how many records are missing only data source, only entity type
    // as well as the number of them missing both
    int missingBothCount = (missingSourceAnalysis == null) ? 0
      : missingSourceAnalysis.getRecordCount()
        - missingSourceAnalysis.getRecordsWithEntityTypeCount();
    int missingDataSourceCount = analysis.getRecordCount()
        - analysis.getRecordsWithDataSourceCount() - missingBothCount;
    int missingEntityTypeCount = analysis.getRecordCount()
        - analysis.getRecordsWithEntityTypeCount() - missingBothCount;

    // check if those missing data source and entity type are mapped
    if (dataSourceMap.containsKey(null)) missingDataSourceCount = 0;
    if (entityTypeMap.containsKey(null)) missingEntityTypeCount = 0;
    if (dataSourceMap.containsKey(null) && entityTypeMap.containsKey(null)) {
      missingBothCount = 0;
    }
    int incompleteCount = missingBothCount + missingDataSourceCount
        + missingEntityTypeCount;

    // check the total incomplete count
    assertEquals(incompleteCount, actual.getIncompleteRecordCount(),
                 "Unexpected number of incomplete records: "
                     + testInfo);

    // now determine how many records should have loaded
    int expectLoaded = totalRecordCount - expectedFailures
        - incompleteCount;

    assertEquals(expectLoaded, actual.getLoadedRecordCount(),
                 "Unexpected number of loaded records: " + testInfo);

    // determine the expected counts by data source
    Map<String, Integer> dataSourceCountMap = new LinkedHashMap<>();
    Map<String, Integer> sourceIncompleteMap = new LinkedHashMap<>();
    analysis.getAnalysisByDataSource().forEach(sourceAnalysis -> {
      String origDataSource = sourceAnalysis.getDataSource();

      String dataSource = dataSourceMap.containsKey(origDataSource)
          ? dataSourceMap.get(origDataSource) : dataSourceMap.get(null);
      if (dataSource == null) dataSource = origDataSource;

      Integer currentCount = dataSourceCountMap.containsKey(dataSource)
        ? dataSourceCountMap.get(dataSource) : ZERO;

      Integer currentIncomplete = sourceIncompleteMap.containsKey(dataSource)
          ? sourceIncompleteMap.get(dataSource) : ZERO;

      // get the total number of records and increase it accordingly
      int sourceCount = sourceAnalysis.getRecordCount() + currentCount;
      if (sourceCount > 0) {
        dataSourceCountMap.put(dataSource, sourceCount);

        // determine how many are incomplete
        int incomplete = 0;
        if (dataSource == null) {
          incomplete = sourceCount;
        } else if (!entityTypeMap.containsKey(null)) {
          incomplete = sourceCount
              - sourceAnalysis.getRecordsWithEntityTypeCount();
        }

        // update the incomplete count
        sourceIncompleteMap.put(dataSource, currentIncomplete + incomplete);
      }
    });

    // determine the expected counts by entity type
    Map<String, Integer> entityTypeCountMap = new LinkedHashMap<>();
    Map<String, Integer> etypeIncompleteMap = new LinkedHashMap<>();
    analysis.getAnalysisByEntityType().forEach(etypeAnalysis -> {
      String origEntityType = etypeAnalysis.getEntityType();

      String entityType = entityTypeMap.containsKey(origEntityType)
          ? entityTypeMap.get(origEntityType) : entityTypeMap.get(null);
      if (entityTypeMap == null) {
        entityType = origEntityType;
      }

      Integer currentCount = entityTypeCountMap.containsKey(entityType)
          ? entityTypeCountMap.get(entityType) : ZERO;

      Integer currentIncomplete = etypeIncompleteMap.containsKey(entityType)
          ? etypeIncompleteMap.get(entityType) : ZERO;

      // get the total number of records and increase the count accordingly
      int etypeCount = etypeAnalysis.getRecordCount() + currentCount;
      if (etypeCount > 0) {
        entityTypeCountMap.put(entityType, etypeCount);

        // determine how many are incomplete
        int incomplete = 0;
        if (entityType == null) {
          incomplete = etypeCount;
        } else if (!dataSourceMap.containsKey(null)) {
          incomplete = etypeCount
              - etypeAnalysis.getRecordsWithDataSourceCount();
        }

        // update the incomplete count
        etypeIncompleteMap.put(entityType, currentIncomplete + incomplete);
      }
    });

    assertEquals(
        dataSourceCountMap.containsKey(null) ? dataSourceCountMap.get(null) : 0,
        actual.getMissingDataSourceCount(),
        "Missing data source count not as expected: " + testInfo);

    assertEquals(
        entityTypeCountMap.containsKey(null) ? entityTypeCountMap.get(null) : 0,
        actual.getMissingEntityTypeCount(),
        "Missing entity type count not as expected: " + testInfo);

    // validate the analysis by data source fields
    List<SzDataSourceBulkLoadResult> dataSourceResults
        = actual.getResultsByDataSource();

    int actualCount   = dataSourceResults.size();
    int expectedCount = dataSourceCountMap.size();

    assertEquals(expectedCount, actualCount,
                 "The number of items in the results-by-data-source "
                     + "list is not as expected: " + testInfo);

    for (SzDataSourceBulkLoadResult sourceResults : dataSourceResults) {
      String dataSource = sourceResults.getDataSource();

      assertTrue(dataSourceCountMap.containsKey(dataSource),
                 "Data source results for unexpected data source ("
                  + dataSource + "): " + testInfo);

      int sourceTotal       = dataSourceCountMap.get(dataSource);
      int sourceFailures    = (failuresBySource != null
                               && failuresBySource.containsKey(dataSource))
                               ? failuresBySource.get(dataSource) : 0;
      int sourceIncomplete  = sourceIncompleteMap.get(dataSource);
      int sourceLoaded      = sourceTotal - sourceIncomplete - sourceFailures;

      assertEquals(sourceTotal,
                   sourceResults.getRecordCount(),
                   "The record counts for the " + dataSource
                       + " data source do not match: " + testInfo);

      assertEquals(sourceIncomplete,
                   sourceResults.getIncompleteRecordCount(),
                   "The incomplete counts for the " + dataSource
                       + " data source do not match: " + testInfo);

      assertEquals(sourceFailures,
                   sourceResults.getFailedRecordCount(),
                   "The failed counts for the " + dataSource
                       + " data source do not match: " + testInfo);

      assertEquals(sourceLoaded,
                   sourceResults.getLoadedRecordCount(),
                   "The loaded counts for the " + dataSource
                       + " data source do not match: " + testInfo);
    }

    // validate the analysis by data source fields
    List<SzEntityTypeBulkLoadResult> entityTypeResults
        = actual.getResultsByEntityType();

    actualCount   = entityTypeResults.size();
    expectedCount = entityTypeCountMap.size();

    assertEquals(expectedCount, actualCount,
                 "The number of items in the results-by-entity-type "
                     + "list is not as expected: " + testInfo);

    for (SzEntityTypeBulkLoadResult etypeResults : entityTypeResults) {
      String entityType = etypeResults.getEntityType();

      assertTrue(entityTypeCountMap.containsKey(entityType),
                 "Entity type results for unexpected entity type ("
                     + entityType + "): " + testInfo);

      int etypeTotal        = entityTypeCountMap.get(entityType);
      int etypeFailures     = (failuresByEType != null
                               && failuresByEType.containsKey(entityType))
                                ? failuresByEType.get(entityType) : 0;
      int etypeIncomplete   = etypeIncompleteMap.get(entityType);
      int etypeLoaded       = etypeTotal - etypeIncomplete - etypeFailures;

      assertEquals(etypeTotal,
                   etypeResults.getRecordCount(),
                   "The record counts for the " + entityType
                       + " entity type do not match: " + testInfo);

      assertEquals(etypeIncomplete,
                   etypeResults.getIncompleteRecordCount(),
                   "The incomplete counts for the " + entityType
                       + " entity type do not match: " + testInfo);

      assertEquals(etypeFailures,
                   etypeResults.getFailedRecordCount(),
                   "The failed counts for the " + entityType
                       + " entity type do not match: " + testInfo);

      assertEquals(etypeLoaded,
                   etypeResults.getLoadedRecordCount(),
                   "The loaded counts for the " + entityType
                       + " entity type do not match: " + testInfo);
    }

  }
}

