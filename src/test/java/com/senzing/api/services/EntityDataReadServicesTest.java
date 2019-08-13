package com.senzing.api.services;

import com.senzing.api.model.*;
import com.senzing.repomgr.RepositoryManager;
import com.senzing.util.JsonUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObjectBuilder;
import javax.ws.rs.core.UriInfo;
import java.io.*;
import java.net.URLEncoder;
import java.util.*;

import static com.senzing.api.model.SzHttpMethod.GET;
import static com.senzing.api.model.SzAttributeClass.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.TestInstance.Lifecycle;
import static com.senzing.api.model.SzFeatureInclusion.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@TestInstance(Lifecycle.PER_CLASS)
public class EntityDataReadServicesTest extends AbstractServiceTest {
  private static final String PASSENGERS = "PASSENGERS";
  private static final String EMPLOYEES  = "EMPLOYEES";
  private static final String VIPS       = "VIPS";

  private static final SzRecordId ABC123 = new SzRecordId(PASSENGERS,
                                                          "ABC123");
  private static final SzRecordId DEF456 = new SzRecordId(PASSENGERS,
                                                          "DEF456");
  private static final SzRecordId GHI789 = new SzRecordId(PASSENGERS,
                                                          "GHI789");
  private static final SzRecordId JKL012 = new SzRecordId(PASSENGERS,
                                                          "JKL012");
  private static final SzRecordId MNO345 = new SzRecordId(EMPLOYEES,
                                                          "MNO345");
  private static final SzRecordId PQR678 = new SzRecordId(EMPLOYEES,
                                                          "PQR678");
  private static final SzRecordId STU901 = new SzRecordId(VIPS,
                                                          "STU901");
  private static final SzRecordId XYZ234 = new SzRecordId(VIPS,
                                                          "XYZ234");

  private EntityDataServices entityDataServices;

  @BeforeAll
  public void initializeEnvironment() {
    this.initializeTestEnvironment();
    this.entityDataServices = new EntityDataServices();
  }

  /**
   * Overridden to configure some data sources.
   */
  protected void prepareRepository() {
    File repoDirectory = this.getRepositoryDirectory();

    Set<String> dataSources = new LinkedHashSet<>();
    dataSources.add("PASSENGERS");
    dataSources.add("EMPLOYEES");
    dataSources.add("VIPS");

    File passengerFile = this.preparePassengerFile();
    File employeeFile = this.prepareEmployeeFile();
    File vipFile = this.prepareVipFile();

    employeeFile.deleteOnExit();
    passengerFile.deleteOnExit();
    vipFile.deleteOnExit();

    RepositoryManager.configSources(repoDirectory,
                                    dataSources,
                                    true);

    RepositoryManager.loadFile(repoDirectory,
                               passengerFile,
                               PASSENGERS,
                               true);

    RepositoryManager.loadFile(repoDirectory,
                               employeeFile,
                               EMPLOYEES,
                               true);

    RepositoryManager.loadFile(repoDirectory,
                               vipFile,
                               VIPS,
                               true);
  }

  private File preparePassengerFile() {
    String[] headers = {
        "RECORD_ID", "NAME_FIRST", "NAME_LAST", "PHONE_NUMBER", "ADDR_FULL",
        "DATE_OF_BIRTH"};

    String[][] passengers = {
        {ABC123.getRecordId(), "Joe", "Schmoe", "702-555-1212",
            "101 Main Street, Las Vegas, NV 89101", "12-JAN-1981"},
        {DEF456.getRecordId(), "Joanne", "Smith", "212-555-1212",
            "101 Fifth Ave, Las Vegas, NV 10018", "15-MAY-1983"},
        {GHI789.getRecordId(), "John", "Doe", "818-555-1313",
            "100 Main Street, Los Angeles, CA 90012", "17-OCT-1978"},
        {JKL012.getRecordId(), "Jane", "Doe", "818-555-1212",
            "100 Main Street, Los Angeles, CA 90012", "5-FEB-1979"}
    };
    return this.prepareCSVFile("test-passengers-", headers, passengers);
  }

  private File prepareEmployeeFile() {
    String[] headers = {
        "RECORD_ID", "NAME_FIRST", "NAME_LAST", "PHONE_NUMBER", "ADDR_FULL",
        "DATE_OF_BIRTH"};

    String[][] employees = {
        {MNO345.getRecordId(), "Joseph", "Schmoe", "702-555-1212",
            "101 Main Street, Las Vegas, NV 89101", "12-JAN-1981"},
        {PQR678.getRecordId(), "Jo Anne", "Smith", "212-555-1212",
            "101 Fifth Ave, Las Vegas, NV 10018", "15-MAY-1983"}
    };

    return this.prepareJsonArrayFile("test-employees-", headers, employees);
  }

  private File prepareVipFile() {
    String[] headers = {
        "RECORD_ID", "NAME_FIRST", "NAME_LAST", "PHONE_NUMBER", "ADDR_FULL",
        "DATE_OF_BIRTH"};

    String[][] vips = {
        {STU901.getRecordId(), "John", "Doe", "818-555-1313",
            "100 Main Street, Los Angeles, CA 90012", "17-OCT-1978"},
        {XYZ234.getRecordId(), "Jane", "Doe", "818-555-1212",
            "100 Main Street, Los Angeles, CA 90012", "5-FEB-1979"}
    };

    return this.prepareJsonFile("test-vips-", headers, vips);
  }

  @AfterAll
  public void teardownEnvironment() {
    this.teardownTestEnvironment(true);
  }

  @Test
  public void getRecordTest() {
    final String dataSource = ABC123.getDataSourceCode();
    final String recordId = ABC123.getRecordId();

    this.assumeNativeApiAvailable();
    String uriText = this.formatServerUri(
        "data-sources/" + dataSource + "/records/" + recordId);
    UriInfo uriInfo = this.newProxyUriInfo(uriText);
    long before = System.currentTimeMillis();
    SzRecordResponse response = this.entityDataServices.getRecord(
        dataSource, recordId, false, uriInfo);
    response.concludeTimers();
    long after = System.currentTimeMillis();

    this.validateRecordResponse(
        response,
        dataSource,
        recordId,
        Collections.singleton("Schmoe Joe"),
        Collections.singleton("101 Main Street, Las Vegas, NV 89101"),
        Collections.singleton("702-555-1212"),
        null,
        Collections.singleton("DOB: 12-JAN-1981"),
        before,
        after,
        null);
  }

  @Test
  public void getRecordTestViaHttp() {
    final String dataSource = DEF456.getDataSourceCode();
    final String recordId = DEF456.getRecordId();

    this.assumeNativeApiAvailable();
    String uriText = this.formatServerUri(
        "data-sources/" + dataSource + "/records/" + recordId);

    long before = System.currentTimeMillis();
    SzRecordResponse response = this.invokeServerViaHttp(
        GET, uriText, SzRecordResponse.class);
    response.concludeTimers();
    long after = System.currentTimeMillis();

    this.validateRecordResponse(
        response,
        dataSource,
        recordId,
        Collections.singleton("Smith Joanne"),
        Collections.singleton("101 Fifth Ave, Las Vegas, NV 10018"),
        Collections.singleton("212-555-1212"),
        null,
        Collections.singleton("DOB: 15-MAY-1983"),
        before,
        after,
        null);
  }

  @Test
  public void getRecordWithRawTest() {
    final String dataSource = GHI789.getDataSourceCode();
    final String recordId = GHI789.getRecordId();

    this.assumeNativeApiAvailable();
    String uriText = this.formatServerUri(
        "data-sources/" + dataSource + "/records/" + recordId
            + "?withRaw=true");
    UriInfo uriInfo = this.newProxyUriInfo(uriText);
    long before = System.currentTimeMillis();
    SzRecordResponse response = this.entityDataServices.getRecord(
        dataSource, recordId, true, uriInfo);
    response.concludeTimers();
    long after = System.currentTimeMillis();

    this.validateRecordResponse(
        response,
        dataSource,
        recordId,
        Collections.singleton("Doe John"),
        Collections.singleton("100 Main Street, Los Angeles, CA 90012"),
        Collections.singleton("818-555-1313"),
        null,
        Collections.singleton("DOB: 17-OCT-1978"),
        before,
        after,
        true);
  }

  @Test
  public void getRecordWithRawTestViaHttp() {
    final String dataSource = JKL012.getDataSourceCode();
    final String recordId = JKL012.getRecordId();

    this.assumeNativeApiAvailable();
    String uriText = this.formatServerUri(
        "data-sources/" + dataSource + "/records/" + recordId
            + "?withRaw=true");

    long before = System.currentTimeMillis();
    SzRecordResponse response = this.invokeServerViaHttp(
        GET, uriText, SzRecordResponse.class);
    response.concludeTimers();
    long after = System.currentTimeMillis();

    this.validateRecordResponse(
        response,
        dataSource,
        recordId,
        Collections.singleton("Doe Jane"),
        Collections.singleton("100 Main Street, Los Angeles, CA 90012"),
        Collections.singleton("818-555-1212"),
        null,
        Collections.singleton("DOB: 5-FEB-1979"),
        before,
        after,
        true);
  }

  @Test
  public void getRecordWithoutRawTest() {
    final String dataSource = MNO345.getDataSourceCode();
    final String recordId = MNO345.getRecordId();

    this.assumeNativeApiAvailable();
    String uriText = this.formatServerUri(
        "data-sources/" + dataSource + "/records/" + recordId
            + "?withRaw=false");
    UriInfo uriInfo = this.newProxyUriInfo(uriText);
    long before = System.currentTimeMillis();
    SzRecordResponse response = this.entityDataServices.getRecord(
        dataSource, recordId, false, uriInfo);
    response.concludeTimers();
    long after = System.currentTimeMillis();

    this.validateRecordResponse(
        response,
        dataSource,
        recordId,
        Collections.singleton("Schmoe Joseph"),
        Collections.singleton("101 Main Street, Las Vegas, NV 89101"),
        Collections.singleton("702-555-1212"),
        null,
        Collections.singleton("DOB: 12-JAN-1981"),
        before,
        after,
        false);
  }

  @Test
  public void getRecordWithoutRawTestViaHttp() {
    final String dataSource = PQR678.getDataSourceCode();
    final String recordId = PQR678.getRecordId();

    this.assumeNativeApiAvailable();
    String uriText = this.formatServerUri(
        "data-sources/" + dataSource + "/records/" + recordId
            + "?withRaw=false");

    long before = System.currentTimeMillis();
    SzRecordResponse response = this.invokeServerViaHttp(
        GET, uriText, SzRecordResponse.class);
    response.concludeTimers();
    long after = System.currentTimeMillis();

    this.validateRecordResponse(
        response,
        dataSource,
        recordId,
        Collections.singleton("Smith Jo Anne"),
        Collections.singleton("101 Fifth Ave, Las Vegas, NV 10018"),
        Collections.singleton("212-555-1212"),
        null,
        Collections.singleton("DOB: 15-MAY-1983"),
        before,
        after,
        false);
  }

  private Long getEntityIdForRecordId(SzRecordId recordId) {
    String uriText = this.formatServerUri(
        "data-sources/" + recordId.getDataSourceCode() + "/records/"
            + recordId.getRecordId() + "/entity");
    UriInfo uriInfo = this.newProxyUriInfo(uriText);

    SzEntityResponse response = this.entityDataServices.getEntityByRecordId(
        recordId.getDataSourceCode(),
        recordId.getRecordId(),
        false,
        false,
        true,
        WITH_DUPLICATES,
        uriInfo);

    SzEntityData data = response.getData();

    SzResolvedEntity entity = data.getResolvedEntity();

    return entity.getEntityId();
  }

  private List<List> joeSchmoeEntityArgs() {
    final SzRecordId recordId1 = ABC123;
    final SzRecordId recordId2 = MNO345;

    Map<SzAttributeClass, Set<String>> expectedDataMap = new LinkedHashMap<>();
    expectedDataMap.put(NAME, set("Joseph Schmoe"));
    expectedDataMap.put(ADDRESS, set("101 Main Street, Las Vegas, NV 89101"));
    expectedDataMap.put(PHONE, set("702-555-1212"));
    expectedDataMap.put(CHARACTERISTIC, set("DOB: 1981-01-12"));

    Map<String, Set<String>> primaryFeatureValues = new LinkedHashMap<>();
    primaryFeatureValues.put("NAME", set("Joseph Schmoe"));

    Map<String, Set<String>> duplicateFeatureValues = new LinkedHashMap<>();
    duplicateFeatureValues.put("NAME", set("Joe Schmoe"));

    final int expectedRecordCount = 2;
    final int expectedRelatedCount = 0;

    Map<String, Integer> expectedFeatureCounts = new LinkedHashMap<>();
    expectedFeatureCounts.put("NAME", 1);
    expectedFeatureCounts.put("DOB", 1);
    expectedFeatureCounts.put("ADDRESS", 1);
    expectedFeatureCounts.put("PHONE", 1);

    Set<SzRecordId> expectedRecordIds = set(recordId1, recordId2);
    List<List> result = new ArrayList<>(2);
    result.add(
        list(recordId1,
             null,
             null,
             null,
             null,
             expectedRecordCount,
             expectedRecordIds,
             expectedRelatedCount,
             expectedFeatureCounts,
             primaryFeatureValues,
             duplicateFeatureValues,
             expectedDataMap));
    result.add(
        list(recordId2,
             null,
             null,
             null,
             null,
             expectedRecordCount,
             expectedRecordIds,
             expectedRelatedCount,
             expectedFeatureCounts,
             primaryFeatureValues,
             duplicateFeatureValues,
             expectedDataMap));
    return result;
  }

  private List<List> joanneSmithEntityArgs() {
    final SzRecordId recordId1 = DEF456;
    final SzRecordId recordId2 = PQR678;
    Map<SzAttributeClass, Set<String>> expectedDataMap = new LinkedHashMap<>();
    expectedDataMap.put(NAME, set("Joanne Smith", "Jo Anne Smith"));
    expectedDataMap.put(ADDRESS, set("101 Fifth Ave, Las Vegas, NV 10018"));
    expectedDataMap.put(PHONE, set("212-555-1212"));
    expectedDataMap.put(CHARACTERISTIC, set("DOB: 1983-05-15"));

    Map<String, Set<String>> primaryFeatureValues = new LinkedHashMap<>();
    primaryFeatureValues.put("NAME", set("Joanne Smith", "Jo Anne Smith"));
    Map<String, Set<String>> duplicateFeatureValues = null;

    final int expectedRecordCount = 2;
    final int expectedRelatedCount = 0;

    Map<String, Integer> expectedFeatureCounts = new LinkedHashMap<>();
    expectedFeatureCounts.put("NAME", 2);
    expectedFeatureCounts.put("DOB", 1);
    expectedFeatureCounts.put("ADDRESS", 1);
    expectedFeatureCounts.put("PHONE", 1);

    Set<SzRecordId> expectedRecordIds = set(recordId1, recordId2);
    List<List> result = new ArrayList<>(2);
    result.add(
        list(recordId1,
             null,
             null,
             null,
             null,
             expectedRecordCount,
             expectedRecordIds,
             expectedRelatedCount,
             expectedFeatureCounts,
             primaryFeatureValues,
             duplicateFeatureValues,
             expectedDataMap));
    result.add(
        list(recordId2,
             null,
             null,
             null,
             null,
             expectedRecordCount,
             expectedRecordIds,
             expectedRelatedCount,
             expectedFeatureCounts,
             primaryFeatureValues,
             duplicateFeatureValues,
             expectedDataMap));
    return result;
  }

  private List<List> johnDoeEntityArgs() {
    final SzRecordId recordId1 = GHI789;
    final SzRecordId recordId2 = STU901;
    Map<SzAttributeClass, Set<String>> expectedDataMap = new LinkedHashMap<>();
    expectedDataMap.put(NAME, set("John Doe"));
    expectedDataMap.put(ADDRESS, set("100 Main Street, Los Angeles, CA 90012"));
    expectedDataMap.put(PHONE, set("818-555-1313"));
    expectedDataMap.put(CHARACTERISTIC, set("DOB: 1978-10-17"));

    Map<String, Set<String>> primaryFeatureValues = new LinkedHashMap<>();
    primaryFeatureValues.put("NAME", set("John Doe"));
    Map<String, Set<String>> duplicateFeatureValues = null;

    final int expectedRecordCount = 2;
    final int expectedRelatedCount = 1;

    Map<String, Integer> expectedFeatureCounts = new LinkedHashMap<>();
    expectedFeatureCounts.put("NAME", 1);
    expectedFeatureCounts.put("DOB", 1);
    expectedFeatureCounts.put("ADDRESS", 1);
    expectedFeatureCounts.put("PHONE", 1);

    Set<SzRecordId> expectedRecordIds = set(recordId1, recordId2);
    List<List> result = new ArrayList<>(2);
    result.add(
        list(recordId1,
             null,
             null,
             null,
             null,
             expectedRecordCount,
             expectedRecordIds,
             expectedRelatedCount,
             expectedFeatureCounts,
             primaryFeatureValues,
             duplicateFeatureValues,
             expectedDataMap));
    result.add(
        list(recordId2,
             null,
             null,
             null,
             null,
             expectedRecordCount,
             expectedRecordIds,
             expectedRelatedCount,
             expectedFeatureCounts,
             primaryFeatureValues,
             duplicateFeatureValues,
             expectedDataMap));
    return result;
  }

  private List<List> janeDoeEntityArgs() {
    final SzRecordId recordId1 = JKL012;
    final SzRecordId recordId2 = XYZ234;
    Map<SzAttributeClass, Set<String>> expectedDataMap = new LinkedHashMap<>();
    expectedDataMap.put(NAME, set("Jane Doe"));
    expectedDataMap.put(ADDRESS, set("100 Main Street, Los Angeles, CA 90012"));
    expectedDataMap.put(PHONE, set("818-555-1212"));
    expectedDataMap.put(CHARACTERISTIC, set("DOB: 1979-02-05"));

    Map<String, Set<String>> primaryFeatureValues = new LinkedHashMap<>();
    primaryFeatureValues.put("NAME", set("Jane Doe"));
    Map<String, Set<String>> duplicateFeatureValues = null;

    final int expectedRecordCount = 2;
    final int expectedRelatedCount = 1;

    Map<String, Integer> expectedFeatureCounts = new LinkedHashMap<>();
    expectedFeatureCounts.put("NAME", 1);
    expectedFeatureCounts.put("DOB", 1);
    expectedFeatureCounts.put("ADDRESS", 1);
    expectedFeatureCounts.put("PHONE", 1);

    Set<SzRecordId> expectedRecordIds = set(recordId1, recordId2);
    List<List> result = new ArrayList<>(2);
    result.add(
        list(recordId1,
             null,
             null,
             null,
             null,
             expectedRecordCount,
             expectedRecordIds,
             expectedRelatedCount,
             expectedFeatureCounts,
             primaryFeatureValues,
             duplicateFeatureValues,
             expectedDataMap));
    result.add(
        list(recordId2,
             null,
             null,
             null,
             null,
             expectedRecordCount,
             expectedRecordIds,
             expectedRelatedCount,
             expectedFeatureCounts,
             primaryFeatureValues,
             duplicateFeatureValues,
             expectedDataMap));
    return result;
  }

  private List<Arguments> getEntityParameters() {

    List<List> baseArgs = new LinkedList<>();
    baseArgs.addAll(joeSchmoeEntityArgs());
    baseArgs.addAll(joanneSmithEntityArgs());
    baseArgs.addAll(johnDoeEntityArgs());
    baseArgs.addAll(janeDoeEntityArgs());

    List<Arguments> result = new LinkedList<>();

    Boolean[] booleanVariants = {null, true, false};
    List<SzFeatureInclusion> featureModes = new LinkedList<>();
    featureModes.add(null);
    for (SzFeatureInclusion featureMode : SzFeatureInclusion.values()) {
      featureModes.add(featureMode);
    }
    baseArgs.forEach(baseArgList -> {
      for (Boolean withRaw : booleanVariants) {
        for (Boolean forceMinimal : booleanVariants) {
          for (Boolean withRelated : booleanVariants) {
            for (SzFeatureInclusion featureMode : featureModes) {
              Object[] argArray = baseArgList.toArray();

              argArray[1] = withRaw;
              argArray[2] = withRelated;
              argArray[3] = forceMinimal;
              argArray[4] = featureMode;

              result.add(arguments(argArray));
            }
          }
        }
      }
    });

    return result;

  }

  private StringBuilder buildEntityQueryString(StringBuilder      sb,
                                               Boolean            withRaw,
                                               Boolean            withRelated,
                                               Boolean            forceMinimal,
                                               SzFeatureInclusion featureMode)
  {
    String prefix = "?";
    if (forceMinimal != null) {
      sb.append(prefix).append("forceMinimal=").append(forceMinimal);
      prefix = "&";
    }
    if (featureMode != null) {
      sb.append(prefix).append("featureMode=").append(featureMode);
      prefix = "&";
    }
    if (withRelated != null) {
      sb.append(prefix).append("withRelated=").append(withRelated);
      prefix = "&";
    }
    if (withRaw != null) {
      sb.append(prefix).append("withRaw=").append(withRaw);
    }
    return sb;
  }

  @ParameterizedTest
  @MethodSource("getEntityParameters")
  public void getEntityByRecordIdTest(
      SzRecordId                          keyRecordId,
      Boolean                             withRaw,
      Boolean                             withRelated,
      Boolean                             forceMinimal,
      SzFeatureInclusion                  featureMode,
      Integer                             expectedRecordCount,
      Set<SzRecordId>                     expectedRecordIds,
      Integer                             relatedEntityCount,
      Map<String,Integer>                 expectedFeatureCounts,
      Map<String,Set<String>>             primaryFeatureValues,
      Map<String,Set<String>>             duplicateFeatureValues,
      Map<SzAttributeClass, Set<String>>  expectedDataValues)
  {
    String testInfo = "keyRecord=[ " + keyRecordId
        + " ], forceMinimal=[ " + forceMinimal
        + " ], featureMode=[ " + featureMode
        + " ], withRelated=[ " + withRelated
        + " ], withRaw=[ " + withRaw + " ]";

    this.assumeNativeApiAvailable();
    StringBuilder sb = new StringBuilder();
    sb.append("data-sources/").append(keyRecordId.getDataSourceCode());
    sb.append("/records/").append(keyRecordId.getRecordId()).append("/entity");
    buildEntityQueryString(sb, withRaw, withRelated, forceMinimal, featureMode);

    String uriText = this.formatServerUri(sb.toString());
    UriInfo uriInfo = this.newProxyUriInfo(uriText);

    long before = System.currentTimeMillis();

    SzEntityResponse response = this.entityDataServices.getEntityByRecordId(
        keyRecordId.getDataSourceCode(),
        keyRecordId.getRecordId(),
        (withRaw != null ? withRaw : false),
        (withRelated != null ? withRelated : false),
        (forceMinimal != null ? forceMinimal : false),
        (featureMode != null ? featureMode : WITH_DUPLICATES),
        uriInfo);

    response.concludeTimers();
    long after = System.currentTimeMillis();

    this.validateEntityResponse(testInfo,
                                response,
                                uriText,
                                withRaw,
                                withRelated,
                                forceMinimal,
                                featureMode,
                                expectedRecordCount,
                                expectedRecordIds,
                                relatedEntityCount,
                                expectedFeatureCounts,
                                primaryFeatureValues,
                                duplicateFeatureValues,
                                expectedDataValues,
                                before,
                                after);
  }

  @ParameterizedTest
  @MethodSource("getEntityParameters")
  public void getEntityByRecordIdTestViaHttp(
      SzRecordId                          keyRecordId,
      Boolean                             withRaw,
      Boolean                             withRelated,
      Boolean                             forceMinimal,
      SzFeatureInclusion                  featureMode,
      Integer                             expectedRecordCount,
      Set<SzRecordId>                     expectedRecordIds,
      Integer                             relatedEntityCount,
      Map<String,Integer>                 expectedFeatureCounts,
      Map<String,Set<String>>             primaryFeatureValues,
      Map<String,Set<String>>             duplicateFeatureValues,
      Map<SzAttributeClass, Set<String>>  expectedDataValues)
  {
    String testInfo = "keyRecord=[ " + keyRecordId
        + " ], forceMinimal=[ " + forceMinimal
        + " ], featureMode=[ " + featureMode
        + " ], withRelated=[ " + withRelated
        + " ], withRaw=[ " + withRaw + " ]";
    this.assumeNativeApiAvailable();

    StringBuilder sb = new StringBuilder();
    sb.append("data-sources/").append(keyRecordId.getDataSourceCode());
    sb.append("/records/").append(keyRecordId.getRecordId()).append("/entity");
    buildEntityQueryString(sb, withRaw, withRelated, forceMinimal, featureMode);

    String uriText = this.formatServerUri(sb.toString());

    long before = System.currentTimeMillis();
    SzEntityResponse response = this.invokeServerViaHttp(
        GET, uriText, SzEntityResponse.class);
    response.concludeTimers();
    long after = System.currentTimeMillis();

    this.validateEntityResponse(testInfo,
                                response,
                                uriText,
                                withRaw,
                                withRelated,
                                forceMinimal,
                                featureMode,
                                expectedRecordCount,
                                expectedRecordIds,
                                relatedEntityCount,
                                expectedFeatureCounts,
                                primaryFeatureValues,
                                duplicateFeatureValues,
                                expectedDataValues,
                                before,
                                after);
  }

  @ParameterizedTest
  @MethodSource("getEntityParameters")
  public void getEntityByEntityIdTest(
      SzRecordId                          keyRecordId,
      Boolean                             withRaw,
      Boolean                             withRelated,
      Boolean                             forceMinimal,
      SzFeatureInclusion                  featureMode,
      Integer                             expectedRecordCount,
      Set<SzRecordId>                     expectedRecordIds,
      Integer                             relatedEntityCount,
      Map<String,Integer>                 expectedFeatureCounts,
      Map<String,Set<String>>             primaryFeatureValues,
      Map<String,Set<String>>             duplicateFeatureValues,
      Map<SzAttributeClass, Set<String>>  expectedDataValues)
  {
    String testInfo = "keyRecord=[ " + keyRecordId
        + " ], forceMinimal=[ " + forceMinimal
        + " ], featureMode=[ " + featureMode
        + " ], withRelated=[ " + withRelated
        + " ], withRaw=[ " + withRaw + " ]";

    this.assumeNativeApiAvailable();

    final Long entityId = this.getEntityIdForRecordId(keyRecordId);

    StringBuilder sb = new StringBuilder();
    sb.append("entities/").append(entityId);
    buildEntityQueryString(sb, withRaw, withRelated, forceMinimal, featureMode);

    String uriText = this.formatServerUri(sb.toString());
    UriInfo uriInfo = this.newProxyUriInfo(uriText);

    long before = System.currentTimeMillis();

    SzEntityResponse response = this.entityDataServices.getEntityByRecordId(
        keyRecordId.getDataSourceCode(),
        keyRecordId.getRecordId(),
        (withRaw != null ? withRaw : false),
        (withRelated != null ? withRelated : false),
        (forceMinimal != null ? forceMinimal : false),
        (featureMode != null ? featureMode : WITH_DUPLICATES),
        uriInfo);

    response.concludeTimers();
    long after = System.currentTimeMillis();

    this.validateEntityResponse(testInfo,
                                response,
                                uriText,
                                withRaw,
                                withRelated,
                                forceMinimal,
                                featureMode,
                                expectedRecordCount,
                                expectedRecordIds,
                                relatedEntityCount,
                                expectedFeatureCounts,
                                primaryFeatureValues,
                                duplicateFeatureValues,
                                expectedDataValues,
                                before,
                                after);
  }

  @ParameterizedTest
  @MethodSource("getEntityParameters")
  public void getEntityByEntityIdTestViaHttp(
      SzRecordId                          keyRecordId,
      Boolean                             withRaw,
      Boolean                             withRelated,
      Boolean                             forceMinimal,
      SzFeatureInclusion                  featureMode,
      Integer                             expectedRecordCount,
      Set<SzRecordId>                     expectedRecordIds,
      Integer                             relatedEntityCount,
      Map<String,Integer>                 expectedFeatureCounts,
      Map<String,Set<String>>             primaryFeatureValues,
      Map<String,Set<String>>             duplicateFeatureValues,
      Map<SzAttributeClass, Set<String>>  expectedDataValues)
  {
    String testInfo = "keyRecord=[ " + keyRecordId
        + " ], forceMinimal=[ " + forceMinimal
        + " ], featureMode=[ " + featureMode
        + " ], withRelated=[ " + withRelated
        + " ], withRaw=[ " + withRaw + " ]";
    this.assumeNativeApiAvailable();

    final Long entityId = this.getEntityIdForRecordId(keyRecordId);

    StringBuilder sb = new StringBuilder();
    sb.append("entities/").append(entityId);
    buildEntityQueryString(sb, withRaw, withRelated, forceMinimal, featureMode);

    String uriText = this.formatServerUri(sb.toString());

    long before = System.currentTimeMillis();
    SzEntityResponse response = this.invokeServerViaHttp(
        GET, uriText, SzEntityResponse.class);
    response.concludeTimers();
    long after = System.currentTimeMillis();

    this.validateEntityResponse(testInfo,
                                response,
                                uriText,
                                withRaw,
                                withRelated,
                                forceMinimal,
                                featureMode,
                                expectedRecordCount,
                                expectedRecordIds,
                                relatedEntityCount,
                                expectedFeatureCounts,
                                primaryFeatureValues,
                                duplicateFeatureValues,
                                expectedDataValues,
                                before,
                                after);
  }

  private static class Criterion {
    private String key;
    private Set<String> values;

    private Criterion(String key, String... values) {
      this.key = key;
      this.values = new LinkedHashSet<>();
      for (String value : values) {
        this.values.add(value);
      }
    }
  }

  private static Criterion criterion(String key, String... values) {
    return new Criterion(key, values);
  }

  private static Map<String, Set<String>> criteria(String key, String... values) {
    Criterion criterion = criterion(key, values);
    return criteria(criterion);
  }

  private static Map<String, Set<String>> criteria(Criterion... criteria) {
    Map<String, Set<String>> result = new LinkedHashMap<>();
    for (Criterion criterion : criteria) {
      Set<String> values = result.get(criterion.key);
      if (values == null) {
        result.put(criterion.key, criterion.values);
      } else {
        values.addAll(criterion.values);
      }
    }
    return result;
  }

  private List<Arguments> searchParameters() {
    Map<Map<String, Set<String>>, Integer> searchCountMap = new LinkedHashMap<>();

    searchCountMap.put(criteria("PHONE_NUMBER", "702-555-1212"), 1);
    searchCountMap.put(criteria("PHONE_NUMBER", "212-555-1212"), 1);
    searchCountMap.put(criteria("PHONE_NUMBER", "818-555-1313"), 1);
    searchCountMap.put(criteria("PHONE_NUMBER", "818-555-1212"), 1);
    searchCountMap.put(criteria("PHONE_NUMBER", "818-555-1212", "818-555-1313"), 2);
    searchCountMap.put(
        criteria(criterion("ADDR_LINE1", "100 MAIN STREET"),
                 criterion("ADDR_CITY", "LOS ANGELES"),
                 criterion("ADDR_STATE", "CALIFORNIA"),
                 criterion("ADDR_POSTAL_CODE", "90012")),
        2);
    searchCountMap.put(
        criteria(criterion("NAME_FULL", "JOHN DOE", "JANE DOE"),
                 criterion("ADDR_LINE1", "100 MAIN STREET"),
                 criterion("ADDR_CITY", "LOS ANGELES"),
                 criterion("ADDR_STATE", "CALIFORNIA"),
                 criterion("ADDR_POSTAL_CODE", "90012")),
        2);
    searchCountMap.put(
        criteria(criterion("NAME_FULL", "JOHN DOE"),
                 criterion("ADDR_LINE1", "100 MAIN STREET"),
                 criterion("ADDR_CITY", "LOS ANGELES"),
                 criterion("ADDR_STATE", "CALIFORNIA"),
                 criterion("ADDR_POSTAL_CODE", "90012")),
        2);

    List<Arguments> list = new LinkedList<>();

    Boolean[] booleanVariants = {null, true, false};
    List<SzFeatureInclusion> featureModes = new LinkedList<>();
    featureModes.add(null);
    for (SzFeatureInclusion featureMode : SzFeatureInclusion.values()) {
      featureModes.add(featureMode);
    }
    searchCountMap.entrySet().forEach(entry -> {
      Map<String, Set<String>> criteria = entry.getKey();
      Integer resultCount = entry.getValue();

      for (Boolean withRaw : booleanVariants) {
        for (Boolean forceMinimal : booleanVariants) {
          for (Boolean withRelationships : booleanVariants) {
            for (SzFeatureInclusion featureMode : featureModes) {
              list.add(arguments(criteria,
                                 resultCount,
                                 forceMinimal,
                                 featureMode,
                                 withRelationships,
                                 withRaw));
            }
          }
        }
      }
    });

    return list;
  }

  @ParameterizedTest
  @MethodSource("searchParameters")
  public void searchByJsonAttrsTest(Map<String, Set<String>> criteria,
                                    Integer expectedCount,
                                    Boolean forceMinimal,
                                    SzFeatureInclusion featureMode,
                                    Boolean withRelationships,
                                    Boolean withRaw)
      throws UnsupportedEncodingException {
    String testInfo = "criteria=[ " + criteria
        + " ], forceMinimal=[ " + forceMinimal
        + " ], featureMode=[ " + featureMode
        + " ], withRelationships=[ " + withRelationships
        + " ], withRaw=[ " + withRaw + " ]";

    JsonObjectBuilder job = Json.createObjectBuilder();
    criteria.entrySet().forEach(entry -> {
      String key = entry.getKey();
      Set<String> values = entry.getValue();
      if (values.size() == 1) {
        job.add(key, values.iterator().next());
      } else {
        JsonArrayBuilder jab = Json.createArrayBuilder();
        for (String value : values) {
          JsonObjectBuilder job2 = Json.createObjectBuilder();
          job2.add(key, value);
          jab.add(job2);
        }
        job.add(key, jab);
      }
    });
    String attrs = JsonUtils.toJsonText(job);

    this.assumeNativeApiAvailable();
    String uriText = this.formatServerUri(
        "entities?attrs=" + URLEncoder.encode(attrs, "UTF-8"));
    if (forceMinimal != null) {
      uriText += ("&forceMinimal=" + forceMinimal);
    }
    if (featureMode != null) {
      uriText += ("&featureMode=" + featureMode);
    }
    if (withRelationships != null) {
      uriText += ("&withRelationships=" + withRelationships);
    }
    if (withRaw != null) {
      uriText += ("&withRaw=" + withRaw);
    }
    UriInfo uriInfo = this.newProxyUriInfo(uriText);

    long before = System.currentTimeMillis();
    SzAttributeSearchResponse response
        = this.entityDataServices.searchByAttributes(
        attrs,
        (forceMinimal != null ? forceMinimal : false),
        (featureMode != null ? featureMode : WITH_DUPLICATES),
        (withRelationships != null ? withRelationships : false),
        (withRaw != null ? withRaw : false),
        uriInfo);

    response.concludeTimers();
    long after = System.currentTimeMillis();

    this.validateSearchResponse(
        testInfo,
        response,
        uriText,
        expectedCount,
        withRelationships,
        forceMinimal,
        featureMode,
        before,
        after,
        withRaw);
  }

  @ParameterizedTest
  @MethodSource("searchParameters")
  public void searchByJsonAttrsTestViaHttp(
      Map<String, Set<String>> criteria,
      Integer expectedCount,
      Boolean forceMinimal,
      SzFeatureInclusion featureMode,
      Boolean withRelationships,
      Boolean withRaw)
      throws UnsupportedEncodingException {
    String testInfo = "criteria=[ " + criteria
        + " ], forceMinimal=[ " + forceMinimal
        + " ], featureMode=[ " + featureMode
        + " ], withRelationships=[ " + withRelationships
        + " ], withRaw=[ " + withRaw + " ]";

    JsonObjectBuilder job = Json.createObjectBuilder();
    criteria.entrySet().forEach(entry -> {
      String key = entry.getKey();
      Set<String> values = entry.getValue();
      if (values.size() == 1) {
        job.add(key, values.iterator().next());
      } else {
        JsonArrayBuilder jab = Json.createArrayBuilder();
        for (String value : values) {
          JsonObjectBuilder job2 = Json.createObjectBuilder();
          job2.add(key, value);
          jab.add(job2);
        }
        job.add(key, jab);
      }
    });
    String attrs = JsonUtils.toJsonText(job);

    this.assumeNativeApiAvailable();
    StringBuilder sb = new StringBuilder();
    sb.append("entities?attrs=").append(URLEncoder.encode(attrs, "UTF-8"));
    if (forceMinimal != null) {
      sb.append("&forceMinimal=").append(forceMinimal);
    }
    if (featureMode != null) {
      sb.append("&featureMode=").append(featureMode);
    }
    if (withRelationships != null) {
      sb.append("&withRelationships=").append(withRelationships);
    }
    if (withRaw != null) {
      sb.append("&withRaw=").append(withRaw);
    }
    String uriText = this.formatServerUri(sb.toString());

    long before = System.currentTimeMillis();
    SzAttributeSearchResponse response = this.invokeServerViaHttp(
        GET, uriText, SzAttributeSearchResponse.class);
    response.concludeTimers();
    long after = System.currentTimeMillis();

    this.validateSearchResponse(
        testInfo,
        response,
        uriText,
        expectedCount,
        withRelationships,
        forceMinimal,
        featureMode,
        before,
        after,
        withRaw);
  }

  @ParameterizedTest
  @MethodSource("searchParameters")
  public void searchByParamAttrsTestViaHttp(
      Map<String, Set<String>> criteria,
      Integer expectedCount,
      Boolean forceMinimal,
      SzFeatureInclusion featureMode,
      Boolean withRelationships,
      Boolean withRaw) {
    String testInfo = "criteria=[ " + criteria
        + " ], forceMinimal=[ " + forceMinimal
        + " ], featureMode=[ " + featureMode
        + " ], withRelationships=[ " + withRelationships
        + " ], withRaw=[ " + withRaw + " ]";

    this.assumeNativeApiAvailable();
    StringBuilder sb = new StringBuilder(criteria.size() * 50);
    criteria.entrySet().forEach(entry -> {
      try {
        String key = entry.getKey();
        Set<String> values = entry.getValue();
        String encodedKey = URLEncoder.encode(key, "UTF-8");
        for (String value : values) {
          String encodedVal = URLEncoder.encode(value, "UTF-8");
          sb.append("&attr_").append(encodedKey).append("=").append(encodedVal);
        }
      } catch (UnsupportedEncodingException cannotHappen) {
        throw new IllegalStateException(cannotHappen);
      }
    });
    // replace the "&" with a "?" at the start
    sb.setCharAt(0, '?');
    if (forceMinimal != null) {
      sb.append("&forceMinimal=").append(forceMinimal);
    }
    if (featureMode != null) {
      sb.append("&featureMode=").append(featureMode);
    }
    if (withRelationships != null) {
      sb.append("&withRelationships=").append(withRelationships);
    }
    if (withRaw != null) {
      sb.append("&withRaw=").append(withRaw);
    }
    String uriText = this.formatServerUri("entities" + sb.toString());

    long before = System.currentTimeMillis();
    SzAttributeSearchResponse response = this.invokeServerViaHttp(
        GET, uriText, SzAttributeSearchResponse.class);
    response.concludeTimers();
    long after = System.currentTimeMillis();

    this.validateSearchResponse(
        testInfo,
        response,
        uriText,
        expectedCount,
        withRelationships,
        forceMinimal,
        featureMode,
        before,
        after,
        withRaw);
  }
}
