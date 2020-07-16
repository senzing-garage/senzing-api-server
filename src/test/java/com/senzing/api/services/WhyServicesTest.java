package com.senzing.api.services;

import com.senzing.api.model.*;
import com.senzing.gen.api.invoker.ApiClient;
import com.senzing.gen.api.services.EntityDataApi;
import com.senzing.repomgr.RepositoryManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.ws.rs.core.UriInfo;
import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.*;

import static com.senzing.api.model.SzFeatureMode.NONE;
import static com.senzing.api.model.SzFeatureMode.WITH_DUPLICATES;
import static com.senzing.api.model.SzHttpMethod.GET;
import static com.senzing.api.services.ResponseValidators.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.TestInstance.Lifecycle;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@TestInstance(Lifecycle.PER_CLASS)
public class WhyServicesTest extends AbstractServiceTest {
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
  private static final SzRecordId ABC567 = new SzRecordId(EMPLOYEES,
                                                          "ABC567");
  private static final SzRecordId DEF890 = new SzRecordId(EMPLOYEES,
                                                          "DEF890");
  private static final SzRecordId STU901 = new SzRecordId(VIPS,
                                                          "STU901");
  private static final SzRecordId XYZ234 = new SzRecordId(VIPS,
                                                          "XYZ234");
  private static final SzRecordId GHI123 = new SzRecordId(VIPS,
                                                          "GHI123");
  private static final SzRecordId JKL456 = new SzRecordId(VIPS,
                                                          "JKL456");

  private static final List<SzRecordId> RECORD_IDS;

  static {
    List<SzRecordId> recordIds = new ArrayList<>(12);
    try {
      recordIds.add(ABC123);
      recordIds.add(DEF456);
      recordIds.add(GHI789);
      recordIds.add(JKL012);
      recordIds.add(MNO345);
      recordIds.add(PQR678);
      recordIds.add(ABC567);
      recordIds.add(DEF890);
      recordIds.add(STU901);
      recordIds.add(XYZ234);
      recordIds.add(GHI123);
      recordIds.add(JKL456);

    } catch (Exception e) {
      e.printStackTrace();
      throw new ExceptionInInitializerError(e);

    } finally {
      RECORD_IDS = Collections.unmodifiableList(recordIds);
    }
  }
  private WhyServices whyServices;
  private EntityDataServices entityDataServices;
  private EntityDataApi entityDataApi;

  @BeforeAll
  public void initializeEnvironment() {
    this.beginTests();
    this.initializeTestEnvironment();
    this.whyServices = new WhyServices();
    this.entityDataServices = new EntityDataServices();
    ApiClient apiClient = new ApiClient();
    apiClient.setBasePath(this.formatServerUri(""));
    this.entityDataApi = new EntityDataApi(apiClient);
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
                               null,
                               true);

    RepositoryManager.loadFile(repoDirectory,
                               employeeFile,
                               EMPLOYEES,
                               null,
                               true);

    RepositoryManager.loadFile(repoDirectory,
                               vipFile,
                               VIPS,
                               null,
                               true);
  }

  private File preparePassengerFile() {
    String[] headers = {
        "RECORD_ID", "NAME_FIRST", "NAME_LAST", "MOBILE_PHONE_NUMBER",
        "HOME_PHONE_NUMBER", "ADDR_FULL", "DATE_OF_BIRTH"};

    String[][] passengers = {
        {ABC123.getRecordId(), "Joe", "Schmoe", "702-555-1212", "702-777-2424",
            "101 Main Street, Las Vegas, NV 89101", "12-JAN-1981"},
        {DEF456.getRecordId(), "Joann", "Smith", "702-555-1212", "702-888-3939",
            "101 Fifth Ave, Las Vegas, NV 10018", "15-MAY-1983"},
        {GHI789.getRecordId(), "John", "Doe", "818-555-1313", "818-999-2121",
            "101 Fifth Ave, Las Vegas, NV 10018", "17-OCT-1978"},
        {JKL012.getRecordId(), "Jane", "Doe", "818-555-1313", "818-222-3131",
            "400 River Street, Pasadena, CA 90034", "23-APR-1974"}
    };
    return this.prepareCSVFile("test-passengers-", headers, passengers);
  }

  private File prepareEmployeeFile() {
    String[] headers = {
        "RECORD_ID", "NAME_FIRST", "NAME_LAST", "MOBILE_PHONE_NUMBER",
        "HOME_PHONE_NUMBER", "ADDR_FULL", "DATE_OF_BIRTH"};

    String[][] employees = {
        {MNO345.getRecordId(), "Bill", "Wright", "702-444-2121", "702-123-4567",
            "101 Main Street, Las Vegas, NV 89101", "22-AUG-1981"},
        {PQR678.getRecordId(), "Craig", "Smith", "212-555-1212", "702-888-3939",
            "451 Dover Street, Las Vegas, NV 89108", "17-NOV-1982"},
        {ABC567.getRecordId(), "Kim", "Long", "702-246-8024", "702-135-7913",
            "451 Dover Street, Las Vegas, NV 89108", "24-OCT-1976"},
        {DEF890.getRecordId(), "Kathy", "Osborne", "702-444-2121", "702-111-2222",
            "707 Seventh Ave, Las Vegas, NV 89143", "27-JUL-1981"}
    };

    return this.prepareJsonArrayFile("test-employees-", headers, employees);
  }

  private File prepareVipFile() {
    String[] headers = {
        "RECORD_ID", "NAME_FIRST", "NAME_LAST", "MOBILE_PHONE_NUMBER",
        "HOME_PHONE_NUMBER", "ADDR_FULL", "DATE_OF_BIRTH"};

    String[][] vips = {
        {STU901.getRecordId(), "Martha", "Wayne", "818-891-9292", "818-987-1234",
            "888 Sepulveda Blvd, Los Angeles, CA 90034", "27-NOV-1973"},
        {XYZ234.getRecordId(), "Jane", "Johnson", "702-333-7171", "702-123-9876",
            "400 River Street, Pasadena, CA 90034", "5-SEP-1975"},
        {GHI123.getRecordId(), "Martha", "Kent", "818-333-5757", "702-123-9876",
            "888 Sepulveda Blvd, Los Angeles, CA 90034", "17-OCT-1978"},
        {JKL456.getRecordId(), "Kelly", "Rogers", "702-333-7171", "702-789-6543",
            "707 Seventh Ave, Las Vegas, NV 89143", "5-FEB-1979"}
    };

    return this.prepareJsonFile("test-vips-", headers, vips);
  }

  @AfterAll
  public void teardownEnvironment() {
    try {
      this.teardownTestEnvironment();
      this.conditionallyLogCounts(true);
    } finally {
      this.endTests();
    }
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
        SzRelationshipMode.NONE,
        true,
        WITH_DUPLICATES,
        false,
        false,
        uriInfo);

    SzEntityData data = response.getData();

    SzResolvedEntity entity = data.getResolvedEntity();

    return entity.getEntityId();
  }

  private List<Arguments> getWhyEntityParameters() {
    Boolean[] booleanVariants = {null, true, false};
    List<Boolean> booleanVariantList = Arrays.asList(booleanVariants);

    List<SzFeatureMode> featureModes = new LinkedList<>();
    featureModes.add(null);
    for (SzFeatureMode featureMode : SzFeatureMode.values()) {
      featureModes.add(featureMode);
    }

    List<SzRecordId> recordIds = (RECORD_IDS.size() > 3)
        ? RECORD_IDS.subList(0, 3) : RECORD_IDS;
    
    List<List> parameters = generateCombinations(
        recordIds,            // recordId
        booleanVariantList,   // forceMinimal
        featureModes,         // featureMode
        booleanVariantList,   // withFeatureStats
        booleanVariantList,   // withInternalFeatures
        booleanVariantList,   // withRelationships
        booleanVariantList);  // withRaw

    List<Arguments> result = new ArrayList<>(parameters.size());

    parameters.forEach(list -> {
      result.add(arguments(list.toArray()));
    });

    return result;
  }

  private List<Arguments> getWhyRecordsParameters() {
    Boolean[] booleanVariants = {null, true, false};
    List<Boolean> booleanVariantList = Arrays.asList(booleanVariants);

    List<SzFeatureMode> featureModes = new LinkedList<>();
    featureModes.add(null);
    for (SzFeatureMode featureMode : SzFeatureMode.values()) {
      featureModes.add(featureMode);
    }

    int count = RECORD_IDS.size();
    int min1 = Math.max(((count / 2) - 3), 0);
    int max1 = count / 2;
    int min2 = Math.max(((count / 2) - 1), 0);
    int max2 = Math.max(((count / 2) + 2), count);

    min1 = (min1 < 0) ? 0 : min1;

    List<SzRecordId> recordIds1 = RECORD_IDS.subList(min1, max1);
    List<SzRecordId> recordIds2 = RECORD_IDS.subList(min2, max2);

    List<List> parameters = generateCombinations(
        recordIds1,                 // recordId1
        recordIds2,                 // recordId2
        booleanVariantList,         // forceMinimal
        featureModes,               // featureMode
        booleanVariantList,         // withFeatureStats
        booleanVariantList,         // withInternalFeatures
        booleanVariantList,         // withRelationships
        booleanVariantList);        // withRaw

    List<Arguments> result = new ArrayList<>(parameters.size());

    parameters.forEach(list -> {
      result.add(arguments(list.toArray()));
    });

    return result;
  }

  private StringBuilder buildWhyEntityQueryString(
      StringBuilder         sb,
      Boolean               forceMinimal,
      SzFeatureMode featureMode,
      Boolean               withFeatureStats,
      Boolean               withInternalFeatures,
      Boolean               withRelationships,
      Boolean               withRaw)
  {
    String prefix = "?";
    if (withRelationships != null) {
      sb.append(prefix).append("withRelationships=").append(withRelationships);
      prefix = "&";
    }
    if (withFeatureStats != null) {
      sb.append(prefix).append("withFeatureStats=").append(withFeatureStats);
      prefix = "&";
    }
    if (withInternalFeatures != null) {
      sb.append(prefix).append("withInternalFeatures=")
          .append(withInternalFeatures);
      prefix = "&";
    }
    if (featureMode != null) {
      sb.append(prefix).append("featureMode=").append(featureMode);
      prefix = "&";
    }
    if (forceMinimal != null) {
      sb.append(prefix).append("forceMinimal=").append(forceMinimal);
      prefix = "&";
    }
    if (withRaw != null) {
      sb.append(prefix).append("withRaw=").append(withRaw);
    }
    return sb;
  }

  private StringBuilder buildWhyRecordsQueryString(
      StringBuilder         sb,
      SzRecordId            recordId1,
      SzRecordId            recordId2,
      Boolean               forceMinimal,
      SzFeatureMode featureMode,
      Boolean               withFeatureStats,
      Boolean               withInternalFeatures,
      Boolean               withRelationships,
      Boolean               withRaw)
  {
    try {
      sb.append("?dataSource1=").append(
          URLEncoder.encode(recordId1.getDataSourceCode(), "UTF-8"));

      sb.append("&recordId1=").append(
          URLEncoder.encode(recordId1.getRecordId(), "UTF-8"));

      sb.append("&dataSource2=").append(
          URLEncoder.encode(recordId2.getDataSourceCode(), "UTF-8"));

      sb.append("&recordId2=").append(
          URLEncoder.encode(recordId2.getRecordId(), "UTF-8"));

      if (withRelationships != null) {
        sb.append("&withRelationships=").append(withRelationships);
      }
      if (withFeatureStats != null) {
        sb.append("&withFeatureStats=").append(withFeatureStats);
      }
      if (withInternalFeatures != null) {
        sb.append("&withInternalFeatures=").append(withInternalFeatures);
      }
      if (featureMode != null) {
        sb.append("&featureMode=").append(featureMode);
      }
      if (forceMinimal != null) {
        sb.append("&forceMinimal=").append(forceMinimal);
      }
      if (withRaw != null) {
        sb.append("&withRaw=").append(withRaw);
      }
      return sb;

    } catch (UnsupportedEncodingException cannotHappen) {
      throw new RuntimeException(cannotHappen);
    }
  }

  @ParameterizedTest
  @MethodSource("getWhyEntityParameters")
  public void whyEntityByRecordIdTest(SzRecordId          recordId,
                                      Boolean             forceMinimal,
                                      SzFeatureMode featureMode,
                                      Boolean             withFeatureStats,
                                      Boolean             withInternalFeatures,
                                      Boolean             withRelationships,
                                      Boolean             withRaw)
  {
    this.performTest(() -> {
      String testInfo = "recordId=[ " + recordId
          + " ], forceMinimal=[ " + forceMinimal
          + " ], featureMode=[ " + featureMode
          + " ], withFeatureStats=[ " + withFeatureStats
          + " ], withInternalFeatures=[ " + withInternalFeatures
          + " ], withRelationships=[ " + withRelationships
          + " ], withRaw=[ " + withRaw + " ]";

      StringBuilder sb = new StringBuilder();
      sb.append("data-sources/").append(urlEncode(recordId.getDataSourceCode()))
          .append("/records/").append(urlEncode(recordId.getRecordId()))
          .append("/entity/why");

      buildWhyEntityQueryString(sb,
                                forceMinimal,
                                featureMode,
                                withFeatureStats,
                                withInternalFeatures,
                                withRelationships,
                                withRaw);

      String uriText = this.formatServerUri(sb.toString());
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      long before = System.currentTimeMillis();

      SzWhyEntityResponse response = this.whyServices.whyEntityByRecordId(
          recordId.getDataSourceCode(),
          recordId.getRecordId(),
          (forceMinimal == null ? false : forceMinimal),
          (featureMode == null ? WITH_DUPLICATES : featureMode),
          (withFeatureStats == null ? true : withFeatureStats),
          (withInternalFeatures == null ? true : withInternalFeatures),
          (withRelationships == null ? false : withRelationships),
          (withRaw == null ? false : withRaw),
          uriInfo);

      response.concludeTimers();
      long after = System.currentTimeMillis();

      this.validateWhyEntityResponse(
          testInfo,
          response,
          GET,
          uriText,
          recordId,
          null,
          (forceMinimal == null ? false : forceMinimal),
          featureMode,
          (withFeatureStats == null ? true : withFeatureStats),
          (withInternalFeatures == null ? true : withInternalFeatures),
          (withRelationships == null ? false : withRelationships),
          withRaw,
          before,
          after);
    });
  }

  @ParameterizedTest
  @MethodSource("getWhyEntityParameters")
  public void whyEntityByRecordIdViaHttpTest(
      SzRecordId          recordId,
      Boolean             forceMinimal,
      SzFeatureMode featureMode,
      Boolean             withFeatureStats,
      Boolean             withInternalFeatures,
      Boolean             withRelationships,
      Boolean             withRaw)
  {
    this.performTest(() -> {
      String testInfo = "recordId=[ " + recordId
          + " ], forceMinimal=[ " + forceMinimal
          + " ], featureMode=[ " + featureMode
          + " ], withFeatureStats=[ " + withFeatureStats
          + " ], withInternalFeatures=[ " + withInternalFeatures
          + " ], withRelationships=[ " + withRelationships
          + " ], withRaw=[ " + withRaw + " ]";

      StringBuilder sb = new StringBuilder();
      sb.append("data-sources/").append(urlEncode(recordId.getDataSourceCode()))
          .append("/records/").append(urlEncode(recordId.getRecordId()))
          .append("/entity/why");

      buildWhyEntityQueryString(sb,
                                forceMinimal,
                                featureMode,
                                withFeatureStats,
                                withInternalFeatures,
                                withRelationships,
                                withRaw);

      String uriText = this.formatServerUri(sb.toString());

      long before = System.currentTimeMillis();
      SzWhyEntityResponse response = this.invokeServerViaHttp(
          GET, uriText, SzWhyEntityResponse.class);
      response.concludeTimers();
      long after = System.currentTimeMillis();

      this.validateWhyEntityResponse(
          testInfo,
          response,
          GET,
          uriText,
          recordId,
          null,
          (forceMinimal == null ? false : forceMinimal),
          featureMode,
          (withFeatureStats == null ? true : withFeatureStats),
          (withInternalFeatures == null ? true : withInternalFeatures),
          (withRelationships == null ? false : withRelationships),
          withRaw,
          before,
          after);
    });
  }

  @ParameterizedTest
  @MethodSource("getWhyEntityParameters")
  public void whyEntityByRecordIdViaJavaClientTest(
      SzRecordId          recordId,
      Boolean             forceMinimal,
      SzFeatureMode       featureMode,
      Boolean             withFeatureStats,
      Boolean             withInternalFeatures,
      Boolean             withRelationships,
      Boolean             withRaw)
  {
    this.performTest(() -> {
      String testInfo = "recordId=[ " + recordId
          + " ], forceMinimal=[ " + forceMinimal
          + " ], featureMode=[ " + featureMode
          + " ], withFeatureStats=[ " + withFeatureStats
          + " ], withInternalFeatures=[ " + withInternalFeatures
          + " ], withRelationships=[ " + withRelationships
          + " ], withRaw=[ " + withRaw + " ]";

      StringBuilder sb = new StringBuilder();
      sb.append("data-sources/").append(urlEncode(recordId.getDataSourceCode()))
          .append("/records/").append(urlEncode(recordId.getRecordId()))
          .append("/entity/why");

      buildWhyEntityQueryString(sb,
                                forceMinimal,
                                featureMode,
                                withFeatureStats,
                                withInternalFeatures,
                                withRelationships,
                                withRaw);

      String uriText = this.formatServerUri(sb.toString());

      com.senzing.gen.api.model.SzFeatureMode clientFeatureMode
          = (featureMode == null)
          ? null
          : com.senzing.gen.api.model.SzFeatureMode.valueOf(
          featureMode.toString());

      long before = System.currentTimeMillis();
      com.senzing.gen.api.model.SzWhyEntityResponse clientResponse
          = this.entityDataApi.whyEntityByRecordID(
          recordId.getDataSourceCode(),
          recordId.getRecordId(),
          withRelationships,
          withFeatureStats,
          withInternalFeatures,
          clientFeatureMode,
          forceMinimal,
          withRaw);
      long after = System.currentTimeMillis();

      SzWhyEntityResponse response = jsonCopy(clientResponse,
                                              SzWhyEntityResponse.class);

      this.validateWhyEntityResponse(
          testInfo,
          response,
          GET,
          uriText,
          recordId,
          null,
          (forceMinimal == null ? false : forceMinimal),
          featureMode,
          (withFeatureStats == null ? true : withFeatureStats),
          (withInternalFeatures == null ? true : withInternalFeatures),
          (withRelationships == null ? false : withRelationships),
          withRaw,
          before,
          after);
    });
  }

  @ParameterizedTest
  @MethodSource("getWhyEntityParameters")
  public void whyEntityByEntityIdTest(SzRecordId          recordId,
                                      Boolean             forceMinimal,
                                      SzFeatureMode featureMode,
                                      Boolean             withFeatureStats,
                                      Boolean             withInternalFeatures,
                                      Boolean             withRelationships,
                                      Boolean             withRaw)
  {
    this.performTest(() -> {
      long entityId = this.getEntityIdForRecordId(recordId);

      String testInfo = "recordId=[ " + recordId
          + " ], entityId=[ " + entityId
          + " ], forceMinimal=[ " + forceMinimal
          + " ], featureMode=[ " + featureMode
          + " ], withFeatureStats=[ " + withFeatureStats
          + " ], withInternalFeatures=[ " + withInternalFeatures
          + " ], withRelationships=[ " + withRelationships
          + " ], withRaw=[ " + withRaw + " ]";

      StringBuilder sb = new StringBuilder();
      sb.append("entities/").append(entityId).append("/why");

      buildWhyEntityQueryString(sb,
                                forceMinimal,
                                featureMode,
                                withFeatureStats,
                                withInternalFeatures,
                                withRelationships,
                                withRaw);

      String uriText = this.formatServerUri(sb.toString());
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      long before = System.currentTimeMillis();

      SzWhyEntityResponse response = this.whyServices.whyEntityByRecordId(
          recordId.getDataSourceCode(),
          recordId.getRecordId(),
          (forceMinimal == null ? false : forceMinimal),
          (featureMode == null ? WITH_DUPLICATES : featureMode),
          (withFeatureStats == null ? true : withFeatureStats),
          (withInternalFeatures == null ? true : withInternalFeatures),
          (withRelationships == null ? false : withRelationships),
          (withRaw == null ? false : withRaw),
          uriInfo);

      response.concludeTimers();
      long after = System.currentTimeMillis();

      this.validateWhyEntityResponse(
          testInfo,
          response,
          GET,
          uriText,
          recordId,
          null,
          (forceMinimal == null ? false : forceMinimal),
          featureMode,
          (withFeatureStats == null ? true : withFeatureStats),
          (withInternalFeatures == null ? true : withInternalFeatures),
          (withRelationships == null ? false : withRelationships),
          withRaw,
          before,
          after);
    });
  }

  @ParameterizedTest
  @MethodSource("getWhyEntityParameters")
  public void whyEntityByEntityIdViaHttpTest(
      SzRecordId          recordId,
      Boolean             forceMinimal,
      SzFeatureMode featureMode,
      Boolean             withFeatureStats,
      Boolean             withInternalFeatures,
      Boolean             withRelationships,
      Boolean             withRaw)
  {
    this.performTest(() -> {
      long entityId = this.getEntityIdForRecordId(recordId);

      String testInfo = "recordId=[ " + recordId
          + " ], entityId=[ " + entityId
          + " ], forceMinimal=[ " + forceMinimal
          + " ], featureMode=[ " + featureMode
          + " ], withFeatureStats=[ " + withFeatureStats
          + " ], withInternalFeatures=[ " + withInternalFeatures
          + " ], withRelationships=[ " + withRelationships
          + " ], withRaw=[ " + withRaw + " ]";

      StringBuilder sb = new StringBuilder();
      sb.append("entities/").append(entityId).append("/why");

      buildWhyEntityQueryString(sb,
                                forceMinimal,
                                featureMode,
                                withFeatureStats,
                                withInternalFeatures,
                                withRelationships,
                                withRaw);

      String uriText = this.formatServerUri(sb.toString());

      long before = System.currentTimeMillis();
      SzWhyEntityResponse response = this.invokeServerViaHttp(
          GET, uriText, SzWhyEntityResponse.class);
      response.concludeTimers();
      long after = System.currentTimeMillis();

      this.validateWhyEntityResponse(
          testInfo,
          response,
          GET,
          uriText,
          recordId,
          null,
          (forceMinimal == null ? false : forceMinimal),
          featureMode,
          (withFeatureStats == null ? true : withFeatureStats),
          (withInternalFeatures == null ? true : withInternalFeatures),
          (withRelationships == null ? false : withRelationships),
          withRaw,
          before,
          after);
    });
  }

  @ParameterizedTest
  @MethodSource("getWhyEntityParameters")
  public void whyEntityByEntityIdTestViaJavaClient(
      SzRecordId          recordId,
      Boolean             forceMinimal,
      SzFeatureMode       featureMode,
      Boolean             withFeatureStats,
      Boolean             withInternalFeatures,
      Boolean             withRelationships,
      Boolean             withRaw)
  {
    this.performTest(() -> {
      long entityId = this.getEntityIdForRecordId(recordId);

      String testInfo = "recordId=[ " + recordId
          + " ], entityId=[ " + entityId
          + " ], forceMinimal=[ " + forceMinimal
          + " ], featureMode=[ " + featureMode
          + " ], withFeatureStats=[ " + withFeatureStats
          + " ], withInternalFeatures=[ " + withInternalFeatures
          + " ], withRelationships=[ " + withRelationships
          + " ], withRaw=[ " + withRaw + " ]";

      StringBuilder sb = new StringBuilder();
      sb.append("entities/").append(entityId).append("/why");

      buildWhyEntityQueryString(sb,
                                forceMinimal,
                                featureMode,
                                withFeatureStats,
                                withInternalFeatures,
                                withRelationships,
                                withRaw);

      String uriText = this.formatServerUri(sb.toString());

      com.senzing.gen.api.model.SzFeatureMode clientFeatureMode
          = (featureMode == null)
          ? null
          : com.senzing.gen.api.model.SzFeatureMode.valueOf(
          featureMode.toString());

      long before = System.currentTimeMillis();
      com.senzing.gen.api.model.SzWhyEntityResponse clientResponse
          = this.entityDataApi.whyEntityByEntityID(
              entityId,
              withRelationships,
              withFeatureStats,
              withInternalFeatures,
              clientFeatureMode,
              forceMinimal,
              withRaw);
      long after = System.currentTimeMillis();

      SzWhyEntityResponse response = jsonCopy(clientResponse,
                                              SzWhyEntityResponse.class);

      this.validateWhyEntityResponse(
          testInfo,
          response,
          GET,
          uriText,
          recordId,
          null,
          (forceMinimal == null ? false : forceMinimal),
          featureMode,
          (withFeatureStats == null ? true : withFeatureStats),
          (withInternalFeatures == null ? true : withInternalFeatures),
          (withRelationships == null ? false : withRelationships),
          withRaw,
          before,
          after);
    });
  }

  public void validateWhyEntityResponse(
      String                              testInfo,
      SzWhyEntityResponse                 response,
      SzHttpMethod                        httpMethod,
      String                              selfLink,
      SzRecordId                          recordId,
      Long                                entityId,
      boolean                             forceMinimal,
      SzFeatureMode featureMode,
      boolean                             withFeatureStats,
      boolean                             withInternalFeatures,
      boolean                             withRelationships,
      Boolean                             withRaw,
      long                                beforeTimestamp,
      long                                afterTimestamp)
  {
    if (testInfo != null && selfLink != null) {
      testInfo = testInfo + ", selfLink=[ " + selfLink + " ]";
    }
    validateBasics(testInfo,
                   response,
                   httpMethod,
                   selfLink,
                   beforeTimestamp,
                   afterTimestamp);

    List<SzWhyEntityResult> whyResults  = response.getData().getWhyResults();
    List<SzEntityData>      entities    = response.getData().getEntities();

    assertNotNull(whyResults, "Why results list is null: " + testInfo);
    assertNotNull(entities, "Entities list is null: " + testInfo);

    Set<SzFocusRecordId> perspectiveIds = (recordId == null) ? null
        : new LinkedHashSet<>();

    for (SzWhyEntityResult result : whyResults) {
      if (entityId != null) {
        assertEquals(entityId, result.getPerspective().getEntityId(),
                     "Unexpected entity ID in perspective: "
                         + testInfo);
      }
      if (recordId != null) {
        for (SzFocusRecordId focusId : result.getPerspective().getFocusRecords())
        {
          perspectiveIds.add(focusId);
        }
      }
    }

    SzFocusRecordId focusRecordId = new SzFocusRecordId(
        recordId.getDataSourceCode(), recordId.getRecordId());

    if (recordId != null) {
      assertTrue(perspectiveIds.contains(focusRecordId),
                 "No perspective from requested record ID (" + recordId
                     + "): " + testInfo);
    }

    if (entityId != null) {
      Set<Long> entityIds = new LinkedHashSet<>();
      for (SzEntityData entityData : entities) {
        entityIds.add(entityData.getResolvedEntity().getEntityId());
      }
      assertTrue(entityIds.contains(entityId),
                 "Requested entity ID (" + entityId
                     + ") not represented in returned entities ("
                     + entityIds + "): " + testInfo);
    }

    if (recordId != null) {
      Set<SzRecordId> recordIds = new LinkedHashSet<>();
      for (SzEntityData entityData : entities) {
        SzResolvedEntity entity = entityData.getResolvedEntity();
        for (SzEntityRecord record : entity.getRecords()) {
          recordIds.add(new SzRecordId(record.getDataSource(),
                                       record.getRecordId()));
        }
      }
      assertTrue(recordIds.contains(recordId),
                 "Requested record ID (" + recordId
                     + ") not represented in returned entities ("
                     + recordIds + "): " + testInfo);
    }

    Map<Long, SzResolvedEntity> entityMap = new LinkedHashMap<>();
    entities.forEach(entityData -> {
      SzResolvedEntity resolvedEntity = entityData.getResolvedEntity();
      entityMap.put(resolvedEntity.getEntityId(), resolvedEntity);
    });

    // verify the with relationships
    if (withRelationships) {
      for (SzEntityData entityData : entities) {
        List<SzRelatedEntity> relatedList = entityData.getRelatedEntities();
        assertNotEquals(0, relatedList.size(),
                        "Expected at least one related entity with "
                            + "withRelationships set to true: " + testInfo);

      }
    } else {
      for (SzEntityData entityData : entities) {
        List<SzRelatedEntity> relatedList = entityData.getRelatedEntities();
        assertFalse(relatedList != null && relatedList.size() > 0,
                    "Got unexpected related entities ("
                        + relatedList.size()
                        + ") when withRelationships set to false: " + testInfo);
      }
    }

    for (SzEntityData entityData : entities) {
      SzResolvedEntity resolvedEntity = entityData.getResolvedEntity();
      List<SzRelatedEntity> relatedEntities = entityData.getRelatedEntities();

      validateEntity(testInfo,
                     resolvedEntity,
                     relatedEntities,
                     forceMinimal,
                     featureMode,
                     withFeatureStats,
                     withInternalFeatures,
                     null,
                     null,
                     !withRelationships,
                     null,
                     true,
                     null,
                     null,
                     null,
                     null,
                     null);
    }

    if (withRaw != null && withRaw) {
      validateRawDataMap(testInfo,
                         response.getRawData(),
                         true,
                         "WHY_RESULTS", "ENTITIES");

      Object rawResults = ((Map) response.getRawData()).get("WHY_RESULTS");

      validateRawDataMapArray(testInfo,
                              rawResults,
                              true,
                              "INTERNAL_ID",
                              "ENTITY_ID",
                              "FOCUS_RECORDS",
                              "MATCH_INFO");

      Object rawEntities = ((Map) response.getRawData()).get("ENTITIES");

      if (withRelationships) {
        validateRawDataMapArray(testInfo,
                                rawEntities,
                                true,
                                "RESOLVED_ENTITY",
                                "RELATED_ENTITIES");
      } else {
        validateRawDataMapArray(testInfo,
                                rawEntities,
                                true,
                                "RESOLVED_ENTITY");
      }

      for (Object entity : ((Collection) rawEntities)) {
        if (featureMode == NONE || forceMinimal) {
          validateRawDataMap(testInfo,
                             ((Map) entity).get("RESOLVED_ENTITY"),
                             false,
                             "ENTITY_ID",
                             "RECORDS");
        } else {
          validateRawDataMap(testInfo,
                             ((Map) entity).get("RESOLVED_ENTITY"),
                             false,
                             "ENTITY_ID",
                             "FEATURES",
                             "RECORD_SUMMARY",
                             "RECORDS");
        }
      }
    }
  }

  @ParameterizedTest
  @MethodSource("getWhyRecordsParameters")
  public void whyRecordsTest(SzRecordId         recordId1,
                             SzRecordId         recordId2,
                             Boolean            forceMinimal,
                             SzFeatureMode      featureMode,
                             Boolean            withFeatureStats,
                             Boolean            withInternalFeatures,
                             Boolean            withRelationships,
                             Boolean            withRaw)
  {
    this.performTest(() -> {
      String testInfo = "recordId1=[ " + recordId1
          + " ], recordId2=[ " + recordId2
          + " ], forceMinimal=[ " + forceMinimal
          + " ], featureMode=[ " + featureMode
          + " ], withFeatureStats=[ " + withFeatureStats
          + " ], withInternalFeatures=[ " + withInternalFeatures
          + " ], withRelationships=[ " + withRelationships
          + " ], withRaw=[ " + withRaw + " ]";

      StringBuilder sb = new StringBuilder();
      sb.append("why/records");

      buildWhyRecordsQueryString(sb,
                                 recordId1,
                                 recordId2,
                                 forceMinimal,
                                 featureMode,
                                 withFeatureStats,
                                 withInternalFeatures,
                                 withRelationships,
                                 withRaw);

      String uriText = this.formatServerUri(sb.toString());
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      long before = System.currentTimeMillis();

      SzWhyRecordsResponse response = this.whyServices.whyRecords(
          recordId1.getDataSourceCode(),
          recordId1.getRecordId(),
          recordId2.getDataSourceCode(),
          recordId2.getRecordId(),
          (forceMinimal == null ? false : forceMinimal),
          (featureMode == null ? WITH_DUPLICATES : featureMode),
          (withFeatureStats == null ? true : withFeatureStats),
          (withInternalFeatures == null ? true : withInternalFeatures),
          (withRelationships == null ? false : withRelationships),
          (withRaw == null ? false : withRaw),
          uriInfo);

      response.concludeTimers();
      long after = System.currentTimeMillis();

      this.validateWhyRecordsResponse(
          testInfo,
          response,
          GET,
          uriText,
          recordId1,
          recordId2,
          (forceMinimal == null ? false : forceMinimal),
          featureMode,
          (withFeatureStats == null ? true : withFeatureStats),
          (withInternalFeatures == null ? true : withInternalFeatures),
          (withRelationships == null ? false : withRelationships),
          withRaw,
          before,
          after);
    });
  }

  @ParameterizedTest
  @MethodSource("getWhyRecordsParameters")
  public void whyRecordsViaHttpTest(
      SzRecordId          recordId1,
      SzRecordId          recordId2,
      Boolean             forceMinimal,
      SzFeatureMode       featureMode,
      Boolean             withFeatureStats,
      Boolean             withInternalFeatures,
      Boolean             withRelationships,
      Boolean             withRaw)
  {
    this.performTest(() -> {
      String testInfo = "recordId1=[ " + recordId1
          + " ], recordId2=[ " + recordId2
          + " ], forceMinimal=[ " + forceMinimal
          + " ], featureMode=[ " + featureMode
          + " ], withFeatureStats=[ " + withFeatureStats
          + " ], withInternalFeatures=[ " + withInternalFeatures
          + " ], withRelationships=[ " + withRelationships
          + " ], withRaw=[ " + withRaw + " ]";

      StringBuilder sb = new StringBuilder();
      sb.append("why/records");

      buildWhyRecordsQueryString(sb,
                                 recordId1,
                                 recordId2,
                                 forceMinimal,
                                 featureMode,
                                 withFeatureStats,
                                 withInternalFeatures,
                                 withRelationships,
                                 withRaw);

      String uriText = this.formatServerUri(sb.toString());

      long before = System.currentTimeMillis();
      SzWhyRecordsResponse response = this.invokeServerViaHttp(
          GET, uriText, SzWhyRecordsResponse.class);
      response.concludeTimers();
      long after = System.currentTimeMillis();

      this.validateWhyRecordsResponse(
          testInfo,
          response,
          GET,
          uriText,
          recordId1,
          recordId2,
          (forceMinimal == null ? false : forceMinimal),
          featureMode,
          (withFeatureStats == null ? true : withFeatureStats),
          (withInternalFeatures == null ? true : withInternalFeatures),
          (withRelationships == null ? false : withRelationships),
          withRaw,
          before,
          after);
    });
  }

  @ParameterizedTest
  @MethodSource("getWhyRecordsParameters")
  public void whyRecordsTestViaJavaClient(
      SzRecordId          recordId1,
      SzRecordId          recordId2,
      Boolean             forceMinimal,
      SzFeatureMode       featureMode,
      Boolean             withFeatureStats,
      Boolean             withInternalFeatures,
      Boolean             withRelationships,
      Boolean             withRaw)
  {
    this.performTest(() -> {
      String testInfo = "recordId1=[ " + recordId1
          + " ], recordId2=[ " + recordId2
          + " ], forceMinimal=[ " + forceMinimal
          + " ], featureMode=[ " + featureMode
          + " ], withFeatureStats=[ " + withFeatureStats
          + " ], withInternalFeatures=[ " + withInternalFeatures
          + " ], withRelationships=[ " + withRelationships
          + " ], withRaw=[ " + withRaw + " ]";

      StringBuilder sb = new StringBuilder();
      sb.append("why/records");

      buildWhyRecordsQueryString(sb,
                                 recordId1,
                                 recordId2,
                                 forceMinimal,
                                 featureMode,
                                 withFeatureStats,
                                 withInternalFeatures,
                                 withRelationships,
                                 withRaw);

      String uriText = this.formatServerUri(sb.toString());

      com.senzing.gen.api.model.SzFeatureMode clientFeatureMode
          = (featureMode == null)
          ? null
          : com.senzing.gen.api.model.SzFeatureMode.valueOf(
              featureMode.toString());

      long before = System.currentTimeMillis();
      com.senzing.gen.api.model.SzWhyRecordsResponse clientResponse
          = this.entityDataApi.whyRecords(recordId1.getDataSourceCode(),
                                          recordId1.getRecordId(),
                                          recordId2.getDataSourceCode(),
                                          recordId2.getRecordId(),
                                          withRelationships,
                                          withFeatureStats,
                                          withInternalFeatures,
                                          clientFeatureMode,
                                          forceMinimal,
                                          withRaw);
      long after = System.currentTimeMillis();

      SzWhyRecordsResponse response = jsonCopy(clientResponse,
                                               SzWhyRecordsResponse.class);

      this.validateWhyRecordsResponse(
          testInfo,
          response,
          GET,
          uriText,
          recordId1,
          recordId2,
          (forceMinimal == null ? false : forceMinimal),
          featureMode,
          (withFeatureStats == null ? true : withFeatureStats),
          (withInternalFeatures == null ? true : withInternalFeatures),
          (withRelationships == null ? false : withRelationships),
          withRaw,
          before,
          after);
    });
  }

  public void validateWhyRecordsResponse(
      String                              testInfo,
      SzWhyRecordsResponse                response,
      SzHttpMethod                        httpMethod,
      String                              selfLink,
      SzRecordId                          recordId1,
      SzRecordId                          recordId2,
      boolean                             forceMinimal,
      SzFeatureMode                       featureMode,
      boolean                             withFeatureStats,
      boolean                             withInternalFeatures,
      boolean                             withRelationships,
      Boolean                             withRaw,
      long                                beforeTimestamp,
      long                                afterTimestamp)
  {
    if (testInfo != null && selfLink != null) {
      testInfo = testInfo + ", selfLink=[ " + selfLink + " ]";
    }

    validateBasics(testInfo,
                   response,
                   httpMethod,
                   selfLink,
                   beforeTimestamp,
                   afterTimestamp);

    SzWhyRecordsResult  whyResult = response.getData().getWhyResult();
    List<SzEntityData>  entities  = response.getData().getEntities();

    assertNotNull(whyResult, "Why result is null: " + testInfo);
    assertNotNull(entities, "Entities list is null: " + testInfo);

    Set<SzRecordId> perspectiveIds = new LinkedHashSet<>();

    Set<SzFocusRecordId> recordIds1
        = whyResult.getPerspective1().getFocusRecords();
    Set<SzFocusRecordId> recordIds2
        = whyResult.getPerspective2().getFocusRecords();

    SzFocusRecordId focusRecord1 = new SzFocusRecordId(
        recordId1.getDataSourceCode(), recordId1.getRecordId());
    SzFocusRecordId focusRecord2 = new SzFocusRecordId(
        recordId2.getDataSourceCode(), recordId2.getRecordId());

    assertTrue(recordIds1.contains(focusRecord1),
               "Perspective 1 focus records (" + recordIds1
                   + ") does not contain first record ID ("
                   + recordId1 + "): " + testInfo);

    assertTrue(recordIds2.contains(focusRecord2),
               "Perspective 2 focus records (" + recordIds2
                   + ") does not contain first record ID ("
                   + recordId2 + "): " + testInfo);


    Set<SzRecordId> recordIds = new LinkedHashSet<>();
    for (SzEntityData entityData : entities) {
      SzResolvedEntity entity = entityData.getResolvedEntity();
      for (SzEntityRecord record : entity.getRecords()) {
        recordIds.add(new SzRecordId(record.getDataSource(),
                                     record.getRecordId()));
      }
    }

    assertTrue(recordIds.contains(recordId1),
               "First requested record ID (" + recordId1
                   + ") not represented in returned entities ("
                   + recordIds + "): " + testInfo);

    assertTrue(recordIds.contains(recordId2),
               "Second requested record ID (" + recordId2
                   + ") not represented in returned entities ("
                   + recordIds + "): " + testInfo);

    Map<Long, SzResolvedEntity> entityMap = new LinkedHashMap<>();
    entities.forEach(entityData -> {
      SzResolvedEntity resolvedEntity = entityData.getResolvedEntity();
      entityMap.put(resolvedEntity.getEntityId(), resolvedEntity);
    });

    // verify the with relationships
    if (withRelationships) {
      for (SzEntityData entityData : entities) {
        List<SzRelatedEntity> relatedList = entityData.getRelatedEntities();
        assertNotEquals(0, relatedList.size(),
                        "Expected at least one related entity with "
                            + "withRelationships set to true: " + testInfo);

      }
    } else {
      for (SzEntityData entityData : entities) {
        List<SzRelatedEntity> relatedList = entityData.getRelatedEntities();
        assertFalse(relatedList != null && relatedList.size() > 0,
                    "Got unexpected related entities ("
                        + relatedList.size()
                        + ") when withRelationships set to false: " + testInfo);
      }
    }

    for (SzEntityData entityData : entities) {
      SzResolvedEntity resolvedEntity = entityData.getResolvedEntity();
      List<SzRelatedEntity> relatedEntities = entityData.getRelatedEntities();

      validateEntity(testInfo,
                     resolvedEntity,
                     relatedEntities,
                     forceMinimal,
                     featureMode,
                     withFeatureStats,
                     withInternalFeatures,
                     null,
                     null,
                     !withRelationships,
                     null,
                     true,
                     null,
                     null,
                     null,
                     null,
                     null);
    }

    if (withRaw != null && withRaw) {
      validateRawDataMap(testInfo,
                         response.getRawData(),
                         true,
                         "WHY_RESULTS", "ENTITIES");

      Object rawResults = ((Map) response.getRawData()).get("WHY_RESULTS");

      validateRawDataMapArray(testInfo,
                              rawResults,
                              true,
                              "INTERNAL_ID",
                              "ENTITY_ID",
                              "FOCUS_RECORDS",
                              "INTERNAL_ID_2",
                              "ENTITY_ID_2",
                              "FOCUS_RECORDS_2",
                              "MATCH_INFO");

      Object rawEntities = ((Map) response.getRawData()).get("ENTITIES");

      if (withRelationships) {
        validateRawDataMapArray(testInfo,
                                rawEntities,
                                true,
                                "RESOLVED_ENTITY",
                                "RELATED_ENTITIES");
      } else {
        validateRawDataMapArray(testInfo,
                                rawEntities,
                                true,
                                "RESOLVED_ENTITY");
      }

      for (Object entity : ((Collection) rawEntities)) {
        if (featureMode == NONE || forceMinimal) {
          validateRawDataMap(testInfo,
                             ((Map) entity).get("RESOLVED_ENTITY"),
                             false,
                             "ENTITY_ID",
                             "RECORDS");
        } else {
          validateRawDataMap(testInfo,
                             ((Map) entity).get("RESOLVED_ENTITY"),
                             false,
                             "ENTITY_ID",
                             "FEATURES",
                             "RECORD_SUMMARY",
                             "RECORDS");
        }
      }
    }
  }

}
