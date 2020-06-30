package com.senzing.api.services;

import com.senzing.api.model.*;
import com.senzing.repomgr.RepositoryManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.ws.rs.core.UriInfo;
import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.*;

import static com.senzing.api.model.SzFeatureMode.NONE;
import static com.senzing.api.model.SzFeatureMode.WITH_DUPLICATES;
import static com.senzing.api.model.SzHttpMethod.GET;
import static com.senzing.util.CollectionUtilities.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.TestInstance.Lifecycle;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static com.senzing.api.services.ResponseValidators.*;

@TestInstance(Lifecycle.PER_CLASS)
public class EntityGraphServicesTest extends AbstractServiceTest {
  private static final int DEFAULT_PATH_DEGREES = 3;
  private static final int DEFAULT_NETWORK_DEGREES = 3;
  private static final int DEFAULT_BUILD_OUT = 1;
  private static final int DEFAULT_MAX_ENTITIES = 1000;

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

  private EntityGraphServices entityGraphServices;
  private EntityDataServices entityDataServices;

  @BeforeAll
  public void initializeEnvironment() {
    this.beginTests();
    this.initializeTestEnvironment();
    this.entityGraphServices  = new EntityGraphServices();
    this.entityDataServices   = new EntityDataServices();
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

  private static List<List> pathArgs(SzRecordId       fromRecord,
                                     SzRecordId       toRecord,
                                     Integer          maxDegrees,
                                     Set<SzRecordId>  avoid,
                                     Boolean          forbid,
                                     List<String>     sources,
                                     Integer          expectedPathLength,
                                     List<SzRecordId> expectedPath)
  {

    List<List> result = new LinkedList<>();

    // creates variants of the specified avoid parameter to exercise both
    // of the parameters that deal with avoiding entities
    List<List> avoidVariants = new LinkedList<>();
    if (avoid == null || avoid.size() == 0) {
      avoidVariants.add(list(null, null));
    } else if (avoid.size() == 1) {
      avoidVariants.add(list(avoid, null));
      avoidVariants.add(list(null, avoid));
    } else if (avoid.size() > 1) {
      avoidVariants.add(list(avoid, null));
      avoidVariants.add(list(null, avoid));

      int avoidCount = avoid.size();
      Set<SzRecordId> avoid1 = new LinkedHashSet<>();
      Set<SzRecordId> avoid2 = new LinkedHashSet<>();
      for (SzRecordId recordId : avoid) {
        if ((avoidCount % 2) == 0) {
          avoid1.add(recordId);
        } else {
          avoid2.add(recordId);
        }
        avoidCount--;
      }
      avoidVariants.add(list(avoid1, avoid2));
      avoidVariants.add(list(avoid2, avoid1));
    }

    // create variants on the forbid parameter to test default values
    List<Boolean> forbidVariants = new LinkedList<>();
    if (avoid == null || avoid.size() == 0) {
      // nothing to avoid so try all variants of forbid
      forbidVariants.add(null);
      forbidVariants.add(true);
      forbidVariants.add(false);

    } else if (forbid == null || forbid == false) {
      // not forbidding so try both "default" and false option
      forbidVariants.add(null);
      forbidVariants.add(false);
    } else {
      // forbid specifically requested so it is the only variant we will use
      forbidVariants.add(true);
    }

    // create variants on the max degrees parameter to test default values
    List<Integer> degreesVariants = new LinkedList<>();
    if (maxDegrees == null || maxDegrees == DEFAULT_PATH_DEGREES) {
      degreesVariants.add(null);
      degreesVariants.add(DEFAULT_PATH_DEGREES);
    } else {
      degreesVariants.add(maxDegrees);
    }

    avoidVariants.forEach(avoidVariant -> {
      degreesVariants.forEach(degreeVariant -> {
        forbidVariants.forEach(forbidVariant -> {
          result.add(
            list(fromRecord,
                 toRecord,
                 degreeVariant,
                 avoidVariant.get(0),
                 avoidVariant.get(1),
                 forbidVariant,
                 sources,
                 null,  // forceMinimal (7)
                 null,  // featureMode (8)
                 null,  // withFeatureStats (9)
                 null,  // withInternalFeatures (10)
                 null,  // withRaw (11)
                 expectedPathLength,
                 expectedPath));
        });
      });
    });

    return result;
  }

  private List<Arguments> getEntityPathParameters() {

    List<List> baseArgs = new LinkedList<>();

    baseArgs.addAll(pathArgs(
        ABC123, DEF890, null, null, null,
        null, 3, list(ABC123, MNO345, DEF890)));

    baseArgs.addAll(pathArgs(
        ABC123, JKL456, null, null, null, null,
        4, list(ABC123, MNO345, DEF890, JKL456)));

    baseArgs.addAll(pathArgs(
        ABC123, JKL456, 2, null, null, null,
        -1, Collections.emptyList()));

    baseArgs.addAll(pathArgs(
        ABC123, JKL456, null, set(DEF890), null, null,
        4, list(ABC123, MNO345, DEF890, JKL456)));

    baseArgs.addAll(pathArgs(
        ABC123, JKL456, null, set(DEF890), true, null,
        -1, Collections.emptyList()));

    baseArgs.addAll(pathArgs(
        ABC123, JKL456, 10, set(DEF890), true, null,
        6, list(ABC123, DEF456, GHI789, JKL012, XYZ234, JKL456)));

    baseArgs.addAll(pathArgs(
        ABC123, JKL012, 10, null, null, list(EMPLOYEES),
        6, list(ABC123, MNO345, DEF890, JKL456, XYZ234, JKL012)));

    baseArgs.addAll(pathArgs(
        ABC123, JKL012, 10, null, null, list(VIPS),
        6, list(ABC123, MNO345, DEF890, JKL456, XYZ234, JKL012)));

    baseArgs.addAll(pathArgs(
        ABC123, JKL012, 10, null, null, list(EMPLOYEES, VIPS),
        6, list(ABC123, MNO345, DEF890, JKL456, XYZ234, JKL012)));

    // make a random-access version of the list
    baseArgs = new ArrayList<>(baseArgs);

    Boolean[] booleanVariants = {null, true, false};
    List<Boolean> booleanVariantList = Arrays.asList(booleanVariants);

    List<SzFeatureMode> featureModes = new LinkedList<>();
    featureModes.add(null);
    for (SzFeatureMode featureMode : SzFeatureMode.values()) {
      featureModes.add(featureMode);
    }
    List<List> optionCombos = generateCombinations(
        booleanVariantList,   // forceMinimal
        featureModes,         // featureMode
        booleanVariantList,   // withFeatureStats
        booleanVariantList,   // withInternalFeatures
        booleanVariantList);  // withRaw

    int count = Math.max(baseArgs.size(), optionCombos.size()) * 2;
    List<Arguments> result = new ArrayList<>(count);

    for (int index = 0; index < count; index++) {
      int argIndex      = index % baseArgs.size();
      int optionsIndex  = index % optionCombos.size();

      List baseArgList = baseArgs.get(argIndex);
      List optionsList = optionCombos.get(optionsIndex);

      Object[] argArray = baseArgList.toArray();
      argArray[7]   = optionsList.get(0);
      argArray[8]   = optionsList.get(1);
      argArray[9]   = optionsList.get(2);
      argArray[10]  = optionsList.get(3);
      argArray[11]  = optionsList.get(4);
      result.add(arguments(argArray));
    }

    return result;
  }

  private StringBuilder buildPathQueryString(
      StringBuilder         sb,
      SzEntityIdentifier    fromIdentifier,
      SzEntityIdentifier    toIdentifier,
      Integer               maxDegrees,
      SzEntityIdentifiers   avoidParam,
      SzEntityIdentifiers   avoidList,
      Boolean               forbidAvoided,
      List<String>          sourcesParam,
      Boolean               forceMinimal,
      SzFeatureMode featureMode,
      Boolean               withFeatureStats,
      Boolean               withInternalFeatures,
      Boolean               withRaw)
  {
    try {
      sb.append("?from=").append(
          URLEncoder.encode(fromIdentifier.toString(), "UTF-8"));
      sb.append("&to=").append(
          URLEncoder.encode(toIdentifier.toString(), "UTF-8"));

      if (maxDegrees != null) {
        sb.append("&maxDegrees=").append(maxDegrees);
      }
      if (avoidParam != null && !avoidParam.isEmpty()) {
        for (SzEntityIdentifier identifier : avoidParam.getIdentifiers()) {
          sb.append("&x=").append(
              URLEncoder.encode(identifier.toString(), "UTF-8"));
        }
      }
      if (avoidList != null && !avoidList.isEmpty()) {
        sb.append("&avoidEntities=").append(
            URLEncoder.encode(avoidList.toString(), "UTF-8"));
      }
      if (forbidAvoided != null) {
        sb.append("&forbidAvoided=").append(forbidAvoided);
      }
      if (sourcesParam != null && sourcesParam.size() > 0) {
        for (String value: sourcesParam) {
          sb.append("&s=").append(URLEncoder.encode(value, "UTF-8"));
        }
      }
      if (forceMinimal != null) {
        sb.append("&forceMinimal=").append(forceMinimal);
      }
      if (featureMode != null) {
        sb.append("&featureMode=").append(featureMode);
      }
      if (withFeatureStats != null) {
        sb.append("&withFeatureStats=").append(withFeatureStats);
      }
      if (withInternalFeatures != null) {
        sb.append("&withInternalFeatures=").append(withInternalFeatures);
      }
      if (withRaw != null) {
        sb.append("&withRaw=").append(withRaw);
      }
      return sb;

    } catch (UnsupportedEncodingException cannotHappen) {
      throw new RuntimeException(cannotHappen);
    }
  }

  private Long asEntityId(SzEntityIdentifier identifier) {
    if (identifier == null) return null;
    if (identifier instanceof SzEntityId) {
      return ((SzEntityId) identifier).getValue();
    }
    return this.getEntityIdForRecordId((SzRecordId) identifier);
  }

  private List<Long> asEntityIds(
      Collection<? extends SzEntityIdentifier> identifiers)
  {
    return this.asEntityIds(new SzEntityIdentifiers(identifiers));
  }

  private List<Long> asEntityIds(SzEntityIdentifiers identifiers) {
    if (identifiers == null || identifiers.isEmpty()) return null;

    List<Long> entityIds = new ArrayList<>(identifiers.getCount());
    for (SzEntityIdentifier identifier : identifiers.getIdentifiers()) {
      if (identifier instanceof SzEntityId) {
        entityIds.add(((SzEntityId) identifier).getValue());
      } else {
        entityIds.add(this.getEntityIdForRecordId((SzRecordId) identifier));
      }
    }
    return entityIds;
  }

  private SzEntityIdentifier normalizeIdentifier(SzRecordId recordId,
                                                 boolean    asEntityId)
  {
   if (recordId == null) return null;
   if (!asEntityId) return recordId;
   Long entityId = this.getEntityIdForRecordId(recordId);
   return new SzEntityId(entityId);
  }

  private SzEntityIdentifiers normalizeIdentifiers(
      Collection<SzRecordId>  recordIds,
      boolean                 asEntityIds)
  {
    if (recordIds == null) return null;
    if (recordIds.size() == 0) return null;
    if (!asEntityIds) return new SzEntityIdentifiers(recordIds);
    List<SzEntityId> entityIds = new ArrayList<>(recordIds.size());
    for (SzRecordId recordId : recordIds) {
      long entityId = this.getEntityIdForRecordId(recordId);
      entityIds.add(new SzEntityId(entityId));
    }
    return new SzEntityIdentifiers(entityIds);
  }

  private List<String> formatIdentifierParam(SzEntityIdentifiers identifiers)
  {
    if (identifiers == null || identifiers.isEmpty()) return null;
    List<String> result = new ArrayList<>(identifiers.getCount());
    for (SzEntityIdentifier identifier : identifiers.getIdentifiers()) {
      result.add(identifier.toString());
    }
    return result;
  }

  private String formatIdentifierList(SzEntityIdentifiers identifiers)
  {
    if (identifiers == null || identifiers.isEmpty()) return null;
    return identifiers.toString();
  }

  @ParameterizedTest
  @MethodSource("getEntityPathParameters")
  public void getPathByRecordIdTest(SzRecordId              fromRecordId,
                                    SzRecordId              toRecordId,
                                    Integer                 maxDegrees,
                                    Collection<SzRecordId>  avoidParam,
                                    Collection<SzRecordId>  avoidList,
                                    Boolean                 forbidAvoided,
                                    List<String>            sourcesParam,
                                    Boolean                 forceMinimal,
                                    SzFeatureMode featureMode,
                                    Boolean                 withFeatureStats,
                                    Boolean                 withInternalFeatures,
                                    Boolean                 withRaw,
                                    Integer                 expectedPathLength,
                                    List<SzRecordId>        expectedPath)
  {
    this.performTest(() -> {
      String testInfo = "fromRecord=[ " + fromRecordId
          + " ], toRecord=[ " + toRecordId
          + " ], maxDegrees=[ " + maxDegrees
          + " ], avoidParam=[ " + avoidParam
          + " ], avoidList=[ " + avoidList
          + " ], forbidAvoided=[ " + forbidAvoided
          + " ], sources=[ " + sourcesParam
          + " ], forceMinimal=[ " + forceMinimal
          + " ], featureMode=[ " + featureMode
          + " ], withFeatureStats=[ " + withFeatureStats
          + " ], withInternalFeatures=[ " + withInternalFeatures
          + " ], withRaw=[ " + withRaw + " ]";

      SzEntityIdentifier fromIdentifer
          = this.normalizeIdentifier(fromRecordId,false);

      SzEntityIdentifier toIdentifier
          = this.normalizeIdentifier(toRecordId,false);

      SzEntityIdentifiers avoidParamIds
          = this.normalizeIdentifiers(avoidParam,false);

      SzEntityIdentifiers avoidListIds
          = this.normalizeIdentifiers(avoidList,false);

      StringBuilder sb = new StringBuilder();
      sb.append("entity-paths");

      buildPathQueryString(sb,
                           fromIdentifer,
                           toIdentifier,
                           maxDegrees,
                           avoidParamIds,
                           avoidListIds,
                           forbidAvoided,
                           sourcesParam,
                           forceMinimal,
                           featureMode,
                           withFeatureStats,
                           withInternalFeatures,
                           withRaw);

      String uriText = this.formatServerUri(sb.toString());
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      long before = System.currentTimeMillis();

      SzEntityPathResponse response = this.entityGraphServices.getEntityPath(
          fromIdentifer.toString(),
          toIdentifier.toString(),
          (maxDegrees == null ? DEFAULT_PATH_DEGREES : maxDegrees),
          formatIdentifierParam(avoidParamIds),
          formatIdentifierList(avoidListIds),
          (forbidAvoided == null ? false : forbidAvoided),
          sourcesParam,
          (forceMinimal == null ? false : forceMinimal),
          (featureMode == null ? WITH_DUPLICATES : featureMode),
          (withFeatureStats == null ? false : withFeatureStats),
          (withInternalFeatures == null ? false : withInternalFeatures),
          (withRaw == null ? false : withRaw),
          uriInfo);

      response.concludeTimers();
      long after = System.currentTimeMillis();

      this.validateEntityPathResponse(
          testInfo,
          response,
          GET,
          uriText,
          fromIdentifer,
          toIdentifier,
          (maxDegrees != null ? maxDegrees : DEFAULT_PATH_DEGREES),
          avoidParamIds,
          avoidListIds,
          forbidAvoided,
          sourcesParam,
          forceMinimal,
          featureMode,
          (withFeatureStats == null ? false : withFeatureStats),
          (withInternalFeatures == null ? false : withInternalFeatures),
          withRaw,
          expectedPathLength,
          expectedPath,
          before,
          after);
    });
  }

  @ParameterizedTest
  @MethodSource("getEntityPathParameters")
  public void getPathByRecordIdViaHttpTest(
      SzRecordId              fromRecordId,
      SzRecordId              toRecordId,
      Integer                 maxDegrees,
      Collection<SzRecordId>  avoidParam,
      Collection<SzRecordId>  avoidList,
      Boolean                 forbidAvoided,
      List<String>            sourcesParam,
      Boolean                 forceMinimal,
      SzFeatureMode featureMode,
      Boolean                 withFeatureStats,
      Boolean                 withInternalFeatures,
      Boolean                 withRaw,
      Integer                 expectedPathLength,
      List<SzRecordId>        expectedPath)
  {
    this.performTest(() -> {
      String testInfo = "fromRecord=[ " + fromRecordId
          + " ], toRecord=[ " + toRecordId
          + " ], maxDegrees=[ " + maxDegrees
          + " ], avoidParam=[ " + avoidParam
          + " ], avoidList=[ " + avoidList
          + " ], forbidAvoided=[ " + forbidAvoided
          + " ], sources=[ " + sourcesParam
          + " ], forceMinimal=[ " + forceMinimal
          + " ], featureMode=[ " + featureMode
          + " ], withFeatureStats=[ " + withFeatureStats
          + " ], withInternalFeatures=[ " + withInternalFeatures
          + " ], withRaw=[ " + withRaw + " ]";

      SzEntityIdentifier fromIdentifer
          = this.normalizeIdentifier(fromRecordId,false);

      SzEntityIdentifier toIdentifier
          = this.normalizeIdentifier(toRecordId,false);

      SzEntityIdentifiers avoidParamIds
          = this.normalizeIdentifiers(avoidParam,false);

      SzEntityIdentifiers avoidListIds
          = this.normalizeIdentifiers(avoidList,false);

      StringBuilder sb = new StringBuilder();
      sb.append("entity-paths");

      buildPathQueryString(sb,
                           fromIdentifer,
                           toIdentifier,
                           maxDegrees,
                           avoidParamIds,
                           avoidListIds,
                           forbidAvoided,
                           sourcesParam,
                           forceMinimal,
                           featureMode,
                           withFeatureStats,
                           withInternalFeatures,
                           withRaw);

      String uriText = this.formatServerUri(sb.toString());
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      long before = System.currentTimeMillis();
      SzEntityPathResponse response = this.invokeServerViaHttp(
          GET, uriText, SzEntityPathResponse.class);
      response.concludeTimers();
      long after = System.currentTimeMillis();

      this.validateEntityPathResponse(
          testInfo,
          response,
          GET,
          uriText,
          fromIdentifer,
          toIdentifier,
          (maxDegrees != null ? maxDegrees : DEFAULT_PATH_DEGREES),
          avoidParamIds,
          avoidListIds,
          forbidAvoided,
          sourcesParam,
          forceMinimal,
          featureMode,
          (withFeatureStats == null ? false : withFeatureStats),
          (withInternalFeatures == null ? false : withInternalFeatures),
          withRaw,
          expectedPathLength,
          expectedPath,
          before,
          after);
    });
  }

  @ParameterizedTest
  @MethodSource("getEntityPathParameters")
  public void getPathByEntityIdTest(SzRecordId              fromRecordId,
                                    SzRecordId              toRecordId,
                                    Integer                 maxDegrees,
                                    Collection<SzRecordId>  avoidParam,
                                    Collection<SzRecordId>  avoidList,
                                    Boolean                 forbidAvoided,
                                    List<String>            sourcesParam,
                                    Boolean                 forceMinimal,
                                    SzFeatureMode featureMode,
                                    Boolean                 withFeatureStats,
                                    Boolean                 withInternalFeatures,
                                    Boolean                 withRaw,
                                    Integer                 expectedPathLength,
                                    List<SzRecordId>        expectedPath)
  {
    this.performTest(() -> {
      String testInfo = "fromRecord=[ " + fromRecordId
          + " ], toRecord=[ " + toRecordId
          + " ], maxDegrees=[ " + maxDegrees
          + " ], avoidParam=[ " + avoidParam
          + " ], avoidList=[ " + avoidList
          + " ], forbidAvoided=[ " + forbidAvoided
          + " ], sources=[ " + sourcesParam
          + " ], forceMinimal=[ " + forceMinimal
          + " ], featureMode=[ " + featureMode
          + " ], withFeatureStats=[ " + withFeatureStats
          + " ], withInternalFeatures=[ " + withInternalFeatures
          + " ], withRaw=[ " + withRaw + " ]";

      SzEntityIdentifier fromIdentifer
          = this.normalizeIdentifier(fromRecordId,true);

      SzEntityIdentifier toIdentifier
          = this.normalizeIdentifier(toRecordId,true);

      SzEntityIdentifiers avoidParamIds
          = this.normalizeIdentifiers(avoidParam,true);

      SzEntityIdentifiers avoidListIds
          = this.normalizeIdentifiers(avoidList,true);

      StringBuilder sb = new StringBuilder();
      sb.append("entity-paths");

      buildPathQueryString(sb,
                           fromIdentifer,
                           toIdentifier,
                           maxDegrees,
                           avoidParamIds,
                           avoidListIds,
                           forbidAvoided,
                           sourcesParam,
                           forceMinimal,
                           featureMode,
                           withFeatureStats,
                           withInternalFeatures,
                           withRaw);

      String uriText = this.formatServerUri(sb.toString());
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      long before = System.currentTimeMillis();

      SzEntityPathResponse response = this.entityGraphServices.getEntityPath(
          fromIdentifer.toString(),
          toIdentifier.toString(),
          (maxDegrees == null ? DEFAULT_PATH_DEGREES : maxDegrees),
          formatIdentifierParam(avoidParamIds),
          formatIdentifierList(avoidListIds),
          (forbidAvoided == null ? false : forbidAvoided),
          sourcesParam,
          (forceMinimal == null ? false : forceMinimal),
          (featureMode == null ? WITH_DUPLICATES : featureMode),
          (withFeatureStats == null ? false : withFeatureStats),
          (withInternalFeatures == null ? false : withInternalFeatures),
          (withRaw == null ? false : withRaw),
          uriInfo);

      response.concludeTimers();
      long after = System.currentTimeMillis();

      this.validateEntityPathResponse(
          testInfo,
          response,
          GET,
          uriText,
          fromIdentifer,
          toIdentifier,
          (maxDegrees != null ? maxDegrees : DEFAULT_PATH_DEGREES),
          avoidParamIds,
          avoidListIds,
          forbidAvoided,
          sourcesParam,
          forceMinimal,
          featureMode,
          (withFeatureStats == null ? false : withFeatureStats),
          (withInternalFeatures == null ? false : withInternalFeatures),
          withRaw,
          expectedPathLength,
          expectedPath,
          before,
          after);
    });
  }

  @ParameterizedTest
  @MethodSource("getEntityPathParameters")
  public void getPathByEntityIdViaHttpTest(
      SzRecordId              fromRecordId,
      SzRecordId              toRecordId,
      Integer                 maxDegrees,
      Collection<SzRecordId>  avoidParam,
      Collection<SzRecordId>  avoidList,
      Boolean                 forbidAvoided,
      List<String>            sourcesParam,
      Boolean                 forceMinimal,
      SzFeatureMode featureMode,
      Boolean                 withFeatureStats,
      Boolean                 withInternalFeatures,
      Boolean                 withRaw,
      Integer                 expectedPathLength,
      List<SzRecordId>        expectedPath)
  {
    this.performTest(() -> {
      String testInfo = "fromRecord=[ " + fromRecordId
          + " ], toRecord=[ " + toRecordId
          + " ], maxDegrees=[ " + maxDegrees
          + " ], avoidParam=[ " + avoidParam
          + " ], avoidList=[ " + avoidList
          + " ], forbidAvoided=[ " + forbidAvoided
          + " ], sources=[ " + sourcesParam
          + " ], forceMinimal=[ " + forceMinimal
          + " ], featureMode=[ " + featureMode
          + " ], withFeatureStats=[ " + withFeatureStats
          + " ], withInternalFeatures=[ " + withInternalFeatures
          + " ], withRaw=[ " + withRaw + " ]";

      SzEntityIdentifier fromIdentifer
          = this.normalizeIdentifier(fromRecordId,true);

      SzEntityIdentifier toIdentifier
          = this.normalizeIdentifier(toRecordId,true);

      SzEntityIdentifiers avoidParamIds
          = this.normalizeIdentifiers(avoidParam,true);

      SzEntityIdentifiers avoidListIds
          = this.normalizeIdentifiers(avoidList,true);

      StringBuilder sb = new StringBuilder();
      sb.append("entity-paths");

      buildPathQueryString(sb,
                           fromIdentifer,
                           toIdentifier,
                           maxDegrees,
                           avoidParamIds,
                           avoidListIds,
                           forbidAvoided,
                           sourcesParam,
                           forceMinimal,
                           featureMode,
                           withFeatureStats,
                           withInternalFeatures,
                           withRaw);

      String uriText = this.formatServerUri(sb.toString());
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      long before = System.currentTimeMillis();
      SzEntityPathResponse response = this.invokeServerViaHttp(
          GET, uriText, SzEntityPathResponse.class);
      response.concludeTimers();
      long after = System.currentTimeMillis();

      this.validateEntityPathResponse(
          testInfo,
          response,
          GET,
          uriText,
          fromIdentifer,
          toIdentifier,
          (maxDegrees != null ? maxDegrees : DEFAULT_PATH_DEGREES),
          avoidParamIds,
          avoidListIds,
          forbidAvoided,
          sourcesParam,
          forceMinimal,
          featureMode,
          (withFeatureStats == null ? false : withFeatureStats),
          (withInternalFeatures == null ? false : withInternalFeatures),
          withRaw,
          expectedPathLength,
          expectedPath,
          before,
          after);
    });
  }

  public void validateEntityPathResponse(
      String                              testInfo,
      SzEntityPathResponse                response,
      SzHttpMethod                        httpMethod,
      String                              selfLink,
      SzEntityIdentifier                  fromIdentifer,
      SzEntityIdentifier                  toIdentifier,
      Integer                             maxDegrees,
      SzEntityIdentifiers                 avoidParam,
      SzEntityIdentifiers                 avoidList,
      Boolean                             forbidAvoided,
      List<String>                        sourcesParam,
      Boolean                             forceMinimal,
      SzFeatureMode featureMode,
      boolean                             withFeatureStats,
      boolean                             withInternalFeatures,
      Boolean                             withRaw,
      Integer                             expectedPathLength,
      List<SzRecordId>                    expectedPath,
      long                                beforeTimestamp,
      long                                afterTimestamp)
  {
    validateBasics(testInfo,
                   response,
                   httpMethod,
                   selfLink,
                   beforeTimestamp,
                   afterTimestamp);

    SzEntityPathData pathData = response.getData();

    assertNotNull(pathData, "Response path data is null: " + testInfo);

    SzEntityPath entityPath = pathData.getEntityPath();

    assertNotNull(entityPath, "Entity path is null: " + testInfo);

    List<SzEntityData> entities = pathData.getEntities();

    assertNotNull(entities, "Entity list from path is null: " + testInfo);

    Long        fromEntityId    = this.asEntityId(fromIdentifer);
    Long        toEntityId      = this.asEntityId(toIdentifier);
    List<Long>  avoidParamIds   = this.asEntityIds(avoidParam);
    List<Long>  avoidListIds    = this.asEntityIds(avoidList);
    List<Long>  expectedPathIds = this.asEntityIds(expectedPath);

    assertEquals(fromEntityId, entityPath.getStartEntityId(),
                 "Unexpected path start point: " + testInfo);
    assertEquals(toEntityId, entityPath.getEndEntityId(),
                 "Unexpected path end point: " + testInfo);

    if (avoidParamIds != null && forbidAvoided != null && forbidAvoided) {
      for (Long entityId: entityPath.getEntityIds()) {
        if (avoidParamIds.contains(entityId)) {
          fail("Entity from avoidParam (" + entityId
                   + ") in path despite being forbidden: " + testInfo);
        }
      }
    }
    if (avoidListIds != null && forbidAvoided != null && forbidAvoided) {
      for (Long entityId: entityPath.getEntityIds()) {
        if (avoidListIds.contains(entityId)) {
          fail("Entity from avoidList (" + entityId
                   + ") in path despite being forbidden: " + testInfo);
        }
      }
    }
    Map<Long, SzResolvedEntity> entityMap = new LinkedHashMap<>();
    entities.forEach(entityData -> {
      SzResolvedEntity resolvedEntity = entityData.getResolvedEntity();
      entityMap.put(resolvedEntity.getEntityId(), resolvedEntity);
    });

    if (sourcesParam != null && sourcesParam.size() > 0) {
      boolean sourcesSatisifed = false;
      for (Long entityId : entityPath.getEntityIds()) {
        if (entityId.equals(entityPath.getStartEntityId())) continue;
        if (entityId.equals(entityPath.getEndEntityId())) continue;
        SzResolvedEntity entity = entityMap.get(entityId);
        for (SzDataSourceRecordSummary summary : entity.getRecordSummaries()) {
          if (sourcesParam.contains(summary.getDataSource())) {
            sourcesSatisifed = true;
            break;
          }
        }
        if (sourcesSatisifed) break;
      }
      if (!sourcesSatisifed) {
        fail("Entity path does not contain required data sources: " + testInfo);
      }
    }

    if (expectedPathLength != null && expectedPathLength <= 0) {
      // expect that no path was found
      assertEquals(0, entityPath.getEntityIds().size(),
                   "Path unexpectedly found between entities: "
                       + testInfo);
    } else if (expectedPathLength != null) {
      // expect the path to be of a certain length
      String unexpectedPathMsg = this.formatUnexpectedPathMessage(
          expectedPath, entityPath.getEntityIds(), entityMap);
      assertEquals(expectedPathLength, entityPath.getEntityIds().size(),
                   "Path found of unexpected length: " + testInfo
                   + unexpectedPathMsg);
    }

    if (maxDegrees != null && maxDegrees < (entityPath.getEntityIds().size()-1))
    {
      String unexpectedPathMsg = this.formatUnexpectedPathMessage(
          expectedPath, entityPath.getEntityIds(), entityMap);
      fail("Entity path exceeds the maximum number of degrees of separation: "
           + testInfo + unexpectedPathMsg);
    }
    if (expectedPathIds != null) {
      if (!expectedPathIds.equals(entityPath.getEntityIds())) {
        String unexpectedPathMsg = this.formatUnexpectedPathMessage(
            expectedPath, entityPath.getEntityIds(), entityMap);
        fail("Path found does not match expected paths (" + testInfo + ")"
              + unexpectedPathMsg);
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
                     false,
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
                         "ENTITY_PATHS", "ENTITIES");

      Object rawPaths = ((Map) response.getRawData()).get("ENTITY_PATHS");

      validateRawDataMapArray(testInfo,
                              rawPaths,
                              true,
                              "START_ENTITY_ID",
                              "END_ENTITY_ID",
                              "ENTITIES");

      Object rawEntities = ((Map) response.getRawData()).get("ENTITIES");

      validateRawDataMapArray(testInfo,
                              rawEntities,
                              true,
                              "RESOLVED_ENTITY",
                              "RELATED_ENTITIES");

      for (Object entity : ((Collection) rawEntities)) {
        if (featureMode == NONE || (forceMinimal != null && forceMinimal)) {
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

  private String formatUnexpectedPathMessage(
      List<SzRecordId>            expectedPath,
      List<Long>                  actualPath,
      Map<Long, SzResolvedEntity> entityMap)
  {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    pw.println();
    if (expectedPath != null) {
      pw.println("EXPECTED PATH: ");
      expectedPath.forEach(recordId -> {
        Long entityId = this.getEntityIdForRecordId(recordId);
        pw.println("    " + entityId + " / " + recordId.getDataSourceCode()
                       + ":" + recordId.getRecordId());
      });
      pw.flush();
    }
    if (actualPath != null) {
      pw.println("ACTUAL PATH: ");
      actualPath.forEach(entityId -> {
        SzResolvedEntity entity = entityMap.get(entityId);
        pw.print("    " + entityId);
        entity.getRecords().forEach(record -> {
          pw.print(" / ");
          pw.print(record.getDataSource() + ":" + record.getRecordId());
        });
        pw.println();
        pw.flush();
      });
    }
    pw.flush();
    return sw.toString();
  }


  private static List<List> networkArgs(
      Collection<SzRecordId>  recordIds,
      Integer                 maxDegrees,
      Integer                 buildOut,
      Integer                 maxEntities,
      Integer                 expectedPathCount,
      List<List<SzRecordId>>  expectedPaths,
      Set<SzRecordId>         expectedEntities)
  {
    List<List> result = new LinkedList<>();

    // creates variants of the specified avoid parameter to exercise both
    // of the parameters that deal with avoiding entities
    List<List> recordVariants = new LinkedList<>();
    if (recordIds.size() == 1) {
      recordVariants.add(list(recordIds, null));
      recordVariants.add(list(null, recordIds));

    } else if (recordIds.size() > 1) {
      recordVariants.add(list(recordIds, null));
      recordVariants.add(list(null, recordIds));

      int recordCount = recordIds.size();
      Set<SzRecordId> records1 = new LinkedHashSet<>();
      Set<SzRecordId> records2 = new LinkedHashSet<>();
      for (SzRecordId recordId : recordIds) {
        if ((recordCount % 2) == 0) {
          records1.add(recordId);
        } else {
          records2.add(recordId);
        }
        recordCount--;
      }
      recordVariants.add(list(records1, records2));
      recordVariants.add(list(records2, records1));
    }

    // create variants on the max degrees parameter to test default values
    List<Integer> degreesVariants = new LinkedList<>();
    if (maxDegrees == null || maxDegrees == DEFAULT_NETWORK_DEGREES) {
      degreesVariants.add(null);
      degreesVariants.add(DEFAULT_NETWORK_DEGREES);
    } else {
      degreesVariants.add(maxDegrees);
    }

    // create variants on the build-out parameter to test default values
    List<Integer> buildOutVariants = new LinkedList<>();
    if (buildOut == null || buildOut == DEFAULT_BUILD_OUT) {
      buildOutVariants.add(null);
      buildOutVariants.add(DEFAULT_BUILD_OUT);
    } else {
      buildOutVariants.add(buildOut);
    }

    // create variants on the max entities parameter to test default values
    List<Integer> maxEntitiesVariants = new LinkedList<>();
    if (maxEntities == null || maxEntities == DEFAULT_MAX_ENTITIES) {
      maxEntitiesVariants.add(null);
      maxEntitiesVariants.add(DEFAULT_MAX_ENTITIES);
    } else {
      maxEntitiesVariants.add(maxEntities);
    }
    int maxEntityCount = (maxEntities != null)
                       ? maxEntities : DEFAULT_MAX_ENTITIES;
    if (expectedEntities != null) {
      int[] addlVariants = {
          expectedEntities.size(),
          expectedEntities.size() - 1,
          expectedEntities.size() / 2,
          expectedEntities.size() * 2
      };
      for (int variant : addlVariants) {
        if (!maxEntitiesVariants.contains(variant)) {
          maxEntitiesVariants.add(variant);
        }
      }
    }

    recordVariants.forEach(recordVariant -> {
      degreesVariants.forEach(degreeVariant -> {
        buildOutVariants.forEach(buildOutVariant -> {
          maxEntitiesVariants.forEach(maxEntitiesVariant -> {
            result.add(
                list(recordVariant.get(0),
                     recordVariant.get(1),
                     degreeVariant,
                     buildOutVariant,
                     maxEntitiesVariant,
                     null,  // forceMinimal (5)
                     null,  // featureMode (6)
                     null,  // withFeatureStats (7)
                     null,  // withInternalFeatures (8)
                     null,  // withRaw (9)
                     expectedPathCount,
                     expectedPaths,
                     expectedEntities));
          });
        });
      });
    });

    return result;
  }

  private List<Arguments> getEntityNetworkParameters() {

    List<List> baseArgs = new LinkedList<>();

    baseArgs.addAll(networkArgs(
        set(ABC123), 1, 0, null,
        0, list(), set(ABC123)));

    baseArgs.addAll(networkArgs(
        set(ABC123), 1, 1, null,
        0, list(), set(ABC123,DEF456,MNO345)));

    baseArgs.addAll(networkArgs(
        set(ABC123,JKL456), 1, 0, null,
        1, list(list()),
        set(ABC123,JKL456)));

    baseArgs.addAll(networkArgs(
        set(ABC123,JKL456), 3, 0, null,
        1,
        list(list(ABC123,MNO345,DEF890,JKL456)),
        set(ABC123,MNO345,DEF890,JKL456)));

    baseArgs.addAll(networkArgs(
        set(ABC123,ABC567,JKL456), 3, 0, null,
        3,
        list(list(ABC123,MNO345,DEF890,JKL456),
             list(ABC123,DEF456,PQR678,ABC567)),
        set(ABC123,MNO345,DEF890,JKL456,DEF456,PQR678,ABC567)));
    
    Boolean[] booleanVariants = {null, true, false};
    List<Boolean> booleanVariantList = Arrays.asList(booleanVariants);

    List<SzFeatureMode> featureModes = new LinkedList<>();
    featureModes.add(null);
    for (SzFeatureMode featureMode : SzFeatureMode.values()) {
      featureModes.add(featureMode);
    }

    // convert to an array list for random access
    baseArgs = new ArrayList<>(baseArgs);

    List<List> optionCombos = generateCombinations(
        booleanVariantList,   // forceMinimal
        featureModes,         // featureMode
        booleanVariantList,   // withFeatureStats
        booleanVariantList,   // withInternalFeatures
        booleanVariantList);  // withRaw

    int count = Math.max(baseArgs.size(), optionCombos.size()) * 2;
    List<Arguments> result = new ArrayList<>(count);

    for (int index = 0; index < count; index++) {
      int argIndex      = index % baseArgs.size();
      int optionsIndex  = index % optionCombos.size();

      List baseArgList = baseArgs.get(argIndex);
      List optionsList = optionCombos.get(optionsIndex);

      Object[] argArray = baseArgList.toArray();
      argArray[5] = optionsList.get(0);
      argArray[6] = optionsList.get(1);
      argArray[7] = optionsList.get(2);
      argArray[8] = optionsList.get(3);
      argArray[9] = optionsList.get(4);
      result.add(arguments(argArray));
    }

    return result;

  }

  private StringBuilder buildNetworkQueryString(
      StringBuilder         sb,
      SzEntityIdentifiers   entitiesParam,
      SzEntityIdentifiers   entityList,
      Integer               maxDegrees,
      Integer               buildOut,
      Integer               maxEntities,
      Boolean               forceMinimal,
      SzFeatureMode featureMode,
      Boolean               withFeatureStats,
      Boolean               withInternalFeatures,
      Boolean               withRaw)
  {
    try {
      String prefix = "?";
      if (entitiesParam != null && !entitiesParam.isEmpty()) {
        for (SzEntityIdentifier identifier : entitiesParam.getIdentifiers()) {
          sb.append(prefix).append("e=").append(
              URLEncoder.encode(identifier.toString(), "UTF-8"));
          prefix = "&";
        }
      }
      if (entityList != null && !entityList.isEmpty()) {
        sb.append(prefix).append("entities=").append(
            URLEncoder.encode(entityList.toString(), "UTF-8"));
      }
      if (maxDegrees != null) {
        sb.append("&maxDegrees=").append(maxDegrees);
      }
      if (buildOut != null) {
        sb.append("&buildOut=").append(buildOut);
      }
      if (maxEntities != null) {
        sb.append("&maxEntities=").append(maxEntities);
      }
      if (forceMinimal != null) {
        sb.append("&forceMinimal=").append(forceMinimal);
      }
      if (featureMode != null) {
        sb.append("&featureMode=").append(featureMode);
      }
      if (withFeatureStats != null) {
        sb.append("&withFeatureStats=").append(withFeatureStats);
      }
      if (withInternalFeatures != null) {
        sb.append("&withInternalFeatures=").append(withInternalFeatures);
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
  @MethodSource("getEntityNetworkParameters")
  public void getNetworkByRecordIdTest(
      Collection<SzRecordId>   entityParam,
      Collection<SzRecordId>   entityList,
      Integer                  maxDegrees,
      Integer                  buildOut,
      Integer                  maxEntities,
      Boolean                  forceMinimal,
      SzFeatureMode featureMode,
      Boolean                  withFeatureStats,
      Boolean                  withInternalFeatures,
      Boolean                  withRaw,
      Integer                  expectedPathCount,
      List<List<SzRecordId>>   expectedPaths,
      Set<SzRecordId>          expectedEntities)
  {
    this.performTest(() -> {
      String testInfo = "entityParam=[ " + entityParam
        + " ], entityList=[ " + entityList
        + " ], maxDegrees=[ " + maxDegrees
        + " ], buildOut=[ " + buildOut
        + " ], maxEntities=[ " + maxEntities
        + " ], forceMinimal=[ " + forceMinimal
        + " ], featureMode=[ " + featureMode
        + " ], withFeatureStats=[ " + withFeatureStats
        + " ], withInternalFeatures=[ " + withInternalFeatures
        + " ], withRaw=[ " + withRaw + " ]";

      SzEntityIdentifiers entityParamIds
          = this.normalizeIdentifiers(entityParam,false);

      SzEntityIdentifiers entityListIds
          = this.normalizeIdentifiers(entityList,false);

      StringBuilder sb = new StringBuilder();
      sb.append("entity-networks");

      buildNetworkQueryString(sb,
                              entityParamIds,
                              entityListIds,
                              maxDegrees,
                              buildOut,
                              maxEntities,
                              forceMinimal,
                              featureMode,
                              withFeatureStats,
                              withInternalFeatures,
                              withRaw);

      String uriText = this.formatServerUri(sb.toString());
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      long before = System.currentTimeMillis();

      SzEntityNetworkResponse response
          = this.entityGraphServices.getEntityNetwork(
          formatIdentifierParam(entityParamIds),
          formatIdentifierList(entityListIds),
          (maxDegrees == null   ? DEFAULT_NETWORK_DEGREES : maxDegrees),
          (buildOut == null     ? DEFAULT_BUILD_OUT : buildOut),
          (maxEntities == null  ? DEFAULT_MAX_ENTITIES : maxEntities),
          (forceMinimal == null ? false : forceMinimal),
          (featureMode == null  ? WITH_DUPLICATES : featureMode),
          (withFeatureStats == null ? false : withFeatureStats),
          (withInternalFeatures == null ? false : withInternalFeatures),
          (withRaw == null      ? false : withRaw),
          uriInfo);

      response.concludeTimers();
      long after = System.currentTimeMillis();

      this.validateEntityNetworkResponse(
          testInfo,
          response,
          GET,
          uriText,
          entityParamIds,
          entityListIds,
          (maxDegrees != null) ? maxDegrees : DEFAULT_NETWORK_DEGREES,
          (buildOut != null) ? buildOut : DEFAULT_BUILD_OUT,
          (maxEntities != null) ? maxEntities : DEFAULT_MAX_ENTITIES,
          forceMinimal,
          featureMode,
          (withFeatureStats == null) ? false : withFeatureStats,
          (withInternalFeatures == null) ? false : withInternalFeatures,
          withRaw,
          expectedPathCount,
          expectedPaths,
          expectedEntities,
          before,
          after);
    });
  }

  @ParameterizedTest
  @MethodSource("getEntityNetworkParameters")
  public void getNetworkByRecordIdViaHttpTest(
      Collection<SzRecordId>   entityParam,
      Collection<SzRecordId>   entityList,
      Integer                  maxDegrees,
      Integer                  buildOut,
      Integer                  maxEntities,
      Boolean                  forceMinimal,
      SzFeatureMode            featureMode,
      Boolean                  withFeatureStats,
      Boolean                  withInternalFeatures,
      Boolean                  withRaw,
      Integer                  expectedPathCount,
      List<List<SzRecordId>>   expectedPaths,
      Set<SzRecordId>          expectedEntities)
  {
    this.performTest(() -> {
      String testInfo = "entityParam=[ " + entityParam
          + " ], entityList=[ " + entityList
          + " ], maxDegrees=[ " + maxDegrees
          + " ], buildOut=[ " + buildOut
          + " ], maxEntities=[ " + maxEntities
          + " ], forceMinimal=[ " + forceMinimal
          + " ], featureMode=[ " + featureMode
          + " ], withFeatureStats=[ " + withFeatureStats
          + " ], withInternalFeatures=[ " + withInternalFeatures
          + " ], withRaw=[ " + withRaw + " ]";

      SzEntityIdentifiers entityParamIds
          = this.normalizeIdentifiers(entityParam,false);

      SzEntityIdentifiers entityListIds
          = this.normalizeIdentifiers(entityList,false);

      StringBuilder sb = new StringBuilder();
      sb.append("entity-networks");

      buildNetworkQueryString(sb,
                              entityParamIds,
                              entityListIds,
                              maxDegrees,
                              buildOut,
                              maxEntities,
                              forceMinimal,
                              featureMode,
                              withFeatureStats,
                              withInternalFeatures,
                              withRaw);

      String uriText = this.formatServerUri(sb.toString());

      long before = System.currentTimeMillis();

      SzEntityNetworkResponse response = this.invokeServerViaHttp(
          GET, uriText, SzEntityNetworkResponse.class);
      response.concludeTimers();
      long after = System.currentTimeMillis();

      this.validateEntityNetworkResponse(
          testInfo,
          response,
          GET,
          uriText,
          entityParamIds,
          entityListIds,
          (maxDegrees != null) ? maxDegrees : DEFAULT_NETWORK_DEGREES,
          (buildOut != null) ? buildOut : DEFAULT_BUILD_OUT,
          (maxEntities != null) ? maxEntities : DEFAULT_MAX_ENTITIES,
          forceMinimal,
          featureMode,
          (withFeatureStats == null) ? false : withFeatureStats,
          (withInternalFeatures == null) ? false : withInternalFeatures,
          withRaw,
          expectedPathCount,
          expectedPaths,
          expectedEntities,
          before,
          after);
    });
  }

  @ParameterizedTest
  @MethodSource("getEntityNetworkParameters")
  public void getNetworkByEntityIdTest(
      Collection<SzRecordId>   entityParam,
      Collection<SzRecordId>   entityList,
      Integer                  maxDegrees,
      Integer                  buildOut,
      Integer                  maxEntities,
      Boolean                  forceMinimal,
      SzFeatureMode featureMode,
      Boolean                  withFeatureStats,
      Boolean                  withInternalFeatures,
      Boolean                  withRaw,
      Integer                  expectedPathCount,
      List<List<SzRecordId>>   expectedPaths,
      Set<SzRecordId>          expectedEntities)
  {
    this.performTest(() -> {
      String testInfo = "entityParam=[ " + entityParam
          + " ], entityList=[ " + entityList
          + " ], maxDegrees=[ " + maxDegrees
          + " ], buildOut=[ " + buildOut
          + " ], maxEntities=[ " + maxEntities
          + " ], forceMinimal=[ " + forceMinimal
          + " ], featureMode=[ " + featureMode
          + " ], withFeatureStats=[ " + withFeatureStats
          + " ], withInternalFeatures=[ " + withInternalFeatures
          + " ], withRaw=[ " + withRaw + " ]";

      SzEntityIdentifiers entityParamIds
          = this.normalizeIdentifiers(entityParam,true);

      SzEntityIdentifiers entityListIds
          = this.normalizeIdentifiers(entityList,true);

      StringBuilder sb = new StringBuilder();
      sb.append("entity-networks");

      buildNetworkQueryString(sb,
                              entityParamIds,
                              entityListIds,
                              maxDegrees,
                              buildOut,
                              maxEntities,
                              forceMinimal,
                              featureMode,
                              withFeatureStats,
                              withInternalFeatures,
                              withRaw);

      String uriText = this.formatServerUri(sb.toString());
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      long before = System.currentTimeMillis();

      SzEntityNetworkResponse response
          = this.entityGraphServices.getEntityNetwork(
          formatIdentifierParam(entityParamIds),
          formatIdentifierList(entityListIds),
          (maxDegrees == null   ? DEFAULT_NETWORK_DEGREES : maxDegrees),
          (buildOut == null     ? DEFAULT_BUILD_OUT : buildOut),
          (maxEntities == null  ? DEFAULT_MAX_ENTITIES : maxEntities),
          (forceMinimal == null ? false : forceMinimal),
          (featureMode == null  ? WITH_DUPLICATES : featureMode),
          (withFeatureStats == null ? false : withFeatureStats),
          (withInternalFeatures == null ? false : withInternalFeatures),
          (withRaw == null      ? false : withRaw),
          uriInfo);

      response.concludeTimers();
      long after = System.currentTimeMillis();

      this.validateEntityNetworkResponse(
          testInfo,
          response,
          GET,
          uriText,
          entityParamIds,
          entityListIds,
          (maxDegrees != null) ? maxDegrees : DEFAULT_NETWORK_DEGREES,
          (buildOut != null) ? buildOut : DEFAULT_BUILD_OUT,
          (maxEntities != null) ? maxEntities : DEFAULT_MAX_ENTITIES,
          forceMinimal,
          featureMode,
          (withFeatureStats == null) ? false : withFeatureStats,
          (withInternalFeatures == null) ? false : withInternalFeatures,
          withRaw,
          expectedPathCount,
          expectedPaths,
          expectedEntities,
          before,
          after);
    });
  }

  @ParameterizedTest
  @MethodSource("getEntityNetworkParameters")
  public void getNetworkByEntityIdViaHttpTest(
      Collection<SzRecordId>   entityParam,
      Collection<SzRecordId>   entityList,
      Integer                  maxDegrees,
      Integer                  buildOut,
      Integer                  maxEntities,
      Boolean                  forceMinimal,
      SzFeatureMode            featureMode,
      Boolean                  withFeatureStats,
      Boolean                  withInternalFeatures,
      Boolean                  withRaw,
      Integer                  expectedPathCount,
      List<List<SzRecordId>>   expectedPaths,
      Set<SzRecordId>          expectedEntities)
  {
    this.performTest(() -> {
      String testInfo = "entityParam=[ " + entityParam
          + " ], entityList=[ " + entityList
          + " ], maxDegrees=[ " + maxDegrees
          + " ], buildOut=[ " + buildOut
          + " ], maxEntities=[ " + maxEntities
          + " ], forceMinimal=[ " + forceMinimal
          + " ], featureMode=[ " + featureMode
          + " ], withFeatureStats=[ " + withFeatureStats
          + " ], withInternalFeatures=[ " + withInternalFeatures
          + " ], withRaw=[ " + withRaw + " ]";

      SzEntityIdentifiers entityParamIds
          = this.normalizeIdentifiers(entityParam,true);

      SzEntityIdentifiers entityListIds
          = this.normalizeIdentifiers(entityList,true);

      StringBuilder sb = new StringBuilder();
      sb.append("entity-networks");

      buildNetworkQueryString(sb,
                              entityParamIds,
                              entityListIds,
                              maxDegrees,
                              buildOut,
                              maxEntities,
                              forceMinimal,
                              featureMode,
                              withFeatureStats,
                              withInternalFeatures,
                              withRaw);

      String uriText = this.formatServerUri(sb.toString());

      long before = System.currentTimeMillis();

      SzEntityNetworkResponse response = this.invokeServerViaHttp(
          GET, uriText, SzEntityNetworkResponse.class);
      response.concludeTimers();
      long after = System.currentTimeMillis();

      this.validateEntityNetworkResponse(
          testInfo,
          response,
          GET,
          uriText,
          entityParamIds,
          entityListIds,
          (maxDegrees != null) ? maxDegrees : DEFAULT_NETWORK_DEGREES,
          (buildOut != null) ? buildOut : DEFAULT_BUILD_OUT,
          (maxEntities != null) ? maxEntities : DEFAULT_MAX_ENTITIES,
          forceMinimal,
          featureMode,
          (withFeatureStats == null ? false : withFeatureStats),
          (withInternalFeatures == null ? false : withInternalFeatures),
          withRaw,
          expectedPathCount,
          expectedPaths,
          expectedEntities,
          before,
          after);
    });
  }

  private void validateEntityNetworkResponse(
      String                              testInfo,
      SzEntityNetworkResponse             response,
      SzHttpMethod                        httpMethod,
      String                              selfLink,
      SzEntityIdentifiers                 entityParam,
      SzEntityIdentifiers                 entityList,
      Integer                             maxDegrees,
      Integer                             buildOut,
      Integer                             maxEntities,
      Boolean                             forceMinimal,
      SzFeatureMode                       featureMode,
      boolean                             withFeatureStats,
      boolean                             withInternalFeatures,
      Boolean                             withRaw,
      Integer                             expectedPathCount,
      List<List<SzRecordId>>              expectedPaths,
      Set<SzRecordId>                     expectedEntities,
      long                                beforeTimestamp,
      long                                afterTimestamp)
  {
    selfLink = this.formatServerUri(selfLink);

    // determine how many entities were requested
    Set<SzEntityIdentifier> entityIdentifiers = new LinkedHashSet<>();
    if (entityParam != null) {
      entityIdentifiers.addAll(entityParam.getIdentifiers());
    }
    if (entityList != null) {
      entityIdentifiers.addAll(entityList.getIdentifiers());
    }
    int entityCount = entityIdentifiers.size();

    validateBasics(testInfo,
                   response,
                   httpMethod,
                   selfLink,
                   beforeTimestamp,
                   afterTimestamp);

    SzEntityNetworkData networkData = response.getData();

    assertNotNull(networkData,
                  "Response network data is null: " + testInfo);

    List<SzEntityPath> entityPaths = networkData.getEntityPaths();

    assertNotNull(entityPaths, "Entity path list is null: " + testInfo);

    // remove the self-paths when only a single entity is requested
    if (entityCount == 1) {
      List<SzEntityPath> list = new ArrayList<>(entityPaths.size());
      for (SzEntityPath path : entityPaths) {
        if (path.getStartEntityId() == path.getEndEntityId()) {
          continue;
        }
        list.add(path);
      }
      entityPaths = list;
    }

    List<SzEntityData> entities = networkData.getEntities();

    assertNotNull(entities,
                  "Entity list from network is null: " + testInfo);

    List<Long>  entityParamIds  = this.asEntityIds(entityParam);
    List<Long>  entityListIds   = this.asEntityIds(entityList);

    Set<Long> pathEntityIds = new HashSet<>();
    for (SzEntityPath entityPath : entityPaths) {
      pathEntityIds.add(entityPath.getStartEntityId());
      pathEntityIds.add(entityPath.getEndEntityId());
      for (Long entityId : entityPath.getEntityIds()) {
        pathEntityIds.add(entityId);
      }
    }

    List<List<Long>>  expectedPathList  = null;
    IdentityHashMap<List<Long>, List<SzRecordId>> epLookup = null;
    if (expectedPaths != null) {
      epLookup = new IdentityHashMap<>();
      expectedPathList = new ArrayList<>(expectedPaths.size());
      for (List<SzRecordId> expectedPath : expectedPaths) {
        List<Long> expectedPathIds = this.asEntityIds(expectedPath);
        if (expectedPathIds != null) {
          expectedPathList.add(expectedPathIds);
        }
        epLookup.put(expectedPathIds, expectedPath);
      }
    }

    Set<Long> allExpectedEntities = new LinkedHashSet<>();
    if (entityParamIds != null) allExpectedEntities.addAll(entityParamIds);
    if (entityListIds != null)  allExpectedEntities.addAll(entityListIds);

    for (long fromEntityId : allExpectedEntities) {
      for (long toEntityId : allExpectedEntities) {
        if (fromEntityId == toEntityId) continue;
        boolean found = false;
        for (SzEntityPath entityPath : entityPaths) {
          long start  = entityPath.getStartEntityId();
          long end    = entityPath.getEndEntityId();
          if (((start == fromEntityId) && (end == toEntityId))
              || ((start == toEntityId) && (end == fromEntityId)))
          {
            found = true;
            break;
          }
        }
        if (!found) {
          fail("Missing entity path between " + fromEntityId + " and "
               + toEntityId + ": " + entityPaths + " / " + testInfo);
        }

      }
    }

    List<Long> expectedEntityIds = this.asEntityIds(expectedEntities);
    if (expectedEntityIds != null) {
      allExpectedEntities.addAll(expectedEntityIds);

      for (SzEntityPath entityPath : entityPaths) {
        for (Long entityId : entityPath.getEntityIds()) {
          if (!expectedEntityIds.contains(entityId)) {
            fail("Unexpected entity found on entity (" + entityId
                 + ") path entity path (" + entityPath.getEntityIds()
                 + "): " + testInfo);
          }
        }
      }
    }

    Map<Long, SzResolvedEntity> entityMap = new LinkedHashMap<>();
    entities.forEach(entityData -> {
      SzResolvedEntity resolvedEntity = entityData.getResolvedEntity();
      entityMap.put(resolvedEntity.getEntityId(), resolvedEntity);
    });

    // augment the path entity IDs for single entities with no path to others
    if (entityParamIds != null) pathEntityIds.addAll(entityParamIds);
    if (entityListIds != null) pathEntityIds.addAll(entityListIds);
    if (maxEntities != null
        && entityMap.size() > Math.max(maxEntities,pathEntityIds.size()))
    {
      fail("The number of entity details (" + entityMap.size()
           + ") exceeded the max entities (" + maxEntities
           +  " / " + pathEntityIds.size() + "): " + testInfo);
    }

    int maxEntityCount = (maxEntities != null)
        ? maxEntities : DEFAULT_MAX_ENTITIES;
    if (allExpectedEntities.size() < maxEntityCount) {
      for (Long entityId : allExpectedEntities) {
        if (!entityMap.containsKey(entityId)) {
          fail("Missing entity details for entity " + entityId + ": " + testInfo);
        }
      }
    } else if (expectedEntities != null) {
      int foundCount = 0;
      for (Long entityId: allExpectedEntities) {
        if (entityMap.containsKey(entityId)) foundCount++;
      }
      if (foundCount < maxEntityCount) {
        fail("Only found " + foundCount + " entity details for "
             + allExpectedEntities.size() + " expected entities with "
             + maxEntityCount + " max entities: " + testInfo);
      }
    }

    if (expectedPathCount != null) {
      assertEquals(expectedPathCount, entityPaths.size(),
                   "Unexpected number of paths found: "
                       + entityPaths + " / " + testInfo);
    }

    if (maxDegrees != null) {
      for (SzEntityPath entityPath : entityPaths) {
        if (maxDegrees < (entityPath.getEntityIds().size() - 1)) {
          String unexpectedPathMsg = this.formatUnexpectedPathMessage(
              null, entityPath.getEntityIds(), entityMap);
          fail("Entity path exceeds the maximum number of degrees of "
                   + "separation: " + testInfo + unexpectedPathMsg);
        }
      }
    }
    if (expectedPathList != null) {
      for (List<Long> expectedPath : expectedPathList) {
        boolean found = false;
        for (SzEntityPath entityPath : entityPaths) {
          if (expectedPath.equals(entityPath.getEntityIds())) {
            found = true;
            break;
          }
        }
        if (!found) {
          String unexpectedPathMsg = this.formatUnexpectedPathMessage(
              epLookup.get(expectedPath), null, entityMap);
          fail("Path found does not match expected paths (" + testInfo + ")"
                   + unexpectedPathMsg);
        }
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
                     false,
                     null,
                     true,
                     null,
                     null,
                     null,
                     null,
                     null);
    }

    if (withRaw != null && withRaw) {
      if (maxEntities == null || expectedEntities.size() < maxEntities) {
        validateRawDataMap(testInfo,
                           response.getRawData(),
                           true,
                           "ENTITY_PATHS", "ENTITIES");
      } else {
        validateRawDataMap(testInfo,
                           response.getRawData(),
                           true,
                           "ENTITY_PATHS",
                           "ENTITIES",
                           "MAX_ENTITY_LIMIT_REACHED");
      }

      Object rawPaths = ((Map) response.getRawData()).get("ENTITY_PATHS");

      validateRawDataMapArray(testInfo,
                              rawPaths,
                              true,
                              "START_ENTITY_ID",
                              "END_ENTITY_ID",
                              "ENTITIES");

      Object rawEntities = ((Map) response.getRawData()).get("ENTITIES");

      validateRawDataMapArray(testInfo,
                              rawEntities,
                              true,
                              "RESOLVED_ENTITY",
                              "RELATED_ENTITIES");

      for (Object entity : ((Collection) rawEntities)) {
        if (featureMode == NONE || (forceMinimal != null && forceMinimal)) {
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
