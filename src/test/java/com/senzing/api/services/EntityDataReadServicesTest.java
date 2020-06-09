package com.senzing.api.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.senzing.api.model.*;
import com.senzing.g2.engine.G2Engine;
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
import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.UriInfo;
import java.io.*;
import java.util.*;

import static com.senzing.api.model.SzHttpMethod.GET;
import static com.senzing.api.model.SzAttributeClass.*;
import static com.senzing.util.CollectionUtilities.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.TestInstance.Lifecycle;
import static com.senzing.api.model.SzFeatureMode.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static com.senzing.api.services.ResponseValidators.*;
import static com.senzing.api.model.SzRelationshipMode.*;

@TestInstance(Lifecycle.PER_CLASS)
public class EntityDataReadServicesTest extends AbstractServiceTest {
  private static final String PASSENGERS = "PASSENGERS";
  private static final String EMPLOYEES  = "EMPLOYEES";
  private static final String VIPS       = "VIPS";
  private static final String MARRIAGES  = "MARRIAGES";

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

  private static final SzRecordId BCD123 = new SzRecordId(MARRIAGES,
                                                          "BCD123");
  private static final SzRecordId CDE456 = new SzRecordId(MARRIAGES,
                                                          "CDE456");
  private static final SzRecordId EFG789 = new SzRecordId(MARRIAGES,
                                                          "EFG789");
  private static final SzRecordId FGH012 = new SzRecordId(MARRIAGES,
                                                          "FGH012");

  private EntityDataServices entityDataServices;

  @BeforeAll
  public void initializeEnvironment() {
    this.beginTests();
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
    dataSources.add("MARRIAGES");

    File passengerFile = this.preparePassengerFile();
    File employeeFile = this.prepareEmployeeFile();
    File vipFile = this.prepareVipFile();
    File marriagesFile = this.prepareMariagesFile();

    employeeFile.deleteOnExit();
    passengerFile.deleteOnExit();
    vipFile.deleteOnExit();
    marriagesFile.deleteOnExit();

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

    RepositoryManager.loadFile(repoDirectory,
                               marriagesFile,
                               MARRIAGES,
                               null,
                               true);
  }

  private static String relationshipKey(SzRecordId recordId1,
                                        SzRecordId recordId2) {
    String rec1 = recordId1.getRecordId();
    String rec2 = recordId2.getRecordId();
    if (rec1.compareTo(rec2) <= 0) {
      return rec1 + "|" + rec2;
    } else {
      return rec2 + "|" + rec1;
    }
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
        "DATE_OF_BIRTH","MOTHERS_MAIDEN_NAME"};

    String[][] employees = {
        {MNO345.getRecordId(), "Joseph", "Schmoe", "702-555-1212",
            "101 Main Street, Las Vegas, NV 89101", "12-JAN-1981", "WILSON"},
        {PQR678.getRecordId(), "Jo Anne", "Smith", "212-555-1212",
            "101 Fifth Ave, Las Vegas, NV 10018", "15-MAY-1983", "JACOBS"}
    };

    return this.prepareJsonArrayFile("test-employees-", headers, employees);
  }

  private File prepareVipFile() {
    String[] headers = {
        "RECORD_ID", "NAME_FIRST", "NAME_LAST", "PHONE_NUMBER", "ADDR_FULL",
        "DATE_OF_BIRTH","MOTHERS_MAIDEN_NAME"};

    String[][] vips = {
        {STU901.getRecordId(), "John", "Doe", "818-555-1313",
            "100 Main Street, Los Angeles, CA 90012", "17-OCT-1978", "GREEN"},
        {XYZ234.getRecordId(), "Jane", "Doe", "818-555-1212",
            "100 Main Street, Los Angeles, CA 90012", "5-FEB-1979", "GRAHAM"}
    };

    return this.prepareJsonFile("test-vips-", headers, vips);
  }

  private File prepareMariagesFile() {
    String[] headers = {
        "RECORD_ID", "NAME_FULL", "AKA_NAME_FULL", "PHONE_NUMBER", "ADDR_FULL",
        "MARRIAGE_DATE", "DATE_OF_BIRTH", "GENDER", "RELATIONSHIP_TYPE",
        "RELATIONSHIP_ROLE", "RELATIONSHIP_KEY" };

    String[][] spouses = {
        {BCD123.getRecordId(), "Bruce Wayne", "Batman", "201-765-3451",
            "101 Wayne Manor Rd; Gotham City, NJ 07017", "05-JUN-2008",
            "08-SEP-1971", "M", "SPOUSE", "HUSBAND",
            relationshipKey(BCD123, CDE456)},
        {CDE456.getRecordId(), "Selina Kyle", "Catwoman", "201-875-2314",
            "101 Wayne Manor Rd; Gotham City, NJ 07017", "05-JUN-2008",
            "05-DEC-1981", "F", "SPOUSE", "WIFE",
            relationshipKey(BCD123, CDE456)},
        {EFG789.getRecordId(), "Barry Allen", "The Flash", "330-982-2133",
            "1201 Main Street; Star City, OH 44308", "07-NOV-2014",
            "04-MAR-1986", "M", "SPOUSE", "HUSBAND",
            relationshipKey(EFG789, FGH012)},
        {FGH012.getRecordId(), "Iris West-Allen", "", "330-675-1231",
            "1201 Main Street; Star City, OH 44308", "07-NOV-2014",
            "14-MAY-1986", "F", "SPOUSE", "WIFE",
            relationshipKey(EFG789, FGH012)}
    };

    return this.prepareJsonFile("test-marriages-", headers, spouses);
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

  @Test
  public void getRecordTest() {
    this.performTest(() -> {
      final String dataSource = ABC123.getDataSourceCode();
      final String recordId = ABC123.getRecordId();

      String uriText = this.formatServerUri(
          "data-sources/" + dataSource + "/records/" + recordId);
      UriInfo uriInfo = this.newProxyUriInfo(uriText);
      long before = System.currentTimeMillis();
      SzRecordResponse response = this.entityDataServices.getRecord(
          dataSource, recordId, false, uriInfo);
      response.concludeTimers();
      long after = System.currentTimeMillis();

      validateRecordResponse(
          response,
          GET,
          uriText,
          dataSource,
          recordId,
          Collections.singleton("Schmoe Joe"),
          Collections.singleton("101 Main Street, Las Vegas, NV 89101"),
          Collections.singleton("702-555-1212"),
          null,
          Collections.singleton("DOB: 12-JAN-1981"),
          null,
          null,
          before,
          after,
          null);
    });
  }

  @Test
  public void getRecordTestViaHttp() {
    this.performTest(() -> {
      final String dataSource = DEF456.getDataSourceCode();
      final String recordId = DEF456.getRecordId();

      String uriText = this.formatServerUri(
          "data-sources/" + dataSource + "/records/" + recordId);

      long before = System.currentTimeMillis();
      SzRecordResponse response = this.invokeServerViaHttp(
          GET, uriText, SzRecordResponse.class);
      response.concludeTimers();
      long after = System.currentTimeMillis();

      validateRecordResponse(
          response,
          GET,
          uriText,
          dataSource,
          recordId,
          Collections.singleton("Smith Joanne"),
          Collections.singleton("101 Fifth Ave, Las Vegas, NV 10018"),
          Collections.singleton("212-555-1212"),
          null,
          Collections.singleton("DOB: 15-MAY-1983"),
          null,
          null,
          before,
          after,
          null);
    });
  }

  @Test
  public void getRecordWithRawTest() {
    this.performTest(() -> {
      final String dataSource = GHI789.getDataSourceCode();
      final String recordId = GHI789.getRecordId();

      String uriText = this.formatServerUri(
          "data-sources/" + dataSource + "/records/" + recordId
              + "?withRaw=true");
      UriInfo uriInfo = this.newProxyUriInfo(uriText);
      long before = System.currentTimeMillis();
      SzRecordResponse response = this.entityDataServices.getRecord(
          dataSource, recordId, true, uriInfo);
      response.concludeTimers();
      long after = System.currentTimeMillis();

      validateRecordResponse(
          response,
          GET,
          uriText,
          dataSource,
          recordId,
          Collections.singleton("Doe John"),
          Collections.singleton("100 Main Street, Los Angeles, CA 90012"),
          Collections.singleton("818-555-1313"),
          null,
          Collections.singleton("DOB: 17-OCT-1978"),
          null,
          null,
          before,
          after,
          true);
    });
  }

  @Test
  public void getRecordWithRawTestViaHttp() {
    this.performTest(() -> {
      final String dataSource = JKL012.getDataSourceCode();
      final String recordId = JKL012.getRecordId();

      String uriText = this.formatServerUri(
          "data-sources/" + dataSource + "/records/" + recordId
              + "?withRaw=true");

      long before = System.currentTimeMillis();
      SzRecordResponse response = this.invokeServerViaHttp(
          GET, uriText, SzRecordResponse.class);
      response.concludeTimers();
      long after = System.currentTimeMillis();

      validateRecordResponse(
          response,
          GET,
          uriText,
          dataSource,
          recordId,
          Collections.singleton("Doe Jane"),
          Collections.singleton("100 Main Street, Los Angeles, CA 90012"),
          Collections.singleton("818-555-1212"),
          null,
          Collections.singleton("DOB: 5-FEB-1979"),
          null,
          null,
          before,
          after,
          true);
    });
  }

  @Test
  public void getRecordWithoutRawTest() {
    this.performTest(() -> {
      final String dataSource = MNO345.getDataSourceCode();
      final String recordId = MNO345.getRecordId();

      String uriText = this.formatServerUri(
          "data-sources/" + dataSource + "/records/" + recordId
              + "?withRaw=false");
      UriInfo uriInfo = this.newProxyUriInfo(uriText);
      long before = System.currentTimeMillis();
      SzRecordResponse response = this.entityDataServices.getRecord(
          dataSource, recordId, false, uriInfo);
      response.concludeTimers();
      long after = System.currentTimeMillis();

      validateRecordResponse(
          response,
          GET,
          uriText,
          dataSource,
          recordId,
          Collections.singleton("Schmoe Joseph"),
          Collections.singleton("101 Main Street, Las Vegas, NV 89101"),
          Collections.singleton("702-555-1212"),
          null,
          Collections.singleton("DOB: 12-JAN-1981"),
          null,
          Collections.singleton("MOTHERS_MAIDEN_NAME: WILSON"),
          before,
          after,
          false);
    });
  }

  @Test
  public void getRecordWithoutRawTestViaHttp() {
    this.performTest(() -> {
      final String dataSource = PQR678.getDataSourceCode();
      final String recordId = PQR678.getRecordId();

      String uriText = this.formatServerUri(
          "data-sources/" + dataSource + "/records/" + recordId
              + "?withRaw=false");

      long before = System.currentTimeMillis();
      SzRecordResponse response = this.invokeServerViaHttp(
          GET, uriText, SzRecordResponse.class);
      response.concludeTimers();
      long after = System.currentTimeMillis();

      validateRecordResponse(
          response,
          GET,
          uriText,
          dataSource,
          recordId,
          Collections.singleton("Smith Jo Anne"),
          Collections.singleton("101 Fifth Ave, Las Vegas, NV 10018"),
          Collections.singleton("212-555-1212"),
          null,
          Collections.singleton("DOB: 15-MAY-1983"),
          null,
          Collections.singleton("MOTHERS_MAIDEN_NAME: JACOBS"),
          before,
          after,
          false);
    });
  }


  @Test
  public void getRelatedRecordTest() {
    this.performTest(() -> {
      final String dataSource = BCD123.getDataSourceCode();
      final String recordId = BCD123.getRecordId();
      final String relKey = relationshipKey(BCD123, CDE456);

      String uriText = this.formatServerUri(
          "data-sources/" + dataSource + "/records/" + recordId);
      UriInfo uriInfo = this.newProxyUriInfo(uriText);
      long before = System.currentTimeMillis();
      SzRecordResponse response = this.entityDataServices.getRecord(
          dataSource, recordId, false, uriInfo);
      response.concludeTimers();
      long after = System.currentTimeMillis();

      validateRecordResponse(
          response,
          GET,
          uriText,
          dataSource,
          recordId,
          set("Bruce Wayne", "AKA: Batman"),
          Collections.singleton("101 Wayne Manor Rd; Gotham City, NJ 07017"),
          Collections.singleton("201-765-3451"),
          null,
          set("DOB: 08-SEP-1971", "GENDER: M"),
          Collections.singleton("REL_LINK: HUSBAND: SPOUSE " + relKey),
          null,
          before,
          after,
          null);
    });
  }

  @Test
  public void getRelatedRecordTestViaHttp() {
    this.performTest(() -> {
      final String dataSource = CDE456.getDataSourceCode();
      final String recordId = CDE456.getRecordId();
      final String relKey = relationshipKey(BCD123, CDE456);

      String uriText = this.formatServerUri(
          "data-sources/" + dataSource + "/records/" + recordId);

      long before = System.currentTimeMillis();
      SzRecordResponse response = this.invokeServerViaHttp(
          GET, uriText, SzRecordResponse.class);
      response.concludeTimers();
      long after = System.currentTimeMillis();

      validateRecordResponse(
          response,
          GET,
          uriText,
          dataSource,
          recordId,
          set("Selina Kyle", "AKA: Catwoman"),
          Collections.singleton("101 Wayne Manor Rd; Gotham City, NJ 07017"),
          Collections.singleton("201-875-2314"),
          null,
          set("DOB: 05-DEC-1981", "GENDER: F"),
          Collections.singleton("REL_LINK: WIFE: SPOUSE " + relKey),
          null,
          before,
          after,
          null);
    });
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

  private List<List> joeSchmoeEntityArgs() {
    final SzRecordId recordId1 = ABC123;
    final SzRecordId recordId2 = MNO345;

    Map<SzAttributeClass, Set<String>> expectedDataMap = new LinkedHashMap<>();
    expectedDataMap.put(NAME, set("Joseph Schmoe"));
    expectedDataMap.put(ADDRESS, set("101 Main Street, Las Vegas, NV 89101"));
    expectedDataMap.put(PHONE, set("702-555-1212"));
    expectedDataMap.put(CHARACTERISTIC, set("DOB: 1981-01-12"));

    Set<String> expectedOtherData = set("MOTHERS_MAIDEN_NAME: WILSON");

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
             null,
             null,
             expectedRecordCount,
             expectedRecordIds,
             expectedRelatedCount,
             expectedFeatureCounts,
             primaryFeatureValues,
             duplicateFeatureValues,
             expectedDataMap,
             expectedOtherData));
    result.add(
        list(recordId2,
             null,
             null,
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
             expectedDataMap,
             expectedOtherData));
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

    Set<String> expectedOtherData = set("MOTHERS_MAIDEN_NAME: JACOBS");

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
             null,
             null,
             expectedRecordCount,
             expectedRecordIds,
             expectedRelatedCount,
             expectedFeatureCounts,
             primaryFeatureValues,
             duplicateFeatureValues,
             expectedDataMap,
             expectedOtherData));
    result.add(
        list(recordId2,
             null,
             null,
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
             expectedDataMap,
             expectedOtherData));
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

    Set<String> expectedOtherData = set("MOTHERS_MAIDEN_NAME: GREEN");

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
             null,
             null,
             expectedRecordCount,
             expectedRecordIds,
             expectedRelatedCount,
             expectedFeatureCounts,
             primaryFeatureValues,
             duplicateFeatureValues,
             expectedDataMap,
             expectedOtherData));
    result.add(
        list(recordId2,
             null,
             null,
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
             expectedDataMap,
             expectedOtherData));
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

    Set<String> expectedOtherData = set("MOTHERS_MAIDEN_NAME: GRAHAM");

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
             null,
             null,
             expectedRecordCount,
             expectedRecordIds,
             expectedRelatedCount,
             expectedFeatureCounts,
             primaryFeatureValues,
             duplicateFeatureValues,
             expectedDataMap,
             expectedOtherData));
    result.add(
        list(recordId2,
             null,
             null,
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
             expectedDataMap,
             expectedOtherData));
    return result;
  }

  private List<List> bruceWayneEntityArgs() {
    final SzRecordId recordId1 = BCD123;
    final SzRecordId recordId2 = CDE456;
    final String key = relationshipKey(recordId1, recordId2);
    Map<SzAttributeClass, Set<String>> expectedDataMap = new LinkedHashMap<>();
    expectedDataMap.put(NAME, set("Bruce Wayne", "AKA: Batman"));
    expectedDataMap.put(ADDRESS, set("101 Wayne Manor Rd; Gotham City, NJ 07017"));
    expectedDataMap.put(PHONE, set("201-765-3451"));
    expectedDataMap.put(CHARACTERISTIC, set("DOB: 1971-09-08", "GENDER: M"));
    expectedDataMap.put(RELATIONSHIP, set("REL_LINK: HUSBAND: SPOUSE " + key));

    Map<String, Set<String>> primaryFeatureValues = new LinkedHashMap<>();
    primaryFeatureValues.put("NAME", set("Bruce Wayne", "Batman"));
    Map<String, Set<String>> duplicateFeatureValues = null;

    final int expectedRecordCount = 1;
    final int expectedRelatedCount = 1;

    Map<String, Integer> expectedFeatureCounts = new LinkedHashMap<>();
    expectedFeatureCounts.put("NAME", 2);
    expectedFeatureCounts.put("DOB", 1);
    expectedFeatureCounts.put("ADDRESS", 1);
    expectedFeatureCounts.put("PHONE", 1);
    expectedFeatureCounts.put("GENDER", 1);
    expectedFeatureCounts.put("REL_LINK", 1);

    Set<SzRecordId> expectedRecordIds = Collections.singleton(recordId1);
    List<List> result = new ArrayList<>(1);
    result.add(
        list(recordId1,
             null,
             null,
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
             expectedDataMap,
             null));
    return result;
  }

  private List<List> selinaKyleEntityArgs() {
    final SzRecordId recordId1 = CDE456;
    final SzRecordId recordId2 = BCD123;
    final String key = relationshipKey(recordId1, recordId2);
    Map<SzAttributeClass, Set<String>> expectedDataMap = new LinkedHashMap<>();
    expectedDataMap.put(NAME, set("Selina Kyle", "AKA: Catwoman"));
    expectedDataMap.put(ADDRESS, set("101 Wayne Manor Rd; Gotham City, NJ 07017"));
    expectedDataMap.put(PHONE, set("201-875-2314"));
    expectedDataMap.put(CHARACTERISTIC, set("DOB: 1981-12-05", "GENDER: F"));
    expectedDataMap.put(RELATIONSHIP, set("REL_LINK: WIFE: SPOUSE " + key));

    Map<String, Set<String>> primaryFeatureValues = new LinkedHashMap<>();
    primaryFeatureValues.put("NAME", set("Selina Kyle", "Catwoman"));
    Map<String, Set<String>> duplicateFeatureValues = null;

    final int expectedRecordCount = 1;
    final int expectedRelatedCount = 1;

    Map<String, Integer> expectedFeatureCounts = new LinkedHashMap<>();
    expectedFeatureCounts.put("NAME", 2);
    expectedFeatureCounts.put("DOB", 1);
    expectedFeatureCounts.put("ADDRESS", 1);
    expectedFeatureCounts.put("PHONE", 1);
    expectedFeatureCounts.put("GENDER", 1);
    expectedFeatureCounts.put("REL_LINK", 1);

    Set<SzRecordId> expectedRecordIds = Collections.singleton(recordId1);
    List<List> result = new ArrayList<>(1);
    result.add(
        list(recordId1,
             null,
             null,
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
             expectedDataMap,
             null));
    return result;
  }

  private List<List> barryAllenEntityArgs() {
    final SzRecordId recordId1 = EFG789;
    final SzRecordId recordId2 = FGH012;
    final String key = relationshipKey(recordId1, recordId2);
    Map<SzAttributeClass, Set<String>> expectedDataMap = new LinkedHashMap<>();
    expectedDataMap.put(NAME, set("Barry Allen", "AKA: The Flash"));
    expectedDataMap.put(ADDRESS, set("1201 Main Street; Star City, OH 44308"));
    expectedDataMap.put(PHONE, set("330-982-2133"));
    expectedDataMap.put(CHARACTERISTIC, set("DOB: 1986-03-04", "GENDER: M"));
    expectedDataMap.put(RELATIONSHIP, set("REL_LINK: HUSBAND: SPOUSE " + key));

    Map<String, Set<String>> primaryFeatureValues = new LinkedHashMap<>();
    primaryFeatureValues.put("NAME", set("Barry Allen", "The Flash"));
    Map<String, Set<String>> duplicateFeatureValues = null;

    final int expectedRecordCount = 1;
    final int expectedRelatedCount = 1;

    Map<String, Integer> expectedFeatureCounts = new LinkedHashMap<>();
    expectedFeatureCounts.put("NAME", 2);
    expectedFeatureCounts.put("DOB", 1);
    expectedFeatureCounts.put("ADDRESS", 1);
    expectedFeatureCounts.put("PHONE", 1);
    expectedFeatureCounts.put("GENDER", 1);
    expectedFeatureCounts.put("REL_LINK", 1);

    Set<SzRecordId> expectedRecordIds = Collections.singleton(recordId1);
    List<List> result = new ArrayList<>(1);
    result.add(
        list(recordId1,
             null,
             null,
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
             expectedDataMap,
             null));
    return result;
  }

  private List<List> irisWestAllenEntityArgs() {
    final SzRecordId recordId1 = FGH012;
    final SzRecordId recordId2 = EFG789;
    final String key = relationshipKey(recordId1, recordId2);
    Map<SzAttributeClass, Set<String>> expectedDataMap = new LinkedHashMap<>();
    expectedDataMap.put(NAME, set("Iris West-Allen"));
    expectedDataMap.put(ADDRESS, set("1201 Main Street; Star City, OH 44308"));
    expectedDataMap.put(PHONE, set("330-675-1231"));
    expectedDataMap.put(CHARACTERISTIC, set("DOB: 1986-05-14", "GENDER: F"));
    expectedDataMap.put(RELATIONSHIP, set("REL_LINK: WIFE: SPOUSE " + key));

    Map<String, Set<String>> primaryFeatureValues = new LinkedHashMap<>();
    primaryFeatureValues.put("NAME", set("Iris West-Allen"));
    Map<String, Set<String>> duplicateFeatureValues = null;

    final int expectedRecordCount = 1;
    final int expectedRelatedCount = 1;

    Map<String, Integer> expectedFeatureCounts = new LinkedHashMap<>();
    expectedFeatureCounts.put("NAME", 1);
    expectedFeatureCounts.put("DOB", 1);
    expectedFeatureCounts.put("ADDRESS", 1);
    expectedFeatureCounts.put("PHONE", 1);
    expectedFeatureCounts.put("GENDER", 1);
    expectedFeatureCounts.put("REL_LINK", 1);

    Set<SzRecordId> expectedRecordIds = Collections.singleton(recordId1);
    List<List> result = new ArrayList<>(1);
    result.add(
        list(recordId1,
             null,
             null,
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
             expectedDataMap,
             null));
    return result;
  }

  private List<Arguments> getEntityParameters() {

    List<List> baseArgs = new LinkedList<>();
    baseArgs.addAll(joeSchmoeEntityArgs());
    baseArgs.addAll(joanneSmithEntityArgs());
    baseArgs.addAll(johnDoeEntityArgs());
    baseArgs.addAll(janeDoeEntityArgs());
    baseArgs.addAll(bruceWayneEntityArgs());
    baseArgs.addAll(selinaKyleEntityArgs());
    baseArgs.addAll(barryAllenEntityArgs());
    baseArgs.addAll(irisWestAllenEntityArgs());

    List<Arguments> result = new LinkedList<>();

    Boolean[] booleanVariants = {null, true, false};
    List<SzFeatureMode> featureModes = new LinkedList<>();
    featureModes.add(null);
    for (SzFeatureMode featureMode : SzFeatureMode.values()) {
      featureModes.add(featureMode);
    }
    List<SzRelationshipMode> relationshipModes = new LinkedList<>();
    relationshipModes.add(null);
    for (SzRelationshipMode mode : SzRelationshipMode.values()) {
      relationshipModes.add(mode);
    }
    baseArgs.forEach(baseArgList -> {
      for (Boolean withRaw : booleanVariants) {
        for (Boolean forceMinimal : booleanVariants) {
          for (SzRelationshipMode withRelated : relationshipModes) {
            for (Boolean withFeatureStats : booleanVariants) {
              for (Boolean withDerivedFeatures : booleanVariants) {
                for (SzFeatureMode featureMode : featureModes) {
                  Object[] argArray = baseArgList.toArray();

                  argArray[1] = withRaw;
                  argArray[2] = withRelated;
                  argArray[3] = forceMinimal;
                  argArray[4] = featureMode;
                  argArray[5] = withFeatureStats;
                  argArray[6] = withDerivedFeatures;

                  result.add(arguments(argArray));
                }
              }
            }
          }
        }
      }
    });

    return result;

  }

  private StringBuilder buildEntityQueryString(
      StringBuilder       sb,
      Boolean             withRaw,
      SzRelationshipMode  withRelated,
      Boolean             forceMinimal,
      SzFeatureMode       featureMode,
      Boolean             withFeatureStats,
      Boolean             withDerivedFeatures)
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
    if (withFeatureStats != null) {
      sb.append(prefix).append("withFeatureStats=").append(withFeatureStats);
      prefix = "&";
    }
    if (withDerivedFeatures != null) {
      sb.append(prefix).append("withDerivedFeatures=")
          .append(withDerivedFeatures);
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
      SzRelationshipMode                  withRelated,
      Boolean                             forceMinimal,
      SzFeatureMode                       featureMode,
      Boolean                             withFeatureStats,
      Boolean                             withDerivedFeatures,
      Integer                             expectedRecordCount,
      Set<SzRecordId>                     expectedRecordIds,
      Integer                             relatedEntityCount,
      Map<String,Integer>                 expectedFeatureCounts,
      Map<String,Set<String>>             primaryFeatureValues,
      Map<String,Set<String>>             duplicateFeatureValues,
      Map<SzAttributeClass, Set<String>>  expectedDataValues,
      Set<String>                         expectedOtherDataValues)
  {
    this.performTest(() -> {
      String testInfo = "keyRecord=[ " + keyRecordId
          + " ], forceMinimal=[ " + forceMinimal
          + " ], featureMode=[ " + featureMode
          + " ], withFeatureStats=[ " + withFeatureStats
          + " ], withDerivedFeatures=[ " + withDerivedFeatures
          + " ], withRelated=[ " + withRelated
          + " ], withRaw=[ " + withRaw + " ]";

      StringBuilder sb = new StringBuilder();
      sb.append("data-sources/").append(keyRecordId.getDataSourceCode());
      sb.append("/records/").append(keyRecordId.getRecordId()).append("/entity");
      buildEntityQueryString(sb,
                             withRaw,
                             withRelated,
                             forceMinimal,
                             featureMode,
                             withFeatureStats,
                             withDerivedFeatures);

      String uriText = this.formatServerUri(sb.toString());
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      long before = System.currentTimeMillis();

      SzEntityResponse response = this.entityDataServices.getEntityByRecordId(
          keyRecordId.getDataSourceCode(),
          keyRecordId.getRecordId(),
          (withRaw != null ? withRaw : false),
          (withRelated != null ? withRelated : PARTIAL),
          (forceMinimal != null ? forceMinimal : false),
          (featureMode != null ? featureMode : WITH_DUPLICATES),
          (withFeatureStats != null ? withFeatureStats : false),
          (withDerivedFeatures != null ? withDerivedFeatures : false),
          uriInfo);

      // TODO(barry): remove this extra code
      int flags = ServicesUtil.getFlags(
          (forceMinimal == null) ? false : forceMinimal,
          (featureMode != null ? featureMode : WITH_DUPLICATES),
          (withFeatureStats != null ? withFeatureStats : false),
          (withDerivedFeatures != null ? withDerivedFeatures : false),
          withRelated != SzRelationshipMode.NONE);

      try {
        ObjectMapper mapper = new ObjectMapper();
        String rawJsonText = mapper.writeValueAsString(response.getRawData());
        testInfo = testInfo + ", flags=[ " + flags + " ], featureFlag=[ "
            + (flags & G2Engine.G2_ENTITY_INCLUDE_REPRESENTATIVE_FEATURES)
            + " ], rawData=[ " + rawJsonText + " ], internalFeaturesFlag=[ "
            + (flags & G2Engine.G2_ENTITY_OPTION_INCLUDE_INTERNAL_FEATURES)
            + " ]";
      } catch (Exception e) {
        throw new RuntimeException(e);
      }

      response.concludeTimers();
      long after = System.currentTimeMillis();

      validateEntityResponse(
          testInfo,
          response,
          GET,
          uriText,
          withRaw,
          withRelated,
          forceMinimal,
          featureMode,
          withFeatureStats == null ? false : withFeatureStats,
          withDerivedFeatures == null ? false : withDerivedFeatures,
          expectedRecordCount,
          expectedRecordIds,
          relatedEntityCount,
          expectedFeatureCounts,
          primaryFeatureValues,
          duplicateFeatureValues,
          expectedDataValues,
          expectedOtherDataValues,
          before,
          after);
    });
  }

  @ParameterizedTest
  @MethodSource("getEntityParameters")
  public void getEntityByRecordIdTestViaHttp(
      SzRecordId                          keyRecordId,
      Boolean                             withRaw,
      SzRelationshipMode                  withRelated,
      Boolean                             forceMinimal,
      SzFeatureMode                       featureMode,
      Boolean                             withFeatureStats,
      Boolean                             withDerivedFeatures,
      Integer                             expectedRecordCount,
      Set<SzRecordId>                     expectedRecordIds,
      Integer                             relatedEntityCount,
      Map<String,Integer>                 expectedFeatureCounts,
      Map<String,Set<String>>             primaryFeatureValues,
      Map<String,Set<String>>             duplicateFeatureValues,
      Map<SzAttributeClass, Set<String>>  expectedDataValues,
      Set<String>                         expectedOtherDataValues)
  {
    this.performTest(() -> {
      String testInfo = "keyRecord=[ " + keyRecordId
          + " ], forceMinimal=[ " + forceMinimal
          + " ], featureMode=[ " + featureMode
          + " ], withFeatureStats=[ " + withFeatureStats
          + " ], withDerivedFeatures=[ " + withDerivedFeatures
          + " ], withRelated=[ " + withRelated
          + " ], withRaw=[ " + withRaw + " ]";

      StringBuilder sb = new StringBuilder();
      sb.append("data-sources/").append(keyRecordId.getDataSourceCode());
      sb.append("/records/").append(keyRecordId.getRecordId()).append("/entity");
      buildEntityQueryString(sb,
                             withRaw,
                             withRelated,
                             forceMinimal,
                             featureMode,
                             withFeatureStats,
                             withDerivedFeatures);

      String uriText = this.formatServerUri(sb.toString());

      long before = System.currentTimeMillis();
      SzEntityResponse response = this.invokeServerViaHttp(
          GET, uriText, SzEntityResponse.class);
      response.concludeTimers();
      long after = System.currentTimeMillis();

      validateEntityResponse(
          testInfo,
          response,
          GET,
          uriText,
          withRaw,
          withRelated,
          forceMinimal,
          featureMode,
          withFeatureStats == null ? false : withFeatureStats,
          withDerivedFeatures == null ? false : withDerivedFeatures,
          expectedRecordCount,
          expectedRecordIds,
          relatedEntityCount,
          expectedFeatureCounts,
          primaryFeatureValues,
          duplicateFeatureValues,
          expectedDataValues,
          expectedOtherDataValues,
          before,
          after);
    });
  }

  @Test
  public void getNotFoundEntityByBadRecordIdTest()
  {
    this.performTest(() -> {
      final String badRecordId = "ABC123DEF456GHI789";
      StringBuilder sb = new StringBuilder();
      sb.append("data-sources/").append(PASSENGERS);
      sb.append("/records/").append(badRecordId).append("/entity");

      String uriText = this.formatServerUri(sb.toString());
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      long before = System.currentTimeMillis();

      try {
        this.entityDataServices.getEntityByRecordId(
            PASSENGERS,
            badRecordId,
            false,
            PARTIAL,
            false,
            WITH_DUPLICATES,
            false,
            false,
            uriInfo);

        fail("Expected entity for data source \"" + PASSENGERS
                 + "\" and record ID \"" + badRecordId + "\" to NOT be found");
      } catch (NotFoundException expected) {
        SzErrorResponse response
            = (SzErrorResponse) expected.getResponse().getEntity();
        response.concludeTimers();
        long after = System.currentTimeMillis();

        validateBasics(
            response, 404, GET, uriText, before, after);
      }
    });
  }

  @Test
  public void getNotFoundEntityByBadDataSourceTest()
  {
    this.performTest(() -> {
      final String badDataSource = "FOOBAR";
      final String badRecordId = "ABC123DEF456GHI789";
      StringBuilder sb = new StringBuilder();
      sb.append("data-sources/").append(badDataSource);
      sb.append("/records/").append(badRecordId).append("/entity");

      String uriText = this.formatServerUri(sb.toString());
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      long before = System.currentTimeMillis();

      try {
        this.entityDataServices.getEntityByRecordId(
            PASSENGERS,
            badRecordId,
            false,
            PARTIAL,
            false,
            WITH_DUPLICATES,
            false,
            false,
            uriInfo);

        fail("Expected entity for data source \"" + badDataSource
                 + "\" and record ID \"" + badRecordId + "\" to NOT be found");
      } catch (NotFoundException expected) {
        SzErrorResponse response
            = (SzErrorResponse) expected.getResponse().getEntity();
        response.concludeTimers();
        long after = System.currentTimeMillis();

        validateBasics(
            response, 404, GET, uriText, before, after);
      }
    });
  }

  @Test
  public void getNotFoundEntityByBadRecordIdTestViaHttp()
  {
    this.performTest(() -> {
      final String badRecordId = "ABC123DEF456GHI789";
      StringBuilder sb = new StringBuilder();
      sb.append("data-sources/").append(PASSENGERS);
      sb.append("/records/").append(badRecordId).append("/entity");

      String uriText = this.formatServerUri(sb.toString());

      long before = System.currentTimeMillis();
      SzErrorResponse response = this.invokeServerViaHttp(
          GET, uriText, SzErrorResponse.class);
      response.concludeTimers();
      long after = System.currentTimeMillis();

      validateBasics(
          response, 404, GET, uriText, before, after);
    });
  }

  @Test
  public void getNotFoundEntityByBadDataSourceTestViaHttp()
  {
    this.performTest(() -> {
      final String badDataSource = "FOOBAR";
      final String badRecordId = "ABC123DEF456GHI789";
      StringBuilder sb = new StringBuilder();
      sb.append("data-sources/").append(badDataSource);
      sb.append("/records/").append(badRecordId).append("/entity");

      String uriText = this.formatServerUri(sb.toString());

      long before = System.currentTimeMillis();
      SzErrorResponse response = this.invokeServerViaHttp(
          GET, uriText, SzErrorResponse.class);
      response.concludeTimers();
      long after = System.currentTimeMillis();

      validateBasics(
          response, 404, GET, uriText, before, after);
    });
  }

  @ParameterizedTest
  @MethodSource("getEntityParameters")
  public void getEntityByEntityIdTest(
      SzRecordId                          keyRecordId,
      Boolean                             withRaw,
      SzRelationshipMode                  withRelated,
      Boolean                             forceMinimal,
      SzFeatureMode                       featureMode,
      Boolean                             withFeatureStats,
      Boolean                             withDerivedFeatures,
      Integer                             expectedRecordCount,
      Set<SzRecordId>                     expectedRecordIds,
      Integer                             relatedEntityCount,
      Map<String,Integer>                 expectedFeatureCounts,
      Map<String,Set<String>>             primaryFeatureValues,
      Map<String,Set<String>>             duplicateFeatureValues,
      Map<SzAttributeClass, Set<String>>  expectedDataValues,
      Set<String>                         expectedOtherDataValues)
  {
    this.performTest(() -> {
      String testInfo = "keyRecord=[ " + keyRecordId
          + " ], forceMinimal=[ " + forceMinimal
          + " ], featureMode=[ " + featureMode
          + " ], withFeatureStats=[ " + withFeatureStats
          + " ], withDerivedFeatures=[ " + withDerivedFeatures
          + " ], withRelated=[ " + withRelated
          + " ], withRaw=[ " + withRaw + " ]";

      final Long entityId = this.getEntityIdForRecordId(keyRecordId);

      StringBuilder sb = new StringBuilder();
      sb.append("entities/").append(entityId);
      buildEntityQueryString(sb,
                             withRaw,
                             withRelated,
                             forceMinimal,
                             featureMode,
                             withFeatureStats,
                             withDerivedFeatures);

      String uriText = this.formatServerUri(sb.toString());
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      long before = System.currentTimeMillis();

      SzEntityResponse response = this.entityDataServices.getEntityByEntityId(
          entityId,
          (withRaw != null ? withRaw : false),
          (withRelated == null ? PARTIAL : withRelated),
          (forceMinimal != null ? forceMinimal : false),
          (featureMode != null ? featureMode : WITH_DUPLICATES),
          (withFeatureStats != null ? withFeatureStats : false),
          (withDerivedFeatures != null ? withDerivedFeatures : false),
          uriInfo);

      response.concludeTimers();
      long after = System.currentTimeMillis();

      validateEntityResponse(
          testInfo,
          response,
          GET,
          uriText,
          withRaw,
          withRelated,
          forceMinimal,
          featureMode,
          withFeatureStats == null ? false : withFeatureStats,
          withDerivedFeatures == null ? false : withDerivedFeatures,
          expectedRecordCount,
          expectedRecordIds,
          relatedEntityCount,
          expectedFeatureCounts,
          primaryFeatureValues,
          duplicateFeatureValues,
          expectedDataValues,
          expectedOtherDataValues,
          before,
          after);
    });
  }

  @ParameterizedTest
  @MethodSource("getEntityParameters")
  public void getEntityByEntityIdTestViaHttp(
      SzRecordId                          keyRecordId,
      Boolean                             withRaw,
      SzRelationshipMode                  withRelated,
      Boolean                             forceMinimal,
      SzFeatureMode                       featureMode,
      Boolean                             withFeatureStats,
      Boolean                             withDerivedFeatures,
      Integer                             expectedRecordCount,
      Set<SzRecordId>                     expectedRecordIds,
      Integer                             relatedEntityCount,
      Map<String,Integer>                 expectedFeatureCounts,
      Map<String,Set<String>>             primaryFeatureValues,
      Map<String,Set<String>>             duplicateFeatureValues,
      Map<SzAttributeClass, Set<String>>  expectedDataValues,
      Set<String>                         expectedOtherDataValues)
  {
    this.performTest(() -> {
      String testInfo = "keyRecord=[ " + keyRecordId
          + " ], forceMinimal=[ " + forceMinimal
          + " ], featureMode=[ " + featureMode
          + " ], withFeatureStats=[ " + withFeatureStats
          + " ], withDerivedFeatures=[ " + withDerivedFeatures
          + " ], withRelated=[ " + withRelated
          + " ], withRaw=[ " + withRaw + " ]";

      final Long entityId = this.getEntityIdForRecordId(keyRecordId);

      StringBuilder sb = new StringBuilder();
      sb.append("entities/").append(entityId);
      buildEntityQueryString(sb,
                             withRaw,
                             withRelated,
                             forceMinimal,
                             featureMode,
                             withFeatureStats,
                             withDerivedFeatures);

      String uriText = this.formatServerUri(sb.toString());

      long before = System.currentTimeMillis();
      SzEntityResponse response = this.invokeServerViaHttp(
          GET, uriText, SzEntityResponse.class);
      response.concludeTimers();
      long after = System.currentTimeMillis();

      validateEntityResponse(
          testInfo,
          response,
          GET,
          uriText,
          withRaw,
          withRelated,
          forceMinimal,
          featureMode,
          withFeatureStats == null ? false : withFeatureStats,
          withDerivedFeatures == null ? false : withDerivedFeatures,
          expectedRecordCount,
          expectedRecordIds,
          relatedEntityCount,
          expectedFeatureCounts,
          primaryFeatureValues,
          duplicateFeatureValues,
          expectedDataValues,
          expectedOtherDataValues,
          before,
          after);
    });
  }


  @Test
  public void getNotFoundEntityByBadEntityIdTest()
  {
    this.performTest(() -> {
      final long badEntityId = Long.MAX_VALUE;

      String uriText = this.formatServerUri("entities/" + badEntityId);
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      long before = System.currentTimeMillis();

      try {
        this.entityDataServices.getEntityByEntityId(
            badEntityId,
            false,
            SzRelationshipMode.NONE,
            false,
            WITH_DUPLICATES,
            false,
            false,
            uriInfo);

        fail("Expected entity for entity ID " + badEntityId + " to NOT be found");

      } catch (NotFoundException expected) {
        SzErrorResponse response
            = (SzErrorResponse) expected.getResponse().getEntity();
        response.concludeTimers();
        long after = System.currentTimeMillis();

        validateBasics(
            response, 404, GET, uriText, before, after);
      }
    });
  }

  @Test
  public void getNotFoundEntityByBadEntityIdTestViaHttp()
  {
    this.performTest(() -> {
      final long badEntityId = Long.MAX_VALUE;

      String uriText = this.formatServerUri("entities/" + badEntityId);

      long before = System.currentTimeMillis();
      SzErrorResponse response = this.invokeServerViaHttp(
          GET, uriText, SzErrorResponse.class);
      response.concludeTimers();
      long after = System.currentTimeMillis();

      validateBasics(
          response, 404, GET, uriText, before, after);

    });
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
    List<SzFeatureMode> featureModes = new LinkedList<>();
    featureModes.add(null);
    for (SzFeatureMode featureMode : SzFeatureMode.values()) {
      featureModes.add(featureMode);
    }
    boolean[] trueFalse = { true, false };

    searchCountMap.entrySet().forEach(entry -> {
      Map<String, Set<String>> criteria = entry.getKey();
      Integer resultCount = entry.getValue();

      for (Boolean withRaw : booleanVariants) {
        for (Boolean forceMinimal : booleanVariants) {
          for (Boolean withRelationships : booleanVariants) {
            for (SzFeatureMode featureMode : featureModes) {
              for (Boolean withFeatureStats: booleanVariants) {
                for (Boolean withDerivedFeatures : booleanVariants) {
                  list.add(arguments(criteria,
                                     resultCount,
                                     forceMinimal,
                                     featureMode,
                                     withFeatureStats,
                                     withDerivedFeatures,
                                     withRelationships,
                                     withRaw));
                }
              }
            }
          }
        }
      }
    });

    return list;
  }

  @ParameterizedTest
  @MethodSource("searchParameters")
  public void searchByJsonAttrsTest(Map<String, Set<String>>  criteria,
                                    Integer                   expectedCount,
                                    Boolean                   forceMinimal,
                                    SzFeatureMode             featureMode,
                                    Boolean                   withFeatureStats,
                                    Boolean                   withDerivedFeatures,
                                    Boolean                   withRelationships,
                                    Boolean                   withRaw)
  {
    this.performTest(() -> {
      String testInfo = "criteria=[ " + criteria
          + " ], forceMinimal=[ " + forceMinimal
          + " ], featureMode=[ " + featureMode
          + " ], withFeatureStats=[ " + withFeatureStats
          + " ], withDerivedFeatures=[ " + withDerivedFeatures
          + " ], withRelationships=[ " + withRelationships
          + " ], withRaw=[ " + withRaw + " ]";

      JsonObjectBuilder job = Json.createObjectBuilder();
      criteria.entrySet().forEach(entry -> {
        String key = entry.getKey();
        Set<String> values = entry.getValue();
        if (values.size() == 0) return;
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

      String uriText = this.formatServerUri(
          "entities?attrs=" + urlEncode(attrs));
      if (forceMinimal != null) {
        uriText += ("&forceMinimal=" + forceMinimal);
      }
      if (featureMode != null) {
        uriText += ("&featureMode=" + featureMode);
      }
      if (withFeatureStats != null) {
        uriText += ("&withFeatureStats=" + withFeatureStats);
      }
      if (withDerivedFeatures != null) {
        uriText += ("&withDerivedFeatures=" + withDerivedFeatures);
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
          null,
          (forceMinimal != null ? forceMinimal : false),
          (featureMode != null ? featureMode : WITH_DUPLICATES),
          (withFeatureStats != null ? withFeatureStats : false),
          (withDerivedFeatures != null ? withDerivedFeatures : false),
          (withRelationships != null ? withRelationships : false),
          (withRaw != null ? withRaw : false),
          uriInfo);

      response.concludeTimers();
      long after = System.currentTimeMillis();

      // TODO(barry): remove this extra code
      int flags = ServicesUtil.getFlags(
          (forceMinimal == null) ? false : forceMinimal,
          (featureMode != null ? featureMode : WITH_DUPLICATES),
          (withFeatureStats != null ? withFeatureStats : false),
          (withDerivedFeatures != null ? withDerivedFeatures : false),
          (withRelationships != null ? withRelationships : false));

      try {
        ObjectMapper mapper = new ObjectMapper();
        String rawJsonText = mapper.writeValueAsString(response.getRawData());
        testInfo = testInfo + ", flags=[ " + flags + " ], featureFlag=[ "
            + (flags & G2Engine.G2_ENTITY_INCLUDE_REPRESENTATIVE_FEATURES)
            + " ], rawData=[ " + rawJsonText + " ], internalFeaturesFlag=[ "
            + (flags & G2Engine.G2_ENTITY_OPTION_INCLUDE_INTERNAL_FEATURES)
            + " ]";
      } catch (Exception e) {
        throw new RuntimeException(e);
      }

      validateSearchResponse(
          testInfo,
          response,
          GET,
          uriText,
          expectedCount,
          withRelationships,
          forceMinimal,
          featureMode,
          withFeatureStats == null ? false : withFeatureStats,
          withDerivedFeatures == null ? false : withDerivedFeatures,
          before,
          after,
          withRaw);

    });
  }

  @ParameterizedTest
  @MethodSource("searchParameters")
  public void searchByJsonAttrsTestViaHttp(
      Map<String, Set<String>>  criteria,
      Integer                   expectedCount,
      Boolean                   forceMinimal,
      SzFeatureMode             featureMode,
      Boolean                   withFeatureStats,
      Boolean                   withDerivedFeatures,
      Boolean                   withRelationships,
      Boolean                   withRaw)
  {
    this.performTest(() -> {
      String testInfo = "criteria=[ " + criteria
          + " ], forceMinimal=[ " + forceMinimal
          + " ], featureMode=[ " + featureMode
          + " ], withFeatureStats=[ " + withFeatureStats
          + " ], withDerivedFeatures=[ " + withDerivedFeatures
          + " ], withRelationships=[ " + withRelationships
          + " ], withRaw=[ " + withRaw + " ]";

      JsonObjectBuilder job = Json.createObjectBuilder();
      criteria.entrySet().forEach(entry -> {
        String key = entry.getKey();
        Set<String> values = entry.getValue();
        if (values.size() == 0) return;
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

      StringBuilder sb = new StringBuilder();
      sb.append("entities?attrs=").append(urlEncode(attrs));
      if (forceMinimal != null) {
        sb.append("&forceMinimal=").append(forceMinimal);
      }
      if (featureMode != null) {
        sb.append("&featureMode=").append(featureMode);
      }
      if (withFeatureStats != null) {
        sb.append("&withFeatureStats=").append(withFeatureStats);
      }
      if (withDerivedFeatures != null) {
        sb.append("&withDerivedFeatures=").append(withDerivedFeatures);
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

      validateSearchResponse(
          testInfo,
          response,
          GET,
          uriText,
          expectedCount,
          withRelationships,
          forceMinimal,
          featureMode,
          withFeatureStats == null ? false : withFeatureStats,
          withDerivedFeatures == null ? false : withDerivedFeatures,
          before,
          after,
          withRaw);
    });
  }

  @ParameterizedTest
  @MethodSource("searchParameters")
  public void searchByParamAttrsTest(
      Map<String, Set<String>>  criteria,
      Integer                   expectedCount,
      Boolean                   forceMinimal,
      SzFeatureMode             featureMode,
      Boolean                   withFeatureStats,
      Boolean                   withDerivedFeatures,
      Boolean                   withRelationships,
      Boolean                   withRaw)
  {
    this.performTest(() -> {
      String testInfo = "criteria=[ " + criteria
          + " ], forceMinimal=[ " + forceMinimal
          + " ], featureMode=[ " + featureMode
          + " ], withFeatureStats=[ " + withFeatureStats
          + " ], withDerivedFeatures=[ " + withDerivedFeatures
          + " ], withRelationships=[ " + withRelationships
          + " ], withRaw=[ " + withRaw + " ]";

      StringBuffer sb = new StringBuffer();
      List<String> attrList = new LinkedList<>();
      criteria.entrySet().forEach(entry -> {
        String key = entry.getKey();
        Set<String> values = entry.getValue();
        for (String value : values) {
          attrList.add(key + ":" + value);
          String encodedVal = urlEncode(key + ":" + value);
          sb.append("&attr=").append(encodedVal);
        }
      });

      sb.setCharAt(0, '?');

      if (forceMinimal != null) {
        sb.append("&forceMinimal=").append(forceMinimal);
      }
      if (featureMode != null) {
        sb.append("&featureMode=").append(featureMode);
      }
      if (withFeatureStats != null) {
        sb.append("&withFeatureStats=").append(withFeatureStats);
      }
      if (withDerivedFeatures != null) {
        sb.append("&withDerivedFeatures=").append(withDerivedFeatures);
      }
      if (withRelationships != null) {
        sb.append("&withRelationships=").append(withRelationships);
      }
      if (withRaw != null) {
        sb.append("&withRaw=").append(withRaw);
      }

      String uriText = this.formatServerUri(
          "entities" + sb.toString());

      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      long before = System.currentTimeMillis();
      SzAttributeSearchResponse response
          = this.entityDataServices.searchByAttributes(
          null,
          attrList,
          (forceMinimal != null ? forceMinimal : false),
          (featureMode != null ? featureMode : WITH_DUPLICATES),
          (withFeatureStats != null ? withFeatureStats : false),
          (withDerivedFeatures != null ? withDerivedFeatures : false),
          (withRelationships != null ? withRelationships : true),
          (withRaw != null ? withRaw : false),
          uriInfo);

      response.concludeTimers();
      long after = System.currentTimeMillis();

      validateSearchResponse(
          testInfo,
          response,
          GET,
          uriText,
          expectedCount,
          withRelationships,
          forceMinimal,
          featureMode,
          withFeatureStats == null ? false : withFeatureStats,
          withDerivedFeatures == null ? false : withDerivedFeatures,
          before,
          after,
          withRaw);

    });
  }

  @ParameterizedTest
  @MethodSource("searchParameters")
  public void searchByParamAttrsTestViaHttp(
      Map<String, Set<String>>  criteria,
      Integer                   expectedCount,
      Boolean                   forceMinimal,
      SzFeatureMode             featureMode,
      Boolean                   withFeatureStats,
      Boolean                   withDerivedFeatures,
      Boolean                   withRelationships,
      Boolean                   withRaw)
  {
    this.performTest(() -> {
      String testInfo = "criteria=[ " + criteria
          + " ], forceMinimal=[ " + forceMinimal
          + " ], featureMode=[ " + featureMode
          + " ], withFeatureStats=[ " + withFeatureStats
          + " ], withDerivedFeatures=[ " + withDerivedFeatures
          + " ], withRelationships=[ " + withRelationships
          + " ], withRaw=[ " + withRaw + " ]";

      StringBuilder sb = new StringBuilder(criteria.size() * 50);
      criteria.entrySet().forEach(entry -> {
        String key = entry.getKey();
        Set<String> values = entry.getValue();
        for (String value : values) {
          String encodedVal = urlEncode(key + ":" + value);
          sb.append("&attr=").append(encodedVal);
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
      if (withFeatureStats != null) {
        sb.append("&withFeatureStats=").append(withFeatureStats);
      }
      if (withDerivedFeatures != null) {
        sb.append("&withDerivedFeatures=").append(withDerivedFeatures);
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

      validateSearchResponse(
          testInfo,
          response,
          GET,
          uriText,
          expectedCount,
          withRelationships,
          forceMinimal,
          featureMode,
          withFeatureStats == null ? false : withFeatureStats,
          withDerivedFeatures == null ? false : withDerivedFeatures,
          before,
          after,
          withRaw);
    });
  }
}
