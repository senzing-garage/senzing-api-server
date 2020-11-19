package com.senzing.api.services;

import com.senzing.api.model.*;
import com.senzing.gen.api.invoker.ApiClient;
import com.senzing.gen.api.services.EntityDataApi;
import com.senzing.repomgr.RepositoryManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObjectBuilder;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.UriInfo;
import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.sql.ClientInfoStatus;
import java.util.*;
import java.util.concurrent.CountDownLatch;

import static com.senzing.api.model.SzFeatureMode.NONE;
import static com.senzing.api.model.SzFeatureMode.WITH_DUPLICATES;
import static com.senzing.api.model.SzHttpMethod.GET;
import static com.senzing.api.services.ResponseValidators.*;
import static java.lang.Enum.valueOf;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.TestInstance.Lifecycle;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@TestInstance(Lifecycle.PER_CLASS)
public class WhyServicesTest extends AbstractServiceTest {
  private static final String PASSENGERS = "PASSENGERS";
  private static final String CUSTOMERS = "CUSTOMERS";
  private static final String VIPS = "VIPS";

  private static final String COMPANIES = "COMPANIES";
  private static final String EMPLOYEES = "EMPLOYEES";
  private static final String CONTACTS = "CONTACTS";

  private static final SzRecordId ABC123 = new SzRecordId(PASSENGERS,
                                                          "ABC123");
  private static final SzRecordId DEF456 = new SzRecordId(PASSENGERS,
                                                          "DEF456");
  private static final SzRecordId GHI789 = new SzRecordId(PASSENGERS,
                                                          "GHI789");
  private static final SzRecordId JKL012 = new SzRecordId(PASSENGERS,
                                                          "JKL012");
  private static final SzRecordId MNO345 = new SzRecordId(CUSTOMERS,
                                                          "MNO345");
  private static final SzRecordId PQR678 = new SzRecordId(CUSTOMERS,
                                                          "PQR678");
  private static final SzRecordId ABC567 = new SzRecordId(CUSTOMERS,
                                                          "ABC567");
  private static final SzRecordId DEF890 = new SzRecordId(CUSTOMERS,
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

  private static final SzRecordId COMPANY_1 = new SzRecordId(COMPANIES,
                                                             "COMPANY_1");
  private static final SzRecordId COMPANY_2 = new SzRecordId(COMPANIES,
                                                             "COMPANY_2");
  private static final SzRecordId EMPLOYEE_1 = new SzRecordId(EMPLOYEES,
                                                              "EMPLOYEE_1");
  private static final SzRecordId EMPLOYEE_2 = new SzRecordId(EMPLOYEES,
                                                              "EMPLOYEE_2");
  private static final SzRecordId EMPLOYEE_3 = new SzRecordId(EMPLOYEES,
                                                              "EMPLOYEE_3");
  private static final SzRecordId CONTACT_1 = new SzRecordId(CONTACTS,
                                                             "CONTACT_1");
  private static final SzRecordId CONTACT_2 = new SzRecordId(CONTACTS,
                                                             "CONTACT_2");
  private static final SzRecordId CONTACT_3 = new SzRecordId(CONTACTS,
                                                             "CONTACT_3");
  private static final SzRecordId CONTACT_4 = new SzRecordId(CONTACTS,
                                                             "CONTACT_4");

  private static final List<SzRecordId> RELATED_RECORD_IDS;

  static {
    List<SzRecordId> recordIds = new ArrayList<>(12);
    List<SzRecordId> relatedIds = new ArrayList<>(9);

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

      relatedIds.add(COMPANY_1);
      relatedIds.add(COMPANY_2);
      relatedIds.add(EMPLOYEE_1);
      relatedIds.add(EMPLOYEE_2);
      relatedIds.add(EMPLOYEE_3);
      relatedIds.add(CONTACT_1);
      relatedIds.add(CONTACT_2);
      relatedIds.add(CONTACT_3);
      relatedIds.add(CONTACT_4);

    } catch (Exception e) {
      e.printStackTrace();
      throw new ExceptionInInitializerError(e);

    } finally {
      RECORD_IDS = Collections.unmodifiableList(recordIds);
      RELATED_RECORD_IDS = Collections.unmodifiableList(relatedIds);
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
    dataSources.add("CUSTOMERS");
    dataSources.add("VIPS");
    dataSources.add("COMPANIES");
    dataSources.add("EMPLOYEES");
    dataSources.add("CONTACTS");

    File passengerFile = this.preparePassengerFile();
    File customerFile = this.prepareCustomerFile();
    File vipFile = this.prepareVipFile();

    File companyFile = this.prepareCompanyFile();
    File employeeFile = this.prepareEmployeeFile();
    File contactFile = this.prepareContactFile();

    customerFile.deleteOnExit();
    passengerFile.deleteOnExit();
    vipFile.deleteOnExit();
    companyFile.deleteOnExit();
    employeeFile.deleteOnExit();
    contactFile.deleteOnExit();

    RepositoryManager.configSources(repoDirectory,
                                    dataSources,
                                    true);

    RepositoryManager.loadFile(repoDirectory,
                               passengerFile,
                               PASSENGERS,
                               null,
                               true);

    RepositoryManager.loadFile(repoDirectory,
                               customerFile,
                               CUSTOMERS,
                               null,
                               true);

    RepositoryManager.loadFile(repoDirectory,
                               vipFile,
                               VIPS,
                               null,
                               true);

    RepositoryManager.loadFile(repoDirectory,
                               companyFile,
                               COMPANIES,
                               null,
                               true);

    RepositoryManager.loadFile(repoDirectory,
                               employeeFile,
                               EMPLOYEES,
                               null,
                               true);

    RepositoryManager.loadFile(repoDirectory,
                               contactFile,
                               CONTACTS,
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

  private File prepareCustomerFile() {
    String[] headers = {
        "RECORD_ID", "NAME_FIRST", "NAME_LAST", "MOBILE_PHONE_NUMBER",
        "HOME_PHONE_NUMBER", "ADDR_FULL", "DATE_OF_BIRTH"};

    String[][] customers = {
        {MNO345.getRecordId(), "Bill", "Wright", "702-444-2121", "702-123-4567",
            "101 Main Street, Las Vegas, NV 89101", "22-AUG-1981"},
        {PQR678.getRecordId(), "Craig", "Smith", "212-555-1212", "702-888-3939",
            "451 Dover Street, Las Vegas, NV 89108", "17-NOV-1982"},
        {ABC567.getRecordId(), "Kim", "Long", "702-246-8024", "702-135-7913",
            "451 Dover Street, Las Vegas, NV 89108", "24-OCT-1976"},
        {DEF890.getRecordId(), "Kathy", "Osborne", "702-444-2121", "702-111-2222",
            "707 Seventh Ave, Las Vegas, NV 89143", "27-JUL-1981"}
    };

    return this.prepareJsonArrayFile("test-customers-", headers, customers);
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

  private File prepareCompanyFile() {
    JsonArrayBuilder jab = Json.createArrayBuilder();
    JsonObjectBuilder job = Json.createObjectBuilder();
    job.add("RECORD_ID", COMPANY_1.getRecordId());
    job.add("DATA_SOURCE", COMPANY_1.getDataSourceCode());
    job.add("ENTITY_TYPE", "GENERIC");
    job.add("NAME_ORG", "Acme Corporation");
    JsonArrayBuilder relJab = Json.createArrayBuilder();
    JsonObjectBuilder relJob = Json.createObjectBuilder();
    relJob.add("REL_ANCHOR_DOMAIN", "EMPLOYER_ID");
    relJob.add("REL_ANCHOR_KEY", "ACME_CORP_KEY");
    relJab.add(relJob);
    relJob = Json.createObjectBuilder();
    relJob.add("REL_ANCHOR_DOMAIN", "CORP_HIERARCHY");
    relJob.add("REL_ANCHOR_KEY", "ACME_CORP_KEY");
    relJab.add(relJob);
    relJob = Json.createObjectBuilder();
    relJob.add("REL_POINTER_DOMAIN", "CORP_HIERARCHY");
    relJob.add("REL_POINTER_KEY", "COYOTE_SOLUTIONS_KEY");
    relJob.add("REL_POINTER_ROLE", "ULTIMATE_PARENT");
    relJab.add(relJob);
    relJob = Json.createObjectBuilder();
    relJob.add("REL_POINTER_DOMAIN", "CORP_HIERARCHY");
    relJob.add("REL_POINTER_KEY", "COYOTE_SOLUTIONS_KEY");
    relJob.add("REL_POINTER_ROLE", "PARENT");
    relJab.add(relJob);
    job.add("RELATIONSHIP_LIST", relJab);

    jab.add(job);
    job = Json.createObjectBuilder();
    job.add("RECORD_ID", COMPANY_2.getRecordId());
    job.add("DATA_SOURCE", COMPANY_2.getDataSourceCode());
    job.add("ENTITY_TYPE", "GENERIC");
    job.add("NAME_ORG", "Coyote Solutions");
    relJab = Json.createArrayBuilder();
    relJob = Json.createObjectBuilder();
    relJob.add("REL_ANCHOR_DOMAIN", "EMPLOYER_ID");
    relJob.add("REL_ANCHOR_KEY", "COYOTE_SOLUTIONS_KEY");
    relJab.add(relJob);
    relJob = Json.createObjectBuilder();
    relJob.add("REL_ANCHOR_DOMAIN", "CORP_HIERARCHY");
    relJob.add("REL_ANCHOR_KEY", "COYOTE_SOLUTIONS_KEY");
    relJab.add(relJob);
    relJob = Json.createObjectBuilder();
    relJob.add("REL_POINTER_DOMAIN", "CORP_HIERARCHY");
    relJob.add("REL_POINTER_KEY", "ACME_CORP_KEY");
    relJob.add("REL_POINTER_ROLE", "SUBSIDIARY");
    relJab.add(relJob);
    job.add("RELATIONSHIP_LIST", relJab);
    jab.add(job);

    return this.prepareJsonFile("test-companies-", jab.build());
  }

  private File prepareEmployeeFile() {
    JsonArrayBuilder jab = Json.createArrayBuilder();
    JsonObjectBuilder job = Json.createObjectBuilder();
    job.add("RECORD_ID", EMPLOYEE_1.getRecordId());
    job.add("DATA_SOURCE", EMPLOYEE_1.getDataSourceCode());
    job.add("ENTITY_TYPE", "GENERIC");
    job.add("NAME_FULL", "Jeff Founder");
    JsonArrayBuilder relJab = Json.createArrayBuilder();
    JsonObjectBuilder relJob = Json.createObjectBuilder();
    relJob.add("REL_ANCHOR_DOMAIN", "EMPLOYEE_NUM");
    relJob.add("REL_ANCHOR_KEY", "1");
    relJab.add(relJob);
    relJob = Json.createObjectBuilder();
    relJob.add("REL_POINTER_DOMAIN", "EMPLOYER_ID");
    relJob.add("REL_POINTER_KEY", "ACME_CORP_KEY");
    relJob.add("REL_POINTER_ROLE", "EMPLOYED_BY");
    relJab.add(relJob);
    job.add("RELATIONSHIP_LIST", relJab);
    jab.add(job);

    job = Json.createObjectBuilder();
    job.add("RECORD_ID", EMPLOYEE_2.getRecordId());
    job.add("DATA_SOURCE", EMPLOYEE_2.getDataSourceCode());
    job.add("ENTITY_TYPE", "GENERIC");
    job.add("NAME_FULL", "Jane Leader");
    relJab = Json.createArrayBuilder();
    relJob = Json.createObjectBuilder();
    relJob.add("REL_ANCHOR_DOMAIN", "EMPLOYEE_NUM");
    relJob.add("REL_ANCHOR_KEY", "2");
    relJab.add(relJob);
    relJob = Json.createObjectBuilder();
    relJob.add("REL_POINTER_DOMAIN", "EMPLOYEE_NUM");
    relJob.add("REL_POINTER_KEY", "1");
    relJob.add("REL_POINTER_ROLE", "MANAGED_BY");
    relJab.add(relJob);
    relJob = Json.createObjectBuilder();
    relJob.add("REL_POINTER_DOMAIN", "EMPLOYER_ID");
    relJob.add("REL_POINTER_KEY", "ACME_CORP_KEY");
    relJob.add("REL_POINTER_ROLE", "EMPLOYED_BY");
    relJab.add(relJob);
    job.add("RELATIONSHIP_LIST", relJab);
    jab.add(job);

    job = Json.createObjectBuilder();
    job.add("RECORD_ID", EMPLOYEE_3.getRecordId());
    job.add("DATA_SOURCE", EMPLOYEE_3.getDataSourceCode());
    job.add("ENTITY_TYPE", "GENERIC");
    job.add("NAME_FULL", "Joe Workman");
    relJab = Json.createArrayBuilder();
    relJob = Json.createObjectBuilder();
    relJob.add("REL_ANCHOR_DOMAIN", "EMPLOYEE_NUM");
    relJob.add("REL_ANCHOR_KEY", "6");
    relJab.add(relJob);
    relJob = Json.createObjectBuilder();
    relJob.add("REL_POINTER_DOMAIN", "EMPLOYEE_NUM");
    relJob.add("REL_POINTER_KEY", "2");
    relJob.add("REL_POINTER_ROLE", "MANAGED_BY");
    relJab.add(relJob);
    relJob = Json.createObjectBuilder();
    relJob.add("REL_POINTER_DOMAIN", "EMPLOYER_ID");
    relJob.add("REL_POINTER_KEY", "ACME_CORP_KEY");
    relJob.add("REL_POINTER_ROLE", "EMPLOYED_BY");
    relJab.add(relJob);
    job.add("RELATIONSHIP_LIST", relJab);
    jab.add(job);

    return this.prepareJsonFile("test-employees-", jab.build());
  }

  private File prepareContactFile() {
    JsonArrayBuilder jab = Json.createArrayBuilder();
    JsonObjectBuilder job = Json.createObjectBuilder();
    job.add("RECORD_ID", CONTACT_1.getRecordId());
    job.add("DATA_SOURCE", CONTACT_1.getDataSourceCode());
    job.add("ENTITY_TYPE", "GENERIC");
    job.add("NAME_FULL", "Richard Couples");
    job.add("PHONE_NUMBER", "718-949-8812");
    job.add("ADDR_FULL", "10010 WOODLAND AVE; ATLANTA, GA 30334");
    JsonArrayBuilder relJab = Json.createArrayBuilder();
    JsonObjectBuilder relJob = Json.createObjectBuilder();
    relJob.add("RELATIONSHIP_TYPE", "SPOUSE");
    relJob.add("RELATIONSHIP_KEY", "SPOUSES-1-2");
    relJob.add("RELATIONSHIP_ROLE", "WIFE");
    relJab.add(relJob);
    job.add("RELATIONSHIP_LIST", relJab);
    jab.add(job);

    job = Json.createObjectBuilder();
    job.add("RECORD_ID", CONTACT_2.getRecordId());
    job.add("DATA_SOURCE", CONTACT_2.getDataSourceCode());
    job.add("ENTITY_TYPE", "GENERIC");
    job.add("NAME_FULL", "Brianna Couples");
    job.add("PHONE_NUMBER", "718-949-8812");
    job.add("ADDR_FULL", "10010 WOODLAND AVE; ATLANTA, GA 30334");
    relJab = Json.createArrayBuilder();
    relJob = Json.createObjectBuilder();
    relJob.add("RELATIONSHIP_TYPE", "SPOUSE");
    relJob.add("RELATIONSHIP_KEY", "SPOUSES-1-2");
    relJob.add("RELATIONSHIP_ROLE", "HUSBAND");
    relJab.add(relJob);
    job.add("RELATIONSHIP_LIST", relJab);
    jab.add(job);

    job = Json.createObjectBuilder();
    job.add("RECORD_ID", CONTACT_3.getRecordId());
    job.add("DATA_SOURCE", CONTACT_3.getDataSourceCode());
    job.add("ENTITY_TYPE", "GENERIC");
    job.add("NAME_FULL", "Samuel Strong");
    job.add("PHONE_NUMBER", "312-889-3340");
    job.add("ADDR_FULL", "10010 LAKE VIEW RD; SPRINGFIELD, MO 65807");
    jab.add(job);

    job = Json.createObjectBuilder();
    job.add("RECORD_ID", CONTACT_4.getRecordId());
    job.add("DATA_SOURCE", CONTACT_4.getDataSourceCode());
    job.add("ENTITY_TYPE", "GENERIC");
    job.add("NAME_FULL", "Melissa Powers");
    job.add("PHONE_NUMBER", "312-885-4236");
    job.add("ADDR_FULL", "10010 LAKE VIEW RD; SPRINGFIELD, MO 65807");
    jab.add(job);

    return this.prepareJsonFile("test-contacts-", jab.build());
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

    List<SzRecordId> recordIds = RECORD_IDS;

    List<List> parameters = generateCombinations(
        recordIds.subList(0, 1),  // placeholder
        booleanVariantList,       // forceMinimal
        featureModes,             // featureMode
        booleanVariantList,       // withFeatureStats
        booleanVariantList,       // withInternalFeatures
        booleanVariantList,       // withRelationships
        booleanVariantList);      // withRaw

    List<Arguments> result = new ArrayList<>(parameters.size());

    for (int index = 0; index < parameters.size(); index++) {
      List params = parameters.get(index);
      params.set(0, recordIds.get(index % recordIds.size()));
      result.add(arguments(params.toArray()));
    }

    return result;
  }

  private List<Arguments> getWhyEntitiesParameters() {
    Boolean[] booleanVariants = {null, true, false};
    Boolean[] asRecordVariants = {true, false};
    List<Boolean> booleanVariantList = Arrays.asList(booleanVariants);
    List<Boolean> asRecordVariantList = Arrays.asList(asRecordVariants);

    List<SzFeatureMode> featureModes = new LinkedList<>();
    featureModes.add(null);
    for (SzFeatureMode featureMode : SzFeatureMode.values()) {
      featureModes.add(featureMode);
    }

    List<List> parameters = generateCombinations(
        RELATED_RECORD_IDS.subList(0, 1), // placeholder
        RELATED_RECORD_IDS.subList(1, 2), // placeholder
        asRecordVariantList,  // asRecord
        booleanVariantList,   // forceMinimal
        featureModes,         // featureMode
        booleanVariantList,   // withFeatureStats
        booleanVariantList,   // withInternalFeatures
        booleanVariantList,   // withRelationships
        booleanVariantList);  // withRaw

    // get the record ID combinations
    List<List> recordIdCombos = generateCombinations(
        RELATED_RECORD_IDS, RELATED_RECORD_IDS);

    // pare down the combinations
    Iterator<List> iter = recordIdCombos.iterator();
    while (iter.hasNext()) {
      List list = (List) iter.next();
      SzRecordId arg0 = (SzRecordId) list.get(0);
      SzRecordId arg1 = (SzRecordId) list.get(1);
      int index1 = RELATED_RECORD_IDS.indexOf(arg0);
      int index2 = RELATED_RECORD_IDS.indexOf(arg1);
      if (Math.abs(index2 - index1) > 4) {
        iter.remove();
      }
    }

    List<Arguments> result = new ArrayList<>(parameters.size());

    for (int index = 0; index < parameters.size(); index++) {
      List params = parameters.get(index);
      List recordIds = recordIdCombos.get(index % recordIdCombos.size());
      params.set(0, recordIds.get(0));
      params.set(1, recordIds.get(1));
      result.add(arguments(params.toArray()));
    }

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

    List<List> recordIdCombos = generateCombinations(RECORD_IDS,
                                                     RECORD_IDS);

    List<List> parameters = generateCombinations(
        RECORD_IDS.subList(0, 1),   // placeholder
        RECORD_IDS.subList(0, 1),   // placeholder
        booleanVariantList,         // forceMinimal
        featureModes,               // featureMode
        booleanVariantList,         // withFeatureStats
        booleanVariantList,         // withInternalFeatures
        booleanVariantList,         // withRelationships
        booleanVariantList);        // withRaw

    List<Arguments> result = new ArrayList<>(parameters.size());

    for (int index = 0; index < parameters.size(); index++) {
      List params = parameters.get(index);
      List recordIds = recordIdCombos.get(index % recordIdCombos.size());
      params.set(0, recordIds.get(0));
      params.set(1, recordIds.get(1));
      result.add(arguments(params.toArray()));
    }

    return result;
  }

  private StringBuilder buildWhyEntityQueryString(
      StringBuilder sb,
      Boolean forceMinimal,
      SzFeatureMode featureMode,
      Boolean withFeatureStats,
      Boolean withInternalFeatures,
      Boolean withRelationships,
      Boolean withRaw) {
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

  private StringBuilder buildWhyEntitiesQueryString(
      StringBuilder sb,
      SzEntityIdentifier entity1,
      SzEntityIdentifier entity2,
      Boolean forceMinimal,
      SzFeatureMode featureMode,
      Boolean withFeatureStats,
      Boolean withInternalFeatures,
      Boolean withRelationships,
      Boolean withRaw) {
    try {
      sb.append("?entity1=").append(
          URLEncoder.encode(entity1.toString(), "UTF-8"));
      sb.append("&entity2=").append(
          URLEncoder.encode(entity2.toString(), "UTF-8"));

    } catch (UnsupportedEncodingException cannotHappen) {
      throw new IllegalStateException("UTF-8 Encoding is not support");
    }

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
  }

  private StringBuilder buildWhyRecordsQueryString(
      StringBuilder sb,
      SzRecordId recordId1,
      SzRecordId recordId2,
      Boolean forceMinimal,
      SzFeatureMode featureMode,
      Boolean withFeatureStats,
      Boolean withInternalFeatures,
      Boolean withRelationships,
      Boolean withRaw) {
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
  @MethodSource("getWhyEntitiesParameters")
  public void whyEntitiesTest(SzRecordId recordId1,
                              SzRecordId recordId2,
                              boolean asRecordIds,
                              Boolean forceMinimal,
                              SzFeatureMode featureMode,
                              Boolean withFeatureStats,
                              Boolean withInternalFeatures,
                              Boolean withRelationships,
                              Boolean withRaw)
  {
    this.performTest(() -> {
      String testInfo = "recordId1=[ " + recordId1
          + " ], recordId2=[ " + recordId2
          + " ], asRecordIds=[ " + asRecordIds
          + " ], forceMinimal=[ " + forceMinimal
          + " ], featureMode=[ " + featureMode
          + " ], withFeatureStats=[ " + withFeatureStats
          + " ], withInternalFeatures=[ " + withInternalFeatures
          + " ], withRelationships=[ " + withRelationships
          + " ], withRaw=[ " + withRaw + " ]";


      StringBuilder sb = new StringBuilder();
      sb.append("why/entities");

      Long entityId1 = getEntityIdForRecordId(recordId1);
      Long entityId2 = getEntityIdForRecordId(recordId2);

      SzEntityIdentifier entityIdent1 = (asRecordIds) ? recordId1
          : new SzEntityId(entityId1);

      SzEntityIdentifier entityIdent2 = (asRecordIds) ? recordId2
          : new SzEntityId(entityId2);

      buildWhyEntitiesQueryString(sb,
                                  entityIdent1,
                                  entityIdent2,
                                  forceMinimal,
                                  featureMode,
                                  withFeatureStats,
                                  withInternalFeatures,
                                  withRelationships,
                                  withRaw);

      String uriText = this.formatServerUri(sb.toString());
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      long before = System.currentTimeMillis();

      SzWhyEntitiesResponse response = this.whyServices.whyEntities(
          entityIdent1.toString(),
          entityIdent2.toString(),
          (forceMinimal == null ? false : forceMinimal),
          (featureMode == null ? WITH_DUPLICATES : featureMode),
          (withFeatureStats == null ? true : withFeatureStats),
          (withInternalFeatures == null ? true : withInternalFeatures),
          (withRelationships == null ? false : withRelationships),
          (withRaw == null ? false : withRaw),
          uriInfo);

      response.concludeTimers();
      long after = System.currentTimeMillis();

      this.validateWhyEntitiesResponse(
          testInfo,
          response,
          GET,
          uriText,
          recordId1,
          recordId2,
          entityId1,
          entityId2,
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
  @MethodSource("getWhyEntitiesParameters")
  public void whyEntitiesViaHttpTest(SzRecordId     recordId1,
                                     SzRecordId     recordId2,
                                     boolean        asRecordIds,
                                     Boolean        forceMinimal,
                                     SzFeatureMode  featureMode,
                                     Boolean        withFeatureStats,
                                     Boolean        withInternalFeatures,
                                     Boolean        withRelationships,
                                     Boolean        withRaw)
  {
    this.performTest(() -> {
      String testInfo = "recordId1=[ " + recordId1
          + " ], recordId2=[ " + recordId2
          + " ], asRecordIds=[ " + asRecordIds
          + " ], forceMinimal=[ " + forceMinimal
          + " ], featureMode=[ " + featureMode
          + " ], withFeatureStats=[ " + withFeatureStats
          + " ], withInternalFeatures=[ " + withInternalFeatures
          + " ], withRelationships=[ " + withRelationships
          + " ], withRaw=[ " + withRaw + " ]";


      StringBuilder sb = new StringBuilder();
      sb.append("why/entities");

      Long entityId1 = getEntityIdForRecordId(recordId1);
      Long entityId2 = getEntityIdForRecordId(recordId2);

      SzEntityIdentifier entityIdent1 = (asRecordIds) ? recordId1
          : new SzEntityId(entityId1);

      SzEntityIdentifier entityIdent2 = (asRecordIds) ? recordId2
          : new SzEntityId(entityId2);

      buildWhyEntitiesQueryString(sb,
                                  entityIdent1,
                                  entityIdent2,
                                  forceMinimal,
                                  featureMode,
                                  withFeatureStats,
                                  withInternalFeatures,
                                  withRelationships,
                                  withRaw);

      String uriText = this.formatServerUri(sb.toString());
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      long before = System.currentTimeMillis();

      SzWhyEntitiesResponse response = this.invokeServerViaHttp(
          GET, uriText, SzWhyEntitiesResponse.class);

      response.concludeTimers();
      long after = System.currentTimeMillis();

      this.validateWhyEntitiesResponse(
          testInfo,
          response,
          GET,
          uriText,
          recordId1,
          recordId2,
          entityId1,
          entityId2,
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
  @MethodSource("getWhyEntitiesParameters")
  public void whyEntitiesViaJavaClientTest(SzRecordId     recordId1,
                                           SzRecordId     recordId2,
                                           boolean        asRecordIds,
                                           Boolean        forceMinimal,
                                           SzFeatureMode  featureMode,
                                           Boolean        withFeatureStats,
                                           Boolean        withInternalFeatures,
                                           Boolean        withRelationships,
                                           Boolean        withRaw)
  {
    this.performTest(() -> {
      String testInfo = "recordId1=[ " + recordId1
          + " ], recordId2=[ " + recordId2
          + " ], asRecordIds=[ " + asRecordIds
          + " ], forceMinimal=[ " + forceMinimal
          + " ], featureMode=[ " + featureMode
          + " ], withFeatureStats=[ " + withFeatureStats
          + " ], withInternalFeatures=[ " + withInternalFeatures
          + " ], withRelationships=[ " + withRelationships
          + " ], withRaw=[ " + withRaw + " ]";


      StringBuilder sb = new StringBuilder();
      sb.append("why/entities");

      Long entityId1 = getEntityIdForRecordId(recordId1);
      Long entityId2 = getEntityIdForRecordId(recordId2);

      SzEntityIdentifier entityIdent1 = (asRecordIds) ? recordId1
          : new SzEntityId(entityId1);

      SzEntityIdentifier entityIdent2 = (asRecordIds) ? recordId2
          : new SzEntityId(entityId2);

      buildWhyEntitiesQueryString(sb,
                                  entityIdent1,
                                  entityIdent2,
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
      com.senzing.gen.api.model.SzWhyEntitiesResponse clientResponse
          = this.entityDataApi.whyEntities(
              entityIdent1.toString(),
              entityIdent2.toString(),
              withRelationships,
              withFeatureStats,
              withInternalFeatures,
              clientFeatureMode,
              forceMinimal,
              withRaw);

      long after = System.currentTimeMillis();

      SzWhyEntitiesResponse response = jsonCopy(clientResponse,
                                                SzWhyEntitiesResponse.class);

      this.validateWhyEntitiesResponse(
          testInfo,
          response,
          GET,
          uriText,
          recordId1,
          recordId2,
          entityId1,
          entityId2,
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

  @Test
  public void whyEntitiesBadRecordIdTest() {
    this.performTest(() -> {
      StringBuilder sb = new StringBuilder();
      sb.append("why/entities");

      SzRecordId recordId1 = new SzRecordId(COMPANIES, "DOES_NOT_EXIST");
      SzRecordId recordId2 = COMPANY_1;

      buildWhyEntitiesQueryString(sb,
                                  recordId1,
                                  recordId2,
                                  false,
                                  SzFeatureMode.REPRESENTATIVE,
                                  false,
                                  false,
                                  false,
                                  false);

      String uriText = this.formatServerUri(sb.toString());
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      long before = System.currentTimeMillis();

      try {
      SzWhyEntitiesResponse response = this.whyServices.whyEntities(
          recordId1.toString(),
          recordId2.toString(),
          false,
          SzFeatureMode.REPRESENTATIVE,
          false,
          false,
          false,
          false,
          uriInfo);

        fail("Expected entity for dataSource \"" + recordId1.getDataSourceCode()
                 + "\" and record ID \"" + recordId1.getRecordId()
                 + "\" to NOT be found");

      } catch (BadRequestException expected) {
        SzErrorResponse response
            = (SzErrorResponse) expected.getResponse().getEntity();
        response.concludeTimers();
        long after = System.currentTimeMillis();

        validateBasics(
            response, 400, GET, uriText, before, after);
      }
    });
  }

  @Test
  public void whyEntitiesBadRecordIdViaHttpTest() {
    this.performTest(() -> {
      StringBuilder sb = new StringBuilder();
      sb.append("why/entities");

      SzRecordId recordId1 = new SzRecordId(COMPANIES, "DOES_NOT_EXIST");
      SzRecordId recordId2 = COMPANY_1;

      buildWhyEntitiesQueryString(sb,
                                  recordId1,
                                  recordId2,
                                  false,
                                  SzFeatureMode.REPRESENTATIVE,
                                  false,
                                  false,
                                  false,
                                  false);

      String uriText = this.formatServerUri(sb.toString());

      long before = System.currentTimeMillis();
      SzErrorResponse response = this.invokeServerViaHttp(
          GET, uriText, SzErrorResponse.class);
      response.concludeTimers();
      long after = System.currentTimeMillis();

      validateBasics(
          response, 400, GET, uriText, before, after);
    });

  }

  @Test
  public void whyEntitiesBadDataSourceTest() {
    this.performTest(() -> {
      StringBuilder sb = new StringBuilder();
      sb.append("why/entities");

      SzRecordId recordId1 = new SzRecordId("DOES_NOT_EXIST",
                                            "ABC123");
      SzRecordId recordId2 = COMPANY_1;

      buildWhyEntitiesQueryString(sb,
                                  recordId1,
                                  recordId2,
                                  false,
                                  SzFeatureMode.REPRESENTATIVE,
                                  false,
                                  false,
                                  false,
                                  false);

      String uriText = this.formatServerUri(sb.toString());
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      long before = System.currentTimeMillis();

      try {
        SzWhyEntitiesResponse response = this.whyServices.whyEntities(
            recordId1.toString(),
            recordId2.toString(),
            false,
            SzFeatureMode.REPRESENTATIVE,
            false,
            false,
            false,
            false,
            uriInfo);

        fail("Expected entity for dataSource \"" + recordId1.getDataSourceCode()
                 + "\" and record ID \"" + recordId1.getRecordId()
                 + "\" to NOT be found");

      } catch (BadRequestException expected) {
        SzErrorResponse response
            = (SzErrorResponse) expected.getResponse().getEntity();
        response.concludeTimers();
        long after = System.currentTimeMillis();

        validateBasics(
            response, 400, GET, uriText, before, after);
      }
    });
  }

  @Test
  public void whyEntitiesBadDataSourceViaHttpTest() {
    this.performTest(() -> {
      StringBuilder sb = new StringBuilder();
      sb.append("why/entities");

      SzRecordId recordId1 = new SzRecordId("DOES_NOT_EXIST",
                                            "ABC123");
      SzRecordId recordId2 = COMPANY_1;

      buildWhyEntitiesQueryString(sb,
                                  recordId1,
                                  recordId2,
                                  false,
                                  SzFeatureMode.REPRESENTATIVE,
                                  false,
                                  false,
                                  false,
                                  false);

      String uriText = this.formatServerUri(sb.toString());

      long before = System.currentTimeMillis();
      SzErrorResponse response = this.invokeServerViaHttp(
          GET, uriText, SzErrorResponse.class);
      response.concludeTimers();
      long after = System.currentTimeMillis();

      validateBasics(
          response, 400, GET, uriText, before, after);
    });
  }

  @Test
  public void whyEntitiesBadEntityIdTest() {
    this.performTest(() -> {
      StringBuilder sb = new StringBuilder();
      sb.append("why/entities");

      SzEntityId entityId1 = new SzEntityId(100000000L);
      SzEntityId entityId2 = new SzEntityId(100000001L);

      buildWhyEntitiesQueryString(sb,
                                  entityId1,
                                  entityId2,
                                  false,
                                  SzFeatureMode.REPRESENTATIVE,
                                  false,
                                  false,
                                  false,
                                  false);

      String uriText = this.formatServerUri(sb.toString());
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      long before = System.currentTimeMillis();

      try {
        SzWhyEntitiesResponse response = this.whyServices.whyEntities(
            entityId1.toString(),
            entityId2.toString(),
            false,
            SzFeatureMode.REPRESENTATIVE,
            false,
            false,
            false,
            false,
            uriInfo);

        fail("Expected entity for entity ID \"" + entityId1
                 + "\" and entity ID \"" + entityId2
                 + "\" to NOT be found");

      } catch (BadRequestException expected) {
        SzErrorResponse response
            = (SzErrorResponse) expected.getResponse().getEntity();
        response.concludeTimers();
        long after = System.currentTimeMillis();

        validateBasics(
            response, 400, GET, uriText, before, after);
      }
    });
  }

  @Test
  public void whyEntitiesBadEntityIdViaHttpTest() {
    this.performTest(() -> {
      StringBuilder sb = new StringBuilder();
      sb.append("why/entities");

      SzEntityId entityId1 = new SzEntityId(100000000L);
      SzEntityId entityId2 = new SzEntityId(100000001L);

      buildWhyEntitiesQueryString(sb,
                                  entityId1,
                                  entityId2,
                                  false,
                                  SzFeatureMode.REPRESENTATIVE,
                                  false,
                                  false,
                                  false,
                                  false);

      String uriText = this.formatServerUri(sb.toString());

      long before = System.currentTimeMillis();
      SzErrorResponse response = this.invokeServerViaHttp(
          GET, uriText, SzErrorResponse.class);
      response.concludeTimers();
      long after = System.currentTimeMillis();

      validateBasics(
          response, 400, GET, uriText, before, after);
    });
  }

  @ParameterizedTest
  @MethodSource("getWhyEntityParameters")
  public void whyEntityByRecordIdTest(SzRecordId recordId,
                                      Boolean forceMinimal,
                                      SzFeatureMode featureMode,
                                      Boolean withFeatureStats,
                                      Boolean withInternalFeatures,
                                      Boolean withRelationships,
                                      Boolean withRaw) {
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
      SzRecordId recordId,
      Boolean forceMinimal,
      SzFeatureMode featureMode,
      Boolean withFeatureStats,
      Boolean withInternalFeatures,
      Boolean withRelationships,
      Boolean withRaw) {
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
      SzRecordId recordId,
      Boolean forceMinimal,
      SzFeatureMode featureMode,
      Boolean withFeatureStats,
      Boolean withInternalFeatures,
      Boolean withRelationships,
      Boolean withRaw) {
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
  public void whyEntityByEntityIdTest(SzRecordId recordId,
                                      Boolean forceMinimal,
                                      SzFeatureMode featureMode,
                                      Boolean withFeatureStats,
                                      Boolean withInternalFeatures,
                                      Boolean withRelationships,
                                      Boolean withRaw) {
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
      SzRecordId recordId,
      Boolean forceMinimal,
      SzFeatureMode featureMode,
      Boolean withFeatureStats,
      Boolean withInternalFeatures,
      Boolean withRelationships,
      Boolean withRaw) {
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
      SzRecordId recordId,
      Boolean forceMinimal,
      SzFeatureMode featureMode,
      Boolean withFeatureStats,
      Boolean withInternalFeatures,
      Boolean withRelationships,
      Boolean withRaw) {
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
      String testInfo,
      SzWhyEntityResponse response,
      SzHttpMethod httpMethod,
      String selfLink,
      SzRecordId recordId,
      Long entityId,
      boolean forceMinimal,
      SzFeatureMode featureMode,
      boolean withFeatureStats,
      boolean withInternalFeatures,
      boolean withRelationships,
      Boolean withRaw,
      long beforeTimestamp,
      long afterTimestamp) {
    if (testInfo != null && selfLink != null) {
      testInfo = testInfo + ", selfLink=[ " + selfLink + " ]";
    }
    validateBasics(testInfo,
                   response,
                   httpMethod,
                   selfLink,
                   beforeTimestamp,
                   afterTimestamp);

    List<SzWhyEntityResult> whyResults = response.getData().getWhyResults();
    List<SzEntityData> entities = response.getData().getEntities();

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
        for (SzFocusRecordId focusId : result.getPerspective().getFocusRecords()) {
          perspectiveIds.add(focusId);
        }
      }

      // check the why result why key
      SzMatchInfo matchInfo = result.getMatchInfo();
      this.validateMatchInfo(testInfo, matchInfo);
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

  public void validateWhyEntitiesResponse(
      String testInfo,
      SzWhyEntitiesResponse response,
      SzHttpMethod httpMethod,
      String selfLink,
      SzRecordId recordId1,
      SzRecordId recordId2,
      Long entityId1,
      Long entityId2,
      boolean forceMinimal,
      SzFeatureMode featureMode,
      boolean withFeatureStats,
      boolean withInternalFeatures,
      boolean withRelationships,
      Boolean withRaw,
      long beforeTimestamp,
      long afterTimestamp) {
    if (testInfo != null && selfLink != null) {
      testInfo = testInfo + ", selfLink=[ " + selfLink + " ]";
    }
    validateBasics(testInfo,
                   response,
                   httpMethod,
                   selfLink,
                   beforeTimestamp,
                   afterTimestamp);

    SzWhyEntitiesResult whyResult = response.getData().getWhyResult();
    List<SzEntityData> entities = response.getData().getEntities();

    assertNotNull(whyResult, "Why result is null: " + testInfo);
    assertNotNull(entities, "Entities list is null: " + testInfo);

    assertTrue((entities.size() > 0), "No entities in entity list");

    Set<Long> entityIds = new LinkedHashSet<>();
    Set<SzRecordId> recordIds = new LinkedHashSet<>();
    for (SzEntityData entityData : entities) {
      SzResolvedEntity resolvedEntity = entityData.getResolvedEntity();
      entityIds.add(resolvedEntity.getEntityId());
      for (SzMatchedRecord record : resolvedEntity.getRecords()) {
        recordIds.add(
            new SzRecordId(record.getDataSource(), record.getRecordId()));
      }
    }

    if (entityId1 != null) {
      assertEquals(entityId1, whyResult.getEntityId1(),
                   "Unexpected first entity ID in why result: "
                       + testInfo);

      assertTrue(entityIds.contains(entityId1),
                 "First entity ID (" + entityId1 + ") not found "
                     + "entities list (" + entityIds + "): " + testInfo);
    }

    if (entityId2 != null) {
      assertEquals(entityId2, whyResult.getEntityId2(),
                   "Unexpected second entity ID in why result: "
                       + testInfo);

      assertTrue(entityIds.contains(entityId2),
                 "First entity ID (" + entityId2 + ") not found "
                     + "entities list (" + entityIds + "): " + testInfo);
    }

    if (recordId1 != null) {
      assertTrue(recordIds.contains(recordId1),
                 "Second record ID (" + recordId1 + ") not present in "
                     + "record IDs of returned entities (" + recordIds + "): "
                     + testInfo);
    }
    if (recordId2 != null) {
      assertTrue(recordIds.contains(recordId2),
                 "Second record ID (" + recordId2 + ") not present in "
                     + "record IDs of returned entities (" + recordIds + "): "
                     + testInfo);
    }

    // check the why result why key
    SzMatchInfo matchInfo = whyResult.getMatchInfo();
    this.validateMatchInfo(testInfo, matchInfo);

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
                              "ENTITY_ID",
                              "ENTITY_ID_2",
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
  public void whyRecordsTest(SzRecordId recordId1,
                             SzRecordId recordId2,
                             Boolean forceMinimal,
                             SzFeatureMode featureMode,
                             Boolean withFeatureStats,
                             Boolean withInternalFeatures,
                             Boolean withRelationships,
                             Boolean withRaw) {
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
      SzRecordId recordId1,
      SzRecordId recordId2,
      Boolean forceMinimal,
      SzFeatureMode featureMode,
      Boolean withFeatureStats,
      Boolean withInternalFeatures,
      Boolean withRelationships,
      Boolean withRaw) {
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
      SzRecordId recordId1,
      SzRecordId recordId2,
      Boolean forceMinimal,
      SzFeatureMode featureMode,
      Boolean withFeatureStats,
      Boolean withInternalFeatures,
      Boolean withRelationships,
      Boolean withRaw) {
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
      String testInfo,
      SzWhyRecordsResponse response,
      SzHttpMethod httpMethod,
      String selfLink,
      SzRecordId recordId1,
      SzRecordId recordId2,
      boolean forceMinimal,
      SzFeatureMode featureMode,
      boolean withFeatureStats,
      boolean withInternalFeatures,
      boolean withRelationships,
      Boolean withRaw,
      long beforeTimestamp,
      long afterTimestamp) {
    if (testInfo != null && selfLink != null) {
      testInfo = testInfo + ", selfLink=[ " + selfLink + " ]";
    }

    validateBasics(testInfo,
                   response,
                   httpMethod,
                   selfLink,
                   beforeTimestamp,
                   afterTimestamp);

    SzWhyRecordsResult whyResult = response.getData().getWhyResult();
    List<SzEntityData> entities = response.getData().getEntities();

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

  private void validateMatchInfo(String testInfo, SzMatchInfo matchInfo)
  {
    String whyKey = matchInfo.getWhyKey();

    Set<String> expectedTokens = new LinkedHashSet<>();
    for (List<SzFeatureScore> scores : matchInfo.getFeatureScores().values())
    {
      for (SzFeatureScore score: scores) {
        if (score.getScoringBucket() == SzScoringBucket.SAME) {
          expectedTokens.add("+" + score.getFeatureType());
        }
      }
    }

    for (SzDisclosedRelation relation : matchInfo.getDisclosedRelations()) {
      expectedTokens.add("+" + relation.getDomain());
      for (String role: relation.getRoles1()) {
        expectedTokens.add(role);
      }
      for (String role: relation.getRoles2()) {
        expectedTokens.add(role);
      }
    }

    for (String token : expectedTokens) {
      assertTrue((whyKey.indexOf(token) >= 0),
                  "Missing expected token (" + token
                      + ") from why key (" + whyKey + "): " + testInfo);
    }
  }
}
