package com.senzing.api.services;

import com.senzing.api.model.*;
import com.senzing.api.server.SzApiServerOptions;
import com.senzing.repomgr.RepositoryManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.json.JsonObject;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.UriInfo;
import java.util.*;

import com.senzing.gen.api.invoker.ApiClient;
import com.senzing.gen.api.services.ConfigApi;
import org.springframework.web.client.HttpStatusCodeException;

import static java.lang.Boolean.*;
import static com.senzing.api.model.SzHttpMethod.GET;
import static com.senzing.util.CollectionUtilities.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.junit.jupiter.api.TestInstance.Lifecycle;
import static com.senzing.api.services.ResponseValidators.*;

@TestInstance(Lifecycle.PER_CLASS)
public class ConfigServicesReadTest extends AbstractServiceTest
{
  private static final Set<String> CUSTOM_DATA_SOURCES;

  static {
    Set<String> customSources = new LinkedHashSet<>();

    try {
      customSources.add("EMPLOYEES");
      customSources.add("CUSTOMERS");
      customSources.add("VENDORS");

    } catch (Exception e) {
      e.printStackTrace();
      throw new ExceptionInInitializerError(e);

    } finally {
      CUSTOM_DATA_SOURCES = Collections.unmodifiableSet(customSources);
    }
  }

  private ConfigServices configServices;

  private ConfigApi configApi;

  private Map<SzAttributeClass,Set<String>> internalAttrTypesByClass
      = new LinkedHashMap<>();

  private Map<SzAttributeClass,Set<String>> standardAttrTypesByClass
      = new LinkedHashMap<>();

  private Map<SzAttributeClass,Set<String>> allAttrTypesByClass
      = new LinkedHashMap<>();

  private Map<String,Set<String>> internalAttrTypesByFeature
      = new LinkedHashMap<>();

  private Map<String,Set<String>> standardAttrTypesByFeature
      = new LinkedHashMap<>();

  private Map<String,Set<String>> allAttrTypesByFeature
      = new LinkedHashMap<>();

  private Set<String> internalAttrTypes = new LinkedHashSet<>();

  private Set<String> standardAttrTypes = new LinkedHashSet<>();

  private Set<String> allAttrTypes = new LinkedHashSet<>();

  private static <T> void putInMap(Map<T,Set<String>> map,
                                   T                  key,
                                   String             value)
  {
    if (key == null) return;
    Set<String> set = map.get(key);
    if (set == null) {
      set = new LinkedHashSet<>();
      map.put(key, set);
    }
    set.add(value);
  }

  @BeforeAll public void initializeEnvironment() {
    this.beginTests();
    this.initializeTestEnvironment();
    this.configServices = new ConfigServices();
    ApiClient apiClient = new ApiClient();
    apiClient.setBasePath(this.formatServerUri(""));
    this.configApi = new ConfigApi(apiClient);
  }

  /**
   * <p>
   * Overidden to skip engine priming.
   * </p>
   *
   * {@inheritDoc}
   */
  @Override
  protected void initializeServerOptions(SzApiServerOptions options) {
    super.initializeServerOptions(options);
    options.setSkippingEnginePriming(true);
  }

  protected void doPostServerInitialization(SzApiProvider provider,
                                            long          configId,
                                            JsonObject    configJson)
  {
    super.doPostServerInitialization(provider, configId, configJson);
    Map<String, SzAttributeType> attrTypes = this.getInitialAttributeTypes();

    for (SzAttributeType attrType: attrTypes.values()) {
      String            attrCode   = attrType.getAttributeCode();
      SzAttributeClass  attrClass  = attrType.getAttributeClass();
      boolean           internal   = attrType.isInternal();
      String            fTypeCode  = attrType.getFeatureType();

      if (internal) {
        putInMap(this.internalAttrTypesByClass, attrClass, attrCode);
        putInMap(this.allAttrTypesByClass, attrClass, attrCode);
        putInMap(this.internalAttrTypesByFeature, fTypeCode, attrCode);
        putInMap(this.allAttrTypesByFeature, fTypeCode, attrCode);
        this.internalAttrTypes.add(attrCode);
        this.allAttrTypes.add(attrCode);

      } else {
        putInMap(this.standardAttrTypesByClass, attrClass, attrCode);
        putInMap(this.allAttrTypesByClass, attrClass, attrCode);
        putInMap(this.standardAttrTypesByFeature, fTypeCode, attrCode);
        putInMap(this.allAttrTypesByFeature, fTypeCode, attrCode);
        this.standardAttrTypes.add(attrCode);
        this.allAttrTypes.add(attrCode);
      }
    }
    this.internalAttrTypes = Collections.unmodifiableSet(this.internalAttrTypes);
    this.standardAttrTypes = Collections.unmodifiableSet(this.standardAttrTypes);
    this.allAttrTypes      = Collections.unmodifiableSet(this.allAttrTypes);
    this.internalAttrTypesByClass   = recursivelyUnmodifiableMap(this.internalAttrTypesByClass);
    this.standardAttrTypesByClass   = recursivelyUnmodifiableMap(this.standardAttrTypesByClass);
    this.allAttrTypesByClass        = recursivelyUnmodifiableMap(this.allAttrTypesByClass);
    this.internalAttrTypesByFeature = recursivelyUnmodifiableMap(this.internalAttrTypesByFeature);
    this.standardAttrTypesByFeature = recursivelyUnmodifiableMap(this.standardAttrTypesByFeature);
    this.allAttrTypesByFeature      = recursivelyUnmodifiableMap(this.allAttrTypesByFeature);
  }

  /**
   * Overridden to configure some data sources.
   */
  protected void prepareRepository() {
    RepositoryManager.configSources(this.getRepositoryDirectory(),
                                    CUSTOM_DATA_SOURCES,
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

  @ParameterizedTest
  @MethodSource("getWithRawVariants")
  public void getDataSourcesTest(Boolean withRaw) {
    this.performTest(() -> {
      String suffix = "";
      if (withRaw != null) {
        suffix = "?withRaw=" + withRaw;
      }
      String  uriText = this.formatServerUri("data-sources" + suffix);
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      long before = System.nanoTime();
      SzDataSourcesResponse response
          = this.configServices.getDataSources(TRUE.equals(withRaw), uriInfo);
      response.concludeTimers();
      long after = System.nanoTime();

      validateDataSourcesResponse(response,
                                  GET,
                                  uriText,
                                  after - before,
                                  TRUE.equals(withRaw),
                                  this.getInitialDataSources());
    });
  }

  @ParameterizedTest
  @MethodSource("getWithRawVariants")
  public void getDataSourcesViaHttpTest(Boolean withRaw) {
    this.performTest(() -> {
      String suffix = "";
      if (withRaw != null) {
        suffix = "?withRaw=" + withRaw;
      }
      String uriText = this.formatServerUri("data-sources" + suffix);
      long before = System.nanoTime();
      SzDataSourcesResponse response
          = this.invokeServerViaHttp(GET, uriText, SzDataSourcesResponse.class);
      long after = System.nanoTime();

      validateDataSourcesResponse(response,
                                  GET,
                                  uriText,
                                  after - before,
                                  TRUE.equals(withRaw),
                                  this.getInitialDataSources());
    });
  }

  @ParameterizedTest
  @MethodSource("getWithRawVariants")
  public void getDataSourcesViaJavaClientTest(Boolean withRaw) {
    this.performTest(() -> {
      String suffix = "";
      if (withRaw != null) {
        suffix = "?withRaw=" + withRaw;
      }
      String uriText = this.formatServerUri("data-sources" + suffix);
      long before = System.nanoTime();
      com.senzing.gen.api.model.SzDataSourcesResponse clientResponse
          = this.configApi.getDataSources(withRaw);
      long after = System.nanoTime();

      SzDataSourcesResponse response = jsonCopy(clientResponse,
                                                SzDataSourcesResponse.class);

      validateDataSourcesResponse(response,
                                  GET,
                                  uriText,
                                  after - before,
                                  TRUE.equals(withRaw),
                                  this.getInitialDataSources());
    });
  }

  private List<Arguments> getDataSourceParameters() {
    List<Arguments> result = new LinkedList<>();
    Boolean[] booleanVariants = {null, true, false};
    for (SzDataSource dataSource: this.getInitialDataSources().values()) {
      for (Boolean withRaw: booleanVariants) {
        Object[] argArray = {
            dataSource.getDataSourceCode(),
            withRaw,
            dataSource};
        result.add(arguments(argArray));
      }
    }
    return result;
  }

  @ParameterizedTest
  @MethodSource("getDataSourceParameters")
  public void getDataSourceTest(String        dataSourceCode,
                                Boolean       withRaw,
                                SzDataSource  expectedDataSource)
  {
    this.performTest(() -> {
      String suffix = "";
      if (withRaw != null) {
        suffix = "?withRaw=" + withRaw;
      }
      String  uriText = this.formatServerUri(
          "data-sources/" + dataSourceCode + suffix);
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      long before = System.nanoTime();
      SzDataSourceResponse response
          = this.configServices.getDataSource(
              dataSourceCode, TRUE.equals(withRaw), uriInfo);
      response.concludeTimers();
      long after = System.nanoTime();

      validateDataSourceResponse(response,
                                 GET,
                                 uriText,
                                 after - before,
                                 TRUE.equals(withRaw),
                                 expectedDataSource);
    });
  }

  @ParameterizedTest
  @MethodSource("getDataSourceParameters")
  public void getDataSourceViaHttpTest(String       dataSourceCode,
                                       Boolean      withRaw,
                                       SzDataSource expectedDataSource)
  {
    this.performTest(() -> {
      String suffix = "";
      if (withRaw != null) {
        suffix = "?withRaw=" + withRaw;
      }
      String uriText = this.formatServerUri(
          "data-sources/" + dataSourceCode + suffix);
      long before = System.nanoTime();
      SzDataSourceResponse response
          = this.invokeServerViaHttp(GET, uriText, SzDataSourceResponse.class);
      long after = System.nanoTime();

      validateDataSourceResponse(response,
                                 GET,
                                 uriText,
                                 after - before,
                                 TRUE.equals(withRaw),
                                 expectedDataSource);
    });
  }

  @ParameterizedTest
  @MethodSource("getDataSourceParameters")
  public void getDataSourceViaJavaClientTest(String       dataSourceCode,
                                             Boolean      withRaw,
                                             SzDataSource expectedDataSource)
  {
    this.performTest(() -> {
      String suffix = "";
      if (withRaw != null) {
        suffix = "?withRaw=" + withRaw;
      }
      String uriText = this.formatServerUri(
          "data-sources/" + dataSourceCode + suffix);
      long before = System.nanoTime();
      com.senzing.gen.api.model.SzDataSourceResponse clientResponse
          = this.configApi.getDataSource(dataSourceCode, withRaw);
      long after = System.nanoTime();

      SzDataSourceResponse response = jsonCopy(clientResponse,
                                               SzDataSourceResponse.class);

      validateDataSourceResponse(response,
                                 GET,
                                 uriText,
                                 after - before,
                                 TRUE.equals(withRaw),
                                 expectedDataSource);
    });
  }

  @ParameterizedTest
  @MethodSource("getWithRawVariants")
  public void getUnrecognizedDataSourceTest(Boolean withRaw)
  {
    this.performTest(() -> {
      String badDataSourceCode = "DOES_NOT_EXIST";
      String suffix = "";
      if (withRaw != null) {
        suffix = "?withRaw=" + withRaw;
      }
      String  uriText = this.formatServerUri(
          "data-sources/" + badDataSourceCode + suffix);
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      long before = System.nanoTime();
      try {
        SzDataSourceResponse response
            = this.configServices.getDataSource(
            badDataSourceCode, TRUE.equals(withRaw), uriInfo);

        fail("Expected data source \"" + badDataSourceCode
                 + "\" to NOT be found");

      } catch (NotFoundException expected) {
        SzErrorResponse response
            = (SzErrorResponse) expected.getResponse().getEntity();
        response.concludeTimers();
        long after = System.nanoTime();
        validateBasics(
            response, 404, GET, uriText, after - before);
      }
    });
  }

  @ParameterizedTest
  @MethodSource("getWithRawVariants")
  public void getUnrecognizedDataSourceViaHttpTest(Boolean withRaw)
  {
    this.performTest(() -> {
      String badDataSourceCode = "DOES_NOT_EXIST";
      String suffix = "";
      if (withRaw != null) {
        suffix = "?withRaw=" + withRaw;
      }
      String uriText = this.formatServerUri(
          "data-sources/" + badDataSourceCode + suffix);
      long before = System.nanoTime();
      SzErrorResponse response
          = this.invokeServerViaHttp(GET, uriText, SzErrorResponse.class);
      long after = System.nanoTime();

      validateBasics(
          response, 404, GET, uriText, after - before);
    });
  }

  @ParameterizedTest
  @MethodSource("getWithRawVariants")
  public void getUnrecognizedDataSourceViaJavaClientTest(Boolean withRaw)
  {
    this.performTest(() -> {
      String badDataSourceCode = "DOES_NOT_EXIST";
      String suffix = "";
      if (withRaw != null) {
        suffix = "?withRaw=" + withRaw;
      }
      String uriText = this.formatServerUri(
          "data-sources/" + badDataSourceCode + suffix);
      long before = System.nanoTime();
      try {
        com.senzing.gen.api.model.SzDataSourceResponse clientResponse
            = this.configApi.getDataSource(badDataSourceCode, withRaw);
        fail("Expected failure, but got success for bad data source: "
                 + "dataSource=[ " + badDataSourceCode
                 + " ], withRaw=[ " + withRaw + " ], response=[ "
                 + clientResponse + " ]tail -");

      } catch (HttpStatusCodeException expected) {
        long after = System.nanoTime();
        com.senzing.gen.api.model.SzErrorResponse clientResponse
            = jsonParse(expected.getResponseBodyAsString(),
                        com.senzing.gen.api.model.SzErrorResponse.class);

        SzErrorResponse response = jsonCopy(clientResponse,
                                            SzErrorResponse.class);
        validateBasics(
            response, 404, GET, uriText, after - before);
      }

    });
  }

  @ParameterizedTest
  @MethodSource("getWithRawVariants")
  public void getEntityClassesTest(Boolean withRaw) {
    this.performTest(() -> {
      String suffix = "";
      if (withRaw != null) {
        suffix = "?withRaw=" + withRaw;
      }
      String  uriText = this.formatServerUri("entity-classes" + suffix);
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      long before = System.nanoTime();
      SzEntityClassesResponse response
          = this.configServices.getEntityClasses(TRUE.equals(withRaw), uriInfo);
      response.concludeTimers();
      long after = System.nanoTime();

      validateEntityClassesResponse(null,
                                    response,
                                    GET,
                                    uriText,
                                    after - before,
                                    TRUE.equals(withRaw),
                                    null,
                                    this.getInitialEntityClasses());
    });
  }

  @ParameterizedTest
  @MethodSource("getWithRawVariants")
  public void getEntityClassesViaHttpTest(Boolean withRaw) {
    this.performTest(() -> {
      String suffix = "";
      if (withRaw != null) {
        suffix = "?withRaw=" + withRaw;
      }
      String uriText = this.formatServerUri("entity-classes" + suffix);
      long before = System.nanoTime();
      SzEntityClassesResponse response = this.invokeServerViaHttp(
              GET, uriText, SzEntityClassesResponse.class);
      long after = System.nanoTime();

      validateEntityClassesResponse(null,
                                    response,
                                    GET,
                                    uriText,
                                    after - before,
                                    TRUE.equals(withRaw),
                                    null,
                                    this.getInitialEntityClasses());
    });
  }

  @ParameterizedTest
  @MethodSource("getWithRawVariants")
  public void getEntityClassesViaJavaClientTest(Boolean withRaw) {
    this.performTest(() -> {
      String suffix = "";
      if (withRaw != null) {
        suffix = "?withRaw=" + withRaw;
      }
      String uriText = this.formatServerUri("entity-classes" + suffix);
      long before = System.nanoTime();
      com.senzing.gen.api.model.SzEntityClassesResponse clientResponse
          = this.configApi.getEntityClasses(withRaw);
      long after = System.nanoTime();

      SzEntityClassesResponse response
          = jsonCopy(clientResponse, SzEntityClassesResponse.class);

      validateEntityClassesResponse(null,
                                    response,
                                    GET,
                                    uriText,
                                    after - before,
                                    TRUE.equals(withRaw),
                                    null,
                                    this.getInitialEntityClasses());
    });
  }

  private List<Arguments> getEntityClassParameters() {
    List<Arguments> result = new LinkedList<>();
    Boolean[] booleanVariants = {null, true, false};
    for (SzEntityClass entityClass: this.getInitialEntityClasses().values()) {
      for (Boolean withRaw: booleanVariants) {
        Object[] argArray1 = {
            entityClass.getEntityClassCode(),
            withRaw,
            entityClass};

        result.add(arguments(argArray1));

        Object[] argArray2 = {
            entityClass.getEntityClassCode().toLowerCase(),
            withRaw,
            entityClass};

        result.add(arguments(argArray2));
      }
    }
    return result;
  }

  @ParameterizedTest
  @MethodSource("getEntityClassParameters")
  public void getEntityClassTest(String         entityClassCode,
                                 Boolean        withRaw,
                                 SzEntityClass  expectedEntityClass)
  {
    this.performTest(() -> {
      String suffix = "";
      if (withRaw != null) {
        suffix = "?withRaw=" + withRaw;
      }
      String  uriText = this.formatServerUri(
          "entity-classes/" + entityClassCode + suffix);
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      long before = System.nanoTime();
      SzEntityClassResponse response
          = this.configServices.getEntityClass(
          entityClassCode, TRUE.equals(withRaw), uriInfo);
      response.concludeTimers();
      long after = System.nanoTime();

      validateEntityClassResponse(response,
                                  uriText,
                                  after - before,
                                  TRUE.equals(withRaw),
                                  expectedEntityClass);
    });
  }

  @ParameterizedTest
  @MethodSource("getEntityClassParameters")
  public void getEntityClassViaHttpTest(String        entityClassCode,
                                        Boolean       withRaw,
                                        SzEntityClass expectedEntityClass)
  {
    this.performTest(() -> {
      String suffix = "";
      if (withRaw != null) {
        suffix = "?withRaw=" + withRaw;
      }
      String uriText = this.formatServerUri(
          "entity-classes/" + entityClassCode + suffix);
      long before = System.nanoTime();
      SzEntityClassResponse response
          = this.invokeServerViaHttp(GET, uriText, SzEntityClassResponse.class);
      long after = System.nanoTime();

      validateEntityClassResponse(response,
                                  uriText,
                                  after - before,
                                  TRUE.equals(withRaw),
                                  expectedEntityClass);
    });
  }

  @ParameterizedTest
  @MethodSource("getEntityClassParameters")
  public void getEntityClassViaJavaClientTest(String        entityClassCode,
                                              Boolean       withRaw,
                                              SzEntityClass expectedEntityClass)
  {
    this.performTest(() -> {
      String suffix = "";
      if (withRaw != null) {
        suffix = "?withRaw=" + withRaw;
      }
      String uriText = this.formatServerUri(
          "entity-classes/" + entityClassCode + suffix);
      long before = System.nanoTime();
      com.senzing.gen.api.model.SzEntityClassResponse clientResponse
          = this.configApi.getEntityClass(entityClassCode, withRaw);
      long after = System.nanoTime();

      SzEntityClassResponse response = jsonCopy(clientResponse,
                                                SzEntityClassResponse.class);

      validateEntityClassResponse(response,
                                  uriText,
                                  after - before,
                                  TRUE.equals(withRaw),
                                  expectedEntityClass);
    });
  }

  @ParameterizedTest
  @MethodSource("getWithRawVariants")
  public void getUnrecognizedEntityClassTest(Boolean withRaw)
  {
    this.performTest(() -> {
      String badEntityClassCode = "DOES_NOT_EXIST";
      String suffix = "";
      if (withRaw != null) {
        suffix = "?withRaw=" + withRaw;
      }
      String  uriText = this.formatServerUri(
          "entity-classes/" + badEntityClassCode + suffix);
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      long before = System.nanoTime();
      try {
        this.configServices.getEntityClass(
             badEntityClassCode, TRUE.equals(withRaw), uriInfo);

        fail("Expected entity class \"" + badEntityClassCode
                 + "\" to NOT be found");

      } catch (NotFoundException expected) {
        SzErrorResponse response
            = (SzErrorResponse) expected.getResponse().getEntity();
        response.concludeTimers();
        long after = System.nanoTime();
        validateBasics(
            response, 404, GET, uriText, after - before);
      }
    });
  }

  @ParameterizedTest
  @MethodSource("getWithRawVariants")
  public void getUnrecognizedEntityClassViaHttpTest(Boolean withRaw)
  {
    this.performTest(() -> {
      String badEntityClassCode = "DOES_NOT_EXIST";
      String suffix = "";
      if (withRaw != null) {
        suffix = "?withRaw=" + withRaw;
      }
      String uriText = this.formatServerUri(
          "entity-classes/" + badEntityClassCode + suffix);
      long before = System.nanoTime();
      SzErrorResponse response
          = this.invokeServerViaHttp(GET, uriText, SzErrorResponse.class);
      long after = System.nanoTime();

      validateBasics(
          response, 404, GET, uriText, after - before);
    });
  }

  @ParameterizedTest
  @MethodSource("getWithRawVariants")
  public void getUnrecognizedEntityClassViaJavaClientTest(Boolean withRaw)
  {
    this.performTest(() -> {
      String badEntityClassCode = "DOES_NOT_EXIST";
      String suffix = "";
      if (withRaw != null) {
        suffix = "?withRaw=" + withRaw;
      }
      String uriText = this.formatServerUri(
          "entity-classes/" + badEntityClassCode + suffix);
      long before = System.nanoTime();
      try {
        com.senzing.gen.api.model.SzEntityClassResponse clientResponse
            = this.configApi.getEntityClass(badEntityClassCode, withRaw);

        fail("Expected failure, but got success for bad data source: "
                 + "entityClass=[ " + badEntityClassCode
                 + " ], withRaw=[ " + withRaw + " ]");

      } catch (HttpStatusCodeException expected) {
        long after = System.nanoTime();
        com.senzing.gen.api.model.SzErrorResponse clientResponse
            = jsonParse(expected.getResponseBodyAsString(),
                        com.senzing.gen.api.model.SzErrorResponse.class);

        SzErrorResponse response = jsonCopy(clientResponse,
                                            SzErrorResponse.class);
        validateBasics(
            response, 404, GET, uriText, after - before);
      }
    });
  }

  private List<Arguments> getEntityTypesByClassParameters() {
    List<Arguments> result = new LinkedList<>();
    Boolean[] booleanVariants = {null, true, false};

    for (SzEntityClass entityClass: this.getInitialEntityClasses().values()) {
      for (Boolean withRaw: booleanVariants) {
        // get the entity class code
        String entityClassCode = entityClass.getEntityClassCode();

        Map<String, SzEntityType> entityTypeMap = new LinkedHashMap<>();
        this.getInitialEntityTypes().values().forEach(entityType -> {
          if (entityType.getEntityClassCode().equals(entityClassCode)) {
            entityTypeMap.put(entityType.getEntityTypeCode(), entityType);
          }
        });

        Object[] argArray1 = {
            entityClassCode,
            withRaw,
            entityTypeMap};

        result.add(arguments(argArray1));

        Object[] argArray2 = {
            entityClassCode.toLowerCase(),
            withRaw,
            entityTypeMap };

        result.add(arguments(argArray2));
      }
    }
    return result;
  }

  private List<Arguments> getEntityTypesParameters() {
    List<Arguments> result = new LinkedList<>();
    Boolean[] booleanVariants = {null, true, false};
    for (Boolean withRaw: booleanVariants) {
      // check if entity class is null
      Object[] argArray = {null, withRaw, this.getInitialEntityTypes()};
      result.add(arguments(argArray));
    }
    result.addAll(this.getEntityTypesByClassParameters());
    return result;
  }

  private String buildEntityTypesQueryString(String   entityClass,
                                             Boolean  withRaw)
  {
    StringBuilder sb = new StringBuilder();
    String prefix = "?";
    if (entityClass != null && entityClass.trim().length() > 0) {
      sb.append(prefix).append("entityClass=").append(entityClass);
      prefix = "&";
    }
    if (withRaw != null) {
      sb.append(prefix).append("withRaw=").append(withRaw);
      prefix = "&";
    }
    return sb.toString();
  }

  @ParameterizedTest
  @MethodSource("getEntityTypesParameters")
  public void getEntityTypesTest(String                     entityClass,
                                 Boolean                    withRaw,
                                 Map<String, SzEntityType>  expectedEntityTypes)
  {
    this.performTest(() -> {
      String  suffix  = this.buildEntityTypesQueryString(entityClass, withRaw);
      String  uriText = this.formatServerUri("entity-types" + suffix);
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      long before = System.nanoTime();
      SzEntityTypesResponse response = this.configServices.getEntityTypes(
              entityClass, TRUE.equals(withRaw), uriInfo);
      response.concludeTimers();
      long after = System.nanoTime();

      validateEntityTypesResponse(null,
                                  response,
                                  GET,
                                  uriText,
                                  after - before,
                                  TRUE.equals(withRaw),
                                  expectedEntityTypes);
    });
  }

  @ParameterizedTest
  @MethodSource("getEntityTypesParameters")
  public void getEntityTypesViaHttpTest(
      String                    entityClass,
      Boolean                   withRaw,
      Map<String, SzEntityType> expectedEntityTypes)
  {
    this.performTest(() -> {
      String suffix  = this.buildEntityTypesQueryString(entityClass, withRaw);
      String uriText = this.formatServerUri("entity-types" + suffix);
      long before = System.nanoTime();
      SzEntityTypesResponse response = this.invokeServerViaHttp(
          GET, uriText, SzEntityTypesResponse.class);
      long after = System.nanoTime();

      validateEntityTypesResponse(null,
                                  response,
                                  GET,
                                  uriText,
                                  after - before,
                                  TRUE.equals(withRaw),
                                  expectedEntityTypes);
    });
  }

  @ParameterizedTest
  @MethodSource("getEntityTypesParameters")
  public void getEntityTypesViaJavaClientTest(
      String                    entityClass,
      Boolean                   withRaw,
      Map<String, SzEntityType> expectedEntityTypes)
  {
    this.performTest(() -> {
      String suffix  = this.buildEntityTypesQueryString(entityClass, withRaw);
      String uriText = this.formatServerUri("entity-types" + suffix);
      long before = System.nanoTime();
      com.senzing.gen.api.model.SzEntityTypesResponse clientResponse
          = this.configApi.getEntityTypes(entityClass, withRaw);
      long after = System.nanoTime();

      SzEntityTypesResponse response = jsonCopy(clientResponse,
                                                SzEntityTypesResponse.class);

      validateEntityTypesResponse(null,
                                  response,
                                  GET,
                                  uriText,
                                  after - before,
                                  TRUE.equals(withRaw),
                                  expectedEntityTypes);
    });
  }

  private List<Arguments> getEntityTypeParameters() {
    List<Arguments> result = new LinkedList<>();
    Boolean[] booleanVariants = {null, true, false};
    for (SzEntityType entityType: this.getInitialEntityTypes().values()) {
      for (Boolean withRaw: booleanVariants) {
        Object[] argArray1 = {
            entityType.getEntityTypeCode(),
            withRaw,
            entityType};

        result.add(arguments(argArray1));

        Object[] argArray2 = {
            entityType.getEntityTypeCode().toLowerCase(),
            withRaw,
            entityType};

        result.add(arguments(argArray2));
      }
    }
    return result;
  }

  @ParameterizedTest
  @MethodSource("getEntityTypeParameters")
  public void getEntityTypeTest(String        entityTypeCode,
                                Boolean       withRaw,
                                SzEntityType  expectedEntityType)
  {
    this.performTest(() -> {
      String suffix = "";
      if (withRaw != null) {
        suffix = "?withRaw=" + withRaw;
      }
      String  uriText = this.formatServerUri(
          "entity-types/" + entityTypeCode + suffix);
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      long before = System.nanoTime();
      SzEntityTypeResponse response = this.configServices.getEntityType(
          entityTypeCode, TRUE.equals(withRaw), uriInfo);
      response.concludeTimers();
      long after = System.nanoTime();

      validateEntityTypeResponse(response,
                                 uriText,
                                 after - before,
                                 TRUE.equals(withRaw),
                                 expectedEntityType);
    });
  }

  @ParameterizedTest
  @MethodSource("getEntityTypeParameters")
  public void getEntityTypeViaHttpTest(String       entityTypeCode,
                                       Boolean      withRaw,
                                       SzEntityType expectedEntityType)
  {
    this.performTest(() -> {
      String suffix = "";
      if (withRaw != null) {
        suffix = "?withRaw=" + withRaw;
      }
      String uriText = this.formatServerUri(
          "entity-types/" + entityTypeCode + suffix);
      long before = System.nanoTime();
      SzEntityTypeResponse response
          = this.invokeServerViaHttp(GET, uriText, SzEntityTypeResponse.class);
      long after = System.nanoTime();

      validateEntityTypeResponse(response,
                                 uriText,
                                 after - before,
                                 TRUE.equals(withRaw),
                                 expectedEntityType);
    });
  }

  @ParameterizedTest
  @MethodSource("getWithRawVariants")
  public void getUnrecognizedEntityTypeTest(Boolean withRaw)
  {
    this.performTest(() -> {
      String badEntityTypeCode = "DOES_NOT_EXIST";
      String suffix = "";
      if (withRaw != null) {
        suffix = "?withRaw=" + withRaw;
      }
      String  uriText = this.formatServerUri(
          "entity-types/" + badEntityTypeCode + suffix);
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      long before = System.nanoTime();
      try {
        SzEntityTypeResponse response
            = this.configServices.getEntityType(
            badEntityTypeCode, TRUE.equals(withRaw), uriInfo);

        fail("Expected entity type \"" + badEntityTypeCode
                 + "\" to NOT be found");

      } catch (NotFoundException expected) {
        SzErrorResponse response
            = (SzErrorResponse) expected.getResponse().getEntity();
        response.concludeTimers();
        long after = System.nanoTime();
        validateBasics(
            response, 404, GET, uriText, after - before);
      }
    });
  }

  @ParameterizedTest
  @MethodSource("getWithRawVariants")
  public void getUnrecognizedEntityTypeViaHttpTest(Boolean withRaw)
  {
    this.performTest(() -> {
      String badEntityTypeCode = "DOES_NOT_EXIST";
      String suffix = "";
      if (withRaw != null) {
        suffix = "?withRaw=" + withRaw;
      }
      String uriText = this.formatServerUri(
          "entity-types/" + badEntityTypeCode + suffix);
      long before = System.nanoTime();
      try {
        com.senzing.gen.api.model.SzEntityTypeResponse clientResponse
            = this.configApi.getEntityType(badEntityTypeCode, withRaw);

        fail("Expected failure, but got success for bad data source: "
                 + "entityType=[ " + badEntityTypeCode
                 + " ], withRaw=[ " + withRaw + " ]");

      } catch (HttpStatusCodeException expected) {
        long after = System.nanoTime();
        com.senzing.gen.api.model.SzErrorResponse clientResponse
            = jsonParse(expected.getResponseBodyAsString(),
                        com.senzing.gen.api.model.SzErrorResponse.class);

        SzErrorResponse response = jsonCopy(clientResponse,
                                            SzErrorResponse.class);
        validateBasics(
            response, 404, GET, uriText, after - before);
      }
    });
  }

  @ParameterizedTest
  @MethodSource("getWithRawVariants")
  public void getUnrecognizedEntityTypeViaJavaClientTest(Boolean withRaw)
  {
    this.performTest(() -> {
      String badEntityTypeCode = "DOES_NOT_EXIST";
      String suffix = "";
      if (withRaw != null) {
        suffix = "?withRaw=" + withRaw;
      }
      String uriText = this.formatServerUri(
          "entity-types/" + badEntityTypeCode + suffix);
      long before = System.nanoTime();
      SzErrorResponse response
          = this.invokeServerViaHttp(GET, uriText, SzErrorResponse.class);
      long after = System.nanoTime();

      validateBasics(
          response, 404, GET, uriText, after - before);
    });
  }

  @ParameterizedTest
  @MethodSource("getEntityTypesByClassParameters")
  public void getEntityTypesByClassTest(
      String                     entityClass,
      Boolean                    withRaw,
      Map<String, SzEntityType>  expectedEntityTypes)
  {
    this.performTest(() -> {
      String suffix = "";
      if (withRaw != null) {
        suffix = "?withRaw=" + withRaw;
      }
      String  uriText = this.formatServerUri(
          "entity-classes/" + entityClass + "/entity-types" + suffix);
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      long before = System.nanoTime();
      SzEntityTypesResponse response = this.configServices.getEntityTypes(
          entityClass, TRUE.equals(withRaw), uriInfo);
      response.concludeTimers();
      long after = System.nanoTime();

      validateEntityTypesResponse(null,
                                  response,
                                  GET,
                                  uriText,
                                  after - before,
                                  TRUE.equals(withRaw),
                                  expectedEntityTypes);
    });
  }

  @ParameterizedTest
  @MethodSource("getEntityTypesByClassParameters")
  public void getEntityTypesByClassViaHttpTest(
      String                    entityClass,
      Boolean                   withRaw,
      Map<String, SzEntityType> expectedEntityTypes)
  {
    this.performTest(() -> {
      String suffix = "";
      if (withRaw != null) {
        suffix = "?withRaw=" + withRaw;
      }
      String uriText = this.formatServerUri(
          "entity-classes/" + entityClass + "/entity-types" + suffix);
      long before = System.nanoTime();
      SzEntityTypesResponse response = this.invokeServerViaHttp(
          GET, uriText, SzEntityTypesResponse.class);
      long after = System.nanoTime();

      validateEntityTypesResponse(null,
                                  response,
                                  GET,
                                  uriText,
                                  after - before,
                                  TRUE.equals(withRaw),
                                  expectedEntityTypes);
    });
  }

  @ParameterizedTest
  @MethodSource("getEntityTypesByClassParameters")
  public void getEntityTypesByClassViaJavaClientTest(
      String                    entityClass,
      Boolean                   withRaw,
      Map<String, SzEntityType> expectedEntityTypes)
  {
    this.performTest(() -> {
      String suffix = "";
      if (withRaw != null) {
        suffix = "?withRaw=" + withRaw;
      }
      String uriText = this.formatServerUri(
          "entity-classes/" + entityClass + "/entity-types" + suffix);
      long before = System.nanoTime();
      com.senzing.gen.api.model.SzEntityTypesResponse clientResponse
          = this.configApi.getEntityTypesByClass(entityClass, withRaw);
      long after = System.nanoTime();

      SzEntityTypesResponse response = jsonCopy(clientResponse,
                                                SzEntityTypesResponse.class);

      validateEntityTypesResponse(null,
                                  response,
                                  GET,
                                  uriText,
                                  after - before,
                                  TRUE.equals(withRaw),
                                  expectedEntityTypes);
    });
  }

  private List<Arguments> getEntityTypeByClassParameters() {
    List<Arguments> result = new LinkedList<>();
    Boolean[] booleanVariants = {null, true, false};
    for (SzEntityType entityType: this.getInitialEntityTypes().values()) {
      for (Boolean withRaw: booleanVariants) {
        Object[] argArray1 = {
            entityType.getEntityClassCode(),
            entityType.getEntityTypeCode(),
            withRaw,
            entityType};

        result.add(arguments(argArray1));

        Object[] argArray2 = {
            entityType.getEntityClassCode().toLowerCase(),
            entityType.getEntityTypeCode().toLowerCase(),
            withRaw,
            entityType};

        result.add(arguments(argArray2));
      }
    }
    return result;
  }

  @ParameterizedTest
  @MethodSource("getEntityTypeByClassParameters")
  public void getEntityTypeByClassTest(String       entityClassCode,
                                       String       entityTypeCode,
                                       Boolean      withRaw,
                                       SzEntityType expectedEntityType)
  {
    this.performTest(() -> {
      String suffix = "";
      if (withRaw != null) {
        suffix = "?withRaw=" + withRaw;
      }
      String  uriText = this.formatServerUri(
          "entity-classes/" + entityClassCode
              + "/entity-types/" + entityTypeCode + suffix);
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      long before = System.nanoTime();
      SzEntityTypeResponse response = this.configServices.getEntityType(
          entityClassCode, entityTypeCode, TRUE.equals(withRaw), uriInfo);
      response.concludeTimers();
      long after = System.nanoTime();

      validateEntityTypeResponse(response,
                                 uriText,
                                 after - before,
                                 TRUE.equals(withRaw),
                                 expectedEntityType);
    });
  }

  @ParameterizedTest
  @MethodSource("getEntityTypeByClassParameters")
  public void getEntityTypeByClassViaHttpTest(String        entityClassCode,
                                              String        entityTypeCode,
                                              Boolean       withRaw,
                                              SzEntityType  expectedEntityType)
  {
    this.performTest(() -> {
      String suffix = "";
      if (withRaw != null) {
        suffix = "?withRaw=" + withRaw;
      }
      String uriText = this.formatServerUri(
          "entity-classes/" + entityClassCode
              + "/entity-types/" + entityTypeCode + suffix);
      long before = System.nanoTime();
      SzEntityTypeResponse response
          = this.invokeServerViaHttp(GET, uriText, SzEntityTypeResponse.class);
      long after = System.nanoTime();

      validateEntityTypeResponse(response,
                                 uriText,
                                 after - before,
                                 TRUE.equals(withRaw),
                                 expectedEntityType);
    });
  }


  @ParameterizedTest
  @MethodSource("getEntityTypeByClassParameters")
  public void getEntityTypeByClassViaJavaClientTest(
      String        entityClassCode,
      String        entityTypeCode,
      Boolean       withRaw,
      SzEntityType  expectedEntityType)
  {
    this.performTest(() -> {
      String suffix = "";
      if (withRaw != null) {
        suffix = "?withRaw=" + withRaw;
      }
      String uriText = this.formatServerUri(
          "entity-classes/" + entityClassCode
              + "/entity-types/" + entityTypeCode + suffix);
      long before = System.nanoTime();
      com.senzing.gen.api.model.SzEntityTypeResponse clientResponse
          = this.configApi.getEntityTypeByClass(
              entityClassCode, entityTypeCode, withRaw);
      long after = System.nanoTime();

      SzEntityTypeResponse response = jsonCopy(clientResponse,
                                               SzEntityTypeResponse.class);

      validateEntityTypeResponse(response,
                                 uriText,
                                 after - before,
                                 TRUE.equals(withRaw),
                                 expectedEntityType);
    });
  }

  private List<Arguments> getBadEntityTypeByClassParameters() {
    List<Arguments> result = new LinkedList<>();
    Boolean[] booleanVariants = {null, true, false};

    // try with both entity class code and entity type code unrecognized
    for (Boolean withRaw: booleanVariants) {
      Object[] argArray = { "DOES_NOT_EXIST", "DOES_NOT_EXIST", withRaw};
      result.add(arguments(argArray));
    }

    // try with combinations with mismatches and with good entity types but
    // bad entity class codes
    for (SzEntityType entityType: this.getInitialEntityTypes().values()) {
      for (Boolean withRaw: booleanVariants) {
        for (String entityClassCode: this.getInitialEntityClasses().keySet()) {
          if (entityClassCode.equals(entityType.getEntityClassCode())) {
            // try with an unrecognized entity class code
            entityClassCode = "DOES_NOT_EXIST";
          }
          Object[] argArray = {
              entityClassCode,
              entityType.getEntityTypeCode(),
              withRaw};

          result.add(arguments(argArray));
        }
      }
    }

    // try with all unrecognized entity type codes but good entity class codes
    for (Boolean withRaw: booleanVariants) {
      for (String entityClassCode: this.getInitialEntityClasses().keySet()) {
        Object[] argArray = {
            entityClassCode,
            "DOES_NOT_EXIST",
            withRaw};

        result.add(arguments(argArray));
      }
    }

    return result;
  }

  @ParameterizedTest
  @MethodSource("getBadEntityTypeByClassParameters")
  public void getUnrecognizedEntityTypeByClassTest(
      String  entityClassCode,
      String  entityTypeCode,
      Boolean withRaw)
  {
    this.performTest(() -> {
      String suffix = "";
      if (withRaw != null) {
        suffix = "?withRaw=" + withRaw;
      }
      String  uriText = this.formatServerUri(
          "entity-classes/" + entityClassCode
              + "/entity-types/" + entityTypeCode + suffix);
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      long before = System.nanoTime();
      try {
        SzEntityTypeResponse response
            = this.configServices.getEntityType(
            entityClassCode, entityTypeCode, TRUE.equals(withRaw), uriInfo);

        fail("Expected entity type \"" + entityTypeCode
              + "\" for entity class \"" + entityClassCode
              + "\" to NOT be found");

      } catch (NotFoundException expected) {
        SzErrorResponse response
            = (SzErrorResponse) expected.getResponse().getEntity();
        response.concludeTimers();
        long after = System.nanoTime();
        validateBasics(
            response, 404, GET, uriText, after - before);
      }
    });
  }

  @ParameterizedTest
  @MethodSource("getBadEntityTypeByClassParameters")
  public void getUnrecognizedEntityTypeByClassViaHttpTest(
      String  entityClassCode,
      String  entityTypeCode,
      Boolean withRaw)
  {
    this.performTest(() -> {
      String suffix = "";
      if (withRaw != null) {
        suffix = "?withRaw=" + withRaw;
      }
      String uriText = this.formatServerUri(
          "entity-classes/" + entityClassCode
              + "/entity-types/" + entityTypeCode + suffix);
      long before = System.nanoTime();
      SzErrorResponse response
          = this.invokeServerViaHttp(GET, uriText, SzErrorResponse.class);
      long after = System.nanoTime();

      validateBasics(
          response, 404, GET, uriText, after - before);
    });
  }

  @ParameterizedTest
  @MethodSource("getBadEntityTypeByClassParameters")
  public void getUnrecognizedEntityTypeByClassViaJavaClientTest(
      String  entityClassCode,
      String  entityTypeCode,
      Boolean withRaw)
  {
    this.performTest(() -> {
      String suffix = "";
      if (withRaw != null) {
        suffix = "?withRaw=" + withRaw;
      }
      String uriText = this.formatServerUri(
          "entity-classes/" + entityClassCode
              + "/entity-types/" + entityTypeCode + suffix);
      long before = System.nanoTime();
      try {
        com.senzing.gen.api.model.SzEntityTypeResponse clientResponse
          = this.configApi.getEntityTypeByClass(
              entityClassCode, entityTypeCode, withRaw);
        fail("Expected failure, but got success for bad entity type by class: "
                 + "entityClass=[ " + entityClassCode
                 + " ], entityType=[ " + entityTypeCode
                 + " ], withRaw=[ " + withRaw + " ]");

      } catch (HttpStatusCodeException expected) {
        long after = System.nanoTime();
        com.senzing.gen.api.model.SzErrorResponse clientResponse
            = jsonParse(expected.getResponseBodyAsString(),
                        com.senzing.gen.api.model.SzErrorResponse.class);

        SzErrorResponse response = jsonCopy(clientResponse,
                                            SzErrorResponse.class);
        validateBasics(
            response, 404, GET, uriText, after - before);
      }
    });
  }

  private List<Arguments> getAttrTypeParameters() {
    List<Arguments> result = new LinkedList<>();
    Boolean[] booleanVariants = {null, true, false};

    // create a list of possible attribute classes
    List<SzAttributeClass> attrClassList
        = new ArrayList<>(this.standardAttrTypesByClass.size() + 1);
    attrClassList.add(null);
    attrClassList.addAll(this.standardAttrTypesByClass.keySet());

    // create a list of the possible feature types
    List<String> featureTypeList
        = new ArrayList<>(this.standardAttrTypesByFeature.size() + 1);
    featureTypeList.add(null);
    featureTypeList.addAll(this.standardAttrTypesByFeature.keySet());

    for (Boolean withRaw: booleanVariants) {
      for (Boolean withInternal : booleanVariants) {
        for (SzAttributeClass attrClass : attrClassList) {
          for (String featureType : featureTypeList) {
            Object[] argArray = {
                withInternal,
                attrClass,
                featureType,
                withRaw,
                null};

            Set<String> expectedAttrTypeCodes;
            if (TRUE.equals(withInternal)) {
              expectedAttrTypeCodes = this.allAttrTypes;
            } else {
              expectedAttrTypeCodes = this.standardAttrTypes;
            }

            // check if we need to copy the expected set to filter it
            if (attrClass != null || featureType != null) {
              Set<String> set = new LinkedHashSet<>(expectedAttrTypeCodes);
              expectedAttrTypeCodes = set;
            }

            // check if filtering on attribute class
            if (attrClass != null) {
              Set<String> classAttrs = this.allAttrTypesByClass.get(attrClass);
              expectedAttrTypeCodes.removeIf(c -> !classAttrs.contains(c));
            }

            // check if filtering on feature type
            if (featureType != null) {
              Set<String> featAttrs = this.allAttrTypesByFeature.get(featureType);
              expectedAttrTypeCodes.removeIf(c -> !featAttrs.contains(c));
            }

            // set the expected set
            argArray[argArray.length - 1] = expectedAttrTypeCodes;

            // add the results
            result.add(arguments(argArray));
          }
        }
      }
    }
    return result;
  }

  private static String formatAttributeTypesTestInfo(
      Boolean           withInternal,
      SzAttributeClass  attributeClass,
      String            featureType,
      Boolean           withRaw)
  {
    return ("withInternal=[ " + withInternal
        + " ], attributeClass=[ " + attributeClass
        + " ], featureType=[ " + featureType
        + " ], withRaw=[ " + withRaw + " ]");
  }

  private static String formatAttributeTypesQueryString(
      Boolean           withInternal,
      SzAttributeClass  attributeClass,
      String            featureType,
      Boolean           withRaw)
  {
    StringBuilder sb = new StringBuilder();
    String prefix = "?";
    if (withInternal != null) {
      sb.append(prefix).append("withInternal=").append(withInternal);
      prefix = "&";
    }
    if (attributeClass != null) {
      sb.append(prefix).append("attributeClass=").append(attributeClass);
      prefix = "&";
    }
    if (featureType != null) {
      sb.append(prefix).append("featureType=").append(featureType);
      prefix = "&";
    }
    if (withRaw != null) {
      sb.append(prefix).append("withRaw=").append(withRaw);
      prefix = "&";
    }
    return sb.toString();
  }

  @ParameterizedTest
  @MethodSource("getAttrTypeParameters")
  public void getAttributeTypesTest(Boolean           withInternal,
                                    SzAttributeClass  attributeClass,
                                    String            featureType,
                                    Boolean           withRaw,
                                    Set<String>       expectedAttrTypeCodes)
  {
    this.performTest(() -> {
      String testInfo = formatAttributeTypesTestInfo(withInternal,
                                                     attributeClass,
                                                     featureType,
                                                     withRaw);

      String suffix = formatAttributeTypesQueryString(withInternal,
                                                      attributeClass,
                                                      featureType,
                                                      withRaw);
      String  uriText = this.formatServerUri("attribute-types" + suffix);
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      long before = System.nanoTime();
      SzAttributeTypesResponse response
          = this.configServices.getAttributeTypes(
              TRUE.equals(withInternal),
              (attributeClass == null) ? null : attributeClass.toString(),
              featureType,
              TRUE.equals(withRaw),
              uriInfo);

      response.concludeTimers();
      long after = System.nanoTime();

      validateAttributeTypesResponse(testInfo,
                                     response,
                                     uriText,
                                     after - before,
                                     TRUE.equals(withRaw),
                                     expectedAttrTypeCodes);
    });
  }

  @ParameterizedTest
  @MethodSource("getAttrTypeParameters")
  public void getAttributeTypesViaHttpTest(
      Boolean           withInternal,
      SzAttributeClass  attributeClass,
      String            featureType,
      Boolean           withRaw,
      Set<String>      expectedAttrTypeCodes)
  {
    this.performTest(() -> {
      String testInfo = formatAttributeTypesTestInfo(withInternal,
                                                     attributeClass,
                                                     featureType,
                                                     withRaw);

      String suffix = formatAttributeTypesQueryString(withInternal,
                                                      attributeClass,
                                                      featureType,
                                                      withRaw);
      String uriText = this.formatServerUri("attribute-types" + suffix);
      long before = System.nanoTime();
      SzAttributeTypesResponse response = this.invokeServerViaHttp(
          GET, uriText, SzAttributeTypesResponse.class);
      long after = System.nanoTime();

      validateAttributeTypesResponse(testInfo,
                                     response,
                                     uriText,
                                     after - before,
                                     TRUE.equals(withRaw),
                                     expectedAttrTypeCodes);
    });
  }

  @ParameterizedTest
  @MethodSource("getAttrTypeParameters")
  public void getAttributeTypesViaJavaClientTest(
      Boolean           withInternal,
      SzAttributeClass  attributeClass,
      String            featureType,
      Boolean           withRaw,
      Set<String>       expectedAttrTypeCodes)
  {
    this.performTest(() -> {
      String testInfo = formatAttributeTypesTestInfo(withInternal,
                                                     attributeClass,
                                                     featureType,
                                                     withRaw);

      String suffix = formatAttributeTypesQueryString(withInternal,
                                                      attributeClass,
                                                      featureType,
                                                      withRaw);
      String uriText = this.formatServerUri("attribute-types" + suffix);

      com.senzing.gen.api.model.SzAttributeClass attrClass = null;
      if (attributeClass != null) {
        attrClass = com.senzing.gen.api.model.SzAttributeClass.valueOf(
            attributeClass.toString());
      }

      long before = System.nanoTime();
      com.senzing.gen.api.model.SzAttributeTypesResponse clientResponse
          = this.configApi.getAttributeTypes(
              withInternal, attrClass, featureType, withRaw);
      long after = System.nanoTime();

      SzAttributeTypesResponse response
          = jsonCopy(clientResponse, SzAttributeTypesResponse.class);

      validateAttributeTypesResponse(testInfo,
                                     response,
                                     uriText,
                                     after - before,
                                     TRUE.equals(withRaw),
                                     expectedAttrTypeCodes);
    });
  }


  private Set<String> allAttributeCodes() {
    return this.allAttrTypes;
  }

  @ParameterizedTest
  @MethodSource("allAttributeCodes")
  public void getAttributeTypeTest(String attrCode) {
    this.performTest(() -> {
      String  uriText = this.formatServerUri(
          "attribute-types/" + attrCode);
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      long before = System.nanoTime();
      SzAttributeTypeResponse response
          = this.configServices.getAttributeType(attrCode, false, uriInfo);
      response.concludeTimers();
      long after = System.nanoTime();

      validateAttributeTypeResponse(response,
                                    uriText,
                                    attrCode,
                                    after - before,
                                    null);
    });
  }

  @ParameterizedTest
  @MethodSource("allAttributeCodes")
  public void getAttributeTypeViaHttpTest(String attrCode) {
    this.performTest(() -> {
      String  uriText = this.formatServerUri(
          "attribute-types/" + attrCode);
      long    before  = System.nanoTime();
      SzAttributeTypeResponse response
          = this.invokeServerViaHttp(GET, uriText, SzAttributeTypeResponse.class);
      long after = System.nanoTime();

      validateAttributeTypeResponse(response,
                                    uriText,
                                    attrCode,
                                    after - before,
                                    null);
    });
  }

  @ParameterizedTest
  @MethodSource("allAttributeCodes")
  public void getAttributeTypeViaJavaClientTest(String attrCode) {
    this.performTest(() -> {
      String  uriText = this.formatServerUri(
          "attribute-types/" + attrCode);
      long    before  = System.nanoTime();

      com.senzing.gen.api.model.SzAttributeTypeResponse clientResponse
          = this.configApi.getAttributeType(attrCode, null);

      long after = System.nanoTime();

      SzAttributeTypeResponse response
          = jsonCopy(clientResponse, SzAttributeTypeResponse.class);

      validateAttributeTypeResponse(response,
                                    uriText,
                                    attrCode,
                                    after - before,
                                    null);
    });
  }

  @ParameterizedTest
  @MethodSource("allAttributeCodes")
  public void getAttributeTypeWithoutRawTest(String attrCode) {
    this.performTest(() -> {
      String  uriText = this.formatServerUri(
          "attribute-types/" + attrCode + "?withRaw=false");
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      long before = System.nanoTime();
      SzAttributeTypeResponse response
          = this.configServices.getAttributeType(attrCode, false, uriInfo);
      response.concludeTimers();
      long after = System.nanoTime();

      validateAttributeTypeResponse(response,
                                    uriText,
                                    attrCode,
                                    after - before,
                                    false);
    });
  }

  @ParameterizedTest
  @MethodSource("allAttributeCodes")
  public void getAttributeTypeWithoutRawViaHttpTest(String attrCode) {
    this.performTest(() -> {
      String  uriText = this.formatServerUri(
          "attribute-types/" + attrCode + "?withRaw=false");
      long before  = System.nanoTime();
      SzAttributeTypeResponse response
          = this.invokeServerViaHttp(GET, uriText, SzAttributeTypeResponse.class);
      long after = System.nanoTime();

      validateAttributeTypeResponse(response,
                                    uriText,
                                    attrCode,
                                    after - before,
                                    false);
    });
  }

  @ParameterizedTest
  @MethodSource("allAttributeCodes")
  public void getAttributeTypeWithoutRawViaJavaClientTest(String attrCode) {
    this.performTest(() -> {
      String  uriText = this.formatServerUri(
          "attribute-types/" + attrCode + "?withRaw=false");
      long before  = System.nanoTime();
      com.senzing.gen.api.model.SzAttributeTypeResponse clientResponse
          = this.configApi.getAttributeType(attrCode, false);
      long after = System.nanoTime();

      SzAttributeTypeResponse response
          = jsonCopy(clientResponse, SzAttributeTypeResponse.class);

      validateAttributeTypeResponse(response,
                                    uriText,
                                    attrCode,
                                    after - before,
                                    false);
    });
  }

  @ParameterizedTest
  @MethodSource("allAttributeCodes")
  public void getAttributeTypeWithRawTest(String attrCode) {
    this.performTest(() -> {
      String  uriText = this.formatServerUri(
          "attribute-types/" + attrCode + "?withRaw=true");
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      long before = System.nanoTime();
      SzAttributeTypeResponse response
          = this.configServices.getAttributeType(attrCode, true, uriInfo);
      response.concludeTimers();
      long after = System.nanoTime();

      validateAttributeTypeResponse(response,
                                    uriText,
                                    attrCode,
                                    after - before,
                                    true);
    });
  }

  @ParameterizedTest
  @MethodSource("allAttributeCodes")
  public void getAttributeTypeWithRawViaHttpTest(String attrCode) {
    this.performTest(() -> {
      String  uriText = this.formatServerUri(
          "attribute-types/" + attrCode + "?withRaw=true");
      long before  = System.nanoTime();
      SzAttributeTypeResponse response
          = this.invokeServerViaHttp(GET, uriText, SzAttributeTypeResponse.class);
      long after = System.nanoTime();

      validateAttributeTypeResponse(response,
                                    uriText,
                                    attrCode,
                                    after - before,
                                    true);
    });
  }

  @ParameterizedTest
  @MethodSource("allAttributeCodes")
  public void getAttributeTypeWithRawViaJavaClientTest(String attrCode) {
    this.performTest(() -> {
      String  uriText = this.formatServerUri(
          "attribute-types/" + attrCode + "?withRaw=true");
      long before  = System.nanoTime();
      com.senzing.gen.api.model.SzAttributeTypeResponse clientResponse
          = this.configApi.getAttributeType(attrCode, true);
      long after = System.nanoTime();

      SzAttributeTypeResponse response
          = jsonCopy(clientResponse, SzAttributeTypeResponse.class);

      validateAttributeTypeResponse(response,
                                    uriText,
                                    attrCode,
                                    after - before,
                                    true);
    });
  }

  @Test public void getActiveConfigTest() {
    this.performTest(() -> {
      String  uriText = this.formatServerUri("configs/active");
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      long before = System.nanoTime();
      SzConfigResponse response
          = this.configServices.getActiveConfig(uriInfo);
      response.concludeTimers();
      long after = System.nanoTime();

      validateConfigResponse(response,
                             uriText,
                             after - before,
                             this.getInitialDataSources().keySet());
    });
  }

  @Test public void getActiveConfigViaHttpTest() {
    this.performTest(() -> {
      String  uriText = this.formatServerUri("configs/active");
      long    before  = System.nanoTime();
      SzConfigResponse response
          = this.invokeServerViaHttp(GET, uriText, SzConfigResponse.class);
      long after = System.nanoTime();

      validateConfigResponse(response,
                             uriText,
                             after - before,
                             this.getInitialDataSources().keySet());
    });
  }

  @Test public void getActiveConfigViaJavaClientTest() {
    this.performTest(() -> {
      String  uriText = this.formatServerUri("configs/active");
      long    before  = System.nanoTime();
      com.senzing.gen.api.model.SzConfigResponse clientResponse
          = this.configApi.getActiveConfig();
      long after = System.nanoTime();

      SzConfigResponse response = jsonCopy(clientResponse,
                                           SzConfigResponse.class);

      validateConfigResponse(response,
                             uriText,
                             after - before,
                             this.getInitialDataSources().keySet());
    });
  }

  @Test public void getTemplateConfigTest() {
    this.performTest(() -> {
      String  uriText = this.formatServerUri("configs/template");
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      long before = System.nanoTime();
      SzConfigResponse response
          = this.configServices.getTemplateConfig(uriInfo);
      response.concludeTimers();
      long after = System.nanoTime();

      validateConfigResponse(response,
                             uriText,
                             after - before,
                             this.getDefaultDataSources().keySet());
    });
  }

  @Test public void getTemplateConfigViaHttpTest() {
    this.performTest(() -> {
      String  uriText = this.formatServerUri("configs/template");
      long    before  = System.nanoTime();
      SzConfigResponse response
          = this.invokeServerViaHttp(GET, uriText, SzConfigResponse.class);
      long after = System.nanoTime();

      validateConfigResponse(response,
                             uriText,
                             after - before,
                             this.getDefaultDataSources().keySet());
    });
  }

  @Test public void getTemplateConfigViaJavaClientTest() {
    this.performTest(() -> {
      String  uriText = this.formatServerUri("configs/template");
      long    before  = System.nanoTime();
      com.senzing.gen.api.model.SzConfigResponse clientResponse
          = this.configApi.getTemplateConfig();
      long after = System.nanoTime();

      SzConfigResponse response = jsonCopy(clientResponse,
                                           SzConfigResponse.class);

      validateConfigResponse(response,
                             uriText,
                             after - before,
                             this.getDefaultDataSources().keySet());
    });
  }
}
