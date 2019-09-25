package com.senzing.api.services;

import com.senzing.api.model.*;
import com.senzing.g2.engine.G2ConfigMgr;
import com.senzing.g2.engine.G2ConfigMgrJNI;
import com.senzing.g2.engine.Result;
import com.senzing.repomgr.RepositoryManager;
import com.senzing.util.JsonUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.ws.rs.core.UriInfo;
import java.io.*;
import java.util.*;

import static com.senzing.api.model.SzHttpMethod.GET;
import static com.senzing.util.CollectionUtilities.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.TestInstance.Lifecycle;

@TestInstance(Lifecycle.PER_CLASS)
public class ConfigServicesTest extends AbstractServiceTest
{
  private static final Set<String> CUSTOM_DATA_SOURCES;

  private static final Set<String> DEFAULT_DATA_SOURCES;

  private static final Set<String> EXPECTED_DATA_SOURCES;

  static {
    Set<String> customSources   = null;
    Set<String> expectedSources = null;
    Set<String> defaultSources  = null;

    try {
      customSources   = new LinkedHashSet<>();
      expectedSources = new LinkedHashSet<>();
      defaultSources  = new LinkedHashSet<>();

      defaultSources.add("TEST");
      defaultSources.add("SEARCH");
      defaultSources = Collections.unmodifiableSet(defaultSources);

      customSources.add("EMPLOYEES");
      customSources.add("CUSTOMERS");
      customSources.add("VENDORS");
      customSources = Collections.unmodifiableSet(customSources);

      expectedSources.addAll(defaultSources);
      expectedSources.addAll(customSources);
      expectedSources = Collections.unmodifiableSet(expectedSources);

    } finally {
      DEFAULT_DATA_SOURCES  = defaultSources;
      CUSTOM_DATA_SOURCES   = customSources;
      EXPECTED_DATA_SOURCES = expectedSources;
    }
  }

  private ConfigServices configServices;

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
    this.initializeTestEnvironment();
    this.configServices = new ConfigServices();
    String configJson = null;
    if (NATIVE_API_AVAILABLE) {
      String initJsonText = readInitJsonFile();
      G2ConfigMgr configMgr = new G2ConfigMgrJNI();
      configMgr.initV2("testApiServer", initJsonText, false);
      try {
        Result<Long> configId = new Result<>();
        int returnCode = configMgr.getDefaultConfigID(configId);
        if (returnCode != 0) {
          throw new IllegalStateException(
              "Failed to get default config ID: "
                  + configMgr.getLastExceptionCode()
                  + configMgr.getLastException());
        }
        StringBuffer sb = new StringBuffer();
        returnCode = configMgr.getConfig(configId.getValue(), sb);
        if (returnCode != 0) {
          throw new IllegalStateException(
              "Failed to get default config for ID ("
                  + configId.getValue() + "): "
                  + configMgr.getLastExceptionCode()
                  + configMgr.getLastException());
        }
        configJson = sb.toString();

      } finally {
        configMgr.destroy();
      }

    } else {
      // use a fixed G2 config file from resources
      final String staticConfig = "static-g2-config.json";
      try (InputStream is = this.getClass().getResourceAsStream(staticConfig);
           InputStreamReader isr = new InputStreamReader(is, "UTF-8");
           BufferedReader br = new BufferedReader(isr))
      {
        StringBuilder sb = new StringBuilder();
        for (int nextChar = br.read(); nextChar >= 0; nextChar = br.read()) {
          if (nextChar == 0) continue;
          sb.append((char) nextChar);
        }
        configJson = sb.toString();

      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    JsonObject rootObject = JsonUtils.parseJsonObject(configJson);
    JsonObject g2Config   = rootObject.getJsonObject("G2_CONFIG");
    JsonArray  cfgAttrs   = g2Config.getJsonArray("CFG_ATTR");

    for (int index = 0; index < cfgAttrs.size(); index++) {
      JsonObject attrType = cfgAttrs.getJsonObject(index);

      String attrCode   = attrType.getString("ATTR_CODE");
      String attrClass  = JsonUtils.getString(attrType,"ATTR_CLASS");
      String internal   = JsonUtils.getString(attrType,"INTERNAL");
      String fTypeCode  = JsonUtils.getString(attrType,"FTYPE_CODE");

      if (attrClass != null && attrClass.trim().length() == 0) {
        attrClass = null;
      }
      if (fTypeCode != null && fTypeCode.trim().length() == 0) {
        fTypeCode = null;
      }

      SzAttributeClass szAttrClass
          = SzAttributeClass.parseAttributeClass(attrClass);

      boolean isInternal = internal.equalsIgnoreCase("YES");

      if (isInternal) {
        putInMap(this.internalAttrTypesByClass, szAttrClass, attrCode);
        putInMap(this.allAttrTypesByClass, szAttrClass, attrCode);
        putInMap(this.internalAttrTypesByFeature, fTypeCode, attrCode);
        putInMap(this.allAttrTypesByFeature, fTypeCode, attrCode);
        this.internalAttrTypes.add(attrCode);
        this.allAttrTypes.add(attrCode);

      } else {
        putInMap(this.standardAttrTypesByClass, szAttrClass, attrCode);
        putInMap(this.allAttrTypesByClass, szAttrClass, attrCode);
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
    this.teardownTestEnvironment();
  }

  @Test public void getDataSourcesTest() {
    this.performTest(() -> {
      String  uriText = this.formatServerUri("data-sources");
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      long            before    = System.currentTimeMillis();
      SzDataSourcesResponse response
          = this.configServices.getDataSources(false, uriInfo);
      response.concludeTimers();
      long            after     = System.currentTimeMillis();

      this.validateDataSourcesResponse(response,
                                       before,
                                       after,
                                       null,
                                       EXPECTED_DATA_SOURCES);
    });
  }

  @Test public void getDataSourcesViaHttpTest() {
    this.performTest(() -> {
      String  uriText = this.formatServerUri("data-sources");
      long    before  = System.currentTimeMillis();
      SzDataSourcesResponse response
          = this.invokeServerViaHttp(GET, uriText, SzDataSourcesResponse.class);
      long after = System.currentTimeMillis();

      this.validateDataSourcesResponse(response,
                                       before,
                                       after,
                                       null,
                                       EXPECTED_DATA_SOURCES);
    });
  }

  @Test public void getDataSourcesWithoutRawTest() {
    this.performTest(() -> {
      String  uriText = this.formatServerUri("data-sources?withRaw=false");
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      long            before    = System.currentTimeMillis();
      SzDataSourcesResponse response
          = this.configServices.getDataSources(false, uriInfo);
      response.concludeTimers();
      long            after     = System.currentTimeMillis();

      this.validateDataSourcesResponse(response,
                                       before,
                                       after,
                                       false,
                                       EXPECTED_DATA_SOURCES);
    });
  }

  @Test public void getDataSourcesWithoutRawViaHttpTest() {
    this.performTest(() -> {
      String  uriText = this.formatServerUri("data-sources?withRaw=false");
      long    before  = System.currentTimeMillis();
      SzDataSourcesResponse response
          = this.invokeServerViaHttp(GET, uriText, SzDataSourcesResponse.class);
      long after = System.currentTimeMillis();

      this.validateDataSourcesResponse(response,
                                       before,
                                       after,
                                       false,
                                       EXPECTED_DATA_SOURCES);
    });
  }

  @Test public void getDataSourcesWithRawTest() {
    this.performTest(() -> {
      String  uriText = this.formatServerUri("data-sources?withRaw=true");
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      long            before    = System.currentTimeMillis();
      SzDataSourcesResponse response
          = this.configServices.getDataSources(true, uriInfo);
      response.concludeTimers();
      long            after     = System.currentTimeMillis();

      this.validateDataSourcesResponse(response,
                                       before,
                                       after,
                                       true,
                                       EXPECTED_DATA_SOURCES);
    });
  }

  @Test public void getDataSourcesWithRawViaHttpTest() {
    this.performTest(() -> {
      String  uriText = this.formatServerUri("data-sources?withRaw=true");
      long    before  = System.currentTimeMillis();
      SzDataSourcesResponse response
          = this.invokeServerViaHttp(GET, uriText, SzDataSourcesResponse.class);
      long after = System.currentTimeMillis();

      this.validateDataSourcesResponse(response,
                                       before,
                                       after,
                                       true,
                                       EXPECTED_DATA_SOURCES);
    });
  }

  @Test public void getAttributeTypesTest() {
    this.performTest(() -> {
      String  uriText = this.formatServerUri("attribute-types");
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      long before = System.currentTimeMillis();
      SzAttributeTypesResponse response
          = this.configServices.getAttributeTypes(false,
                                                  null,
                                                  null,
                                                  false,
                                                  uriInfo);
      response.concludeTimers();
      long after = System.currentTimeMillis();

      this.validateAttributeTypesResponse(response,
                                          uriText,
                                          before,
                                          after,
                                          null,
                                          this.standardAttrTypes);
    });
  }

  @Test public void getAttributeTypesViaHttpTest() {
    this.performTest(() -> {
      String  uriText = this.formatServerUri("attribute-types");
      long    before  = System.currentTimeMillis();
      SzAttributeTypesResponse response
          = this.invokeServerViaHttp(GET, uriText, SzAttributeTypesResponse.class);
      long after = System.currentTimeMillis();

      this.validateAttributeTypesResponse(response,
                                          uriText,
                                          before,
                                          after,
                                          null,
                                          this.standardAttrTypes);
    });
  }

  @Test public void getAttributeTypesWithRawTest() {
    this.performTest(() -> {
      String  uriText = this.formatServerUri("attribute-types?withRaw=true");
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      long before = System.currentTimeMillis();
      SzAttributeTypesResponse response
          = this.configServices.getAttributeTypes(false,
                                                  null,
                                                  null,
                                                  true,
                                                  uriInfo);
      response.concludeTimers();
      long after = System.currentTimeMillis();

      this.validateAttributeTypesResponse(response,
                                          uriText,
                                          before,
                                          after,
                                          true,
                                          this.standardAttrTypes);
    });
  }

  @Test public void getAttributeTypesWithRawViaHttpTest() {
    this.performTest(() -> {
      String  uriText = this.formatServerUri("attribute-types?withRaw=true");
      long    before  = System.currentTimeMillis();
      SzAttributeTypesResponse response
          = this.invokeServerViaHttp(GET, uriText, SzAttributeTypesResponse.class);
      long after = System.currentTimeMillis();

      this.validateAttributeTypesResponse(response,
                                          uriText,
                                          before,
                                          after,
                                          true,
                                          this.standardAttrTypes);
    });
  }

  @Test public void getAttributeTypesWithoutInternalTest() {
    this.performTest(() -> {
      String  uriText = this.formatServerUri(
          "attribute-types?withInternal=false");
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      long before = System.currentTimeMillis();
      SzAttributeTypesResponse response
          = this.configServices.getAttributeTypes(false,
                                                  null,
                                                  null,
                                                  false,
                                                  uriInfo);
      response.concludeTimers();
      long after = System.currentTimeMillis();

      this.validateAttributeTypesResponse(response,
                                          uriText,
                                          before,
                                          after,
                                          null,
                                          this.standardAttrTypes);
    });
  }

  @Test public void getAttributeTypesWithoutInternalViaHttpTest() {
    this.performTest(() -> {
      String  uriText = this.formatServerUri(
          "attribute-types?withInternal=false");
      long    before  = System.currentTimeMillis();
      SzAttributeTypesResponse response
          = this.invokeServerViaHttp(GET, uriText, SzAttributeTypesResponse.class);
      long after = System.currentTimeMillis();

      this.validateAttributeTypesResponse(response,
                                          uriText,
                                          before,
                                          after,
                                          null,
                                          this.standardAttrTypes);
    });
  }

  @Test public void getAttributeTypesWithInternalTest() {
    this.performTest(() -> {
      String  uriText = this.formatServerUri(
          "attribute-types?withInternal=true");
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      long before = System.currentTimeMillis();
      SzAttributeTypesResponse response
          = this.configServices.getAttributeTypes(true,
                                                  null,
                                                  null,
                                                  false,
                                                  uriInfo);
      response.concludeTimers();
      long after = System.currentTimeMillis();

      this.validateAttributeTypesResponse(response,
                                          uriText,
                                          before,
                                          after,
                                          null,
                                          this.allAttrTypes);
    });
  }

  @Test public void getAttributeTypesWithInternalViaHttpTest() {
    this.performTest(() -> {
      String  uriText = this.formatServerUri(
          "attribute-types?withInternal=true");
      long    before  = System.currentTimeMillis();
      SzAttributeTypesResponse response
          = this.invokeServerViaHttp(GET, uriText, SzAttributeTypesResponse.class);
      long after = System.currentTimeMillis();

      this.validateAttributeTypesResponse(response,
                                          uriText,
                                          before,
                                          after,
                                          null,
                                          this.allAttrTypes);
    });
  }

  private Set<SzAttributeClass> allAttributeClasses() {
    return this.allAttrTypesByClass.keySet();
  }

  private Set<SzAttributeClass> standardAttributeClasses() {
    return this.standardAttrTypesByClass.keySet();
  }

  @ParameterizedTest
  @MethodSource("standardAttributeClasses")
  public void getAttributeTypesForClassTest(SzAttributeClass attrClass) {
    this.performTest(() -> {
      Set<String> attrTypes = this.standardAttrTypesByClass.get(attrClass);
      String  uriText = this.formatServerUri(
          "attribute-types?attributeClass=" + attrClass);
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      long before = System.currentTimeMillis();
      SzAttributeTypesResponse response
          = this.configServices.getAttributeTypes(false,
                                                  attrClass.toString(),
                                                  null,
                                                  false,
                                                  uriInfo);
      response.concludeTimers();
      long after = System.currentTimeMillis();

      this.validateAttributeTypesResponse(response,
                                          uriText,
                                          before,
                                          after,
                                          null,
                                          attrTypes);
    });
  }

  @ParameterizedTest
  @MethodSource("standardAttributeClasses")
  public void getAttributeTypesForClassViaHttpTest(SzAttributeClass attrClass) {
    this.performTest(() -> {
      Set<String> attrTypes = this.standardAttrTypesByClass.get(attrClass);
      String  uriText = this.formatServerUri(
          "attribute-types?attributeClass=" + attrClass);
      long    before  = System.currentTimeMillis();
      SzAttributeTypesResponse response
          = this.invokeServerViaHttp(GET, uriText, SzAttributeTypesResponse.class);
      long after = System.currentTimeMillis();

      this.validateAttributeTypesResponse(response,
                                          uriText,
                                          before,
                                          after,
                                          null,
                                          attrTypes);
    });
  }

  @ParameterizedTest
  @MethodSource("allAttributeClasses")
  public void getAttributeTypesForClassWithInternalTest(SzAttributeClass attrClass) {
    this.performTest(() -> {
      Set<String> attrTypes = this.allAttrTypesByClass.get(attrClass);
      String  uriText = this.formatServerUri(
          "attribute-types?withInternal=true&attributeClass=" + attrClass);
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      long before = System.currentTimeMillis();
      SzAttributeTypesResponse response
          = this.configServices.getAttributeTypes(true,
                                                  attrClass.toString(),
                                                  null,
                                                  false,
                                                  uriInfo);
      response.concludeTimers();
      long after = System.currentTimeMillis();

      this.validateAttributeTypesResponse(response,
                                          uriText,
                                          before,
                                          after,
                                          null,
                                          attrTypes);
    });
  }

  @ParameterizedTest
  @MethodSource("allAttributeClasses")
  public void getAttributeTypesForClassWithInternalViaHttpTest(
      SzAttributeClass attrClass) {
    this.performTest(() -> {
      Set<String> attrTypes = this.allAttrTypesByClass.get(attrClass);
      String  uriText = this.formatServerUri(
          "attribute-types?withInternal=true&attributeClass=" + attrClass);
      long    before  = System.currentTimeMillis();
      SzAttributeTypesResponse response
          = this.invokeServerViaHttp(GET, uriText, SzAttributeTypesResponse.class);
      long after = System.currentTimeMillis();

      this.validateAttributeTypesResponse(response,
                                          uriText,
                                          before,
                                          after,
                                          null,
                                          attrTypes);
    });
  }

  private Set<String> allFeatureTypes() {
    return this.allAttrTypesByFeature.keySet();
  }

  private Set<String> standardFeatureTypes() {
    return this.standardAttrTypesByFeature.keySet();
  }

  @ParameterizedTest
  @MethodSource("standardFeatureTypes")
  public void getAttributeTypesForFeatureTest(String featureType) {
    this.performTest(() -> {
      Set<String> attrTypes = this.standardAttrTypesByFeature.get(featureType);
      String  uriText = this.formatServerUri(
          "attribute-types?featureType=" + featureType);
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      long before = System.currentTimeMillis();
      SzAttributeTypesResponse response
          = this.configServices.getAttributeTypes(false,
                                                  null,
                                                  featureType,
                                                  false,
                                                  uriInfo);
      response.concludeTimers();
      long after = System.currentTimeMillis();

      this.validateAttributeTypesResponse(response,
                                          uriText,
                                          before,
                                          after,
                                          null,
                                          attrTypes);
    });
  }

  @ParameterizedTest
  @MethodSource("standardFeatureTypes")
  public void getAttributeTypesForFeatureViaHttpTest(String featureType) {
    this.performTest(() -> {
      Set<String> attrTypes = this.standardAttrTypesByFeature.get(featureType);
      String  uriText = this.formatServerUri(
          "attribute-types?featureType=" + featureType);
      long    before  = System.currentTimeMillis();
      SzAttributeTypesResponse response
          = this.invokeServerViaHttp(GET, uriText, SzAttributeTypesResponse.class);
      long after = System.currentTimeMillis();

      this.validateAttributeTypesResponse(response,
                                          uriText,
                                          before,
                                          after,
                                          null,
                                          attrTypes);
    });
  }

  @ParameterizedTest
  @MethodSource("allFeatureTypes")
  public void getAttributeTypesForFeatureWithInternalTest(String featureType) {
    this.performTest(() -> {
      Set<String> attrTypes = this.allAttrTypesByFeature.get(featureType);
      String  uriText = this.formatServerUri(
          "attribute-types?withInternal=true&featureType=" + featureType);
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      long before = System.currentTimeMillis();
      SzAttributeTypesResponse response
          = this.configServices.getAttributeTypes(true,
                                                  null,
                                                  featureType,
                                                  false,
                                                  uriInfo);
      response.concludeTimers();
      long after = System.currentTimeMillis();

      this.validateAttributeTypesResponse(response,
                                          uriText,
                                          before,
                                          after,
                                          null,
                                          attrTypes);
    });
  }

  @ParameterizedTest
  @MethodSource("allFeatureTypes")
  public void getAttributeTypesForFeatureWithInternalViaHttpTest(
      String featureType)
  {
    this.performTest(() -> {
      Set<String> attrTypes = this.allAttrTypesByFeature.get(featureType);
      String  uriText = this.formatServerUri(
          "attribute-types?withInternal=true&featureType=" + featureType);
      long    before  = System.currentTimeMillis();
      SzAttributeTypesResponse response
          = this.invokeServerViaHttp(GET, uriText, SzAttributeTypesResponse.class);
      long after = System.currentTimeMillis();

      this.validateAttributeTypesResponse(response,
                                          uriText,
                                          before,
                                          after,
                                          null,
                                          attrTypes);
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

      long before = System.currentTimeMillis();
      SzAttributeTypeResponse response
          = this.configServices.getAttributeType(attrCode, false, uriInfo);
      response.concludeTimers();
      long after = System.currentTimeMillis();

      this.validateAttributeTypeResponse(response,
                                         attrCode,
                                         before,
                                         after,
                                         null);
    });
  }

  @ParameterizedTest
  @MethodSource("allAttributeCodes")
  public void getAttributeTypeViaHttpTest(String attrCode) {
    this.performTest(() -> {
      String  uriText = this.formatServerUri(
          "attribute-types/" + attrCode);
      long    before  = System.currentTimeMillis();
      SzAttributeTypeResponse response
          = this.invokeServerViaHttp(GET, uriText, SzAttributeTypeResponse.class);
      long after = System.currentTimeMillis();

      this.validateAttributeTypeResponse(response,
                                         attrCode,
                                         before,
                                         after,
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

      long before = System.currentTimeMillis();
      SzAttributeTypeResponse response
          = this.configServices.getAttributeType(attrCode, false, uriInfo);
      response.concludeTimers();
      long after = System.currentTimeMillis();

      this.validateAttributeTypeResponse(response,
                                         attrCode,
                                         before,
                                         after,
                                         false);
    });
  }

  @ParameterizedTest
  @MethodSource("allAttributeCodes")
  public void getAttributeTypeWithoutRawViaHttpTest(String attrCode) {
    this.performTest(() -> {
      String  uriText = this.formatServerUri(
          "attribute-types/" + attrCode + "?withRaw=false");
      long before  = System.currentTimeMillis();
      SzAttributeTypeResponse response
          = this.invokeServerViaHttp(GET, uriText, SzAttributeTypeResponse.class);
      long after = System.currentTimeMillis();

      this.validateAttributeTypeResponse(response,
                                         attrCode,
                                         before,
                                         after,
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

      long before = System.currentTimeMillis();
      SzAttributeTypeResponse response
          = this.configServices.getAttributeType(attrCode, true, uriInfo);
      response.concludeTimers();
      long after = System.currentTimeMillis();

      this.validateAttributeTypeResponse(response,
                                         attrCode,
                                         before,
                                         after,
                                         true);
    });
  }

  @ParameterizedTest
  @MethodSource("allAttributeCodes")
  public void getAttributeTypeWithRawViaHttpTest(String attrCode) {
    this.performTest(() -> {
      String  uriText = this.formatServerUri(
          "attribute-types/" + attrCode + "?withRaw=true");
      long before  = System.currentTimeMillis();
      SzAttributeTypeResponse response
          = this.invokeServerViaHttp(GET, uriText, SzAttributeTypeResponse.class);
      long after = System.currentTimeMillis();

      this.validateAttributeTypeResponse(response,
                                         attrCode,
                                         before,
                                         after,
                                         true);
    });
  }

  @Test public void getCurrentConfigTest() {
    this.performTest(() -> {
      String  uriText = this.formatServerUri("config/current");
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      long before = System.currentTimeMillis();
      SzConfigResponse response
          = this.configServices.getCurrentConfig(uriInfo);
      response.concludeTimers();
      long after = System.currentTimeMillis();

      this.validateConfigResponse(response,
                                  uriText,
                                  before,
                                  after,
                                  EXPECTED_DATA_SOURCES);
    });
  }

  @Test public void getCurrentConfigViaHttpTest() {
    this.performTest(() -> {
      String  uriText = this.formatServerUri("config/current");
      long    before  = System.currentTimeMillis();
      SzConfigResponse response
          = this.invokeServerViaHttp(GET, uriText, SzConfigResponse.class);
      long after = System.currentTimeMillis();

      this.validateConfigResponse(response,
                                  uriText,
                                  before,
                                  after,
                                  EXPECTED_DATA_SOURCES);
    });
  }

  @Test public void getDefaultConfigTest() {
    this.performTest(() -> {
      String  uriText = this.formatServerUri("config/default");
      UriInfo uriInfo = this.newProxyUriInfo(uriText);

      long before = System.currentTimeMillis();
      SzConfigResponse response
          = this.configServices.getDefaultConfig(uriInfo);
      response.concludeTimers();
      long after = System.currentTimeMillis();

      this.validateConfigResponse(response,
                                  uriText,
                                  before,
                                  after,
                                  DEFAULT_DATA_SOURCES);
    });
  }

  @Test public void getDefaultConfigViaHttpTest() {
    this.performTest(() -> {
      String  uriText = this.formatServerUri("config/default");
      long    before  = System.currentTimeMillis();
      SzConfigResponse response
          = this.invokeServerViaHttp(GET, uriText, SzConfigResponse.class);
      long after = System.currentTimeMillis();

      this.validateConfigResponse(response,
                                  uriText,
                                  before,
                                  after,
                                  DEFAULT_DATA_SOURCES);
    });
  }
}
