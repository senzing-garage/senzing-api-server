package com.senzing.api.services;

import com.senzing.api.model.*;
import com.senzing.g2.engine.G2Config;
import com.senzing.g2.engine.G2ConfigJNI;
import com.senzing.repomgr.RepositoryManager;
import com.senzing.util.JsonUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.ws.rs.core.UriInfo;
import java.io.*;
import java.util.*;

import static com.senzing.api.model.SzHttpMethod.GET;
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

  private static <T> Map<T, Set<String>> unmodifiable(
      Map<T, Set<String>> map)
  {
    Iterator<Map.Entry<T, Set<String>>> iter =
      map.entrySet().iterator();
    while (iter.hasNext()) {
      Map.Entry<T,Set<String>> entry = iter.next();

      Set<String> set = entry.getValue();
      set = Collections.unmodifiableSet(set);
      entry.setValue(set);
    }
    return Collections.unmodifiableMap(map);
  }

  @BeforeAll public void initializeEnvironment() {
    this.initializeTestEnvironment();
    this.configServices = new ConfigServices();
    File iniFile    = new File(this.getRepositoryDirectory(), "g2.ini");
    File configFile = null;
    try (FileInputStream   fis = new FileInputStream(iniFile);
         InputStreamReader isr = new InputStreamReader(fis, "UTF-8");
         BufferedReader    br  = new BufferedReader(isr))
    {
      String configPath = null;
      for (String line = br.readLine(); line != null; line = br.readLine()) {
        if (line.trim().toUpperCase().startsWith("G2CONFIGFILE=")) {
          configPath = line.trim().substring("G2CONFIGFILE=".length());
          break;
        }
      }
      configFile = new File(configPath);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    try (FileInputStream   fis = new FileInputStream(configFile);
         InputStreamReader isr = new InputStreamReader(fis, "UTF-8");
         JsonReader        jr  = Json.createReader(isr))
    {
      JsonObject rootObject = jr.readObject();
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
      this.internalAttrTypesByClass   = unmodifiable(this.internalAttrTypesByClass);
      this.standardAttrTypesByClass   = unmodifiable(this.standardAttrTypesByClass);
      this.allAttrTypesByClass        = unmodifiable(this.allAttrTypesByClass);
      this.internalAttrTypesByFeature = unmodifiable(this.internalAttrTypesByFeature);
      this.standardAttrTypesByFeature = unmodifiable(this.standardAttrTypesByFeature);
      this.allAttrTypesByFeature      = unmodifiable(this.allAttrTypesByFeature);

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
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
    this.teardownTestEnvironment(true);
  }

  @Test public void getDataSourcesTest() {
    this.assumeNativeApiAvailable();
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
  }

  @Test public void getDataSourcesViaHttpTest() {
    this.assumeNativeApiAvailable();
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
  }

  @Test public void getDataSourcesWithoutRawTest() {
    this.assumeNativeApiAvailable();
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
  }

  @Test public void getDataSourcesWithoutRawViaHttpTest() {
    this.assumeNativeApiAvailable();
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
  }

  @Test public void getDataSourcesWithRawTest() {
    this.assumeNativeApiAvailable();
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
  }

  @Test public void getDataSourcesWithRawViaHttpTest() {
    this.assumeNativeApiAvailable();
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
  }

  @Test public void getAttributeTypesTest() {
    this.assumeNativeApiAvailable();
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

  }

  @Test public void getAttributeTypesViaHttpTest() {
    this.assumeNativeApiAvailable();
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
  }

  @Test public void getAttributeTypesWithRawTest() {
    this.assumeNativeApiAvailable();
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

  }

  @Test public void getAttributeTypesWithRawViaHttpTest() {
    this.assumeNativeApiAvailable();
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
  }

  @Test public void getAttributeTypesWithoutInternalTest() {
    this.assumeNativeApiAvailable();
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

  }

  @Test public void getAttributeTypesWithoutInternalViaHttpTest() {
    this.assumeNativeApiAvailable();
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
  }

  @Test public void getAttributeTypesWithInternalTest() {
    this.assumeNativeApiAvailable();
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

  }

  @Test public void getAttributeTypesWithInternalViaHttpTest() {
    this.assumeNativeApiAvailable();
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
    this.assumeNativeApiAvailable();
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

  }

  @ParameterizedTest
  @MethodSource("standardAttributeClasses")
  public void getAttributeTypesForClassViaHttpTest(SzAttributeClass attrClass) {
    this.assumeNativeApiAvailable();
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
  }

  @ParameterizedTest
  @MethodSource("allAttributeClasses")
  public void getAttributeTypesForClassWithInternalTest(SzAttributeClass attrClass) {
    this.assumeNativeApiAvailable();
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

  }

  @ParameterizedTest
  @MethodSource("allAttributeClasses")
  public void getAttributeTypesForClassWithInternalViaHttpTest(
      SzAttributeClass attrClass) {
    this.assumeNativeApiAvailable();
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
    this.assumeNativeApiAvailable();
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

  }

  @ParameterizedTest
  @MethodSource("standardFeatureTypes")
  public void getAttributeTypesForFeatureViaHttpTest(String featureType) {
    this.assumeNativeApiAvailable();
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
  }

  @ParameterizedTest
  @MethodSource("allFeatureTypes")
  public void getAttributeTypesForFeatureWithInternalTest(String featureType) {
    this.assumeNativeApiAvailable();
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

  }

  @ParameterizedTest
  @MethodSource("allFeatureTypes")
  public void getAttributeTypesForFeatureWithInternalViaHttpTest(
      String featureType)
  {
    this.assumeNativeApiAvailable();
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
  }


  private Set<String> allAttributeCodes() {
    return this.allAttrTypes;
  }

  @ParameterizedTest
  @MethodSource("allAttributeCodes")
  public void getAttributeTypeTest(String attrCode) {
    this.assumeNativeApiAvailable();
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

  }

  @ParameterizedTest
  @MethodSource("allAttributeCodes")
  public void getAttributeTypeViaHttpTest(String attrCode) {
    this.assumeNativeApiAvailable();
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
  }

  @ParameterizedTest
  @MethodSource("allAttributeCodes")
  public void getAttributeTypeWithoutRawTest(String attrCode) {
    this.assumeNativeApiAvailable();
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
  }

  @ParameterizedTest
  @MethodSource("allAttributeCodes")
  public void getAttributeTypeWithoutRawViaHttpTest(String attrCode) {
    this.assumeNativeApiAvailable();
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
  }

  @ParameterizedTest
  @MethodSource("allAttributeCodes")
  public void getAttributeTypeWithRawTest(String attrCode) {
    this.assumeNativeApiAvailable();
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

  }

  @ParameterizedTest
  @MethodSource("allAttributeCodes")
  public void getAttributeTypeWithRawViaHttpTest(String attrCode) {
    this.assumeNativeApiAvailable();
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
  }

  private void validateDataSourcesResponse(
      SzDataSourcesResponse   response,
      long                    beforeTimestamp,
      long                    afterTimestamp,
      Boolean                 expectRawData,
      Set<String>             expectedDataSources)
  {
    String selfLink = this.formatServerUri("data-sources");
    if (expectRawData != null) {
      selfLink += "?withRaw=" + expectRawData;
    } else {
      expectRawData = false;
    }

    this.validateBasics(
        response, selfLink, beforeTimestamp, afterTimestamp, expectRawData);

    SzDataSourcesResponse.Data data = response.getData();

    assertNotNull(data, "Response data is null");

    Set<String> sources = data.getDataSources();

    assertNotNull(sources, "Data sources set is null");

    assertEquals(expectedDataSources, sources,
                 "Unexpected or missing data sources in set");

    if (expectRawData) {
      this.validateRawDataMap(response.getRawData(), "DSRC_CODE");
    }
  }

  private void validateAttributeTypesResponse(
      SzAttributeTypesResponse  response,
      String                    selfLink,
      long                      beforeTimestamp,
      long                      afterTimestamp,
      Boolean                   expectRawData,
      Set<String>               expectedAttrTypeCodes)
  {
    selfLink = this.formatServerUri(selfLink);
    if (expectRawData == null) {
      expectRawData = false;
    }

    this.validateBasics(
        response, selfLink, beforeTimestamp, afterTimestamp, expectRawData);

    SzAttributeTypesResponse.Data data = response.getData();

    assertNotNull(data, "Response data is null");

    List<SzAttributeType> attrTypes = data.getAttributeTypes();

    assertNotNull(attrTypes, "List of attribute types is null");

    Map<String, SzAttributeType> map = new LinkedHashMap<>();
    for (SzAttributeType attrType : attrTypes) {
      map.put(attrType.getAttributeCode(), attrType);
    }

    assertEquals(expectedAttrTypeCodes, map.keySet(),
                 "Unexpected or missing attribute types");

    if (expectRawData) {
      this.validateRawDataMapArray(response.getRawData(),
                                   false,
                                   "DEFAULT_VALUE",
                                   "ATTR_CODE",
                                   "FELEM_REQ",
                                   "ATTR_CLASS",
                                   "INTERNAL",
                                   "ATTR_ID",
                                   "FTYPE_CODE",
                                   "FELEM_CODE",
                                   "ADVANCED");
    }
  }

  private void validateAttributeTypeResponse(
      SzAttributeTypeResponse response,
      String                  attributeCode,
      long                    beforeTimestamp,
      long                    afterTimestamp,
      Boolean                 expectRawData)
  {
    String selfLink = this.formatServerUri(
        "attribute-types/" + attributeCode);
    if (expectRawData != null) {
      selfLink += "?withRaw=" + expectRawData;
    } else {
      expectRawData = false;
    }

    this.validateBasics(
        response, selfLink, beforeTimestamp, afterTimestamp, expectRawData);

    SzAttributeTypeResponse.Data data = response.getData();

    assertNotNull(data, "Response data is null");

    SzAttributeType attrType = data.getAttributeType();

    assertNotNull(attrType, "Attribute Type is null");

    assertEquals(attributeCode, attrType.getAttributeCode(),
                 "Unexpected attribute type code");

    if (expectRawData) {
      this.validateRawDataMap(response.getRawData(),
                              "DEFAULT_VALUE",
                              "ATTR_CODE",
                              "FELEM_REQ",
                              "ATTR_CLASS",
                              "INTERNAL",
                              "ATTR_ID",
                              "FTYPE_CODE",
                              "FELEM_CODE",
                              "ADVANCED");
    }
  }

  @Test public void getCurrentConfigTest() {
    this.assumeNativeApiAvailable();
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
  }

  @Test public void getCurrentConfigViaHttpTest() {
    this.assumeNativeApiAvailable();
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
  }

  @Test public void getDefaultConfigTest() {
    this.assumeNativeApiAvailable();
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
  }

  @Test public void getDefaultConfigViaHttpTest() {
    this.assumeNativeApiAvailable();
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
  }

  private void validateConfigResponse(
      SzConfigResponse        response,
      String                  selfLink,
      long                    beforeTimestamp,
      long                    afterTimestamp,
      Set<String>             expectedDataSources)
  {
    selfLink = this.formatServerUri(selfLink);

    this.validateBasics(
        response, selfLink, beforeTimestamp, afterTimestamp, true);

    Object rawData = response.getRawData();

    this.validateRawDataMap(rawData, true, "G2_CONFIG");

    Object g2Config = ((Map) rawData).get("G2_CONFIG");

    this.validateRawDataMap(g2Config,
                            false,
                            "CFG_ATTR",
                            "CFG_FELEM",
                            "CFG_DSRC");

    Object cfgDsrc = ((Map) g2Config).get("CFG_DSRC");

    this.validateRawDataMapArray(cfgDsrc,
                                 false,
                                 "DSRC_ID",
                                 "DSRC_DESC",
                                 "DSRC_CODE");

    Set<String> actualDataSources = new LinkedHashSet<>();
    for (Object dsrc : ((Collection) cfgDsrc)) {
      Map dsrcMap = (Map) dsrc;
      String dsrcCode = (String) dsrcMap.get("DSRC_CODE");
      actualDataSources.add(dsrcCode);
    }

    assertEquals(expectedDataSources, actualDataSources,
                 "Unexpected set of data sources in config.");
  }

}
