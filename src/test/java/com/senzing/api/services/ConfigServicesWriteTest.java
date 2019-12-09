package com.senzing.api.services;

import com.senzing.api.model.*;
import com.senzing.api.server.SzApiServer;
import com.senzing.api.server.SzApiServerOptions;
import com.senzing.util.JsonUtils;
import org.eclipse.jetty.util.thread.TryExecutor;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObjectBuilder;
import javax.ws.rs.core.UriInfo;
import java.io.UnsupportedEncodingException;
import java.security.SecureRandom;
import java.util.*;

import static java.lang.Boolean.TRUE;
import static org.junit.jupiter.api.TestInstance.Lifecycle;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static com.senzing.api.services.ResponseValidators.*;
import static com.senzing.api.model.SzHttpMethod.*;

@TestInstance(Lifecycle.PER_CLASS)
public class ConfigServicesWriteTest extends AbstractServiceTest
{
  private static final SecureRandom PRNG = new SecureRandom();

  private static final int ID_STEP = 2;

  private static final int RESOLVE_STEP = 3;

  private static final int CLASS_STEP = 3;

  private ConfigServices configServices;

  private int nextDataSourceId = 10001;

  private int nextEntityClassId = 10001;

  private int nextEntityTypeId = 10001;

  private enum SpecifiedMode {
    AUTOMATIC,
    EXPLICIT,
    ALTERNATING;

    public boolean isSpecified(String code, int step) {
      switch (this) {
        case AUTOMATIC:
          return false;
        case EXPLICIT:
          return true;
        case ALTERNATING:
          return ((code.hashCode() % step) == 0);
        default:
          throw new IllegalStateException("Unhandled specified mode: " + this);
      }
    }
  }

  private static final SpecifiedMode AUTOMATIC   = SpecifiedMode.AUTOMATIC;
  private static final SpecifiedMode EXPLICIT    = SpecifiedMode.EXPLICIT;
  private static final SpecifiedMode ALTERNATING = SpecifiedMode.ALTERNATING;

  private enum Formatting {
    BARE_TEXT_CODE(0, 1, EnumSet.of(AUTOMATIC)),
    QUOTED_TEXT_CODE(0, -1, EnumSet.of(AUTOMATIC)),
    JSON_OBJECT(1, 1, EnumSet.of(AUTOMATIC, EXPLICIT)),
    JSON_OBJECT_ARRAY(0, -1, EnumSet.allOf(SpecifiedMode.class)),
    MIXED_FORMATTING(2, -1, EnumSet.allOf(SpecifiedMode.class));

    private int minCount = 0;
    private int maxCount = 0;
    private EnumSet<SpecifiedMode> detailsSupport;

    Formatting(int                    minCount,
               int                    maxCount,
               EnumSet<SpecifiedMode> detailsSupport)
    {
      this.minCount   = minCount;
      this.maxCount   = maxCount;
      this.detailsSupport = detailsSupport;
    }

    public int getMinCount() {
      return this.minCount;
    }

    public int getMaxCount() {
      return this.maxCount;
    }

    public EnumSet<SpecifiedMode> getDetailsSupport() {
      return this.detailsSupport;
    }

    public boolean isBare() {
      return this == BARE_TEXT_CODE;
    }

    public String getContentType() {
      return (this == BARE_TEXT_CODE)
          ? "text/plain; charset=UTF-8" : "application/json; charset=UTF-8";
    }
  }

  private static final Formatting BARE_TEXT_CODE   = Formatting.BARE_TEXT_CODE;
  private static final Formatting QUOTED_TEXT_CODE = Formatting.QUOTED_TEXT_CODE;
  private static final Formatting JSON_OBJECT      = Formatting.JSON_OBJECT;
  private static final Formatting FULL_JSON_OBJECT = Formatting.JSON_OBJECT_ARRAY;
  private static final Formatting MIXED_FORMATTING = Formatting.MIXED_FORMATTING;

  private static class DataSourceBodyVariant {
    /**
     * The formatting to use.
     */
    private Formatting formatting;

    /**
     * The number of data sources.
     */
    private int count;

    /**
     * Whether or not the data sources have explicit IDs.
     */
    private SpecifiedMode identifierMode;

    /**
     * Constructs the various parameters.
     */
    private DataSourceBodyVariant(Formatting      formatting,
                                  int             count,
                                  SpecifiedMode identifierMode)
    {
      this.formatting     = formatting;
      this.count          = count;
      this.identifierMode = identifierMode;
    }

    /**
     * Gets the formatting.
     */
    public Formatting getFormatting() {
      return this.formatting;
    }

    /**
     * Returns the number of data sources.
     */
    public int getCount() {
      return this.count;
    }

    /**
     * Returns the identifier mode for the data sources.
     */
    public SpecifiedMode getIdentifierMode() {
      return this.identifierMode;
    }

    /**
     * Formats the values.
     */
    public String formatValues(List<SzDataSource> values) {
      switch (this.formatting) {
        case BARE_TEXT_CODE:
        {
          int count = values.size();
          if (count == 0) return "";
          if (count != 1) {
            throw new IllegalStateException(
                "Cannot format bare with multiple values.");
          }
          return values.get(0).getDataSourceCode();
        }

        case QUOTED_TEXT_CODE:
        {
          JsonArrayBuilder jab = Json.createArrayBuilder();
          for (SzDataSource value : values) {
            jab.add(value.getDataSourceCode());
          }
          return JsonUtils.toJsonText(jab);
        }

        case JSON_OBJECT:
        {
          SpecifiedMode idMode  = this.getIdentifierMode();
          SzDataSource      value   = values.get(0);
          String            code    = value.getDataSourceCode();
          boolean           withId  = idMode.isSpecified(code, ID_STEP);
          JsonObjectBuilder job     = Json.createObjectBuilder();
          value.buildJson(job);
          if (!withId) job.remove("dataSourceId");
          return JsonUtils.toJsonText(job);
        }

        case JSON_OBJECT_ARRAY:
        {
          SpecifiedMode idMode = this.getIdentifierMode();
          JsonArrayBuilder jab = Json.createArrayBuilder();
          for (int index = 0; index < values.size(); index++) {
            SzDataSource      value   = values.get(index);
            String            code    = value.getDataSourceCode();
            boolean           withId  = idMode.isSpecified(code, ID_STEP);
            JsonObjectBuilder job     = Json.createObjectBuilder();
            value.buildJson(job);
            if (!withId) job.remove("dataSourceId");
            jab.add(job);
          }
          return JsonUtils.toJsonText(jab);
        }

        case MIXED_FORMATTING:
        {
          SpecifiedMode idMode    = this.getIdentifierMode();
          boolean           codeOnly  = true;
          JsonArrayBuilder  jab     = Json.createArrayBuilder();
          for (int index = 0; index < values.size(); index++) {
            SzDataSource      value   = values.get(index);
            String            code    = value.getDataSourceCode();
            boolean           withId  = idMode.isSpecified(code, ID_STEP);
            if (withId || !codeOnly) {
              JsonObjectBuilder job = Json.createObjectBuilder();
              value.buildJson(job);
              if (!withId) {
                job.remove("dataSourceId");
                codeOnly = true;
              }
              jab.add(job);

            } else {
              codeOnly = false;
              jab.add(value.getDataSourceCode());
            }
          }
          return JsonUtils.toJsonText(jab);
        }

        default:
          throw new IllegalStateException(
              "Unhandled formatting: " + this.formatting);
      }
    }

    /**
     * Converts this instance to a string.
     */
    public String toString() {
      return "formatting=[ " + this.formatting + " ], identifierMode=[ "
          + this.identifierMode + " ], count=[ " + this.count + " ]";
    }
  }


  private static class EntityClassBodyVariant {
    /**
     * The formatting to use.
     */
    private Formatting formatting;

    /**
     * The number of entity classes.
     */
    private int count;

    /**
     * Whether or not the entity classes have explicit IDs.
     */
    private SpecifiedMode identifierMode;

    /**
     * Whether or not the entity classes have resolving flags.
     */
    private SpecifiedMode resolvingMode;

    /**
     * Constructs the various parameters.
     */
    private EntityClassBodyVariant(Formatting     formatting,
                                   int            count,
                                   SpecifiedMode  identifierMode,
                                   SpecifiedMode  resolvingMode)
    {
      this.formatting     = formatting;
      this.count          = count;
      this.identifierMode = identifierMode;
      this.resolvingMode  = resolvingMode;
    }

    /**
     * Gets the formatting.
     */
    public Formatting getFormatting() {
      return this.formatting;
    }

    /**
     * Returns the number of entity classes.
     */
    public int getCount() {
      return this.count;
    }

    /**
     * Returns the identifier mode for the entity classes.
     */
    public SpecifiedMode getIdentifierMode() {
      return this.identifierMode;
    }

    /**
     * Returns the resolving mode for the entity classes.
     */
    public SpecifiedMode getResolvingMode() {
      return this.resolvingMode;
    }

    /**
     * Formats the values.
     */
    public String formatValues(List<SzEntityClass> values) {
      switch (this.formatting) {
        case BARE_TEXT_CODE:
        {
          int count = values.size();
          if (count == 0) return "";
          if (count != 1) {
            throw new IllegalStateException(
                "Cannot format bare with multiple values.");
          }
          return values.get(0).getEntityClassCode();
        }

        case QUOTED_TEXT_CODE:
        {
          JsonArrayBuilder jab = Json.createArrayBuilder();
          for (SzEntityClass value : values) {
            jab.add(value.getEntityClassCode());
          }
          return JsonUtils.toJsonText(jab);
        }

        case JSON_OBJECT:
        {
          SpecifiedMode     idMode  = this.getIdentifierMode();
          SpecifiedMode     resMode = this.getResolvingMode();
          SzEntityClass     value   = values.get(0);
          String            code    = value.getEntityClassCode();
          boolean           withId  = idMode.isSpecified(code, ID_STEP);
          boolean           withRes = resMode.isSpecified(code, RESOLVE_STEP);
          JsonObjectBuilder job     = Json.createObjectBuilder();
          value.buildJson(job);
          if (!withId) job.remove("entityClassId");
          if (!withRes) job.remove("resolving");
          return JsonUtils.toJsonText(job);
        }

        case JSON_OBJECT_ARRAY:
        {
          SpecifiedMode idMode  = this.getIdentifierMode();
          SpecifiedMode resMode = this.getResolvingMode();
          JsonArrayBuilder jab = Json.createArrayBuilder();
          for (int index = 0; index < values.size(); index++) {
            SzEntityClass     value   = values.get(index);
            String            code    = value.getEntityClassCode();
            boolean           withId  = idMode.isSpecified(code, ID_STEP);
            boolean           withRes = resMode.isSpecified(code, RESOLVE_STEP);
            JsonObjectBuilder job     = Json.createObjectBuilder();
            value.buildJson(job);
            if (!withId) job.remove("entityClassId");
            if (!withRes) job.remove("resolving");
            jab.add(job);
          }
          return JsonUtils.toJsonText(jab);
        }

        case MIXED_FORMATTING:
        {
          SpecifiedMode idMode  = this.getIdentifierMode();
          SpecifiedMode resMode = this.getResolvingMode();

          boolean           codeOnly  = true;
          JsonArrayBuilder  jab     = Json.createArrayBuilder();
          for (int index = 0; index < values.size(); index++) {
            SzEntityClass     value   = values.get(index);
            String            code    = value.getEntityClassCode();
            boolean           withId  = idMode.isSpecified(code, ID_STEP);
            boolean           withRes = resMode.isSpecified(code, RESOLVE_STEP);

            if (withRes || withId || !codeOnly) {
              JsonObjectBuilder job = Json.createObjectBuilder();
              value.buildJson(job);
              if (!withId) {
                job.remove("entityClassId");
                codeOnly = true;
              }
              if (!withRes) {
                job.remove("resolving");
                codeOnly = true;
              }
              jab.add(job);

            } else {
              codeOnly = false;
              jab.add(value.getEntityClassCode());
            }
          }
          return JsonUtils.toJsonText(jab);
        }

        default:
          throw new IllegalStateException(
              "Unhandled formatting: " + this.formatting);
      }
    }

    /**
     * Converts this instance to a string.
     */
    public String toString() {
      return "formatting=[ " + this.formatting + " ], identifierMode=[ "
          + this.identifierMode + " ], resolvingMode=[ " + this.resolvingMode
          + " ], count=[ " + this.count + " ]";
    }
  }

  /**
   * Sets the desired options for the {@link SzApiServer} during server
   * initialization.
   *
   * @param options The {@link SzApiServerOptions} to initialize.
   */
  protected void initializeServerOptions(SzApiServerOptions options) {
    super.initializeServerOptions(options);
    options.setAdminEnabled(true);
  }

  @BeforeAll public void initializeEnvironment() {
    this.initializeTestEnvironment();
    this.configServices = new ConfigServices();
  }

  @AfterAll public void teardownEnvironment() {
    this.teardownTestEnvironment();
  }

  private List<Arguments> getWithRawVariants() {
    List<Arguments> result = new LinkedList<>();
    Boolean[] booleanVariants = {null, true, false};
    for (Boolean withRaw: booleanVariants) {
      Object[] argArray = new Object[1];
      argArray[0] = withRaw;
      result.add(arguments(argArray));
    }
    return result;
  }

  private SzDataSource nextDataSource(SpecifiedMode idMode) {
    Integer sourceId    = this.nextDataSourceId++;
    String  sourceCode  = "TEST_SOURCE_" + sourceId;
    boolean withId      = idMode.isSpecified(sourceCode, ID_STEP);
    return new SzDataSource(sourceCode, withId ? sourceId : null);
  }

  private List<SzDataSource> nextDataSources(int count, SpecifiedMode idMode)
  {
    List<SzDataSource> result = new ArrayList<>(count);
    for (int index = 0; index < count; index++) {
      result.add(this.nextDataSource(idMode));
    }
    return result;
  }

  private SzEntityClass nextEntityClass(SpecifiedMode resolveMode,
                                        SpecifiedMode idMode)
  {
    Integer classId   = this.nextEntityClassId++;
    String  classCode = "TEST_CLASS_" + classId;
    boolean withId    = idMode.isSpecified(classCode, ID_STEP);
    boolean withRes   = resolveMode.isSpecified(classCode, RESOLVE_STEP);
    Boolean resolving = (withRes) ? PRNG.nextBoolean() : null;
    return new SzEntityClass(classCode, withId ? classId : null, resolving);
  }

  private List<SzEntityClass> nextEntityClasses(int            count,
                                               SpecifiedMode  resolveMode,
                                               SpecifiedMode  idMode)
  {
    List<SzEntityClass> result = new ArrayList<>(count);
    for (int index = 0; index < count; index++) {
      result.add(this.nextEntityClass(resolveMode, idMode));
    }
    return result;
  }


  private SzEntityType nextEntityType(String classCode, SpecifiedMode idMode) {
    Integer typeId    = this.nextEntityTypeId++;
    String  typeCode  = "TEST_TYPE_" + typeId;
    boolean withId    = idMode.isSpecified(typeCode, ID_STEP);
    if (classCode == null) classCode = "ACTOR";
    return new SzEntityType(typeCode, withId ? typeId : null, classCode);
  }

  private List<DataSourceBodyVariant> createDataSourceBodyVariants() {
    List<DataSourceBodyVariant> result = new LinkedList<>();

    for (Formatting formatting: Formatting.values()) {
      int minCount = formatting.getMinCount();
      int maxCount = formatting.getMaxCount();
      if (maxCount < 0) maxCount = minCount + 3;
      EnumSet<SpecifiedMode> idModes = formatting.getDetailsSupport();
      for (int count = minCount; count <= maxCount; count++) {
        for (SpecifiedMode idMode: idModes) {
          DataSourceBodyVariant variant
              = new DataSourceBodyVariant(formatting, count, idMode);
          result.add(variant);
        }
      }
    }

    return result;
  }

  private List<EntityClassBodyVariant> createEntityClassBodyVariants() {
    List<EntityClassBodyVariant> result = new LinkedList<>();

    for (Formatting formatting: Formatting.values()) {
      int minCount = formatting.getMinCount();
      int maxCount = formatting.getMaxCount();
      if (maxCount < 0) maxCount = minCount + 3;
      EnumSet<SpecifiedMode> idModes = formatting.getDetailsSupport();
      EnumSet<SpecifiedMode> resModes = formatting.getDetailsSupport();
      for (int count = minCount; count <= maxCount; count++) {
        for (SpecifiedMode idMode: idModes) {
          for (SpecifiedMode resMode: resModes) {
            EntityClassBodyVariant variant = new EntityClassBodyVariant(
                formatting, count, idMode, resMode);
            result.add(variant);
          }
        }
      }
    }

    return result;
  }

  public List<Arguments> getPostDataSourcesParameters() {
    List<Arguments> result = new LinkedList<>();
    Boolean[] booleanVariants = { null, true, false };
    int[] queryCounts = { 0, 1, 3 };

    int rawIndex = 0;
    for (int queryCount : queryCounts) {
      Boolean withRaw = booleanVariants[rawIndex];
      rawIndex = (rawIndex + 1) % booleanVariants.length;

      List<DataSourceBodyVariant> bodyVariants
          = this.createDataSourceBodyVariants();

      for (DataSourceBodyVariant bodyVariant : bodyVariants) {
        int bodyCount = bodyVariant.getCount();
        int maxOverlap = Math.min(queryCount, bodyCount);
        if (maxOverlap > 2) maxOverlap = 2;
        for (int overlapCount = 0; overlapCount <= maxOverlap; overlapCount++)
        {
          Object[] argArray = {
              queryCount,
              bodyVariant,
              overlapCount,
              withRaw
          };
          result.add(arguments(argArray));
        }
      }
    }
    return result;
  }

  public List<Arguments> getPostEntityClassesParameters() {
    List<Arguments> result = new LinkedList<>();
    Boolean[] booleanVariants = { null, true, false };

    int rawIndex = 0, rawStep = 2;
    int resolveIndex = 0, resolveStep = 1;

    for (int queryCount = 0; queryCount <= 3; queryCount++) {
      Boolean withRaw = booleanVariants[rawIndex];
      Boolean queryResolve = booleanVariants[resolveIndex];
      rawIndex = (rawIndex + rawStep) % booleanVariants.length;
      resolveIndex = (resolveIndex + resolveStep) % booleanVariants.length;

      List<EntityClassBodyVariant> bodyVariants
          = this.createEntityClassBodyVariants();

      for (EntityClassBodyVariant bodyVariant : bodyVariants) {
        int bodyCount = bodyVariant.getCount();
        int maxOverlap = Math.min(queryCount, bodyCount);
        if (maxOverlap > 2) maxOverlap = 2;
        for (int overlapCount = 0; overlapCount <= maxOverlap; overlapCount++) {
          Object[] argArray = {
              queryCount,
              bodyVariant,
              overlapCount,
              queryResolve,
              withRaw
          };
          result.add(arguments(argArray));
        }
      }
    }
    return result;
  }

  private String formatTestInfo(String uriText, String bodyContent)
  {
    return "uriText=[ " + uriText + " ], bodyContent=[ " + bodyContent + " ]";
  }

  private String buildPostDataSourcesQuery(List<String> querySources,
                                           Boolean      withRaw)
  {
    StringBuilder sb = new StringBuilder();
    String prefix = "?";
    for (String sourceCode: querySources) {
      sb.append(prefix).append("dataSource=").append(sourceCode);
      prefix = "&";
    }
    if (withRaw != null) {
      sb.append(prefix).append("withRaw=").append(withRaw);
      prefix = "&";
    }
    return sb.toString();
  }

  private String buildPostEntityClassesQuery(List<String> queryClasses,
                                             Boolean      queryResolve,
                                             Boolean      withRaw)
  {
    StringBuilder sb = new StringBuilder();
    String prefix = "?";
    for (String classCode: queryClasses) {
      sb.append(prefix).append("entityClass=").append(classCode);
      prefix = "&";
    }
    if (queryResolve != null) {
      sb.append(prefix).append("resolving=").append(queryResolve);
      prefix = "&";
    }
    if (withRaw != null) {
      sb.append(prefix).append("withRaw=").append(withRaw);
      prefix = "&";
    }
    return sb.toString();
  }

  @ParameterizedTest
  @MethodSource("getPostDataSourcesParameters")
  public void postDataSourcesTest(int                   querySourceCount,
                                  DataSourceBodyVariant bodySourceVariant,
                                  int                   overlapCount,
                                  Boolean               withRaw)
  {
    this.performTest(() -> {
      this.revertToInitialConfig();

      Map<String, SzDataSource> expectedSources = new LinkedHashMap<>();
      expectedSources.putAll(this.getInitialDataSources());

      int queryOnlyCount = querySourceCount - overlapCount;
      List<SzDataSource> querySources
          = this.nextDataSources(queryOnlyCount, AUTOMATIC);

      int             bodyCount = bodySourceVariant.getCount();
      SpecifiedMode idMode    = bodySourceVariant.getIdentifierMode();

      List<SzDataSource> overlapSources
          = this.nextDataSources(overlapCount, idMode);

      List<SzDataSource> bodySources = new ArrayList<>(bodyCount);
      bodySources.addAll(overlapSources);

      int bodyOnlyCount = bodyCount - overlapCount;
      bodySources.addAll(this.nextDataSources(bodyOnlyCount, idMode));

      querySources.addAll(overlapSources);

      List<String> queryCodes = new ArrayList<>(querySources.size());
      for (SzDataSource source: querySources) {
        queryCodes.add(source.getDataSourceCode());
        expectedSources.put(source.getDataSourceCode(), source);
      }
      for (SzDataSource source: bodySources) {
        expectedSources.put(source.getDataSourceCode(), source);
      }

      this.performTest(() -> {
        String suffix = this.buildPostDataSourcesQuery(queryCodes, withRaw);
        String relativeUri = "data-sources" + suffix;
        String uriText = this.formatServerUri(relativeUri);
        UriInfo uriInfo = this.newProxyUriInfo(uriText);
        String bodyContent = bodySourceVariant.formatValues(bodySources);
        String testInfo = this.formatTestInfo(relativeUri,
                                              bodyContent);

        long before = System.currentTimeMillis();
        SzDataSourcesResponse response = this.configServices.addDataSources(
            queryCodes,
            TRUE.equals(withRaw),
            uriInfo,
            bodyContent);

        response.concludeTimers();
        long after = System.currentTimeMillis();

        validateDataSourcesResponse(testInfo,
                                    response,
                                    POST,
                                    uriText,
                                    before,
                                    after,
                                    TRUE.equals(withRaw),
                                    expectedSources);
      });
    });
  }

  @ParameterizedTest
  @MethodSource("getPostDataSourcesParameters")
  public void postDataSourcesTestViaHttp(
      int                   querySourceCount,
      DataSourceBodyVariant bodySourceVariant,
      int                   overlapCount,
      Boolean               withRaw)
  {
    this.performTest(() -> {
      this.revertToInitialConfig();

      Map<String, SzDataSource> expectedSources = new LinkedHashMap<>();
      expectedSources.putAll(this.getInitialDataSources());

      int queryOnlyCount = querySourceCount - overlapCount;
      List<SzDataSource> querySources
          = this.nextDataSources(queryOnlyCount, AUTOMATIC);

      int             bodyCount = bodySourceVariant.getCount();
      SpecifiedMode idMode    = bodySourceVariant.getIdentifierMode();

      List<SzDataSource> overlapSources
          = this.nextDataSources(overlapCount, idMode);

      List<SzDataSource> bodySources = new ArrayList<>(bodyCount);
      bodySources.addAll(overlapSources);

      int bodyOnlyCount = bodyCount - overlapCount;
      bodySources.addAll(this.nextDataSources(bodyOnlyCount, idMode));

      querySources.addAll(overlapSources);

      List<String> queryCodes = new ArrayList<>(querySources.size());
      for (SzDataSource source: querySources) {
        queryCodes.add(source.getDataSourceCode());
        expectedSources.put(source.getDataSourceCode(), source);
      }
      for (SzDataSource source: bodySources) {
        expectedSources.put(source.getDataSourceCode(), source);
      }

      this.performTest(() -> {
        String  suffix      = this.buildPostDataSourcesQuery(queryCodes, withRaw);
        String  relativeUri = "data-sources" + suffix;
        String  uriText     = this.formatServerUri(relativeUri);
        String  bodyContent = bodySourceVariant.formatValues(bodySources);
        String  testInfo    = this.formatTestInfo(relativeUri,
                                                  bodyContent);

        // convert the body content to a byte array
        byte[] bodyContentData;
        try {
          bodyContentData = bodyContent.getBytes("UTF-8");
        } catch (UnsupportedEncodingException cannotHappen) {
          throw new IllegalStateException(cannotHappen);
        }
        long before = System.currentTimeMillis();

        SzDataSourcesResponse response = this.invokeServerViaHttp(
            POST,
            uriText,
            null,
            bodySourceVariant.getFormatting().getContentType(),
            bodyContentData,
            SzDataSourcesResponse.class);

        response.concludeTimers();
        long after = System.currentTimeMillis();

        validateDataSourcesResponse(testInfo,
                                    response,
                                    POST,
                                    uriText,
                                    before,
                                    after,
                                    TRUE.equals(withRaw),
                                    expectedSources);
      });
    });
  }

  @ParameterizedTest
  @MethodSource("getPostEntityClassesParameters")
  public void postEntityClassesTest(int                     queryClassCount,
                                    EntityClassBodyVariant  bodyClassVariant,
                                    int                     overlapCount,
                                    Boolean                 queryResolve,
                                    Boolean                 withRaw)
  {
    this.performTest(() -> {
      this.revertToInitialConfig();

      Map<String, SzEntityClass> expectedClasses = new LinkedHashMap<>();
      expectedClasses.putAll(this.getInitialEntityClasses());

      int queryOnlyCount = queryClassCount - overlapCount;
      List<SzEntityClass> queryClasses
          = this.nextEntityClasses(queryOnlyCount, AUTOMATIC, AUTOMATIC);

      int             bodyCount   = bodyClassVariant.getCount();
      SpecifiedMode   resolveMode = bodyClassVariant.getResolvingMode();
      SpecifiedMode   idMode      = bodyClassVariant.getIdentifierMode();

      List<SzEntityClass> overlapClasses
          = this.nextEntityClasses(overlapCount, resolveMode, idMode);

      List<SzEntityClass> bodyClasses = new ArrayList<>(bodyCount);
      bodyClasses.addAll(overlapClasses);

      int bodyOnlyCount = bodyCount - overlapCount;
      bodyClasses.addAll(
          this.nextEntityClasses(bodyOnlyCount, resolveMode, idMode));

      queryClasses.addAll(overlapClasses);

      List<String> queryCodes = new ArrayList<>(queryClasses.size());
      for (SzEntityClass entityClass: queryClasses) {
        queryCodes.add(entityClass.getEntityClassCode());
        expectedClasses.put(entityClass.getEntityClassCode(), entityClass);
      }
      for (SzEntityClass entityClass: bodyClasses) {
        expectedClasses.put(entityClass.getEntityClassCode(), entityClass);
      }

      this.performTest(() -> {
        String  suffix      = this.buildPostEntityClassesQuery(queryCodes,
                                                               queryResolve,
                                                               withRaw);
        String  relativeUri = "entity-classes" + suffix;
        String  uriText     = this.formatServerUri(relativeUri);
        UriInfo uriInfo     = this.newProxyUriInfo(uriText);
        String  bodyContent = bodyClassVariant.formatValues(bodyClasses);
        String  testInfo    = this.formatTestInfo(relativeUri,
                                                  bodyContent);

        long before = System.currentTimeMillis();
        SzEntityClassesResponse response = this.configServices.addEntityClasses(
            queryCodes,
            queryResolve,
            TRUE.equals(withRaw),
            uriInfo,
            bodyContent);

        response.concludeTimers();

        long after = System.currentTimeMillis();

        validateEntityClassesResponse(testInfo,
                                      response,
                                      POST,
                                      uriText,
                                      before,
                                      after,
                                      TRUE.equals(withRaw),
                                      queryResolve,
                                      expectedClasses);
      });
    });
  }

  @ParameterizedTest
  @MethodSource("getPostEntityClassesParameters")
  public void postEntityClassesTestViaHttp(
      int                     queryClassCount,
      EntityClassBodyVariant  bodyClassVariant,
      int                     overlapCount,
      Boolean                 queryResolve,
      Boolean                 withRaw)
  {
    this.performTest(() -> {
      this.revertToInitialConfig();

      Map<String, SzEntityClass> expectedClasses = new LinkedHashMap<>();
      expectedClasses.putAll(this.getInitialEntityClasses());

      int queryOnlyCount = queryClassCount - overlapCount;
      List<SzEntityClass> queryClasses
          = this.nextEntityClasses(queryOnlyCount, AUTOMATIC, AUTOMATIC);

      int           bodyCount = bodyClassVariant.getCount();
      SpecifiedMode idMode    = bodyClassVariant.getIdentifierMode();
      SpecifiedMode resMode   = bodyClassVariant.getResolvingMode();

      List<SzEntityClass> overlapClasses
          = this.nextEntityClasses(overlapCount, resMode, idMode);

      List<SzEntityClass> bodyClasses = new ArrayList<>(bodyCount);
      bodyClasses.addAll(overlapClasses);

      int bodyOnlyCount = bodyCount - overlapCount;
      bodyClasses.addAll(this.nextEntityClasses(bodyOnlyCount, resMode, idMode));

      queryClasses.addAll(overlapClasses);

      List<String> queryCodes = new ArrayList<>(queryClasses.size());
      for (SzEntityClass entityClass: queryClasses) {
        queryCodes.add(entityClass.getEntityClassCode());
        expectedClasses.put(entityClass.getEntityClassCode(), entityClass);
      }
      for (SzEntityClass entityClass: bodyClasses) {
        expectedClasses.put(entityClass.getEntityClassCode(), entityClass);
      }

      this.performTest(() -> {
        String  suffix      = this.buildPostEntityClassesQuery(queryCodes,
                                                               queryResolve,
                                                               withRaw);
        String  relativeUri = "entity-classes" + suffix;
        String  uriText     = this.formatServerUri(relativeUri);
        String  bodyContent = bodyClassVariant.formatValues(bodyClasses);
        String  testInfo    = this.formatTestInfo(relativeUri,
                                                  bodyContent);

        // convert the body content to a byte array
        byte[] bodyContentData;
        try {
          bodyContentData = bodyContent.getBytes("UTF-8");
        } catch (UnsupportedEncodingException cannotHappen) {
          throw new IllegalStateException(cannotHappen);
        }
        long before = System.currentTimeMillis();

        SzEntityClassesResponse response = this.invokeServerViaHttp(
            POST,
            uriText,
            null,
            bodyClassVariant.getFormatting().getContentType(),
            bodyContentData,
            SzEntityClassesResponse.class);

        response.concludeTimers();
        long after = System.currentTimeMillis();

        validateEntityClassesResponse(testInfo,
                                      response,
                                      POST,
                                      uriText,
                                      before,
                                      after,
                                      TRUE.equals(withRaw),
                                      queryResolve,
                                      expectedClasses);
      });
    });
  }

}
