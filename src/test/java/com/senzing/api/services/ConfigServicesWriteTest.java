package com.senzing.api.services;

import com.senzing.api.model.*;
import com.senzing.api.server.SzApiServer;
import com.senzing.api.server.SzApiServerOptions;
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
import javax.ws.rs.BadRequestException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.UriInfo;
import java.io.UnsupportedEncodingException;
import java.util.*;

import static com.senzing.api.services.ServicesUtil.newBadRequestException;
import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static org.junit.jupiter.api.TestInstance.Lifecycle;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static com.senzing.api.services.ResponseValidators.*;
import static com.senzing.api.model.SzHttpMethod.*;

@TestInstance(Lifecycle.PER_CLASS)
public class ConfigServicesWriteTest extends AbstractServiceTest
{
  private static final int ID_STEP = 2;

  private static final int RESOLVE_STEP = 3;

  private static final int CLASS_STEP = 3;

  private static final List<String> SINGLE_ENITTY_CLASS_LIST
      = Collections.singletonList("ACTOR");

  private static final List<String> ALL_ENTITY_CLASSES_LIST;

  static {
    List<String> list = new ArrayList<>();
    list.add("ACTOR");
    ALL_ENTITY_CLASSES_LIST = Collections.unmodifiableList(list);
  }
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

  private static class EntityTypeBodyVariant {
    /**
     * The formatting to use.
     */
    private Formatting formatting;

    /**
     * The number of entity classes.
     */
    private int count;

    /**
     * The list of entity classes to use for entity types.
     */
    private List<String> entityClasses;

    /**
     * Whether or not the entity classes have explicit IDs.
     */
    private SpecifiedMode identifierMode;

    /**
     * Constructs the various parameters.
     */
    private EntityTypeBodyVariant(Formatting    formatting,
                                  int           count,
                                  List<String>  entityClasses,
                                  SpecifiedMode identifierMode)
    {
      this.formatting     = formatting;
      this.count          = count;
      this.entityClasses  = entityClasses;
      this.identifierMode = identifierMode;
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
     * Returns the entity classes to use for the entity types.
     *
     * @return The entity classes to use for the entity types.
     */
    public List<String> getEntityClasses() {
      return this.entityClasses;
    }

    /**
     * Formats the values.
     */
    public String inferQueryClassCode(List<SzEntityType> values) {
      switch (this.formatting) {
        case BARE_TEXT_CODE:
        {
          int count = values.size();
          if (count == 0) return null;
          if (count != 1) {
            throw new IllegalStateException(
                "Cannot format bare with multiple values.");
          }
          String code = values.get(0).getEntityClassCode();
          return code;
        }

        case QUOTED_TEXT_CODE:
        {
          String code = null;
          for (SzEntityType value: values) {
            if (code != null && !code.equals(value.getEntityClassCode())) {
              throw new IllegalStateException(
                  "Cannot format as a quoted text array and have different "
                  + "entity class codes: " + values);
            }
            if (code == null) code = value.getEntityClassCode();
          }
          return code;
        }

        case JSON_OBJECT:
        case JSON_OBJECT_ARRAY:
        {
          return null;
        }

        case MIXED_FORMATTING:
        {
          SpecifiedMode idMode  = this.getIdentifierMode();

          for (SzEntityType value: values) {
            String  code    = value.getEntityTypeCode();
            boolean withId  = idMode.isSpecified(code, ID_STEP);

            if (!withId) {
              String classCode = value.getEntityClassCode();
              return classCode;
            }
          }
          return null;
        }

        default:
          throw new IllegalStateException(
              "Unhandled formatting: " + this.formatting);
      }
    }

    /**
     * Formats the values.
     */
    public String formatValues(String queryClassCode, List<SzEntityType> values)
    {
      switch (this.formatting) {
        case BARE_TEXT_CODE:
        {
          int count = values.size();
          if (count == 0) return "";
          if (count != 1) {
            throw new IllegalStateException(
                "Cannot format bare with multiple values.");
          }
          return values.get(0).getEntityTypeCode();
        }

        case QUOTED_TEXT_CODE:
        {
          JsonArrayBuilder jab = Json.createArrayBuilder();
          for (SzEntityType value : values) {
            jab.add(value.getEntityTypeCode());
          }
          return JsonUtils.toJsonText(jab);
        }

        case JSON_OBJECT:
        {
          SpecifiedMode     idMode  = this.getIdentifierMode();
          SzEntityType      value   = values.get(0);
          String            code    = value.getEntityTypeCode();
          boolean           withId  = idMode.isSpecified(code, ID_STEP);
          JsonObjectBuilder job     = Json.createObjectBuilder();
          value.buildJson(job);
          if (!withId) job.remove("entityTypeId");
          return JsonUtils.toJsonText(job);
        }

        case JSON_OBJECT_ARRAY:
        {
          SpecifiedMode idMode  = this.getIdentifierMode();
          JsonArrayBuilder jab = Json.createArrayBuilder();
          for (int index = 0; index < values.size(); index++) {
            SzEntityType      value   = values.get(index);
            String            code    = value.getEntityTypeCode();
            boolean           withId  = idMode.isSpecified(code, ID_STEP);
            JsonObjectBuilder job     = Json.createObjectBuilder();
            value.buildJson(job);
            if (!withId) job.remove("entityTypeId");
            jab.add(job);
          }
          return JsonUtils.toJsonText(jab);
        }

        case MIXED_FORMATTING:
        {
          SpecifiedMode idMode  = this.getIdentifierMode();

          boolean           codeOnly  = true;
          JsonArrayBuilder  jab     = Json.createArrayBuilder();
          for (int index = 0; index < values.size(); index++) {
            SzEntityType      value   = values.get(index);
            String            code    = value.getEntityTypeCode();
            boolean           withId  = idMode.isSpecified(code, ID_STEP);

            boolean differentClassCode
                = !value.getEntityClassCode().equals(queryClassCode);

            if (withId || !codeOnly || differentClassCode) {
              JsonObjectBuilder job = Json.createObjectBuilder();
              value.buildJson(job);
              if (!withId) {
                job.remove("entityTypeId");
                codeOnly = true;
              }
              jab.add(job);

            } else {
              codeOnly = false;
              jab.add(value.getEntityTypeCode());
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
    this.beginTests();
    this.initializeTestEnvironment();
    this.configServices = new ConfigServices();
  }

  @AfterAll public void teardownEnvironment() {
    try {
      this.teardownTestEnvironment();
      this.conditionallyLogCounts(true);
    } finally {
      this.endTests();
    }
  }

  protected void revertToInitialConfig() {
    super.revertToInitialConfig();
    this.nextDataSourceId   = 10001;
    this.nextEntityTypeId   = 10001;
    this.nextEntityClassId  = 10001;
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
    Boolean resolving = (withRes) ? (classId.intValue()%2 == 0) : null;
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

  private SzEntityType nextEntityType(List<String>  entityClasses,
                                      SpecifiedMode idMode) {
    Integer typeId    = this.nextEntityTypeId++;
    String  typeCode  = "TEST_TYPE_" + typeId;
    boolean withId    = idMode.isSpecified(typeCode, ID_STEP);

    int hash = typeCode.hashCode();
    String classCode = "ACTOR";
    if (entityClasses.size() > 0) {
      int index = Math.abs(hash % entityClasses.size());
      classCode = entityClasses.get(index);
    }
    return new SzEntityType(typeCode, withId ? typeId : null, classCode);
  }

  private List<SzEntityType> nextEntityTypes(int            count,
                                             List<String>   entityClasses,
                                             SpecifiedMode  idMode)
  {
    List<SzEntityType> result = new ArrayList<>(count);
    for (int index = 0; index < count; index++) {
      result.add(this.nextEntityType(entityClasses, idMode));
    }
    return result;
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

  private List<EntityTypeBodyVariant> createEntityTypeBodyVariants(
      List<String> entityClasses)
  {
    int classIndex = (entityClasses == null) ? 0 : -1;
    List<EntityTypeBodyVariant> result = new LinkedList<>();

    for (Formatting formatting: Formatting.values()) {
      int minCount = formatting.getMinCount();
      int maxCount = formatting.getMaxCount();
      if (maxCount < 0) maxCount = minCount + 3;
      EnumSet<SpecifiedMode> idModes = formatting.getDetailsSupport();
      for (int count = minCount; count <= maxCount; count++) {
        // if doing QUOTED_TEXT_CODE array all must have the same entity class
        if (count > 1 && formatting == Formatting.QUOTED_TEXT_CODE
            && entityClasses.size() > 1)
        {
          continue;
        }
        for (SpecifiedMode idMode: idModes) {
          if (classIndex >= 0) {
            String entityClass = ALL_ENTITY_CLASSES_LIST.get(classIndex);
            classIndex    = (classIndex + 1) % ALL_ENTITY_CLASSES_LIST.size();
            entityClasses = Collections.singletonList(entityClass);
          }
          EntityTypeBodyVariant variant = new EntityTypeBodyVariant(
              formatting, count, entityClasses, idMode);
          result.add(variant);
        }
      }
    }

    return result;
  }

  public List<Arguments> getPostDataSourcesParameters() {
    List<Arguments> result = new LinkedList<>();
    Boolean[] booleanVariants = { null, true, false };
    int[] queryCounts = { 0, 1, 3 };

    List<DataSourceBodyVariant> bodyVariants
        = this.createDataSourceBodyVariants();

    int rawIndex = 0;
    for (int queryCount : queryCounts) {
      Boolean withRaw = booleanVariants[rawIndex];
      rawIndex = (rawIndex + 1) % booleanVariants.length;

      for (DataSourceBodyVariant bodyVariant : bodyVariants) {
        int bodyCount = bodyVariant.getCount();
        Object[] argArray = {
            queryCount,
            bodyVariant,
            0,
            withRaw
        };
        result.add(arguments(argArray));

        // handle an overlap test as well conditionally
        if (queryCount == 3 && bodyCount > 1) {
          Object[] argArray2 = {
              queryCount,
              bodyVariant,
              2,
              withRaw
          };

          result.add(arguments(argArray2));
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

    List<EntityClassBodyVariant> bodyVariants
        = this.createEntityClassBodyVariants();

    for (int queryCount = 0; queryCount <= 3; queryCount++) {
      if (queryCount == 2) continue; // reduce the number of tests
      Boolean withRaw = booleanVariants[rawIndex];
      Boolean queryResolve = booleanVariants[resolveIndex];
      rawIndex = (rawIndex + rawStep) % booleanVariants.length;
      resolveIndex = (resolveIndex + resolveStep) % booleanVariants.length;

      for (EntityClassBodyVariant bodyVariant : bodyVariants) {
        int bodyCount = bodyVariant.getCount();
        Object[] argArray1 = {
            queryCount,
            bodyVariant,
            0,
            queryResolve,
            withRaw
        };
        result.add(arguments(argArray1));

        // handle an overlap test as well conditionally
        if (queryCount == 3 && bodyCount > 1) {
          Object[] argArray2 = {
              queryCount,
              bodyVariant,
              2,
              queryResolve,
              withRaw
          };

          result.add(arguments(argArray2));
        }
      }
    }
    return result;
  }

  public List<Arguments> getPostEntityTypeParameters()
  {
    List<String> entityClasses = ALL_ENTITY_CLASSES_LIST;
    List<Arguments> result = new LinkedList<>();
    Boolean[] booleanVariants = { null, true, false };

    int rawIndex = 0, rawStep = 2;
    boolean hasQueryClass = true;

    List<EntityTypeBodyVariant> bodyVariants
        = this.createEntityTypeBodyVariants(entityClasses);

    for (int queryCount = 0; queryCount <= 3; queryCount++) {
      if (queryCount == 2) continue; // skip some to reduce build time
      Boolean withRaw = booleanVariants[rawIndex];
      Boolean queryClass = hasQueryClass ? TRUE : FALSE;
      rawIndex = (rawIndex + rawStep) % booleanVariants.length;
      hasQueryClass = !hasQueryClass;

      for (EntityTypeBodyVariant bodyVariant : bodyVariants) {
        int         bodyCount   = bodyVariant.getCount();
        Formatting  formatting  = bodyVariant.getFormatting();
        if (queryCount > 0 && bodyCount > 0
            && (formatting == BARE_TEXT_CODE || formatting == QUOTED_TEXT_CODE))
        {
          // skip this one
          continue;
        }
        Object[] argArray = {
            queryCount,
            bodyVariant,
            0,
            queryClass,
            withRaw
        };
        result.add(arguments(argArray));

        // handle an overlap test as well conditionally
        if (queryCount == 3 && bodyCount > 1) {
          Object[] argArray2 = {
              queryCount,
              bodyVariant,
              2,
              queryClass,
              withRaw
          };

          result.add(arguments(argArray2));
        }
      }
    }
    return result;
  }

  public List<Arguments> getPostEntityTypeForClassParameters()
  {
    List<Arguments> result = new LinkedList<>();
    Boolean[] booleanVariants = { null, true, false };

    int rawIndex = 0, rawStep = 2;
    List<EntityTypeBodyVariant> bodyVariants
        = this.createEntityTypeBodyVariants(null);

    for (int queryCount = 0; queryCount <= 3; queryCount++) {
      if (queryCount == 2) continue; // skip some tests to speed up build
      Boolean       withRaw       = booleanVariants[rawIndex];

      rawIndex = (rawIndex + rawStep) % booleanVariants.length;

      for (EntityTypeBodyVariant bodyVariant : bodyVariants) {
        int           bodyCount     = bodyVariant.getCount();
        Object[] argArray = {
            queryCount,
            bodyVariant,
            0,
            withRaw
        };
        result.add(arguments(argArray));

        // handle an overlap test as well conditionally
        if (queryCount == 3 && bodyCount > 1) {
          Object[] argArray2 = {
              queryCount,
              bodyVariant,
              2,
              withRaw
          };

          result.add(arguments(argArray2));
        }
      }
    }
    return result;
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

  private String buildPostEntityTypesQuery(List<String> queryTypes,
                                           String       queryClass,
                                           Boolean      withRaw)
  {
    StringBuilder sb = new StringBuilder();
    String prefix = "?";
    for (String classCode: queryTypes) {
      sb.append(prefix).append("entityType=").append(classCode);
      prefix = "&";
    }
    if (queryClass != null) {
      sb.append(prefix).append("entityClass=").append(queryClass);
      prefix = "&";
    }
    if (withRaw != null) {
      sb.append(prefix).append("withRaw=").append(withRaw);
      prefix = "&";
    }
    return sb.toString();
  }

  private String buildPostEntityTypesQuery(List<String> queryTypes,
                                           Boolean      withRaw)
  {
    StringBuilder sb = new StringBuilder();
    String prefix = "?";
    for (String classCode: queryTypes) {
      sb.append(prefix).append("entityType=").append(classCode);
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
      SpecifiedMode idMode      = bodySourceVariant.getIdentifierMode();

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
  }


  //---------------------------------------------------------------------
  // TODO(bcaceres) -- remove this code when entity classes other than
  // ACTOR are supported
  //@ParameterizedTest
  //@MethodSource("getPostEntityClassesParameters")
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
  }

  //---------------------------------------------------------------------
  // TODO(bcaceres) -- remove this code when entity classes other than
  // ACTOR are supported
  //@ParameterizedTest
  //@MethodSource("getPostEntityClassesParameters")
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
  }

  @ParameterizedTest
  @MethodSource("getPostEntityTypeParameters")
  public void postEntityTypesTest(int                     queryTypeCount,
                                  EntityTypeBodyVariant   bodyTypeVariant,
                                  int                     overlapCount,
                                  boolean                 queryClass,
                                  Boolean                 withRaw)
  {
    this.performTest(() -> {
      this.revertToInitialConfig();

      Map<String, SzEntityType> expectedTypes = new LinkedHashMap<>();
      expectedTypes.putAll(this.getInitialEntityTypes());

      int queryOnlyCount = queryTypeCount - overlapCount;

      List<SzEntityType> queryTypes = this.nextEntityTypes(
          queryOnlyCount, SINGLE_ENITTY_CLASS_LIST, AUTOMATIC);

      // get the information on the body entity types
      int           bodyCount     = bodyTypeVariant.getCount();
      SpecifiedMode idMode        = bodyTypeVariant.getIdentifierMode();
      List<String>  entityClasses = bodyTypeVariant.getEntityClasses();

      List<SzEntityType> overlapTypes
          = this.nextEntityTypes(overlapCount, entityClasses, idMode);

      List<SzEntityType> bodyTypes = new ArrayList<>(bodyCount);
      bodyTypes.addAll(overlapTypes);

      int bodyOnlyCount = bodyCount - overlapCount;
      bodyTypes.addAll(
          this.nextEntityTypes(bodyOnlyCount, entityClasses, idMode));

      queryTypes.addAll(overlapTypes);

      List<String> queryCodes = new ArrayList<>(queryTypes.size());
      for (SzEntityType entityType: queryTypes) {
        queryCodes.add(entityType.getEntityTypeCode());
        expectedTypes.put(entityType.getEntityTypeCode(), entityType);
      }

      for (SzEntityType entityType: bodyTypes) {
        expectedTypes.put(entityType.getEntityTypeCode(), entityType);
      }

      // if we have query-only entity types then they MUST have an entity class
      String queryClassCode = (queryOnlyCount > 0)
          ? queryTypes.get(0).getEntityClassCode() : null;

      if (queryClassCode == null && queryClass) {
        queryClassCode = bodyTypeVariant.inferQueryClassCode(bodyTypes);
      }

      String  suffix      = this.buildPostEntityTypesQuery(queryCodes,
                                                           queryClassCode,
                                                           withRaw);
      String  relativeUri = "entity-types" + suffix;
      String  uriText     = this.formatServerUri(relativeUri);
      UriInfo uriInfo     = this.newProxyUriInfo(uriText);
      String  bodyContent = bodyTypeVariant.formatValues(queryClassCode,
                                                         bodyTypes);
      String  testInfo    = this.formatTestInfo(relativeUri,
                                                bodyContent);

      long before = System.currentTimeMillis();

      SzEntityTypesResponse response = this.configServices.addEntityTypes(
          queryClassCode,
          queryCodes,
          TRUE.equals(withRaw),
          uriInfo,
          bodyContent);

      response.concludeTimers();
      long after = System.currentTimeMillis();

      validateEntityTypesResponse(testInfo,
                                  response,
                                  POST,
                                  uriText,
                                  before,
                                  after,
                                  TRUE.equals(withRaw),
                                  expectedTypes);
    });
  }

  @ParameterizedTest
  @MethodSource("getPostEntityTypeParameters")
  public void postEntityTypesTestViaHttp(int                    queryTypeCount,
                                         EntityTypeBodyVariant  bodyTypeVariant,
                                         int                    overlapCount,
                                         boolean                queryClass,
                                         Boolean                withRaw)
  {
    this.performTest(() -> {
      this.revertToInitialConfig();

      Map<String, SzEntityType> expectedTypes = new LinkedHashMap<>();
      expectedTypes.putAll(this.getInitialEntityTypes());

      int queryOnlyCount = queryTypeCount - overlapCount;

      List<SzEntityType> queryTypes = this.nextEntityTypes(
          queryOnlyCount, SINGLE_ENITTY_CLASS_LIST, AUTOMATIC);

      // get the information on the body entity types
      int           bodyCount     = bodyTypeVariant.getCount();
      SpecifiedMode idMode        = bodyTypeVariant.getIdentifierMode();
      List<String>  entityClasses = bodyTypeVariant.getEntityClasses();

      List<SzEntityType> overlapTypes
          = this.nextEntityTypes(overlapCount, entityClasses, idMode);

      List<SzEntityType> bodyTypes = new ArrayList<>(bodyCount);
      bodyTypes.addAll(overlapTypes);

      int bodyOnlyCount = bodyCount - overlapCount;
      bodyTypes.addAll(
          this.nextEntityTypes(bodyOnlyCount, entityClasses, idMode));

      queryTypes.addAll(overlapTypes);

      List<String> queryCodes = new ArrayList<>(queryTypes.size());
      for (SzEntityType entityType: queryTypes) {
        queryCodes.add(entityType.getEntityTypeCode());
        expectedTypes.put(entityType.getEntityTypeCode(), entityType);
      }

      for (SzEntityType entityType: bodyTypes) {
        expectedTypes.put(entityType.getEntityTypeCode(), entityType);
      }

      // if we have query-only entity types then they MUST have an entity class
      String queryClassCode = (queryOnlyCount > 0)
          ? queryTypes.get(0).getEntityClassCode() : null;

      if (queryClassCode == null && queryClass) {
        queryClassCode = bodyTypeVariant.inferQueryClassCode(bodyTypes);
      }

      String  suffix      = this.buildPostEntityTypesQuery(queryCodes,
                                                           queryClassCode,
                                                           withRaw);
      String  relativeUri = "entity-types" + suffix;
      String  uriText     = this.formatServerUri(relativeUri);
      UriInfo uriInfo     = this.newProxyUriInfo(uriText);
      String  bodyContent = bodyTypeVariant.formatValues(queryClassCode,
                                                         bodyTypes);
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

      SzEntityTypesResponse response = this.invokeServerViaHttp(
          POST,
          uriText,
          null,
          bodyTypeVariant.getFormatting().getContentType(),
          bodyContentData,
          SzEntityTypesResponse.class);

      response.concludeTimers();
      long after = System.currentTimeMillis();

      validateEntityTypesResponse(testInfo,
                                  response,
                                  POST,
                                  uriText,
                                  before,
                                  after,
                                  TRUE.equals(withRaw),
                                  expectedTypes);
    });
  }

  @ParameterizedTest
  @MethodSource("getPostEntityTypeForClassParameters")
  public void postEntityTypesForClassTest(
      int                     queryTypeCount,
      EntityTypeBodyVariant   bodyTypeVariant,
      int                     overlapCount,
      Boolean                 withRaw)
  {
    this.performTest(() -> {
      this.revertToInitialConfig();

      List<String>  singleClassList = bodyTypeVariant.getEntityClasses();
      String        entityClass     = singleClassList.get(0);

      Map<String, SzEntityType> expectedTypes = new LinkedHashMap<>();
      expectedTypes.putAll(this.getInitialEntityTypes());

      int queryOnlyCount = queryTypeCount - overlapCount;

      List<SzEntityType> queryTypes = this.nextEntityTypes(
          queryOnlyCount, singleClassList, AUTOMATIC);

      // get the information on the body entity types
      int           bodyCount     = bodyTypeVariant.getCount();
      SpecifiedMode idMode        = bodyTypeVariant.getIdentifierMode();

      List<SzEntityType> overlapTypes
          = this.nextEntityTypes(overlapCount, singleClassList, idMode);

      List<SzEntityType> bodyTypes = new ArrayList<>(bodyCount);
      bodyTypes.addAll(overlapTypes);

      int bodyOnlyCount = bodyCount - overlapCount;
      bodyTypes.addAll(
          this.nextEntityTypes(bodyOnlyCount, singleClassList, idMode));

      queryTypes.addAll(overlapTypes);

      List<String> queryCodes = new ArrayList<>(queryTypes.size());
      for (SzEntityType entityType: queryTypes) {
        queryCodes.add(entityType.getEntityTypeCode());
        expectedTypes.put(entityType.getEntityTypeCode(), entityType);
      }

      for (SzEntityType entityType: bodyTypes) {
        expectedTypes.put(entityType.getEntityTypeCode(), entityType);
      }

      String  suffix      = this.buildPostEntityTypesQuery(queryCodes,
                                                           withRaw);
      String  relativeUri = "entity-classes/" + entityClass + "/entity-types"
          + suffix;
      String  uriText     = this.formatServerUri(relativeUri);
      UriInfo uriInfo     = this.newProxyUriInfo(uriText);
      String  bodyContent = bodyTypeVariant.formatValues(entityClass,
                                                         bodyTypes);
      String  testInfo    = this.formatTestInfo(relativeUri,
                                                bodyContent);

      long before = System.currentTimeMillis();

      SzEntityTypesResponse response
          = this.configServices.addEntityTypesForClass(entityClass,
                                                       queryCodes,
                                                       TRUE.equals(withRaw),
                                                       uriInfo,
                                                       bodyContent);

      response.concludeTimers();
      long after = System.currentTimeMillis();

      validateEntityTypesResponse(testInfo,
                                  response,
                                  POST,
                                  uriText,
                                  before,
                                  after,
                                  TRUE.equals(withRaw),
                                  expectedTypes);
    });
  }

  @ParameterizedTest
  @MethodSource("getPostEntityTypeForClassParameters")
  public void postEntityTypesForClassTestViaHttp(
      int                    queryTypeCount,
      EntityTypeBodyVariant  bodyTypeVariant,
      int                    overlapCount,
      Boolean                withRaw)
  {
    this.performTest(() -> {
      this.revertToInitialConfig();

      List<String>  singleClassList = bodyTypeVariant.getEntityClasses();
      String        entityClass     = singleClassList.get(0);

      Map<String, SzEntityType> expectedTypes = new LinkedHashMap<>();
      expectedTypes.putAll(this.getInitialEntityTypes());

      int queryOnlyCount = queryTypeCount - overlapCount;

      List<SzEntityType> queryTypes = this.nextEntityTypes(
          queryOnlyCount, singleClassList, AUTOMATIC);

      // get the information on the body entity types
      int           bodyCount     = bodyTypeVariant.getCount();
      SpecifiedMode idMode        = bodyTypeVariant.getIdentifierMode();

      List<SzEntityType> overlapTypes
          = this.nextEntityTypes(overlapCount, singleClassList, idMode);

      List<SzEntityType> bodyTypes = new ArrayList<>(bodyCount);
      bodyTypes.addAll(overlapTypes);

      int bodyOnlyCount = bodyCount - overlapCount;
      bodyTypes.addAll(
          this.nextEntityTypes(bodyOnlyCount, singleClassList, idMode));

      queryTypes.addAll(overlapTypes);

      List<String> queryCodes = new ArrayList<>(queryTypes.size());
      for (SzEntityType entityType: queryTypes) {
        queryCodes.add(entityType.getEntityTypeCode());
        expectedTypes.put(entityType.getEntityTypeCode(), entityType);
      }

      for (SzEntityType entityType: bodyTypes) {
        expectedTypes.put(entityType.getEntityTypeCode(), entityType);
      }

      String  suffix      = this.buildPostEntityTypesQuery(queryCodes,
                                                           withRaw);

      String  relativeUri = "entity-classes/" + entityClass + "/entity-types"
          + suffix;
      String  uriText     = this.formatServerUri(relativeUri);
      UriInfo uriInfo     = this.newProxyUriInfo(uriText);
      String  bodyContent = bodyTypeVariant.formatValues(entityClass,
                                                         bodyTypes);
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

      SzEntityTypesResponse response = this.invokeServerViaHttp(
          POST,
          uriText,
          null,
          bodyTypeVariant.getFormatting().getContentType(),
          bodyContentData,
          SzEntityTypesResponse.class);

      response.concludeTimers();
      long after = System.currentTimeMillis();

      validateEntityTypesResponse(testInfo,
                                  response,
                                  POST,
                                  uriText,
                                  before,
                                  after,
                                  TRUE.equals(withRaw),
                                  expectedTypes);
    });
  }

  // TODO(bcaceres): Renable this test when entity classes other than ACTOR
  // are supported by the product.
  //@Test
  public void postEntityTypesForWrongClassTest()
  {
    this.performTest(() -> {
      this.revertToInitialConfig();

      SzEntityType entityType = this.nextEntityType(SINGLE_ENITTY_CLASS_LIST,
                                                    EXPLICIT);

      List<String> emptyList = Collections.emptyList();

      String  relativeUri = "entity-classes/OBJECT/entity-types";
      String  uriText     = this.formatServerUri(relativeUri);
      UriInfo uriInfo     = this.newProxyUriInfo(uriText);
      String  bodyContent = entityType.toJson();
      String  testInfo    = this.formatTestInfo(relativeUri,
                                                bodyContent);

      long before = System.currentTimeMillis();

      try {
        this.configServices.addEntityTypesForClass("OBJECT",
                                                   emptyList,
                                                   true,
                                                   uriInfo,
                                                   bodyContent);

        fail("Expected a BadRequestException for mismatched entity classes");

      } catch (BadRequestException expected) {
        SzErrorResponse response
            = (SzErrorResponse) expected.getResponse().getEntity();
        response.concludeTimers();
        long after = System.currentTimeMillis();
        validateBasics(
            response, 400, POST, uriText, before, after);

      }
    });
  }

  // TODO(bcaceres): Renable this test when entity classes other than ACTOR
  // are supported by the product.
  //@Test
  public void postEntityTypesForWrongClassTestViaHttp() {
    this.performTest(() -> {
      this.revertToInitialConfig();

      SzEntityType entityType = this.nextEntityType(SINGLE_ENITTY_CLASS_LIST,
                                                    EXPLICIT);

      List<String> emptyList = Collections.emptyList();

      String  relativeUri = "entity-classes/OBJECT/entity-types";
      String  uriText     = this.formatServerUri(relativeUri);
      UriInfo uriInfo     = this.newProxyUriInfo(uriText);
      String  bodyContent = entityType.toJson();
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

      SzErrorResponse response = this.invokeServerViaHttp(
          POST,
          uriText,
          null,
          "application/json",
          bodyContentData,
          SzErrorResponse.class);

      response.concludeTimers();
      long after = System.currentTimeMillis();

      validateBasics(
          testInfo, response, 400, POST, uriText, before, after);
    });
  }

  // TODO(bcaceres): Renable this test when entity classes other than ACTOR
  // are supported by the product.
  //@Test
  public void postEntityTypesForInvalidClassTest()
  {
    this.performTest(() -> {
      this.revertToInitialConfig();

      String entityClass = "DOES_NOT_EXIST";
      List<String> entityClassList = Collections.singletonList(entityClass);

      SzEntityType entityType = this.nextEntityType(entityClassList,
                                                    EXPLICIT);

      List<String> emptyList = Collections.emptyList();

      String  relativeUri = "entity-classes/" + entityClass + "/entity-types";
      String  uriText     = this.formatServerUri(relativeUri);
      UriInfo uriInfo     = this.newProxyUriInfo(uriText);
      String  bodyContent = entityType.toJson();
      String  testInfo    = this.formatTestInfo(relativeUri,
                                                bodyContent);

      long before = System.currentTimeMillis();

      try {
        this.configServices.addEntityTypesForClass(entityClass,
                                                   emptyList,
                                                   true,
                                                   uriInfo,
                                                   bodyContent);

        fail("Expected a NotFoundException for an invalid entity class");

      } catch (NotFoundException expected) {
        SzErrorResponse response
            = (SzErrorResponse) expected.getResponse().getEntity();
        response.concludeTimers();
        long after = System.currentTimeMillis();
        validateBasics(
            response, 404, POST, uriText, before, after);

      }
    });
  }

  // TODO(bcaceres): Renable this test when entity classes other than ACTOR
  // are supported by the product.
  //@Test
  public void postEntityTypesForInvalidClassTestViaHttp() {
    this.performTest(() -> {
      this.revertToInitialConfig();

      String entityClass = "DOES_NOT_EXIST";
      List<String> entityClassList = Collections.singletonList(entityClass);

      SzEntityType entityType = this.nextEntityType(entityClassList,
                                                    EXPLICIT);

      List<String> emptyList = Collections.emptyList();

      String  relativeUri = "entity-classes/" + entityClass + "/entity-types";
      String  uriText     = this.formatServerUri(relativeUri);
      UriInfo uriInfo     = this.newProxyUriInfo(uriText);
      String  bodyContent = entityType.toJson();
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

      SzErrorResponse response = this.invokeServerViaHttp(
          POST,
          uriText,
          null,
          "application/json",
          bodyContentData,
          SzErrorResponse.class);

      response.concludeTimers();
      long after = System.currentTimeMillis();

      validateBasics(
          testInfo, response, 404, POST, uriText, before, after);
    });
  }
}
