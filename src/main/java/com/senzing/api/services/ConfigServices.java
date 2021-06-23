package com.senzing.api.services;

import com.senzing.api.model.*;
import com.senzing.g2.engine.G2Config;
import com.senzing.g2.engine.G2ConfigMgr;
import com.senzing.g2.engine.G2Engine;
import com.senzing.g2.engine.Result;
import com.senzing.util.JsonUtils;
import com.senzing.util.Timers;

import javax.json.*;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriInfo;

import java.util.*;

import static com.senzing.api.model.SzHttpMethod.*;

/**
 * Provides config related API services.
 */
@Produces("application/json; charset=UTF-8")
@Path("/")
public class ConfigServices implements ServicesSupport {
  /**
   * The maximum length for comments used when adding a config via the
   * {@link G2ConfigMgr#addConfig(String, String, Result)} function.
   */
  private static final int MAX_CONFIG_COMMENT_LENGTH = 150;

  /**
   * Provides the implementation of <tt>GET /data-sources</tt>.
   *
   * @param withRaw Whether or not the raw native Senzing JSON should be
   *                included in the response.
   * @param uriInfo The {@link UriInfo} for the request.
   *
   * @return The {@link SzDataSourcesResponse} describing the response.
   */
  @GET
  @Path("data-sources")
  public SzDataSourcesResponse getDataSources(
      @DefaultValue("false") @QueryParam("withRaw") boolean withRaw,
      @Context UriInfo uriInfo) {
    Timers timers = this.newTimers();
    SzApiProvider provider = this.getApiProvider();

    try {
      // get the engine API and the config API
      G2Engine engineApi = provider.getEngineApi();
      G2Config configApi = provider.getConfigApi();

      this.enteringQueue(timers);
      String rawData = provider.executeInThread(() -> {
        this.exitingQueue(timers);
        return this.doGetDataSources(GET, uriInfo, timers, engineApi, configApi);
      });

      return this.buildDataSourcesResponse(
          GET, uriInfo, timers, rawData, withRaw);

    } catch (ServerErrorException e) {
      e.printStackTrace();
      throw e;

    } catch (WebApplicationException e) {
      throw e;

    } catch (Exception e) {
      e.printStackTrace();
      throw newInternalServerErrorException(GET, uriInfo, timers, e);
    }
  }

  /**
   * Provides the implementation of <tt>GET /data-sources/{dataSourceCode}</tt>.
   *
   * @param dataSourceCode The data source code from the URL path.
   * @param withRaw Whether or not the raw native Senzing JSON should be
   *                included in the response.
   * @param uriInfo The {@link UriInfo} for the request.
   *
   * @return The {@link SzDataSourceResponse} describing the response.
   */
  @GET
  @Path("data-sources/{dataSourceCode}")
  public SzDataSourceResponse getDataSource(
      @PathParam("dataSourceCode") String dataSourceCode,
      @DefaultValue("false") @QueryParam("withRaw") boolean withRaw,
      @Context UriInfo uriInfo)
  {
    Timers timers = this.newTimers();
    SzApiProvider provider = this.getApiProvider();

    try {
      // get the engine API and the config API
      G2Engine engineApi = provider.getEngineApi();
      G2Config configApi = provider.getConfigApi();

      this.enteringQueue(timers);
      String rawData = provider.executeInThread(() -> {
        this.exitingQueue(timers);
        String code = dataSourceCode.trim().toUpperCase();
        if (!provider.getDataSources(code).contains(code)) {
          throw this.newNotFoundException(
              GET, uriInfo, timers,
              "The specified data source code was not recognized: " + code);
        }
        return this.doGetDataSources(GET, uriInfo, timers, engineApi, configApi);
      });

      return this.buildDataSourceResponse(
          GET, uriInfo, timers, dataSourceCode, rawData, withRaw);

    } catch (ServerErrorException e) {
      e.printStackTrace();
      throw e;

    } catch (WebApplicationException e) {
      throw e;

    } catch (Exception e) {
      e.printStackTrace();
      throw newInternalServerErrorException(GET, uriInfo, timers, e);
    }
  }

  /**
   * Handles getting the data sources for an operation.
   *
   * @param httpMethod The {@link SzHttpMethod} for the request.
   * @param uriInfo The {@link UriInfo} for the request.
   * @param timers The {@link Timers} for the operation.
   * @param engineApi The {@link G2Engine} instance to use.
   * @param configApi The {@link G2Config} instance to use.
   *
   * @return The raw data describing the data sources.
   */
  protected String doGetDataSources(SzHttpMethod httpMethod,
                                    UriInfo uriInfo,
                                    Timers timers,
                                    G2Engine engineApi,
                                    G2Config configApi)
  {
    String config = this.exportConfig(GET, uriInfo, timers, engineApi);

    Long configId = null;
    try {
      // load into a config object by ID
      this.callingNativeAPI(timers, "config", "load");
      configId = configApi.load(config);
      this.calledNativeAPI(timers, "config", "load");

      if (configId < 0) {
        throw this.newInternalServerErrorException(
            GET, uriInfo, timers, configApi);
      }

      StringBuffer sb = new StringBuffer();

      // list the data sources on the config
      this.callingNativeAPI(timers, "config", "listDataSourcesV2");
      int returnCode = configApi.listDataSourcesV2(configId, sb);
      if (returnCode != 0) {
        throw this.newInternalServerErrorException(
            GET, uriInfo, timers, configApi);
      }
      this.calledNativeAPI(timers, "config", "listDataSourcesV2");

      return sb.toString();

    } finally {
      if (configId != null) {
        configApi.close(configId);
      }
    }
  }

  /**
   * Builds an {@link SzDataSourcesResponse} from the specified raw data using
   * the specified parameters of the request.
   *
   * @param httpMethod The {@link SzHttpMethod} for the request.
   * @param uriInfo The {@link UriInfo} for the request.
   * @param timers The {@link Timers} for the operation.
   * @param rawData The raw data JSON text describing the data sources.
   * @param withRaw <tt>true</tt> if the raw data should be included in the
   *                response, and <tt>false</tt> if it should be excluded.
   *
   * @return The {@link SzDataSourcesResponse} created with the specified
   *         parameters.
   */
  protected SzDataSourcesResponse buildDataSourcesResponse(
      SzHttpMethod httpMethod,
      UriInfo uriInfo,
      Timers timers,
      String rawData,
      boolean withRaw)
  {
    this.processingRawData(timers);
    // parse the raw data
    JsonObject jsonObject = JsonUtils.parseJsonObject(rawData);

    // get the array and construct the response
    JsonArray jsonArray = jsonObject.getJsonArray("DATA_SOURCES");
    List<SzDataSource> dataSources = this.parseDataSourceList(jsonArray);

    SzDataSourcesResponse response = this.newDataSourcesResponse(
        httpMethod, 200, uriInfo, timers, dataSources);
    this.processedRawData(timers);

    // if including raw data then add it
    if (withRaw) response.setRawData(rawData);

    return response;
  }

  /**
   * Parses the specified {@link JsonArray} describing the data sources in the
   * raw format and produces a {@link List} of {@link SzDataSource} instances.
   *
   * @param jsonArray The {@link JsonArray} describing the data sources.
   *
   * @return The created {@link List} of {@link SzDataSource} instances.
   */
  protected List<SzDataSource> parseDataSourceList(JsonArray jsonArray) {
    return SzDataSource.parseDataSourceList(null, jsonArray);
  }

  /**
   * Creates a new instance of {@link SzDataSourceResponse} and configures it
   * with the specified {@link List} of {@link SzDataSource} instances.
   *
   * @param httpMethod The {@link SzHttpMethod} for the request.
   * @param httpStatusCode The HTTP status code for the response.
   * @param uriInfo The {@link UriInfo} for the request.
   * @param timers The {@link Timers} for the operation.
   * @param dataSources The {@link List} of {@link SzDataSource} instances.
   *
   * @return The newly created {@link SzDataSourcesResponse}.
   */
  protected SzDataSourcesResponse newDataSourcesResponse(
      SzHttpMethod        httpMethod,
      int                 httpStatusCode,
      UriInfo             uriInfo,
      Timers              timers,
      List<SzDataSource>  dataSources)
  {
    return SzDataSourcesResponse.FACTORY.create(
        this.newMeta(httpMethod, httpStatusCode, timers),
        this.newLinks(uriInfo),
        this.newDataSourcesResponseData(dataSources));
  }

  /**
   * Creates a new instance of {@link SzDataSourceResponseData} and configures
   * it with the specified {@link List} of {@link SzDataSource} instances.
   *
   * @param dataSources The {@link List} of {@link SzDataSource} instances.
   *
   * @return The newly created {@link SzDataSourcesResponse}.
   */
  protected SzDataSourcesResponseData newDataSourcesResponseData(
      List<SzDataSource>  dataSources)
  {
    return SzDataSourcesResponseData.FACTORY.create(dataSources);
  }

  /**
   * Builds an {@link SzDataSourceResponse} from the specified raw data using
   * the specified parameters of the request.
   *
   * @param httpMethod The {@link SzHttpMethod} for the request.
   * @param uriInfo The {@link UriInfo} for the request.
   * @param timers The {@link Timers} for the operation.
   * @param dataSourceCode The data source code for the data source.
   * @param rawData The raw data JSON text describing the data sources.
   * @param withRaw <tt>true</tt> if the raw data should be included in the
   *                response, and <tt>false</tt> if it should be excluded.
   *
   * @return The {@link SzDataSourceResponse} created with the specified
   *         parameters.
   */
  protected SzDataSourceResponse buildDataSourceResponse(
      SzHttpMethod  httpMethod,
      UriInfo       uriInfo,
      Timers        timers,
      String        dataSourceCode,
      String        rawData,
      boolean       withRaw)
  {
    this.processingRawData(timers);
    dataSourceCode = dataSourceCode.trim().toUpperCase();

    // parse the raw data
    JsonObject jsonObject = JsonUtils.parseJsonObject(rawData);

    // get the array of data sources
    JsonArray jsonArray = jsonObject.getJsonArray("DATA_SOURCES");

    // find the one matching the specified data source code
    jsonObject = null;
    for (JsonObject jsonObj : jsonArray.getValuesAs(JsonObject.class)) {
      String code = JsonUtils.getString(jsonObj, "DSRC_CODE");
      if (code.contentEquals(dataSourceCode)) {
        jsonObject = jsonObj;
        break;
      }
    }

    // check if not found
    if (jsonObject == null) {
      throw this.newNotFoundException(
          httpMethod, uriInfo, timers,
          "The specified data source was not recognized: " + dataSourceCode);
    }

    // parse the data source
    SzDataSource dataSource = this.parseDataSource(jsonObject);

    // build the response
    SzDataSourceResponse response = this.newDataSourceResponse(
        httpMethod, 200, uriInfo, timers, dataSource);

    this.processedRawData(timers);

    // if including raw data then add it
    if (withRaw) response.setRawData(JsonUtils.toJsonText(jsonObject));

    return response;
  }

  /**
   * Parses the JSON described by the specified {@link JsonObject} as a
   * data source in the raw format and produces an instance of {@link
   * SzDataSource}.
   *
   * @param jsonObject The {@link JsonObject} describing the data source in
   *                   the raw format.
   *
   * @return The newly created and configured {@link SzDataSource}.
   */
  protected SzDataSource parseDataSource(JsonObject jsonObject) {
    return SzDataSource.parseDataSource(null, jsonObject);
  }

  /**
   * Creates a new {@link SzDataSourceResponse} with the specified parameters.
   *
   * @param httpMethod The {@link SzHttpMethod} for the request.
   * @param httpStatusCode The HTTP status code for the response.
   * @param uriInfo The {@link UriInfo} for the request.
   * @param timers The {@link Timers} for the operation.
   * @param dataSource The {@link SzDataSource} instance.
   *
   * @return The newly created {@link SzDataSourceResponse}.
   */
  protected SzDataSourceResponse newDataSourceResponse(
      SzHttpMethod  httpMethod,
      int           httpStatusCode,
      UriInfo       uriInfo,
      Timers        timers,
      SzDataSource  dataSource)
  {
    return SzDataSourceResponse.FACTORY.create(
        this.newMeta(httpMethod, httpStatusCode, timers),
        this.newLinks(uriInfo),
        this.newDataSourceResponseData(dataSource));
  }

  /**
   * Creates a new {@link SzDataSourceResponse} with the specified parameters.
   *
   * @param dataSource The {@link SzDataSource} instance.
   *
   * @return The newly created {@link SzDataSourceResponse}.
   */
  protected SzDataSourceResponseData newDataSourceResponseData(
      SzDataSource  dataSource)
  {
    return SzDataSourceResponseData.FACTORY.create(dataSource);
  }

  /**
   * Provides the implementation of <tt>GET /entity-classes</tt>.
   *
   * @param withRaw Whether or not the raw native Senzing JSON should be
   *                included in the response.
   * @param uriInfo The {@link UriInfo} for the request.
   *
   * @return The {@link SzEntityClassesResponse} describing the response.
   */
  @GET
  @Path("entity-classes")
  public SzEntityClassesResponse getEntityClasses(
      @DefaultValue("false") @QueryParam("withRaw") boolean withRaw,
      @Context UriInfo uriInfo) {
    Timers timers = this.newTimers();
    SzApiProvider provider = this.getApiProvider();

    try {
      // get the engine API and the config API
      G2Engine engineApi = provider.getEngineApi();
      G2Config configApi = provider.getConfigApi();

      this.enteringQueue(timers);
      String rawData = provider.executeInThread(() -> {
        this.exitingQueue(timers);
        return this.doGetEntityClasses(
            GET, uriInfo, timers, engineApi, configApi);
      });

      //---------------------------------------------------------------------
      // strip out any entity classes other than ACTOR
      // TODO(bcaceres) -- remove this code when entity classes other than
      // ACTOR are supported ** OR ** when the API server no longer supports
      // product versions that ship with alternate entity classes
      JsonObjectBuilder job = Json.createObjectBuilder();
      JsonArrayBuilder  jab = Json.createArrayBuilder();
      JsonObject  jsonObject    = JsonUtils.parseJsonObject(rawData);
      JsonArray   entityClasses = jsonObject.getJsonArray("ENTITY_CLASSES");
      for (JsonObject entityClass : entityClasses.getValuesAs(JsonObject.class))
      {
        if (entityClass.getString("ECLASS_CODE").equals("ACTOR")) {
          jab.add(Json.createObjectBuilder(entityClass));
          break;
        }
      }
      job.add("ENTITY_CLASSES", jab);
      rawData = JsonUtils.toJsonText(job);
      //---------------------------------------------------------------------

      return this.buildEntityClassesResponse(
          GET, uriInfo, timers, rawData, withRaw);

    } catch (ServerErrorException e) {
      e.printStackTrace();
      throw e;

    } catch (WebApplicationException e) {
      throw e;

    } catch (Exception e) {
      e.printStackTrace();
      throw newInternalServerErrorException(GET, uriInfo, timers, e);
    }
  }

  /**
   * Provides the implementation of
   * <tt>GET /entity-classes/{entityClassCode}</tt>.
   *
   * @param entityClassCode The entity class code from the URI path.
   * @param withRaw Whether or not the raw native Senzing JSON should be
   *                included in the response.
   * @param uriInfo The {@link UriInfo} for the request.
   *
   * @return The {@link SzEntityClassResponse} describing the response.
   */  @GET
  @Path("entity-classes/{entityClassCode}")
  public SzEntityClassResponse getEntityClass(
      @PathParam("entityClassCode") String entityClassCode,
      @DefaultValue("false") @QueryParam("withRaw") boolean withRaw,
      @Context UriInfo uriInfo)
  {
    Timers timers = this.newTimers();
    SzApiProvider provider = this.getApiProvider();

    try {
      // get the engine API and the config API
      G2Engine engineApi = provider.getEngineApi();
      G2Config configApi = provider.getConfigApi();

      //---------------------------------------------------------------------
      // check the entity class code to ensure it is ACTOR
      // TODO(bcaceres) -- remove this code when entity classes other than
      // ACTOR are supported ** OR ** when the API server no longer supports
      // product versions that ship with alternate entity classes
      if (!entityClassCode.trim().equalsIgnoreCase("ACTOR")) {
        throw this.newNotFoundException(
            GET, uriInfo, timers,
            "The entity class code was not recognized: " + entityClassCode);
      }
      //---------------------------------------------------------------------

      this.enteringQueue(timers);
      String rawData = provider.executeInThread(() -> {
        this.exitingQueue(timers);
        String code = entityClassCode.trim().toUpperCase();
        if (!provider.getEntityClasses(code).contains(code)) {
          throw this.newNotFoundException(
              GET, uriInfo, timers,
              "The specified entity class code was not recognized: " + code);
        }
        return this.doGetEntityClasses(
            GET, uriInfo, timers, engineApi, configApi);
      });

      return this.buildEntityClassResponse(
          GET, uriInfo, timers, entityClassCode, rawData, withRaw);

    } catch (ServerErrorException e) {
      e.printStackTrace();
      throw e;

    } catch (WebApplicationException e) {
      throw e;

    } catch (Exception e) {
      e.printStackTrace();
      throw newInternalServerErrorException(GET, uriInfo, timers, e);
    }
  }

  /**
   * Handles getting the entity classes for an operation.
   *
   * @param httpMethod The {@link SzHttpMethod} for the request.
   * @param uriInfo The {@link UriInfo} for the request.
   * @param timers The {@link Timers} for the operation.
   * @param engineApi The {@link G2Engine} instance to use.
   * @param configApi The {@link G2Config} instance to use.
   *
   * @return The raw data describing the data sources.
   */
  protected String doGetEntityClasses(SzHttpMethod  httpMethod,
                                      UriInfo       uriInfo,
                                      Timers        timers,
                                      G2Engine      engineApi,
                                      G2Config      configApi)
  {
    String config = this.exportConfig(httpMethod, uriInfo, timers, engineApi);

    Long configId = null;
    try {
      // load into a config object by ID
      this.callingNativeAPI(timers, "config", "load");
      configId = configApi.load(config);
      this.calledNativeAPI(timers, "config", "load");

      if (configId < 0) {
        throw this.newInternalServerErrorException(
            httpMethod, uriInfo, timers, configApi);
      }

      StringBuffer sb = new StringBuffer();

      // list the data sources on the config
      this.callingNativeAPI(timers, "config", "listEntityClassesV2");
      int returnCode = configApi.listEntityClassesV2(configId, sb);
      this.calledNativeAPI(timers, "config", "listEntityClassesV2");
      if (returnCode != 0) {
        throw this.newInternalServerErrorException(
            GET, uriInfo, timers, configApi);
      }

      return sb.toString();

    } finally {
      if (configId != null) {
        configApi.close(configId);
      }
    }
  }

  /**
   * Builds an {@link SzEntityClassesResponse} from the specified raw data using
   * the specified parameters of the request.
   *
   * @param httpMethod The {@link SzHttpMethod} for the request.
   * @param uriInfo The {@link UriInfo} for the request.
   * @param timers The {@link Timers} for the operation.
   * @param rawData The raw data JSON text describing the entity classes.
   * @param withRaw <tt>true</tt> if the raw data should be included in the
   *                response, and <tt>false</tt> if it should be excluded.
   *
   * @return The {@link SzEntityClassesResponse} created with the specified
   *         parameters.
   */
  protected SzEntityClassesResponse buildEntityClassesResponse(
      SzHttpMethod  httpMethod,
      UriInfo       uriInfo,
      Timers        timers,
      String        rawData,
      boolean       withRaw)
  {
    this.processingRawData(timers);
    // parse the raw data
    JsonObject jsonObject = JsonUtils.parseJsonObject(rawData);

    // get the array and construct the response
    JsonArray jsonArray = jsonObject.getJsonArray("ENTITY_CLASSES");
    List<SzEntityClass> entityClasses = this.parseEntityClassList(jsonArray);

    SzEntityClassesResponse response = this.newEntityClassesResponse(
        httpMethod, 200, uriInfo, timers, entityClasses);

    processedRawData(timers);

    // if including raw data then add it
    if (withRaw) response.setRawData(rawData);

    return response;
  }

  /**
   * Parses the specified {@link JsonArray} describing the entity classes in the
   * raw format and produces a {@link List} of {@link SzEntityClass} instances.
   *
   * @param jsonArray The {@link JsonArray} describing the data sources.
   *
   * @return The created {@link List} of {@link SzEntityClass} instances.
   */
  protected List<SzEntityClass> parseEntityClassList(JsonArray jsonArray) {
    return SzEntityClass.parseEntityClassList(null, jsonArray);
  }

  /**
   * Creates a new instance of {@link SzEntityClassesResponse} and configures
   * it with the specified {@link List} of {@link SzEntityClass} instances.
   *
   * @param httpMethod The {@link SzHttpMethod} for the request.
   * @param httpStatusCode The HTTP status code for the response.
   * @param uriInfo The {@link UriInfo} for the request.
   * @param timers The {@link Timers} for the operation.
   * @param entityClasses The {@link List} of {@link SzEntityClass} instances.
   *
   * @return The newly created {@link SzEntityClassesResponse}.
   */
  protected SzEntityClassesResponse newEntityClassesResponse(
      SzHttpMethod        httpMethod,
      int                 httpStatusCode,
      UriInfo             uriInfo,
      Timers              timers,
      List<SzEntityClass> entityClasses)
  {
    return SzEntityClassesResponse.FACTORY.create(
        this.newMeta(httpMethod, httpStatusCode, timers),
        this.newLinks(uriInfo),
        this.newEntityClassesResponseData(entityClasses));
  }

  /**
   * Creates a new instance of {@link SzEntityClassesResponseData} and
   * configures it with the specified {@link List} of {@link SzEntityClass}
   * instances.
   *
   * @param entityClasses The {@link List} of {@link SzEntityClass} instances.
   *
   * @return The newly created {@link SzEntityClassesResponseData}.
   */
  protected SzEntityClassesResponseData newEntityClassesResponseData(
      List<SzEntityClass> entityClasses)
  {
    return SzEntityClassesResponseData.FACTORY.create(entityClasses);
  }

  /**
   * Builds an {@link SzEntityClassResponse} from the specified raw data using
   * the specified parameters of the request.
   *
   * @param httpMethod The {@link SzHttpMethod} for the request.
   * @param uriInfo The {@link UriInfo} for the request.
   * @param timers The {@link Timers} for the operation.
   * @param entityClassCode The entity class code for the entity class.
   * @param rawData The raw data JSON text describing the data sources.
   * @param withRaw <tt>true</tt> if the raw data should be included in the
   *                response, and <tt>false</tt> if it should be excluded.
   *
   * @return The {@link SzEntityClassResponse} created with the specified
   *         parameters.
   */
  protected SzEntityClassResponse buildEntityClassResponse(
      SzHttpMethod  httpMethod,
      UriInfo       uriInfo,
      Timers        timers,
      String        entityClassCode,
      String        rawData,
      boolean       withRaw)
  {
    this.processingRawData(timers);
    entityClassCode = entityClassCode.trim().toUpperCase();

    // parse the raw data
    JsonObject jsonObject = JsonUtils.parseJsonObject(rawData);

    // get the array of data sources
    JsonArray jsonArray = jsonObject.getJsonArray("ENTITY_CLASSES");

    // find the one matching the specified data source code
    jsonObject = null;
    for (JsonObject jsonObj : jsonArray.getValuesAs(JsonObject.class)) {
      String code = JsonUtils.getString(jsonObj, "ECLASS_CODE");
      if (code.contentEquals(entityClassCode)) {
        jsonObject = jsonObj;
        break;
      }
    }

    // check if not found
    if (jsonObject == null) {
      throw this.newNotFoundException(
          httpMethod, uriInfo, timers,
          "The specified entity class was not recognized: " + entityClassCode);
    }

    // parse the data source
    SzEntityClass entityClass = this.parseEntityClass(jsonObject);

    // build the response
    SzEntityClassResponse response = this.newEntityClassResponse(
        httpMethod, 200, uriInfo, timers, entityClass);

    this.processedRawData(timers);

    // if including raw data then add it
    if (withRaw) response.setRawData(JsonUtils.toJsonText(jsonObject));

    return response;
  }

  /**
   * Parses the JSON described by the specified {@link JsonObject} as a
   * entity class in the raw format and produces an instance of {@link
   * SzEntityClass}.
   *
   * @param jsonObject The {@link JsonObject} describing the data source in
   *                   the raw format.
   *
   * @return The newly created and configured {@link SzEntityClass}.
   */
  protected SzEntityClass parseEntityClass(JsonObject jsonObject) {
    return SzEntityClass.parseEntityClass(null, jsonObject);
  }

  /**
   * Creates a new {@link SzEntityClassResponse} with the specified parameters.
   *
   * @param httpMethod The {@link SzHttpMethod} for the request.
   * @param httpStatusCode The HTTP status code for the response.
   * @param uriInfo The {@link UriInfo} for the request.
   * @param timers The {@link Timers} for the operation.
   * @param entityClass The {@link SzEntityClass} instance.
   *
   * @return The newly created {@link SzEntityClassResponse}.
   */
  protected SzEntityClassResponse newEntityClassResponse(
      SzHttpMethod  httpMethod,
      int           httpStatusCode,
      UriInfo       uriInfo,
      Timers        timers,
      SzEntityClass entityClass)
  {
    return SzEntityClassResponse.FACTORY.create(
        this.newMeta(httpMethod, httpStatusCode, timers),
        this.newLinks(uriInfo),
        this.newEntityClassResponseData(entityClass));
  }

  /**
   * Creates a new {@link SzEntityClassResponseData} with the specified
   * {@link SzEntityClass}.
   *
   * @param entityClass The {@link SzEntityClass} instance.
   *
   * @return The newly created {@link SzEntityClassResponseData}.
   */
  protected SzEntityClassResponseData newEntityClassResponseData(
      SzEntityClass entityClass)
  {
    return SzEntityClassResponseData.FACTORY.create(entityClass);
  }

  /**
   * Provides the implementation of
   * <tt>GET /entity-classes/{entityClassCode}/entity-types</tt>.
   *
   * @param entityClass The entity class code from the URI path.
   * @param withRaw Whether or not the raw native Senzing JSON should be
   *                included in the response.
   * @param uriInfo The {@link UriInfo} for the request.
   *
   * @return The {@link SzEntityTypesResponse} describing the response.
   */
  @GET
  @Path("entity-classes/{entityClass}/entity-types")
  public SzEntityTypesResponse getEntityTypesByClass(
      @PathParam("entityClass") String entityClass,
      @DefaultValue("false") @QueryParam("withRaw") boolean withRaw,
      @Context UriInfo uriInfo)
  {
    return this.getEntityTypes(entityClass, withRaw, uriInfo);
  }

  /**
   * Provides the implementation of <tt>GET /entity-types</tt>.
   *
   * @param entityClass The optional entity class query parameter.
   * @param withRaw Whether or not the raw native Senzing JSON should be
   *                included in the response.
   * @param uriInfo The {@link UriInfo} for the request.
   *
   * @return The {@link SzEntityTypesResponse} describing the response.
   */
  @GET
  @Path("entity-types")
  public SzEntityTypesResponse getEntityTypes(
      @QueryParam("entityClass") String entityClass,
      @DefaultValue("false") @QueryParam("withRaw") boolean withRaw,
      @Context UriInfo uriInfo) {
    Timers timers = this.newTimers();
    SzApiProvider provider = this.getApiProvider();

    try {
      // get the engine API and the config API
      G2Engine engineApi = provider.getEngineApi();
      G2Config configApi = provider.getConfigApi();

      //---------------------------------------------------------------------
      // check the entity class code to ensure it is ACTOR
      // TODO(bcaceres) -- remove this code when entity classes other than
      // ACTOR are supported ** OR ** when the API server no longer supports
      // product versions that ship with alternate entity classes
      if (entityClass != null
          && !entityClass.trim().toUpperCase().equals("ACTOR"))
      {
        throw this.newNotFoundException(
            GET, uriInfo, timers,
            "The entity class code was not recognized: " + entityClass);
      }
      //---------------------------------------------------------------------

      this.enteringQueue(timers);
      String rawData = provider.executeInThread(() -> {
        this.exitingQueue(timers);
        return this.doGetEntityTypes(
            GET, uriInfo, timers, engineApi, configApi);
      });

      return this.buildEntityTypesResponse(
          GET, uriInfo, timers, entityClass, rawData, withRaw);

    } catch (ServerErrorException e) {
      e.printStackTrace();
      throw e;

    } catch (WebApplicationException e) {
      throw e;

    } catch (Exception e) {
      e.printStackTrace();
      throw this.newInternalServerErrorException(GET, uriInfo, timers, e);
    }
  }

  /**
   * Provides the implementation of <tt>GET /entity-types/{entityTypeCode}</tt>.
   *
   * @param entityTypeCode The entity type code path parameter.
   * @param withRaw Whether or not the raw native Senzing JSON should be
   *                included in the response.
   * @param uriInfo The {@link UriInfo} for the request.
   *
   * @return The {@link SzEntityTypesResponse} describing the response.
   */
  @GET
  @Path("entity-types/{entityTypeCode}")
  public SzEntityTypeResponse getEntityType(
      @PathParam("entityTypeCode") String entityTypeCode,
      @DefaultValue("false") @QueryParam("withRaw") boolean withRaw,
      @Context UriInfo uriInfo)
  {
    Timers timers = this.newTimers();
    SzApiProvider provider = this.getApiProvider();

    try {
      // get the engine API and the config API
      G2Engine engineApi = provider.getEngineApi();
      G2Config configApi = provider.getConfigApi();

      this.enteringQueue(timers);
      String rawData = provider.executeInThread(() -> {
        this.exitingQueue(timers);
        String code = entityTypeCode.trim().toUpperCase();
        if (!provider.getEntityTypes(code).contains(code)) {
          throw this.newNotFoundException(
              GET, uriInfo, timers,
              "The specified entity type code was not recognized: " + code);
        }
        return this.doGetEntityTypes(
            GET, uriInfo, timers, engineApi, configApi);
      });

      return this.buildEntityTypeResponse(
          GET, uriInfo, timers, null,
          entityTypeCode, rawData, withRaw);

    } catch (ServerErrorException e) {
      e.printStackTrace();
      throw e;

    } catch (WebApplicationException e) {
      throw e;

    } catch (Exception e) {
      e.printStackTrace();
      throw this.newInternalServerErrorException(GET, uriInfo, timers, e);
    }
  }

  /**
   * Provides the implementation of
   * <tt>GET /entity-classes/{entityClassCode}/entity-types/{entityTypeCode}</tt>.
   *
   * @param entityClassCode The entity class code path parameter.
   * @param entityTypeCode The entity type code path parameter.
   * @param withRaw Whether or not the raw native Senzing JSON should be
   *                included in the response.
   * @param uriInfo The {@link UriInfo} for the request.
   *
   * @return The {@link SzEntityTypeResponse} describing the response.
   */
  @GET
  @Path("entity-classes/{entityClassCode}/entity-types/{entityTypeCode}")
  public SzEntityTypeResponse getEntityType(
      @PathParam("entityClassCode") String entityClassCode,
      @PathParam("entityTypeCode") String entityTypeCode,
      @DefaultValue("false") @QueryParam("withRaw") boolean withRaw,
      @Context UriInfo uriInfo)
  {
    Timers timers = this.newTimers();
    SzApiProvider provider = this.getApiProvider();

    //---------------------------------------------------------------------
    // check the entity class code to ensure it is ACTOR
    // TODO(bcaceres) -- remove this code when entity classes other than
    // ACTOR are supported ** OR ** when the API server no longer supports
    // product versions that ship with alternate entity classes
    if (!entityClassCode.trim().toUpperCase().equals("ACTOR")) {
      throw newNotFoundException(
          GET, uriInfo, timers,
          "The entity class code was not recognized: " + entityClassCode);
    }
    //---------------------------------------------------------------------

    try {
      // get the engine API and the config API
      G2Engine engineApi = provider.getEngineApi();
      G2Config configApi = provider.getConfigApi();

      this.enteringQueue(timers);
      String rawData = provider.executeInThread(() -> {
        this.exitingQueue(timers);
        String classCode = entityClassCode.trim().toUpperCase();
        if (!provider.getEntityClasses(classCode).contains(classCode)) {
          throw this.newNotFoundException(
              GET, uriInfo, timers,
              "The specified entity class code was not recognized: "
                  + classCode);
        }
        String typeCode = entityTypeCode.trim().toUpperCase();
        if (!provider.getEntityTypes(typeCode).contains(typeCode)) {
          throw this.newNotFoundException(
              GET, uriInfo, timers,
              "The specified entity type code was not recognized: " + typeCode);
        }
        return this.doGetEntityTypes(
            GET, uriInfo, timers, engineApi, configApi);
      });

      return this.buildEntityTypeResponse(
          GET, uriInfo, timers, entityClassCode,
          entityTypeCode, rawData, withRaw);

    } catch (ServerErrorException e) {
      e.printStackTrace();
      throw e;

    } catch (WebApplicationException e) {
      throw e;

    } catch (Exception e) {
      e.printStackTrace();
      throw this.newInternalServerErrorException(GET, uriInfo, timers, e);
    }
  }

  /**
   * Handles getting the entity types for an operation.
   *
   * @param httpMethod The {@link SzHttpMethod} for the request.
   * @param uriInfo The {@link UriInfo} for the request.
   * @param timers The {@link Timers} for the operation.
   * @param engineApi The {@link G2Engine} instance to use.
   * @param configApi The {@link G2Config} instance to use.
   *
   * @return The raw data describing the entity types.
   */
  protected String doGetEntityTypes(SzHttpMethod  httpMethod,
                                    UriInfo       uriInfo,
                                    Timers        timers,
                                    G2Engine      engineApi,
                                    G2Config      configApi)
  {
    String config = this.exportConfig(httpMethod, uriInfo, timers, engineApi);

    Long configId = null;
    try {
      // load into a config object by ID
      this.callingNativeAPI(timers, "config", "load");
      configId = configApi.load(config);
      this.calledNativeAPI(timers, "config", "load");

      if (configId < 0) {
        throw this.newInternalServerErrorException(
            httpMethod, uriInfo, timers, configApi);
      }

      StringBuffer sb = new StringBuffer();

      // list the data sources on the config
      this.callingNativeAPI(timers, "config", "listEntityTypesV2");
      int returnCode = configApi.listEntityTypesV2(configId, sb);
      if (returnCode != 0) {
        throw this.newInternalServerErrorException(
            GET, uriInfo, timers, configApi);
      }
      this.calledNativeAPI(timers, "config", "listEntityTypesV2");

      return sb.toString();

    } finally {
      if (configId != null) {
        configApi.close(configId);
      }
    }
  }

  /**
   * Builds an {@link SzEntityTypesResponse} from the specified raw data using
   * the specified parameters of the request.
   *
   * @param httpMethod The {@link SzHttpMethod} for the request.
   * @param uriInfo The {@link UriInfo} for the request.
   * @param timers The {@link Timers} for the operation.
   * @param entityClass The entity class code for the entity types, or
   *                    <tt>null</tt> if not filtering.
   * @param rawData The raw data JSON text describing the entity types.
   * @param withRaw <tt>true</tt> if the raw data should be included in the
   *                response, and <tt>false</tt> if it should be excluded.
   *
   * @return The {@link SzEntityTypesResponse} created with the specified
   *         parameters.
   */
  protected SzEntityTypesResponse buildEntityTypesResponse(
      SzHttpMethod  httpMethod,
      UriInfo       uriInfo,
      Timers        timers,
      String        entityClass,
      String        rawData,
      boolean       withRaw)
  {
    this.processingRawData(timers);
    // parse the raw data
    JsonObject jsonObject = JsonUtils.parseJsonObject(rawData);

    // get the array and construct the response
    JsonArray jsonArray = jsonObject.getJsonArray("ENTITY_TYPES");
    List<SzEntityType> entityTypes = this.parseEntityTypeList(jsonArray);

    // check if we are filtering on entity class
    if (entityClass != null) {
      final String ec = entityClass.trim();
      entityTypes.removeIf(et -> !et.getEntityClassCode().equalsIgnoreCase(ec));
    }

    // creat the response with the entity types
    SzEntityTypesResponse response = this.newEntityTypesResponse(
        httpMethod, 200, uriInfo, timers, entityTypes);

    // conclude the raw data processing
    this.processedRawData(timers);

    // if including raw data then add it
    if (withRaw) response.setRawData(rawData);

    return response;
  }

  /**
   * Parses the specified {@link JsonArray} describing the entity types in the
   * raw format and produces a {@link List} of {@link SzEntityType} instances.
   *
   * @param jsonArray The {@link JsonArray} describing the data sources.
   *
   * @return The created {@link List} of {@link SzEntityType} instances.
   */
  protected List<SzEntityType> parseEntityTypeList(JsonArray jsonArray) {
    return SzEntityType.parseEntityTypeList(null, jsonArray);
  }

  /**
   * Creates a new instance of {@link SzEntityTypesResponse} and configures
   * it with the specified {@link List} of {@link SzEntityType} instances.
   *
   * @param httpMethod The {@link SzHttpMethod} for the request.
   * @param httpStatusCode The HTTP status code for the response.
   * @param uriInfo The {@link UriInfo} for the request.
   * @param timers The {@link Timers} for the operation.
   * @param entityTypes The {@link List} of {@link SzEntityType} instances.
   *
   * @return The newly created {@link SzEntityTypesResponse}.
   */
  protected SzEntityTypesResponse newEntityTypesResponse(
      SzHttpMethod        httpMethod,
      int                 httpStatusCode,
      UriInfo             uriInfo,
      Timers              timers,
      List<SzEntityType>  entityTypes)
  {
    return SzEntityTypesResponse.FACTORY.create(
        this.newMeta(httpMethod, httpStatusCode, timers),
        this.newLinks(uriInfo),
        this.newEntityTypesResponseData(entityTypes));
  }

  /**
   * Creates a new instance of {@link SzEntityTypesResponseData} and configures
   * it with the specified {@link List} of {@link SzEntityType} instances.
   *
   * @param entityTypes The {@link List} of {@link SzEntityType} instances.
   *
   * @return The newly created {@link SzEntityTypesResponseData}.
   */
  protected SzEntityTypesResponseData newEntityTypesResponseData(
      List<SzEntityType>  entityTypes)
  {
    return SzEntityTypesResponseData.FACTORY.create(entityTypes);
  }

  /**
   * Builds an {@link SzEntityTypeResponse} from the specified raw data using
   * the specified parameters of the request.
   *
   * @param httpMethod The {@link SzHttpMethod} for the request.
   * @param uriInfo The {@link UriInfo} for the request.
   * @param timers The {@link Timers} for the operation.
   * @param entityClassCode The entity class code for the entity class.
   * @param entityTypeCode The entity type code for the entity type.
   * @param rawData The raw data JSON text describing the data sources.
   * @param withRaw <tt>true</tt> if the raw data should be included in the
   *                response, and <tt>false</tt> if it should be excluded.
   *
   * @return The {@link SzEntityTypeResponse} created with the specified
   *         parameters.
   */
  protected SzEntityTypeResponse buildEntityTypeResponse(
      SzHttpMethod  httpMethod,
      UriInfo       uriInfo,
      Timers        timers,
      String        entityClassCode,
      String        entityTypeCode,
      String        rawData,
      boolean       withRaw)
  {
    this.processingRawData(timers);
    entityTypeCode = entityTypeCode.trim().toUpperCase();

    // parse the raw data
    JsonObject jsonObject = JsonUtils.parseJsonObject(rawData);

    // get the array of data sources
    JsonArray jsonArray = jsonObject.getJsonArray("ENTITY_TYPES");

    // find the one matching the specified data source code
    jsonObject = null;
    for (JsonObject jsonObj : jsonArray.getValuesAs(JsonObject.class)) {
      String code = JsonUtils.getString(jsonObj, "ETYPE_CODE");
      if (code.equalsIgnoreCase(entityTypeCode)) {
        jsonObject = jsonObj;
        break;
      }
    }

    // check if not found
    if (jsonObject == null) {
      System.out.println("ENTITY TYPE CODE NOT FOUND: " + rawData);
      throw this.newNotFoundException(
          httpMethod, uriInfo, timers,
          "The specified entity type was not recognized: " + entityTypeCode);
    }

    // parse the data source
    SzEntityType entityType = this.parseEntityType(jsonObject);

    // check if being found for a specific entity class and verify
    if (entityClassCode != null
        && !entityType.getEntityClassCode().equalsIgnoreCase(entityClassCode)) {
      throw this.newNotFoundException(
          httpMethod, uriInfo, timers,
          "The specified entity type code (" + entityTypeCode
              + ") does not have the specified entity class code: "
              + entityClassCode);
    }

    // build the response
    SzEntityTypeResponse response = this.newEntityTypeResponse(
        httpMethod, 200, uriInfo, timers, entityType);

    this.processedRawData(timers);

    // if including raw data then add it
    if (withRaw) response.setRawData(JsonUtils.toJsonText(jsonObject));

    return response;
  }

  /**
   * Parses the JSON described by the specified {@link JsonObject} as a
   * entity type in the raw format and produces an instance of {@link
   * SzEntityType}.
   *
   * @param jsonObject The {@link JsonObject} describing the data source in
   *                   the raw format.
   *
   * @return The newly created and configured {@link SzEntityType}.
   */
  protected SzEntityType parseEntityType(JsonObject jsonObject) {
    return SzEntityType.parseEntityType(null, jsonObject);
  }

  /**
   * Creates a new {@link SzEntityTypeResponse} with the specified parameters.
   *
   * @param httpMethod The {@link SzHttpMethod} for the request.
   * @param httpStatusCode The HTTP status code for the response.
   * @param uriInfo The {@link UriInfo} for the request.
   * @param timers The {@link Timers} for the operation.
   * @param entityType The {@link SzEntityType} instance.
   *
   * @return The newly created {@link SzEntityTypeResponse}.
   */
  protected SzEntityTypeResponse newEntityTypeResponse(
      SzHttpMethod  httpMethod,
      int           httpStatusCode,
      UriInfo       uriInfo,
      Timers        timers,
      SzEntityType  entityType)
  {
    return SzEntityTypeResponse.FACTORY.create(
        this.newMeta(httpMethod, httpStatusCode, timers),
        this.newLinks(uriInfo),
        this.newEntityTypeResponseData(entityType));
  }

  /**
   * Creates a new {@link SzEntityTypeResponseData} with the specified parameters.
   *
   * @param entityType The {@link SzEntityType} for the instance.
   *
   * @return The newly created {@link SzEntityTypeResponseData}.
   */
  protected SzEntityTypeResponseData newEntityTypeResponseData(
      SzEntityType  entityType)
  {
    return SzEntityTypeResponseData.FACTORY.create(entityType);
  }

  /**
   * Provides the implementation of <tt>POST /data-sources</tt>.
   *
   * @param dataSourceCodes The {@link List} of data source codes from the
   *                        query parameters.
   * @param withRaw Whether or not the raw native Senzing JSON should be
   *                included in the response.
   * @param uriInfo The {@link UriInfo} for the request.
   *
   * @return The {@link SzDataSourcesResponse} describing the response.
   */
  @POST
  @Path("data-sources")
  public SzDataSourcesResponse addDataSources(
      @QueryParam("dataSource") List<String> dataSourceCodes,
      @DefaultValue("false") @QueryParam("withRaw") boolean withRaw,
      @Context UriInfo uriInfo,
      String dataSourcesInBody)
  {
    Timers timers = this.newTimers();

    SzApiProvider provider = this.getApiProvider();

    this.ensureConfigChangesAllowed(provider, POST, uriInfo, timers);

    Map<String, SzDataSource> map = new LinkedHashMap<>();
    for (String code : dataSourceCodes) {
      SzDataSource dataSource = this.newDataSource(code, null);
      map.put(dataSource.getDataSourceCode(), dataSource);
    }

    // get the body data sources
    if (dataSourcesInBody != null && dataSourcesInBody.trim().length() > 0) {
      // trim the string
      dataSourcesInBody = dataSourcesInBody.trim();

      // create a list for the parsed data sources
      SzDataSourceDescriptors descriptors = null;

      try {
        descriptors = this.parseDataSourceDescriptors(dataSourcesInBody);

      } catch (Exception e) {
        throw this.newBadRequestException(POST, uriInfo, timers, e);
      }

      // loop through the data sources and put them in the map
      for (SzDataSourceDescriptor desc : descriptors.getDescriptors()) {
        SzDataSource dataSource = desc.toDataSource();
        map.put(dataSource.getDataSourceCode(), dataSource);
      }
    }

    return this.doAddDataSources(POST, map.values(), uriInfo, withRaw, timers);
  }

  /**
   * Constructs a new instance of {@link SzDataSource} with specified
   * data source code and optional data source ID.
   *
   * @param dataSourceCode The data source code to construct the instance with.
   * @param dataSourceId The data source ID to construct with or <tt>null</tt>
   *                     not known.
   *
   * @return The newly created instance of {@link SzDataSource}.
   */
  protected SzDataSource newDataSource(String   dataSourceCode,
                                       Integer  dataSourceId)
  {
    return SzDataSource.FACTORY.create(dataSourceCode, dataSourceId);
  }

  /**
   * Constructs a new instance of {@link SzEntityClass} with the specified
   * parameters.
   *
   * @param entityClassCode The entity class code for the entity class.
   * @param entityClassId The optional entity class ID for the entity class.
   * @param resolving The flag indicating if the entity class resolves.
   *
   * @return The new {@link SzEntityClass} instance.
   */
  protected SzEntityClass newEntityClass(String   entityClassCode,
                                         Integer  entityClassId,
                                         Boolean  resolving)
  {
    return SzEntityClass.FACTORY.create(
        entityClassCode, entityClassId, resolving);
  }

  /**
   * Constructs a new instance of {@link SzEntityType} with the specified
   * parameters.
   *
   * @param entityTypeCode The entity type code for the entity type.
   * @param entityTypeId The optional entity class ID for the entity type.
   * @param entityClassCode The entity class code for the entity type.
   *
   * @return The new {@link SzEntityClass} instance.
   */
  protected SzEntityType newEntityType(String   entityTypeCode,
                                       Integer  entityTypeId,
                                       String   entityClassCode)
  {
    return SzEntityType.FACTORY.create(
        entityTypeCode, entityTypeId, entityClassCode);
  }

  /**
   * Parses the specified text as an instance of {@link
   * SzDataSourceDescriptors}.
   *
   * @param text The text to parse.
   *
   * @return The parsed instance of {@link SzDataSourceDescriptors}.
   */
  protected SzDataSourceDescriptors parseDataSourceDescriptors(String text) {
    return SzDataSourceDescriptors.valueOf(text);
  }

  /**
   * Internal method for adding one or more data sources.
   *
   * @param httpMethod The {@link SzHttpMethod} for the operation.
   * @param dataSources The {@link Collection} of {@link SzDataSource} instances
   *                    describing the data sources to be added.
   * @param uriInfo The {@link UriInfo} for the request.
   * @param withRaw Whether or not raw data is being requested.
   * @param timers The {@link Timers} for the operation.
   * @return An SzDataSourcesResponse with all the configured data sources.
   */
  protected SzDataSourcesResponse doAddDataSources(
      SzHttpMethod              httpMethod,
      Collection<SzDataSource>  dataSources,
      UriInfo                   uriInfo,
      boolean                   withRaw,
      Timers                    timers)
  {
    SzApiProvider provider = this.getApiProvider();

    try {
      // get the engine API and the config API
      G2Engine    engineApi     = provider.getEngineApi();
      G2Config    configApi     = provider.getConfigApi();
      G2ConfigMgr configMgrApi  = provider.getConfigMgrApi();
      Set<String> createdSet    = new LinkedHashSet<>();

      if (configMgrApi == null) {
        throw this.newForbiddenException(
            httpMethod, uriInfo, timers, "Configuration changes not permitted.");
      }

      // loop until the provider has the data source code we are looking for
      this.enteringQueue(timers);
      String rawData = provider.executeInThread(() -> {
        this.exitingQueue(timers);

        // get an array of the data source codes
        String[] arr = dataSources.stream()
            .map(ds -> ds.getDataSourceCode())
            .toArray(String[]::new);

        // create a set of the data source codes
        Set<String> dataSourceCodes = new LinkedHashSet<>();
        for (String code : arr) {
          dataSourceCodes.add(code);
        }

        while (!provider.getDataSources(arr).containsAll(dataSourceCodes)) {
          // get the current default config
          Result<Long> result = new Result<>();
          String configJSON = this.getDefaultConfig(httpMethod,
                                                    uriInfo,
                                                    configMgrApi,
                                                    timers,
                                                    result);
          Long defaultConfigId = result.getValue();

          Long configHandle = null;
          try {
            // load into a config object by ID
            this.callingNativeAPI(timers, "config", "load");
            configHandle = configApi.load(configJSON);
            this.calledNativeAPI(timers, "config", "load");

            if (configHandle <= 0) {
              throw this.newInternalServerErrorException(
                  httpMethod, uriInfo, timers, configApi);
            }

            // get the current data sources
            Map<String, SzDataSource> dataSourceMap
                = this.getDataSourcesMap(httpMethod,
                                         uriInfo,
                                         configApi,
                                         timers,
                                         configHandle);

            // check for consistency against existing data sources
            for (SzDataSource dataSource : dataSources) {
              String        dataSourceCode  = dataSource.getDataSourceCode();
              Integer       dataSourceId    = dataSource.getDataSourceId();
              SzDataSource  existingDS      = dataSourceMap.get(dataSourceCode);
              if (existingDS != null && dataSourceId != null
                  && !dataSourceId.equals(existingDS.getDataSourceId()))
              {
                throw this.newBadRequestException(
                    httpMethod, uriInfo, timers,
                    "At least one data source already exists, but "
                    + "with a different data source ID.  specified=[ "
                    + dataSource + " ], existing=[ " + existingDS + " ]");
              }
            }

            // loop through the data sources that need to be created
            for (SzDataSource dataSource : dataSources) {
              // skip attempting to create the data source if it already exists
              if (dataSourceMap.containsKey(dataSource.getDataSourceCode())) {
                continue;
              }

              // add the data source to the config without a data source ID
              this.callingNativeAPI(timers, "config", "addDataSourceV2");
              int returnCode = configApi.addDataSourceV2(
                  configHandle, dataSource.toNativeJson(), new StringBuffer());
              this.calledNativeAPI(timers, "config", "addDataSourceV2");

              if (returnCode != 0) {
                throw this.newInternalServerErrorException(
                    httpMethod, uriInfo, timers, configApi);
              }

              createdSet.add(dataSource.getDataSourceCode());
            }

            if (createdSet.size() > 0) {
              this.updateDefaultConfig(
                  httpMethod,
                  uriInfo,
                  configApi,
                  configMgrApi,
                  timers,
                  defaultConfigId,
                  configHandle,
                  "Added data source(s): " + createdSet);
            }

          } finally {
            if (configHandle != null) {
              configApi.close(configHandle);
            }
          }
        }

        // return the raw data sources string
        return this.doGetDataSources(
            httpMethod, uriInfo, timers, engineApi, configApi);

      });

      // return the data sources response
      return this.buildDataSourcesResponse(
          httpMethod, uriInfo, timers, rawData, withRaw);

    } catch (ServerErrorException e) {
      e.printStackTrace();
      throw e;

    } catch (WebApplicationException e) {
      throw e;

    } catch (Exception e) {
      e.printStackTrace();
      throw this.newInternalServerErrorException(
          httpMethod, uriInfo, timers, e);
    }
  }

  /**
   * Provides the implementation of <tt>POST /entity-classes</tt>.
   *
   * @param entityClassCodes The {@link List} of entity class codes from the
   *                         query parameters.
   * @param resolving The flag indicating the resolving state for entity class
   *                  codes specified via query parameters or those missing
   *                  the resolving flag in the body.
   * @param withRaw Whether or not the raw native Senzing JSON should be
   *                included in the response.
   * @param uriInfo The {@link UriInfo} for the request.
   * @param entityClassesInBody The request body text (usually JSON) describing
   *                            the entity classes to be created.
   *
   * @return The {@link SzEntityClassesResponse} describing the response.
   */
  @POST
  @Path("entity-classes")
  public SzEntityClassesResponse addEntityClasses(
      @QueryParam("entityClass") List<String> entityClassCodes,
      @QueryParam("resolving") Boolean resolving,
      @DefaultValue("false") @QueryParam("withRaw") boolean withRaw,
      @Context UriInfo uriInfo,
      String entityClassesInBody)
  {
    Timers timers = this.newTimers();

    //---------------------------------------------------------------------
    // TODO(bcaceres) -- remove this code when entity classes other than
    // ACTOR are supported
    boolean methodNotAllowed = true;
    if (methodNotAllowed) {
      throw newNotAllowedException(
          GET, uriInfo, timers,
          "Adding new entity classes has been disabled in version 2.0 of the "
          + "API Server.  The only configured and supported entity class at "
          + "this time is ACTOR.");
    }
    //---------------------------------------------------------------------

    SzApiProvider provider = this.getApiProvider();
    this.ensureConfigChangesAllowed(provider, POST, uriInfo, timers);

    Map<String, SzEntityClass> map = new LinkedHashMap<>();

    // add the entity class codes from the query to the map
    for (String code : entityClassCodes) {
      SzEntityClass entityClass
          = this.newEntityClass(code, null, resolving);
      map.put(entityClass.getEntityClassCode(), entityClass);
    }

    // get the body entity classes
    if (entityClassesInBody != null && entityClassesInBody.trim().length() > 0) {
      // trim the string
      entityClassesInBody = entityClassesInBody.trim();

      // create a list for the parsed data sources
      List<SzEntityClass> entityClasses = new LinkedList<>();

      try {
        SzEntityClassDescriptors descriptors
            = SzEntityClassDescriptors.valueOf(entityClassesInBody);

        for (SzEntityClassDescriptor desc : descriptors.getDescriptors()) {
          // convert to an SzEntityClass
          SzEntityClass entityClass = desc.toEntityClass();

          // add it to the list
          entityClasses.add(entityClass);
        }

      } catch (Exception e) {
        throw this.newBadRequestException(POST, uriInfo, timers, e);
      }

      // loop through the entity classes and put them in the map
      for (SzEntityClass entityClass : entityClasses) {
        map.put(entityClass.getEntityClassCode(), entityClass);
      }
    }

    // loop through the map and default the resolving flags if necessary
    for (SzEntityClass entityClass: map.values()) {
      // check the resolving flag and default it if necessary
      if (entityClass.isResolving() == null) {
        entityClass.setResolving(resolving == null ? true : resolving);
      }
    }

    // add the entity classes in the map
    return this.doAddEntityClasses(
        POST, map.values(), uriInfo, withRaw, timers);
  }

  /**
   * Internal method for adding one or more entity classes.
   *
   * @return An SzEntityClassesResponse with all the configured entity classes.
   */
  protected SzEntityClassesResponse doAddEntityClasses(
      SzHttpMethod httpMethod,
      Collection<SzEntityClass> entityClasses,
      UriInfo uriInfo,
      boolean withRaw,
      Timers timers) {
    SzApiProvider provider = this.getApiProvider();

    try {
      // get the engine API and the config API
      G2Engine    engineApi     = provider.getEngineApi();
      G2ConfigMgr configMgrApi  = provider.getConfigMgrApi();
      G2Config    configApi     = provider.getConfigApi();
      Set<String> createdSet    = new LinkedHashSet<>();

      if (configMgrApi == null) {
        throw this.newForbiddenException(
            httpMethod, uriInfo, timers, "Configuration changes not permitted.");
      }

      // loop until the provider has the data source code we are looking for
      this.enteringQueue(timers);
      String rawData = provider.executeInThread(() -> {
        this.exitingQueue(timers);

        // get an array of the data source codes
        String[] arr = entityClasses.stream()
            .map(ec -> ec.getEntityClassCode())
            .toArray(String[]::new);

        // create a set of the data source codes
        Set<String> entityClassCodes = new LinkedHashSet<>();
        for (String code : arr) {
          entityClassCodes.add(code);
        }

        while (!provider.getEntityClasses(arr).containsAll(entityClassCodes)) {
          Result<Long> result = new Result<>();
          String configJSON = this.getDefaultConfig(httpMethod,
                                                    uriInfo,
                                                    configMgrApi,
                                                    timers,
                                                    result);
          Long defaultConfigId = result.getValue();

          Long configHandle = null;
          try {
            // load into a config object by ID
            this.callingNativeAPI(timers, "config", "load");
            configHandle = configApi.load(configJSON);
            this.calledNativeAPI(timers, "config", "load");

            if (configHandle <= 0) {
              throw this.newInternalServerErrorException(
                  httpMethod, uriInfo, timers, configApi);
            }

            // get the current data sources
            Map<String, SzEntityClass> entityClassMap
                = this.getEntityClassesMap(httpMethod,
                                           uriInfo,
                                           configApi,
                                           timers,
                                           configHandle);

            // check for consistency against existing data sources
            for (SzEntityClass entityClass : entityClasses) {
              String        classCode     = entityClass.getEntityClassCode();
              Integer       classId       = entityClass.getEntityClassId();
              Boolean       resolving     = entityClass.isResolving();
              SzEntityClass existingEC    = entityClassMap.get(classCode);
              if (existingEC != null && classId != null
                  && !classId.equals(existingEC.getEntityClassId()))
              {
                throw this.newBadRequestException(
                    httpMethod, uriInfo, timers,
                    "At least one entity class already exists, but "
                        + "with a different entity class ID.  specified=[ "
                        + entityClass + " ], existing=[ " + existingEC + " ]");
              }
              if (existingEC != null && resolving != null
                  && !resolving.equals(existingEC.isResolving()))
              {
                throw this.newBadRequestException(
                    httpMethod, uriInfo, timers,
                    "At least one entity class already exists, but "
                        + "with a different resolving flag.  specified=[ "
                        + entityClass + " ], existing=[ " + existingEC + " ]");
              }
            }

            for (SzEntityClass entityClass : entityClasses) {
              // check if the entity class already exists and skip it if it does
              if (entityClassMap.containsKey(entityClass.getEntityClassCode()))
              {
                continue;
              }

              String nativeJson = entityClass.toNativeJson();

              // add the data source to the config without a data source ID
              this.callingNativeAPI(timers, "config", "addEntityClassV2");
              int returnCode = configApi.addEntityClassV2(
                  configHandle, nativeJson, new StringBuffer());
              this.calledNativeAPI(timers, "config", "addEntityClassV2");

              if (returnCode != 0) {
                throw this.newInternalServerErrorException(
                    httpMethod, uriInfo, timers, configApi);
              }

              createdSet.add(entityClass.getEntityClassCode());
            }

            if (createdSet.size() > 0) {
              this.updateDefaultConfig(
                  httpMethod,
                  uriInfo,
                  configApi,
                  configMgrApi,
                  timers,
                  defaultConfigId,
                  configHandle,
                  "Added entity class(es): " + createdSet);
            }

          } finally {
            if (configHandle != null) {
              configApi.close(configHandle);
            }
          }
        }

        // return the raw data sources string
        return this.doGetEntityClasses(
            httpMethod, uriInfo, timers, engineApi, configApi);

      });

      // return the data sources response
      return this.buildEntityClassesResponse(
          httpMethod, uriInfo, timers, rawData, withRaw);

    } catch (ServerErrorException e) {
      e.printStackTrace();
      throw e;

    } catch (WebApplicationException e) {
      throw e;

    } catch (Exception e) {
      e.printStackTrace();
      throw this.newInternalServerErrorException(
          httpMethod, uriInfo, timers, e);
    }
  }

  /**
   * Provides the implementation of
   * <tt>POST /entity-classes/{entityClassCode}/entity-types</tt>.
   *
   * @param entityClassCode The entity class code from the URI path.
   * @param entityTypeCodes The {@link List} of entity type codes from the
   *                        query parameters.
   * @param withRaw Whether or not the raw native Senzing JSON should be
   *                included in the response.
   * @param uriInfo The {@link UriInfo} for the request.
   * @param entityTypesInBody The request body text (usually JSON) describing
   *                          the entity types to be created.
   *
   * @return The {@link SzEntityTypesResponse} describing the response.
   */
  @POST
  @Path("entity-classes/{entityClassCode}/entity-types")
  public SzEntityTypesResponse addEntityTypesForClass(
      @PathParam("entityClassCode") String entityClassCode,
      @QueryParam("entityType") List<String> entityTypeCodes,
      @DefaultValue("false") @QueryParam("withRaw") boolean withRaw,
      @Context UriInfo uriInfo,
      String entityTypesInBody)
      throws BadRequestException, NotFoundException
  {
    Timers timers = this.newTimers();

    // normalize the entity class
    entityClassCode = entityClassCode.trim().toUpperCase();

    this.checkEntityClassNotFound(POST, uriInfo, timers, entityClassCode);

    return this.createEntityTypes(POST,
                                  entityClassCode,
                                  true,
                                  entityTypeCodes,
                                  withRaw,
                                  uriInfo,
                                  entityTypesInBody,
                                  timers);
  }

  /**
   * Provides the implementation of <tt>POST /entity-types</tt>.
   *
   * @param entityClassCode The optional entity class code from the query
   *                        parameters.
   * @param entityTypeCodes The {@link List} of entity type codes from the
   *                        query parameters.
   * @param withRaw Whether or not the raw native Senzing JSON should be
   *                included in the response.
   * @param uriInfo The {@link UriInfo} for the request.
   * @param entityTypesInBody The request body text (usually JSON) describing
   *                          the entity types to be created.
   *
   * @return The {@link SzEntityTypesResponse} describing the response.
   */
  @POST
  @Path("entity-types")
  public SzEntityTypesResponse addEntityTypes(
      @DefaultValue("ACTOR") @QueryParam("entityClass") String entityClassCode,
      @QueryParam("entityType") List<String> entityTypeCodes,
      @DefaultValue("false") @QueryParam("withRaw") boolean withRaw,
      @Context UriInfo uriInfo,
      String entityTypesInBody)
  {
    Timers timers = this.newTimers();

    // normalize the entity class
    if (entityClassCode != null) {
      entityClassCode = entityClassCode.trim().toUpperCase();
    }

    this.checkEntityClassInvalid(POST, uriInfo, timers, entityClassCode);

    return this.createEntityTypes(POST,
                                  entityClassCode,
                                  false,
                                  entityTypeCodes,
                                  withRaw,
                                  uriInfo,
                                  entityTypesInBody,
                                  timers);
  }

  /**
   * Common method for handling the creation of entity types.  This
   * method aggregates the various query parameters and the body content to
   * create a list of {@link SzEntityType} instances and then call {@link
   * #doAddEntityTypes(SzHttpMethod, Collection, UriInfo, boolean, Timers)}.
   *
   * @param httpMethod The {@link SzHttpMethod} for the request.
   * @param entityClassCode The entity class code for the created entity types
   *                        (this may be the one specified in the URL or may be
   *                        a query parameter that is the default for those that
   *                        don't have an entity class in the body definitions).
   * @param enforceSameClass Whether or not to enforce the same entity class
   *                         for all created entity types (e.g.: all must match
   *                         the one in the specified URL).
   * @param entityTypeCodes The entity type codes for the entity types to create
   * @param withRaw Whether or not the raw JSON is being requested in the
   *                response.
   * @param uriInfo The {@link UriInfo} for the request.
   * @param entityTypesInBody The JSON text describing any entity types in the
   *                          body of the request.
   * @param timers The {@link Timers} for the operation.
   * @return The {@link SzEntityTypesResponse} that was created.
   */
  protected SzEntityTypesResponse createEntityTypes(
      SzHttpMethod  httpMethod,
      String        entityClassCode,
      boolean       enforceSameClass,
      List<String>  entityTypeCodes,
      boolean       withRaw,
      UriInfo       uriInfo,
      String        entityTypesInBody,
      Timers        timers)
  {
    SzApiProvider provider = this.getApiProvider();
    this.ensureConfigChangesAllowed(provider, httpMethod, uriInfo, timers);

    Map<String, SzEntityType> map = new LinkedHashMap<>();

    // add the entity class codes from the query to the map
    for (String code : entityTypeCodes) {
      SzEntityType entityType
          = this.newEntityType(code,null, entityClassCode);
      map.put(entityType.getEntityTypeCode(), entityType);
    }

    // get the body entity classes
    if (entityTypesInBody != null && entityTypesInBody.trim().length() > 0) {
      // trim the string
      entityTypesInBody = entityTypesInBody.trim();

      // create a list for the parsed data sources
      List<SzEntityType> entityTypes = new LinkedList<>();

      try {
        SzEntityTypeDescriptors descriptors
            = SzEntityTypeDescriptors.valueOf(entityTypesInBody);

        for (SzEntityTypeDescriptor desc : descriptors.getDescriptors()) {
          SzEntityType entityType = desc.toEntityType();
          entityTypes.add(entityType);
        }

        // check for consistency on the entity class codes
        if (enforceSameClass) {
          for (SzEntityType entityType : entityTypes) {
            if (entityType.getEntityClassCode() != null
                && entityClassCode != null
                && !entityClassCode.equals(entityType.getEntityClassCode()))
            {
              throw this.newBadRequestException(
                  httpMethod, uriInfo, timers,
                  "Entity class code in URL (" + entityClassCode + ") does not "
                      + "match entity class code in at least one of the "
                      + "specified entity types: " + entityType);
            }
          }
        }

        // default entity class if missing from the entity types
        for (SzEntityType entityType : entityTypes) {
          if (entityType.getEntityClassCode() == null) {
            entityType.setEntityClassCode(entityClassCode);
          }
        }
      } catch (WebApplicationException e) {
        throw e;
      } catch (Exception e) {
        throw this.newBadRequestException(httpMethod, uriInfo, timers, e);
      }

      // loop through the entity types and put them in the map
      for (SzEntityType entityType : entityTypes) {
        map.put(entityType.getEntityTypeCode(), entityType);
      }
    }

    // add the entity types
    return this.doAddEntityTypes(
        httpMethod, map.values(), uriInfo, withRaw, timers);
  }

  /**
   * Internal method for adding one or more entity types.
   *
   * @param httpMethod The {@link SzHttpMethod} for the request.
   * @param entityTypes The {@link Collection} of {@link SzEntityType} instances
   *                    for the entity types to be created.
   * @param uriInfo The {@link UriInfo} for the request.
   * @param withRaw Whether or not the raw JSON is being requested in the
   *                response.
   * @param timers The {@link Timers} for the operation.
   * @return The {@link SzEntityTypesResponse} that was created.
   */
  protected SzEntityTypesResponse doAddEntityTypes(
      SzHttpMethod              httpMethod,
      Collection<SzEntityType>  entityTypes,
      UriInfo                   uriInfo,
      boolean                   withRaw,
      Timers                    timers)
  {
    SzApiProvider provider = this.getApiProvider();

    try {
      // get the engine API and the config API
      G2ConfigMgr configMgrApi  = provider.getConfigMgrApi();
      G2Config    configApi     = provider.getConfigApi();
      G2Engine    engineApi     = provider.getEngineApi();
      Set<String> createdSet    = new LinkedHashSet<>();

      if (configMgrApi == null) {
        throw this.newForbiddenException(
            httpMethod, uriInfo, timers,
            "Configuration changes not permitted.");
      }

      // loop until the provider has the data source code we are looking for
      this.enteringQueue(timers);
      String rawData = provider.executeInThread(() -> {
        this.exitingQueue(timers);

        // get an array of the data source codes
        String[] arr = entityTypes.stream()
            .map(et -> et.getEntityTypeCode())
            .toArray(String[]::new);

        // create a set of the data source codes
        Set<String> entityTypeCodes = new LinkedHashSet<>();
        for (String code : arr) {
          entityTypeCodes.add(code);
        }

        while (!provider.getEntityTypes(arr).containsAll(entityTypeCodes)) {
          Result<Long> result = new Result<>();
          String configJSON = this.getDefaultConfig(httpMethod,
                                                    uriInfo,
                                                    configMgrApi,
                                                    timers,
                                                    result);
          Long defaultConfigId = result.getValue();

          Long configHandle = null;
          try {
            // load into a config object by ID
            this.callingNativeAPI(timers, "config", "load");
            configHandle = configApi.load(configJSON);
            this.calledNativeAPI(timers, "config", "load");

            if (configHandle <= 0) {
              throw this.newInternalServerErrorException(
                  httpMethod, uriInfo, timers, configApi);
            }

            // get the current data sources
            Map<String, SzEntityType> entityTypeMap
                = this.getEntityTypesMap(httpMethod,
                                         uriInfo,
                                         configApi,
                                         timers,
                                         configHandle);

            // check for consistency against existing entity types
            for (SzEntityType entityType : entityTypes) {
              String        typeCode      = entityType.getEntityTypeCode();
              Integer       typeId        = entityType.getEntityTypeId();
              String        classCode     = entityType.getEntityClassCode();
              SzEntityType  existingET    = entityTypeMap.get(typeCode);

              // check if it already exists with a different entity type ID
              if (existingET != null && typeId != null
                  && !typeId.equals(existingET.getEntityTypeId()))
              {
                throw this.newBadRequestException(
                    httpMethod, uriInfo, timers,
                    "At least one entity type already exists, but "
                        + "with a different entity type ID.  specified=[ "
                        + entityType + " ], existing=[ " + existingET + " ]");
              }

              // check if it already exists with a different entity class
              if (existingET != null && classCode != null
                  && !classCode.equals(existingET.getEntityClassCode()))
              {
                throw this.newBadRequestException(
                    httpMethod, uriInfo, timers,
                    "At least one entity type already exists, but "
                        + "with a different entity class code.  specified=[ "
                        + entityType + " ], existing=[ " + existingET + " ]");
              }

              // check if the entity class code is not recognized
              if (!provider.getEntityClasses(classCode).contains(classCode)) {
                throw this.newBadRequestException(
                    httpMethod, uriInfo, timers,
                    "Entity type (" + typeCode + ") specified with "
                    + "unrecognized entity class: " + classCode);
              }
            }

            for (SzEntityType entityType : entityTypes) {
              // check if already exists
              if (entityTypeMap.containsKey(entityType.getEntityTypeCode())) {
                continue;
              }

              // add the data source to the config without a data source ID
              this.callingNativeAPI(timers, "config", "addEntityTypeV2");
              int returnCode = configApi.addEntityTypeV2(
                  configHandle, entityType.toNativeJson(), new StringBuffer());
              this.calledNativeAPI(timers, "config", "addEntityTypeV2");

              if (returnCode != 0) {
                throw this.newInternalServerErrorException(
                    httpMethod, uriInfo, timers, configApi);
              }

              createdSet.add(entityType.getEntityTypeCode());
            }

            if (createdSet.size() > 0) {
              this.updateDefaultConfig(
                  httpMethod,
                  uriInfo,
                  configApi,
                  configMgrApi,
                  timers,
                  defaultConfigId,
                  configHandle,
                  "Added entity type(s): " + createdSet);
            }

          } finally {
            if (configHandle != null) {
              configApi.close(configHandle);
            }
          }
        }

        // return the raw data sources string
        return this.doGetEntityTypes(
            httpMethod, uriInfo, timers, engineApi, configApi);

      });

      // return the entity types response
      return this.buildEntityTypesResponse(
          httpMethod, uriInfo, timers, null, rawData, withRaw);

    } catch (ServerErrorException e) {
      e.printStackTrace();
      throw e;

    } catch (WebApplicationException e) {
      throw e;

    } catch (Exception e) {
      e.printStackTrace();
      throw newInternalServerErrorException(httpMethod, uriInfo, timers, e);
    }
  }

  /**
   * Obtains the JSON text for the configuration that is currently
   * designated as the default configuration.
   *
   * @param httpMethod The {@link SzHttpMethod} for the request.
   * @param uriInfo The {@link UriInfo} for the request.
   * @param configMgrApi The {@link G2ConfigMgr} instance to use.
   * @param timers The {@link Timers} for the operation.
   * @param configId The {@link Result} object to populate with the config ID.
   * @return The JSON text for the default configuration.
   */
  protected String getDefaultConfig(SzHttpMethod  httpMethod,
                                    UriInfo       uriInfo,
                                    G2ConfigMgr   configMgrApi,
                                    Timers        timers,
                                    Result<Long>  configId)
  {
    synchronized (configMgrApi) {
      this.callingNativeAPI(timers, "configMgr", "getDefaultConfigID");
      int returnCode = configMgrApi.getDefaultConfigID(configId);
      this.calledNativeAPI(timers, "configMgr", "getDefaultConfigID");
      // check the return code
      if (returnCode != 0) {
        throw this.newInternalServerErrorException(
            httpMethod, uriInfo, timers, configMgrApi);
      }

      long defaultConfigId = configId.getValue();
      StringBuffer sb = new StringBuffer();

      // get the config
      this.callingNativeAPI(timers, "configMgr", "getConfig");
      returnCode = configMgrApi.getConfig(defaultConfigId, sb);
      this.calledNativeAPI(timers, "configMgr", "getConfig");
      if (returnCode != 0) {
        throw this.newInternalServerErrorException(
            httpMethod, uriInfo, timers, configMgrApi);
      }

      // get the JSON
      return sb.toString();
    }
  }

  /**
   * Replaces the current default configuration with the newly updated
   * default configuration assuming the default configuration has not changed
   * since the updates were made.
   *
   * @param httpMethod The {@link SzHttpMethod} for the request.
   * @param uriInfo The {@link UriInfo} the request.
   * @param configApi The {@link G2Config} instance to use.
   * @param configMgrApi The {@link G2ConfigMgr} instance to use.
   * @param timers The {@link Timers} for the operation.
   * @param defaultConfigId The default configuration ID that was configured
   *                        prior to the modifications.
   * @param configHandle The configuration handle for the modified
   *                     configuration that will be set as the new default.
   * @param configComment The comments to associate with the modified
   *                      configuration.
   * @return <tt>true</tt> if the current default configuration ID is the same
   *         as the specified configuration ID, and <tt>false</tt> if it has
   *         changed and the update was aborted.
   */
  protected boolean updateDefaultConfig(SzHttpMethod  httpMethod,
                                        UriInfo       uriInfo,
                                        G2Config      configApi,
                                        G2ConfigMgr   configMgrApi,
                                        Timers        timers,
                                        long          defaultConfigId,
                                        long          configHandle,
                                        String        configComment)
  {
    StringBuffer sb     = new StringBuffer();
    Result<Long> result = new Result<>();

    // check the size of the config comment
    if (configComment.length() > MAX_CONFIG_COMMENT_LENGTH) {
      configComment = configComment.substring(0, MAX_CONFIG_COMMENT_LENGTH - 4)
          + "....";
    }
    
    // convert the config to a JSON string
    this.callingNativeAPI(timers, "config", "save");
    int returnCode = configApi.save(configHandle, sb);
    if (returnCode != 0) {
      throw this.newInternalServerErrorException(
          httpMethod, uriInfo, timers, configApi);
    }
    this.calledNativeAPI(timers, "config", "save");

    String configJSON = sb.toString();

    // save the configuration
    this.obtainingLock(timers, "configMgrApi");
    synchronized (configMgrApi) {
      this.obtainedLock(timers, "configMgrApi");
      this.callingNativeAPI(timers, "configMgr", "addConfig");
      returnCode = configMgrApi.addConfig(configJSON, configComment, result);

      if (returnCode != 0) {
        throw this.newInternalServerErrorException(
            httpMethod, uriInfo, timers, configMgrApi);
      }
      this.calledNativeAPI(timers, "configMgr", "addConfig");

      // get the config ID for the newly saved config
      long newConfigId = result.getValue();

      this.callingNativeAPI(timers, "configMgr", "getDefaultConfigID");
      returnCode = configMgrApi.getDefaultConfigID(result);
      if (returnCode != 0) {
        throw this.newInternalServerErrorException(
            httpMethod, uriInfo, timers, configMgrApi);
      }
      this.calledNativeAPI(timers, "configMgr", "getDefaultConfigID");

      // check if the default configuration ID has changed
      if (!result.getValue().equals(defaultConfigId)) {
        System.out.println(
            "Concurrent configuration change detected.  Retrying...");
        return false;
      }

      this.callingNativeAPI(timers, "configMgr", "setDefaultConfigID");
      returnCode = configMgrApi.setDefaultConfigID(newConfigId);
      if (returnCode != 0) {
        throw this.newInternalServerErrorException(
            httpMethod, uriInfo, timers, configMgrApi);
      }
      this.calledNativeAPI(timers, "configMgr", "setDefaultConfigID");
    }
    return true;
  }

  /**
   * Parses the data sources in the configuration with the specified
   * configuration handle as {@link SzDataSource} instances and returns a map
   * of data source code keys to {@link SzDataSource} instances.
   *
   * @param httpMethod The {@link SzHttpMethod} for the request.
   * @param uriInfo The {@link UriInfo} for the request.
   * @param configApi The {@link G2Config} instance to use.
   * @param timers The {@link Timers} for the operation.
   * @param configHandle The configuration handle for the config to get the
   *                     data sources from.
   * @return The {@link Map} of {@link String} data source code keys to
   *         {@link SzDataSource} instances.
   */
  protected Map<String, SzDataSource> getDataSourcesMap(
      SzHttpMethod  httpMethod,
      UriInfo       uriInfo,
      G2Config      configApi,
      Timers        timers,
      long          configHandle)
  {
    StringBuffer sb = new StringBuffer();
    this.callingNativeAPI(timers, "config", "listDataSourcesV2");
    int returnCode = configApi.listDataSourcesV2(configHandle, sb);
    this.calledNativeAPI(timers, "config", "listDataSourcesV2");

    if (returnCode != 0) {
      throw this.newInternalServerErrorException(
          httpMethod, uriInfo, timers, configApi);
    }
    JsonObject jsonObject = JsonUtils.parseJsonObject(sb.toString());
    JsonArray jsonArray = jsonObject.getJsonArray("DATA_SOURCES");

    List<SzDataSource> dataSources = this.parseDataSourceList(jsonArray);

    Map<String, SzDataSource> dataSourceMap = new LinkedHashMap<>();
    dataSources.forEach(dataSource -> {
      dataSourceMap.put(dataSource.getDataSourceCode(), dataSource);
    });

    return dataSourceMap;
  }

  /**
   * Parses the entity classes in the configuration with the specified
   * configuration handle as {@link SzEntityClass} instances and returns a map
   * of entity class code keys to {@link SzEntityClass} instances.
   *
   * @param httpMethod The {@link SzHttpMethod} for the request.
   * @param uriInfo The {@link UriInfo} for the request.
   * @param configApi The {@link G2Config} instance to use.
   * @param timers The {@link Timers} for the operation.
   * @param configHandle The configuration handle for the config to get the
   *                     entity classes from.
   * @return The {@link Map} of {@link String} entity class code keys to
   *         {@link SzEntityClass} instances.
   */
  protected Map<String, SzEntityClass> getEntityClassesMap(
      SzHttpMethod  httpMethod,
      UriInfo       uriInfo,
      G2Config      configApi,
      Timers        timers,
      long          configHandle)
  {
    StringBuffer sb = new StringBuffer();
    this.callingNativeAPI(timers, "config", "listEntityClassesV2");
    int returnCode = configApi.listEntityClassesV2(configHandle, sb);
    this.calledNativeAPI(timers, "config", "listEntityClassesV2");

    if (returnCode != 0) {
      throw this.newInternalServerErrorException(
          httpMethod, uriInfo, timers, configApi);
    }
    JsonObject jsonObject = JsonUtils.parseJsonObject(sb.toString());
    JsonArray jsonArray = jsonObject.getJsonArray("ENTITY_CLASSES");

    List<SzEntityClass> entityClasses = this.parseEntityClassList(jsonArray);

    Map<String, SzEntityClass> entityClassMap = new LinkedHashMap<>();
    entityClasses.forEach(entityClass -> {
      entityClassMap.put(entityClass.getEntityClassCode(), entityClass);
    });

    return entityClassMap;
  }

  /**
   * Parses the entity types in the configuration with the specified
   * configuration handle as {@link SzEntityType} instances and returns a map
   * of entity type code keys to {@link SzEntityType} instances.
   *
   * @param httpMethod The {@link SzHttpMethod} for the request.
   * @param uriInfo The {@link UriInfo} for the request.
   * @param configApi The {@link G2Config} instance to use.
   * @param timers The {@link Timers} for the operation.
   * @param configHandle The configuration handle for the config to get the
   *                     entity types from.
   * @return The {@link Map} of {@link String} entity type code keys to
   *         {@link SzEntityType} instances.
   */
  protected Map<String, SzEntityType> getEntityTypesMap(
      SzHttpMethod  httpMethod,
      UriInfo       uriInfo,
      G2Config      configApi,
      Timers        timers,
      long          configHandle)
  {
    StringBuffer sb = new StringBuffer();
    this.callingNativeAPI(timers, "config", "listEntityTypesV2");
    int returnCode = configApi.listEntityTypesV2(configHandle, sb);
    this.calledNativeAPI(timers, "config", "listEntityTypesV2");

    if (returnCode != 0) {
      throw this.newInternalServerErrorException(
          httpMethod, uriInfo, timers, configApi);
    }
    JsonObject jsonObject = JsonUtils.parseJsonObject(sb.toString());
    JsonArray jsonArray = jsonObject.getJsonArray("ENTITY_TYPES");

    List<SzEntityType> entityTypes = this.parseEntityTypeList(jsonArray);

    Map<String, SzEntityType> entityTypeMap = new LinkedHashMap<>();
    entityTypes.forEach(entityType -> {
      entityTypeMap.put(entityType.getEntityTypeCode(), entityType);
    });

    return entityTypeMap;
  }

  /**
   * Provides the implementation of <tt>GET /attribute-types</tt>.
   *
   * @param withInternal Boolean flag from the query parameter indicating if
   *                     internal attribute types should be included in the
   *                     response.
   * @param attributeClass The optional attribute class for filtering the
   *                       attribute types to be include in the response.
   * @param featureType The optional feature type for filtering the attribute
   *                    types to be include in the response.
   * @param withRaw Whether or not the raw native Senzing JSON should be
   *                included in the response.
   * @param uriInfo The {@link UriInfo} for the request.
   *
   * @return The {@link SzAttributeTypesResponse} describing the response.
   */
  @GET
  @Path("attribute-types")
  public SzAttributeTypesResponse getAttributeTypes(
      @DefaultValue("false") @QueryParam("withInternal") boolean withInternal,
      @QueryParam("attributeClass")                      String  attributeClass,
      @QueryParam("featureType")                         String  featureType,
      @DefaultValue("false") @QueryParam("withRaw")      boolean withRaw,
      @Context                                           UriInfo uriInfo)
  {
    Timers timers = this.newTimers();
    SzApiProvider provider = this.getApiProvider();

    SzAttributeClass ac = null;
    if (attributeClass != null && attributeClass.trim().length() > 0) {
      try {
        ac = SzAttributeClass.valueOf(attributeClass.trim().toUpperCase());

      } catch (IllegalArgumentException e) {
        throw this.newBadRequestException(
            GET, uriInfo, timers, "Unrecognized attribute class: " + attributeClass);
      }
    }
    final SzAttributeClass attrClass = ac;
    final String featType
        = ((featureType != null && featureType.trim().length() > 0)
        ? featureType.trim() : null);

    try {
      this.enteringQueue(timers);
      JsonObject configRoot = provider.executeInThread(() -> {
        this.exitingQueue(timers);

        // get the engine API and the config API
        G2Engine engineApi = provider.getEngineApi();

        return this.getCurrentConfigRoot(GET, uriInfo, timers, engineApi);
      });
      
      this.processingRawData(timers);
      // get the array and construct the response
      JsonArray jsonArray = configRoot.getJsonArray("CFG_ATTR");

      List<SzAttributeType> attrTypes = this.parseAttributeTypeList(jsonArray);

      // check if filtering out internal attribute types
      if (!withInternal) {
        attrTypes.removeIf(SzAttributeType::isInternal);
      }

      // filter by attribute class if filter is specified
      if (attrClass != null) {
        attrTypes.removeIf(
            attrType -> (!attrType.getAttributeClass().equals(attrClass)));
      }

      // filter by feature type if filter is specified
      if (featType != null) {
        attrTypes.removeIf(
            at -> (!featType.equalsIgnoreCase(at.getFeatureType())));
      }
      processedRawData(timers);

      // build the response
      SzAttributeTypesResponse response = this.newAttributeTypesResponse(
          GET, 200, uriInfo, timers, attrTypes);

      // if including raw data then add it
      if (withRaw) {
        this.processingRawData(timers);
        // check if we need to filter the raw value as well
        if (!withInternal || attrClass != null || featType != null) {
          JsonArrayBuilder jab = Json.createArrayBuilder();
          for (JsonObject jsonObj : jsonArray.getValuesAs(JsonObject.class)) {
            if (!withInternal) {
              if (SzAttributeType.interpretBoolean(jsonObj, "INTERNAL")) {
                continue;
              }
            }
            if (attrClass != null) {
              String rawAC = jsonObj.getString("ATTR_CLASS");
              if (!attrClass.getRawValue().equalsIgnoreCase(rawAC)) continue;
            }
            if (featType != null) {
              String ft = jsonObj.getString("FTYPE_CODE");
              if (!featType.equalsIgnoreCase(ft)) continue;
            }
            jab.add(jsonObj);
          }
          jsonArray = jab.build();
        }
        JsonObjectBuilder job = Json.createObjectBuilder();
        job.add("CFG_ATTR", jsonArray);
        String rawData = JsonUtils.toJsonText(job.build());
        this.processedRawData(timers);
        response.setRawData(rawData);
      }

      // return the response
      return response;

    } catch (ServerErrorException e) {
      e.printStackTrace();
      throw e;

    } catch (WebApplicationException e) {
      throw e;

    } catch (Exception e) {
      e.printStackTrace();
      throw newInternalServerErrorException(GET, uriInfo, timers, e);
    }
  }

  /**
   * Parses the specified {@link JsonArray} describing the attribute types in
   * the raw format and produces a {@link List} of {@link SzAttributeType}
   * instances.
   *
   * @param jsonArray The {@link JsonArray} describing the data sources.
   *
   * @return The created {@link List} of {@link SzAttributeType} instances.
   */
  protected List<SzAttributeType> parseAttributeTypeList(JsonArray jsonArray) {
    return SzAttributeType.parseAttributeTypeList(null, jsonArray);
  }

  /**
   * Creates a new instance of {@link SzAttributeTypesResponse} and configures
   * it with the specified {@link List} of {@link SzAttributeType} instances.
   *
   * @param httpMethod The {@link SzHttpMethod} for the request.
   * @param httpStatusCode The HTTP status code for the response.
   * @param uriInfo The {@link UriInfo} for the request.
   * @param timers The {@link Timers} for the operation.
   * @param attributeTypes The {@link List} of {@link SzAttributeType}
   *                       instances.
   *
   * @return The newly created {@link SzAttributeTypesResponse}.
   */
  protected SzAttributeTypesResponse newAttributeTypesResponse(
      SzHttpMethod          httpMethod,
      int                   httpStatusCode,
      UriInfo               uriInfo,
      Timers                timers,
      List<SzAttributeType> attributeTypes)
  {
    return SzAttributeTypesResponse.FACTORY.create(
        this.newMeta(httpMethod, httpStatusCode, timers),
        this.newLinks(uriInfo),
        this.newAttributeTypesResponseData(attributeTypes));
  }

  /**
   * Creates a new instance of {@link SzAttributeTypesResponseData} with the
   * specified {@link List} of {@link SzAttributeType} instances.
   *
   * @param attributeTypes The {@link Collection} of {@link SzAttributeType}
   *                       instances.
   *
   * @return The newly created {@link SzAttributeTypesResponse}.
   */
  protected SzAttributeTypesResponseData newAttributeTypesResponseData(
      Collection<? extends SzAttributeType> attributeTypes)
  {
    return SzAttributeTypesResponseData.FACTORY.create(attributeTypes);
  }

  /**
   * Provides the implementation of
   * <tt>GET /attribute-types/{attributeTypeCode}</tt>.
   *
   * @param attributeCode The attribute code from the URI path identifying the
   *                      attribute type.
   * @param withRaw Whether or not the raw native Senzing JSON should be
   *                included in the response.
   * @param uriInfo The {@link UriInfo} for the request.
   *
   * @return The {@link SzAttributeTypeResponse} describing the response.
   */
  @GET
  @Path("attribute-types/{attributeCode}")
  public SzAttributeTypeResponse getAttributeType(
      @PathParam("attributeCode")                   String  attributeCode,
      @DefaultValue("false") @QueryParam("withRaw") boolean withRaw,
      @Context                                      UriInfo uriInfo)
  {
    Timers timers = this.newTimers();
    SzApiProvider provider = this.getApiProvider();

    try {
      this.enteringQueue(timers);
      JsonObject configRoot = provider.executeInThread(() -> {
        this.exitingQueue(timers);
        // get the engine API and the config API
        G2Engine engineApi = provider.getEngineApi();

        JsonObject obj = this.getCurrentConfigRoot(
            GET, uriInfo, timers, engineApi);

        return obj;
      });

      this.processingRawData(timers);

      // get the array and construct the response
      JsonArray jsonArray = configRoot.getJsonArray("CFG_ATTR");

      JsonObject jsonAttrType = null;

      for (JsonObject jsonObject : jsonArray.getValuesAs(JsonObject.class)) {
        String attrCode = jsonObject.getString("ATTR_CODE");
        if (attrCode == null) continue;
        attrCode = attrCode.trim().toUpperCase();
        if (attrCode.equals(attributeCode)) {
          jsonAttrType = jsonObject;
          break;
        }
      }

      if (jsonAttrType == null) {
        throw this.newNotFoundException(
            GET, uriInfo, timers,
            "Attribute code not recognized: " + attributeCode);
      }

      SzAttributeType attrType = this.parseAttributeType(jsonAttrType);

      SzAttributeTypeResponse response = this.newAttributeTypeResponse(
          GET, 200, uriInfo, timers, attrType);

      // if including raw data then add it
      if (withRaw) {
        String rawData = JsonUtils.toJsonText(jsonAttrType);

        response.setRawData(rawData);
      }
      this.processedRawData(timers);

      // return the response
      return response;

    } catch (ServerErrorException e) {
      e.printStackTrace();
      throw e;

    } catch (WebApplicationException e) {
      throw e;

    } catch (Exception e) {
      e.printStackTrace();
      throw this.newInternalServerErrorException(GET, uriInfo, timers, e);
    }
  }

  /**
   * Parses the JSON described by the specified {@link JsonObject} as an
   * attribute type in the raw format and produces an instance of {@link
   * SzAttributeType}.
   *
   * @param jsonObject The {@link JsonObject} describing the attribute type in
   *                   the raw format.
   *
   * @return The newly created and configured {@link SzAttributeType}.
   */
  protected SzAttributeType parseAttributeType(JsonObject jsonObject) {
    return SzAttributeType.parseAttributeType(null, jsonObject);
  }

  /**
   * Creates a new {@link SzAttributeTypeResponse} with the specified
   * parameters.
   *
   * @param httpMethod The {@link SzHttpMethod} for the request.
   * @param httpStatusCode The HTTP status code for the response.
   * @param uriInfo The {@link UriInfo} for the request.
   * @param timers The {@link Timers} for the operation.
   * @param attributeType The {@link SzAttributeType} instance.
   *
   * @return The newly created {@link SzAttributeTypeResponse}.
   */
  protected SzAttributeTypeResponse newAttributeTypeResponse(
      SzHttpMethod    httpMethod,
      int             httpStatusCode,
      UriInfo         uriInfo,
      Timers          timers,
      SzAttributeType attributeType)
  {
    return SzAttributeTypeResponse.FACTORY.create(
        this.newMeta(httpMethod, httpStatusCode, timers),
        this.newLinks(uriInfo),
        this.newAttributeTypeResponseData(attributeType));
  }

  /**
   * Creates a new {@link SzAttributeTypeResponseData} with the specified
   * parameters.
   *
   * @param attributeType The {@link SzAttributeType} instance.
   *
   * @return The newly created {@link SzAttributeTypeResponseData}.
   */
  protected SzAttributeTypeResponseData newAttributeTypeResponseData(
      SzAttributeType attributeType)
  {
    return SzAttributeTypeResponseData.FACTORY.create(attributeType);
  }

  /**
   * Provides the implementation of <tt>GET /configs/active</tt>.
   *
   * @param uriInfo The {@link UriInfo} for the request.
   *
   * @return The {@link SzConfigResponse} describing the response.
   */
  @GET
  @Path("configs/active")
  public SzConfigResponse getActiveConfig(@Context UriInfo uriInfo)
  {
    Timers timers = this.newTimers();
    SzApiProvider provider = this.getApiProvider();

    try {
      this.enteringQueue(timers);
      JsonObject configObject = provider.executeInThread(() -> {
        this.exitingQueue(timers);
        // get the engine API and the config API
        G2Engine engineApi = provider.getEngineApi();

        // export the config
        String config = this.exportConfig(GET, uriInfo, timers, engineApi);

        // parse the raw data
        this.processingRawData(timers);
        JsonObject configObj = JsonUtils.parseJsonObject(config);
        this.processedRawData(timers);

        return configObj;
      });

      this.processingRawData(timers);
      String rawData = JsonUtils.toJsonText(configObject);
      SzConfigResponse response = this.newConfigResponse(
          GET, 200, uriInfo, timers, rawData);
      this.processedRawData(timers);

      // return the response
      return response;

    } catch (ServerErrorException e) {
      e.printStackTrace();
      throw e;

    } catch (WebApplicationException e) {
      throw e;

    } catch (Exception e) {
      e.printStackTrace();
      throw newInternalServerErrorException(GET, uriInfo, timers, e);
    }
  }

  /**
   * Creates a new {@link SzConfigResponse} with the specified parameters.
   *
   * @param httpMethod The {@link SzHttpMethod} for the request.
   * @param httpStatusCode The HTTP status code for the response.
   * @param uriInfo The {@link UriInfo} for the request.
   * @param timers The {@link Timers} for the operation.
   * @param configRawData The JSON text for the config.
   *
   * @return The newly created {@link SzDataSourceResponse}.
   */
  protected SzConfigResponse newConfigResponse(SzHttpMethod httpMethod,
                                               int          httpStatusCode,
                                               UriInfo      uriInfo,
                                               Timers       timers,
                                               String       configRawData)
  {
    return SzConfigResponse.FACTORY.create(
        this.newMeta(httpMethod, httpStatusCode, timers),
        this.newLinks(uriInfo),
        configRawData);
  }

  /**
   * Provides the implementation of <tt>GET /configs/template</tt>.
   *
   * @param uriInfo The {@link UriInfo} for the request.
   *
   * @return The {@link SzConfigResponse} describing the response.
   */
  @GET
  @Path("configs/template")
  public SzConfigResponse getTemplateConfig(@Context UriInfo uriInfo)
  {
    Timers timers = this.newTimers();
    SzApiProvider provider = this.getApiProvider();

    try {
      this.enteringQueue(timers);
      JsonObject configObject = provider.executeInThread(() -> {
        this.exitingQueue(timers);
        // get the engine API and the config API
        G2Config configApi = provider.getConfigApi();

        // create the default config
        StringBuffer sb = new StringBuffer();
        this.callingNativeAPI(timers, "config", "create");
        long configId = configApi.create();
        if (configId <= 0L) {
          throw this.newInternalServerErrorException(
              GET, uriInfo, timers, configApi);
        }
        this.calledNativeAPI(timers, "config", "create");
        try {
          this.callingNativeAPI(timers, "config", "save");
          int returnCode = configApi.save(configId, sb);
          if (returnCode != 0) {
            throw this.newInternalServerErrorException(
                GET, uriInfo, timers, configApi);
          }
          this.calledNativeAPI(timers, "config", "save");
        } finally {
          this.callingNativeAPI(timers, "config", "close");
          configApi.close(configId);
          this.calledNativeAPI(timers, "config", "close");
        }

        String config = sb.toString();

        // parse the raw data
        this.processingRawData(timers);
        JsonObject configObj = JsonUtils.parseJsonObject(config);
        this.processedRawData(timers);

        return configObj;
      });

      this.processingRawData(timers);
      String rawData = JsonUtils.toJsonText(configObject);
      SzConfigResponse response = this.newConfigResponse(
          GET, 200, uriInfo, timers, rawData);
      this.processedRawData(timers);

      // return the response
      return response;

    } catch (ServerErrorException e) {
      e.printStackTrace();
      throw e;

    } catch (WebApplicationException e) {
      throw e;

    } catch (Exception e) {
      e.printStackTrace();
      throw this.newInternalServerErrorException(GET, uriInfo, timers, e);
    }
  }


  /**
   * Exports the config using the specified {@link G2Engine} instance.
   *
   * @param httpMethod The {@link SzHttpMethod} for the request.
   * @param uriInfo The {@link UriInfo} for the request.
   * @param timers The {@link Timers} for the operation.
   * @param engineApi The {@link G2Engine} instance to use.
   */
  protected String exportConfig(SzHttpMethod  httpMethod,
                                UriInfo       uriInfo,
                                Timers        timers,
                                G2Engine      engineApi)
  {
    StringBuffer sb = new StringBuffer();
    this.callingNativeAPI(timers, "engine", "exportConfig");
    int result = engineApi.exportConfig(sb);
    this.calledNativeAPI(timers, "engine", "exportConfig");
    if (result != 0) {
      throw this.newInternalServerErrorException(
          httpMethod, uriInfo, timers, engineApi);
    }
    return sb.toString();
  }

  /**
   * From an exported config, this pulls the <tt>"G2_CONFIG"</tt>
   * {@link JsonObject} from it.
   *
   * @param httpMethod The {@link SzHttpMethod} for the request.
   * @param uriInfo The {@link UriInfo} for the request.
   * @param timers The {@link Timers} for the operation.
   * @param engineApi The {@link G2Engine} instance to use.
   */
  protected JsonObject getCurrentConfigRoot(SzHttpMethod  httpMethod,
                                            UriInfo       uriInfo,
                                            Timers        timers,
                                            G2Engine      engineApi)
  {
    // export the config
    String config = this.exportConfig(httpMethod, uriInfo, timers, engineApi);

    // parse the raw data
    this.processingRawData(timers);
    JsonObject configObj = JsonUtils.parseJsonObject(config);
    this.processedRawData(timers);
    return configObj.getJsonObject("G2_CONFIG");
  }

  /**
   * Checks if the specified entity class code is found or not.  If invalid
   * then this method throws a {@link NotFoundException}.  If the specified
   * entity class code is <tt>null</tt> then this method throws a
   * {@link NullPointerException}.
   *
   * @param httpMethod The HTTP method associated with the request.
   * @param uriInfo The {@link UriInfo} associated with the request.
   * @param timers The {@link Timers} being used by the request.
   * @param entityClassCode The non-null entity class code to check.
   *
   * @throws NotFoundException If the entity class code is not recognized.
   * @throws NullPointerException If the specified entity class code is null.
   */
  protected void checkEntityClassNotFound(SzHttpMethod  httpMethod,
                                          UriInfo       uriInfo,
                                          Timers        timers,
                                          String        entityClassCode)
  {
    // normalize the entity class
    entityClassCode = entityClassCode.trim().toUpperCase();

    //---------------------------------------------------------------------
    // check the entity class code to ensure it is ACTOR
    // TODO(bcaceres) -- remove this code when entity classes other than
    // ACTOR are supported ** OR ** when the API server no longer supports
    // product versions that ship with alternate entity classes
    if (!entityClassCode.equals("ACTOR")) {
      throw newNotFoundException(
          httpMethod, uriInfo, timers,
          "The entity class code was not recognized: " + entityClassCode);
    }
    //---------------------------------------------------------------------

    // check that the entity class exists
    SzApiProvider provider = this.getApiProvider();
    Set<String> entityClasses = provider.getEntityClasses(entityClassCode);
    if (!entityClasses.contains(entityClassCode)) {
      throw this.newNotFoundException(
          httpMethod, uriInfo, timers,
          "The entity class code was not recognized: " + entityClassCode);
    }
  }

  /**
   * Checks if the specified entity class code is valid or not.  If invalid
   * then this method throws a {@link BadRequestException}.  If the specified
   * entity class code is <tt>null</tt> then this method does nothing.
   *
   * @param httpMethod The HTTP method associated with the request.
   * @param uriInfo The {@link UriInfo} associated with the request.
   * @param timers The {@link Timers} being used by the request.
   * @param entityClassCode The optional entity class code to check.
   *
   * @throws BadRequestException If the entity class code is not recognized.
   */
  protected void checkEntityClassInvalid(SzHttpMethod httpMethod,
                                         UriInfo      uriInfo,
                                         Timers       timers,
                                         String       entityClassCode)
    throws BadRequestException
  {
    // if null then it is simply missing, not invalid
    if (entityClassCode == null) return;

    // normalize the entity class
    entityClassCode = entityClassCode.trim().toUpperCase();

    //---------------------------------------------------------------------
    // check the entity class code to ensure it is ACTOR
    // TODO(bcaceres) -- remove this code when entity classes other than
    // ACTOR are supported ** OR ** when the API server no longer supports
    // product versions that ship with alternate entity classes
    if (!entityClassCode.equals("ACTOR")) {
      throw newBadRequestException(
          httpMethod, uriInfo, timers,
          "The entity class code was not recognized: " + entityClassCode);
    }
    //---------------------------------------------------------------------

    // check that the entity class exists
    SzApiProvider provider = this.getApiProvider();
    Set<String> entityClasses = provider.getEntityClasses(entityClassCode);
    if (!entityClasses.contains(entityClassCode)) {
      throw this.newBadRequestException(
          httpMethod, uriInfo, timers,
          "The entity class code was not recognized: " + entityClassCode);
    }
  }
}
