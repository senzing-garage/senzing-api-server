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
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import java.util.*;

import static com.senzing.api.model.SzHttpMethod.*;
import static com.senzing.api.services.ServicesUtil.*;

/**
 * Provides config related API services.
 */
@Produces("application/json; charset=UTF-8")
@Path("/")
public class ConfigServices {
  @GET
  @Path("data-sources")
  public SzDataSourcesResponse getDataSources(
      @DefaultValue("false") @QueryParam("withRaw") boolean withRaw,
      @Context UriInfo uriInfo) {
    Timers timers = newTimers();
    SzApiProvider provider = SzApiProvider.Factory.getProvider();

    try {
      // get the engine API and the config API
      G2Engine engineApi = provider.getEngineApi();
      G2Config configApi = provider.getConfigApi();

      enteringQueue(timers);
      String rawData = provider.executeInThread(() -> {
        exitingQueue(timers);
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

  @GET
  @Path("data-sources/{dataSourceCode}")
  public SzDataSourceResponse getDataSource(
      @PathParam("dataSourceCode") String dataSourceCode,
      @DefaultValue("false") @QueryParam("withRaw") boolean withRaw,
      @Context UriInfo uriInfo)
  {
    Timers timers = newTimers();
    SzApiProvider provider = SzApiProvider.Factory.getProvider();


    try {
      // get the engine API and the config API
      G2Engine engineApi = provider.getEngineApi();
      G2Config configApi = provider.getConfigApi();

      enteringQueue(timers);
      String rawData = provider.executeInThread(() -> {
        exitingQueue(timers);
        String code = dataSourceCode.trim().toUpperCase();
        if (!provider.getDataSources(code).contains(code)) {
          throw newNotFoundException(
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

  private String doGetDataSources(SzHttpMethod httpMethod,
                                  UriInfo uriInfo,
                                  Timers timers,
                                  G2Engine engineApi,
                                  G2Config configApi) {
    String config = exportConfig(GET, uriInfo, timers, engineApi);

    Long configId = null;
    try {
      // load into a config object by ID
      callingNativeAPI(timers, "config", "load");
      configId = configApi.load(config);
      calledNativeAPI(timers, "config", "load");

      if (configId < 0) {
        throw newInternalServerErrorException(GET, uriInfo, timers, configApi);
      }

      StringBuffer sb = new StringBuffer();

      // list the data sources on the config
      callingNativeAPI(timers, "config", "listDataSourcesV2");
      int returnCode = configApi.listDataSourcesV2(configId, sb);
      if (returnCode != 0) {
        throw newInternalServerErrorException(
            GET, uriInfo, timers, configApi);
      }
      calledNativeAPI(timers, "config", "listDataSourcesV2");

      return sb.toString();

    } finally {
      if (configId != null) {
        configApi.close(configId);
      }
    }
  }

  private SzDataSourcesResponse buildDataSourcesResponse(
      SzHttpMethod httpMethod,
      UriInfo uriInfo,
      Timers timers,
      String rawData,
      boolean withRaw) {
    processingRawData(timers);
    // parse the raw data
    JsonObject jsonObject = JsonUtils.parseJsonObject(rawData);

    // get the array and construct the response
    JsonArray jsonArray = jsonObject.getJsonArray("DATA_SOURCES");
    List<SzDataSource> dataSources
        = SzDataSource.parseDataSourceList(null, jsonArray);

    SzDataSourcesResponse response = new SzDataSourcesResponse(
        httpMethod, 200, uriInfo, timers);

    response.setDataSources(dataSources);
    processedRawData(timers);

    // if including raw data then add it
    if (withRaw) response.setRawData(rawData);

    return response;
  }

  private SzDataSourceResponse buildDataSourceResponse(
      SzHttpMethod httpMethod,
      UriInfo uriInfo,
      Timers timers,
      String dataSourceCode,
      String rawData,
      boolean withRaw)
  {
    processingRawData(timers);
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
      throw newNotFoundException(
          httpMethod, uriInfo, timers,
          "The specified data source was not recognized: " + dataSourceCode);
    }

    // parse the data source
    SzDataSource dataSource
        = SzDataSource.parseDataSource(null, jsonObject);

    // build the response
    SzDataSourceResponse response = new SzDataSourceResponse(
        httpMethod, 200, uriInfo, timers);

    response.setDataSource(dataSource);
    processedRawData(timers);

    // if including raw data then add it
    if (withRaw) response.setRawData(JsonUtils.toJsonText(jsonObject));

    return response;
  }

  @GET
  @Path("entity-classes")
  public SzEntityClassesResponse getEntityClasses(
      @DefaultValue("false") @QueryParam("withRaw") boolean withRaw,
      @Context UriInfo uriInfo) {
    Timers timers = newTimers();
    SzApiProvider provider = SzApiProvider.Factory.getProvider();

    try {
      // get the engine API and the config API
      G2Engine engineApi = provider.getEngineApi();
      G2Config configApi = provider.getConfigApi();

      enteringQueue(timers);
      String rawData = provider.executeInThread(() -> {
        exitingQueue(timers);
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

  @GET
  @Path("entity-classes/{entityClassCode}")
  public SzEntityClassResponse getEntityClass(
      @PathParam("entityClassCode") String entityClassCode,
      @DefaultValue("false") @QueryParam("withRaw") boolean withRaw,
      @Context UriInfo uriInfo)
  {
    Timers timers = newTimers();
    SzApiProvider provider = SzApiProvider.Factory.getProvider();

    try {
      // get the engine API and the config API
      G2Engine engineApi = provider.getEngineApi();
      G2Config configApi = provider.getConfigApi();

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

      enteringQueue(timers);
      String rawData = provider.executeInThread(() -> {
        exitingQueue(timers);
        String code = entityClassCode.trim().toUpperCase();
        if (!provider.getEntityClasses(code).contains(code)) {
          throw newNotFoundException(
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

  private String doGetEntityClasses(SzHttpMethod httpMethod,
                                    UriInfo uriInfo,
                                    Timers timers,
                                    G2Engine engineApi,
                                    G2Config configApi) {
    String config = exportConfig(GET, uriInfo, timers, engineApi);

    Long configId = null;
    try {
      // load into a config object by ID
      callingNativeAPI(timers, "config", "load");
      configId = configApi.load(config);
      calledNativeAPI(timers, "config", "load");

      if (configId < 0) {
        throw newInternalServerErrorException(GET, uriInfo, timers, configApi);
      }

      StringBuffer sb = new StringBuffer();

      // list the data sources on the config
      callingNativeAPI(timers, "config", "listEntityClassesV2");
      int returnCode = configApi.listEntityClassesV2(configId, sb);
      if (returnCode != 0) {
        throw newInternalServerErrorException(
            GET, uriInfo, timers, configApi);
      }
      calledNativeAPI(timers, "config", "listEntityClassesV2");

      return sb.toString();

    } finally {
      if (configId != null) {
        configApi.close(configId);
      }
    }
  }

  private SzEntityClassesResponse buildEntityClassesResponse(
      SzHttpMethod httpMethod,
      UriInfo uriInfo,
      Timers timers,
      String rawData,
      boolean withRaw) {
    processingRawData(timers);
    // parse the raw data
    JsonObject jsonObject = JsonUtils.parseJsonObject(rawData);

    // get the array and construct the response
    JsonArray jsonArray = jsonObject.getJsonArray("ENTITY_CLASSES");
    List<SzEntityClass> entityClasses
        = SzEntityClass.parseEntityClassList(null, jsonArray);

    SzEntityClassesResponse response = new SzEntityClassesResponse(
        httpMethod, 200, uriInfo, timers);

    response.setEntityClasses(entityClasses);
    processedRawData(timers);

    // if including raw data then add it
    if (withRaw) response.setRawData(rawData);

    return response;
  }

  private SzEntityClassResponse buildEntityClassResponse(
      SzHttpMethod httpMethod,
      UriInfo uriInfo,
      Timers timers,
      String entityClassCode,
      String rawData,
      boolean withRaw)
  {
    processingRawData(timers);
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
      throw newNotFoundException(
          httpMethod, uriInfo, timers,
          "The specified entity class was not recognized: " + entityClassCode);
    }

    // parse the data source
    SzEntityClass entityClass
        = SzEntityClass.parseEntityClass(null, jsonObject);

    // build the response
    SzEntityClassResponse response = new SzEntityClassResponse(
        httpMethod, 200, uriInfo, timers);

    response.setEntityClass(entityClass);
    processedRawData(timers);

    // if including raw data then add it
    if (withRaw) response.setRawData(JsonUtils.toJsonText(jsonObject));

    return response;
  }

  @GET
  @Path("entity-classes/{entityClass}/entity-types")
  public SzEntityTypesResponse getEntityTypesByClass(
      @PathParam("entityClass") String entityClass,
      @DefaultValue("false") @QueryParam("withRaw") boolean withRaw,
      @Context UriInfo uriInfo) {
    return this.getEntityTypes(entityClass, withRaw, uriInfo);
  }

  @GET
  @Path("entity-types")
  public SzEntityTypesResponse getEntityTypes(
      @QueryParam("entityClass") String entityClass,
      @DefaultValue("false") @QueryParam("withRaw") boolean withRaw,
      @Context UriInfo uriInfo) {
    Timers timers = newTimers();
    SzApiProvider provider = SzApiProvider.Factory.getProvider();

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
        throw newNotFoundException(
            GET, uriInfo, timers,
            "The entity class code was not recognized: " + entityClass);
      }
      //---------------------------------------------------------------------

      enteringQueue(timers);
      String rawData = provider.executeInThread(() -> {
        exitingQueue(timers);
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
      throw newInternalServerErrorException(GET, uriInfo, timers, e);
    }
  }

  @GET
  @Path("entity-types/{entityTypeCode}")
  public SzEntityTypeResponse getEntityType(
      @PathParam("entityTypeCode") String entityTypeCode,
      @DefaultValue("false") @QueryParam("withRaw") boolean withRaw,
      @Context UriInfo uriInfo)
  {
    Timers timers = newTimers();
    SzApiProvider provider = SzApiProvider.Factory.getProvider();

    try {
      // get the engine API and the config API
      G2Engine engineApi = provider.getEngineApi();
      G2Config configApi = provider.getConfigApi();

      enteringQueue(timers);
      String rawData = provider.executeInThread(() -> {
        exitingQueue(timers);
        String code = entityTypeCode.trim().toUpperCase();
        if (!provider.getEntityTypes(code).contains(code)) {
          throw newNotFoundException(
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
      throw newInternalServerErrorException(GET, uriInfo, timers, e);
    }
  }

  @GET
  @Path("entity-classes/{entityClassCode}/entity-types/{entityTypeCode}")
  public SzEntityTypeResponse getEntityType(
      @PathParam("entityClassCode") String entityClassCode,
      @PathParam("entityTypeCode") String entityTypeCode,
      @DefaultValue("false") @QueryParam("withRaw") boolean withRaw,
      @Context UriInfo uriInfo)
  {
    Timers timers = newTimers();
    SzApiProvider provider = SzApiProvider.Factory.getProvider();

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

      enteringQueue(timers);
      String rawData = provider.executeInThread(() -> {
        exitingQueue(timers);
        String classCode = entityClassCode.trim().toUpperCase();
        if (!provider.getEntityClasses(classCode).contains(classCode)) {
          throw newNotFoundException(
              GET, uriInfo, timers,
              "The specified entity class code was not recognized: "
                  + classCode);
        }
        String typeCode = entityTypeCode.trim().toUpperCase();
        if (!provider.getEntityTypes(typeCode).contains(typeCode)) {
          throw newNotFoundException(
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
      throw newInternalServerErrorException(GET, uriInfo, timers, e);
    }
  }

  private String doGetEntityTypes(SzHttpMethod httpMethod,
                                  UriInfo uriInfo,
                                  Timers timers,
                                  G2Engine engineApi,
                                  G2Config configApi) {
    String config = exportConfig(GET, uriInfo, timers, engineApi);

    Long configId = null;
    try {
      // load into a config object by ID
      callingNativeAPI(timers, "config", "load");
      configId = configApi.load(config);
      calledNativeAPI(timers, "config", "load");

      if (configId < 0) {
        throw newInternalServerErrorException(GET, uriInfo, timers, configApi);
      }

      StringBuffer sb = new StringBuffer();

      // list the data sources on the config
      callingNativeAPI(timers, "config", "listEntityTypesV2");
      int returnCode = configApi.listEntityTypesV2(configId, sb);
      if (returnCode != 0) {
        throw newInternalServerErrorException(
            GET, uriInfo, timers, configApi);
      }
      calledNativeAPI(timers, "config", "listEntityTypesV2");

      return sb.toString();

    } finally {
      if (configId != null) {
        configApi.close(configId);
      }
    }
  }

  private SzEntityTypesResponse buildEntityTypesResponse(
      SzHttpMethod httpMethod,
      UriInfo uriInfo,
      Timers timers,
      String entityClass,
      String rawData,
      boolean withRaw) {
    processingRawData(timers);
    // parse the raw data
    JsonObject jsonObject = JsonUtils.parseJsonObject(rawData);

    // get the array and construct the response
    JsonArray jsonArray = jsonObject.getJsonArray("ENTITY_TYPES");
    List<SzEntityType> entityTypes
        = SzEntityType.parseEntityTypeList(null, jsonArray);

    SzEntityTypesResponse response = new SzEntityTypesResponse(
        httpMethod, 200, uriInfo, timers);

    // check if we are filtering on entity class
    if (entityClass != null) {
      final String ec = entityClass.trim();
      entityTypes.removeIf(et -> !et.getEntityClassCode().equalsIgnoreCase(ec));
    }

    // set the entity types
    response.setEntityTypes(entityTypes);
    processedRawData(timers);

    // if including raw data then add it
    if (withRaw) response.setRawData(rawData);

    return response;
  }

  private SzEntityTypeResponse buildEntityTypeResponse(
      SzHttpMethod httpMethod,
      UriInfo uriInfo,
      Timers timers,
      String entityClassCode,
      String entityTypeCode,
      String rawData,
      boolean withRaw)
  {
    processingRawData(timers);
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
      throw newNotFoundException(
          httpMethod, uriInfo, timers,
          "The specified entity type was not recognized: " + entityTypeCode);
    }

    // parse the data source
    SzEntityType entityType
        = SzEntityType.parseEntityType(null, jsonObject);

    // check if being found for a specific entity class and verify
    if (entityClassCode != null
        && !entityType.getEntityClassCode().equalsIgnoreCase(entityClassCode)) {
      throw newNotFoundException(
          httpMethod, uriInfo, timers,
          "The specified entity type code (" + entityTypeCode
              + ") does not have the specified entity class code: "
              + entityClassCode);
    }

    // build the response
    SzEntityTypeResponse response = new SzEntityTypeResponse(
        httpMethod, 200, uriInfo, timers);

    response.setEntityType(entityType);
    processedRawData(timers);

    // if including raw data then add it
    if (withRaw) response.setRawData(JsonUtils.toJsonText(jsonObject));

    return response;
  }

  @POST
  @Path("data-sources")
  public SzDataSourcesResponse addDataSources(
      @QueryParam("dataSource") List<String> dataSourceCodes,
      @DefaultValue("false") @QueryParam("withRaw") boolean withRaw,
      @Context UriInfo uriInfo,
      String dataSourcesInBody)
  {
    Timers timers = newTimers();

    SzApiProvider provider = SzApiProvider.Factory.getProvider();
    ensureConfigChangesAllowed(provider, POST, uriInfo, timers);

    Map<String, SzDataSource> map = new LinkedHashMap<>();
    for (String code : dataSourceCodes) {
      SzDataSource dataSource = new SzDataSource(code);
      map.put(dataSource.getDataSourceCode(), dataSource);
    }

    // get the body data sources
    if (dataSourcesInBody != null && dataSourcesInBody.trim().length() > 0) {
      // trim the string
      dataSourcesInBody = dataSourcesInBody.trim();

      // create a list for the parsed data sources
      SzDataSourceDescriptors descriptors = null;

      try {
        descriptors = SzDataSourceDescriptors.valueOf(dataSourcesInBody);

      } catch (Exception e) {
        throw newBadRequestException(POST, uriInfo, timers, e);
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
   * Internal method for adding one or more data sources.
   *
   * @return An SzDataSourcesResponse with all the configured data sources.
   */
  private SzDataSourcesResponse doAddDataSources(
      SzHttpMethod httpMethod,
      Collection<SzDataSource> dataSources,
      UriInfo uriInfo,
      boolean withRaw,
      Timers timers) {
    SzApiProvider provider = SzApiProvider.Factory.getProvider();

    try {
      // get the engine API and the config API
      G2Engine    engineApi     = provider.getEngineApi();
      G2Config    configApi     = provider.getConfigApi();
      G2ConfigMgr configMgrApi  = provider.getConfigMgrApi();
      Set<String> createdSet    = new LinkedHashSet<>();

      if (configMgrApi == null) {
        throw newForbiddenException(
            httpMethod, uriInfo, timers, "Configuration changes not permitted.");
      }

      // loop until the provider has the data source code we are looking for
      enteringQueue(timers);
      String rawData = provider.executeInThread(() -> {
        exitingQueue(timers);

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
            callingNativeAPI(timers, "config", "load");
            configHandle = configApi.load(configJSON);
            calledNativeAPI(timers, "config", "load");

            if (configHandle <= 0) {
              throw newInternalServerErrorException(
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
                throw newBadRequestException(
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
              callingNativeAPI(timers, "config", "addDataSourceV2");
              int returnCode = configApi.addDataSourceV2(
                  configHandle, dataSource.toNativeJson(), new StringBuffer());
              calledNativeAPI(timers, "config", "addDataSourceV2");

              if (returnCode != 0) {
                throw newInternalServerErrorException(
                    httpMethod, uriInfo, timers, configApi);
              }

              createdSet.add(dataSource.getDataSourceCode());
            }

            if (createdSet.size() > 0) {
              this.updateCurrentConfig(
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
      throw newInternalServerErrorException(PUT, uriInfo, timers, e);
    }
  }

  @POST
  @Path("entity-classes")
  public SzEntityClassesResponse addEntityClasses(
      @QueryParam("entityClass") List<String> entityClassCodes,
      @QueryParam("resolving") Boolean resolving,
      @DefaultValue("false") @QueryParam("withRaw") boolean withRaw,
      @Context UriInfo uriInfo,
      String entityClassesInBody)
  {
    Timers timers = newTimers();

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

    SzApiProvider provider = SzApiProvider.Factory.getProvider();
    ensureConfigChangesAllowed(provider, POST, uriInfo, timers);

    Map<String, SzEntityClass> map = new LinkedHashMap<>();

    // add the entity class codes from the query to the map
    for (String code : entityClassCodes) {
      SzEntityClass entityClass = new SzEntityClass(code, null, resolving);
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
        throw newBadRequestException(POST, uriInfo, timers, e);
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
    return this.doAddEntityClasses(POST, map.values(), uriInfo, withRaw, timers);
  }

  /**
   * Internal method for adding one or more entity classes.
   *
   * @return An SzEntityClassesResponse with all the configured entity classes.
   */
  private SzEntityClassesResponse doAddEntityClasses(
      SzHttpMethod httpMethod,
      Collection<SzEntityClass> entityClasses,
      UriInfo uriInfo,
      boolean withRaw,
      Timers timers) {
    SzApiProvider provider = SzApiProvider.Factory.getProvider();

    try {
      // get the engine API and the config API
      G2Engine    engineApi     = provider.getEngineApi();
      G2ConfigMgr configMgrApi  = provider.getConfigMgrApi();
      G2Config    configApi     = provider.getConfigApi();
      Set<String> createdSet    = new LinkedHashSet<>();

      if (configMgrApi == null) {
        throw newForbiddenException(
            httpMethod, uriInfo, timers, "Configuration changes not permitted.");
      }

      // loop until the provider has the data source code we are looking for
      enteringQueue(timers);
      String rawData = provider.executeInThread(() -> {
        exitingQueue(timers);

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
            callingNativeAPI(timers, "config", "load");
            configHandle = configApi.load(configJSON);
            calledNativeAPI(timers, "config", "load");

            if (configHandle <= 0) {
              throw newInternalServerErrorException(
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
                throw newBadRequestException(
                    httpMethod, uriInfo, timers,
                    "At least one entity class already exists, but "
                        + "with a different entity class ID.  specified=[ "
                        + entityClass + " ], existing=[ " + existingEC + " ]");
              }
              if (existingEC != null && resolving != null
                  && !resolving.equals(existingEC.isResolving()))
              {
                throw newBadRequestException(
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
              callingNativeAPI(timers, "config", "addEntityClassV2");
              int returnCode = configApi.addEntityClassV2(
                  configHandle, nativeJson, new StringBuffer());
              calledNativeAPI(timers, "config", "addEntityClassV2");

              if (returnCode != 0) {
                throw newInternalServerErrorException(
                    httpMethod, uriInfo, timers, configApi);
              }

              createdSet.add(entityClass.getEntityClassCode());
            }

            if (createdSet.size() > 0) {
              this.updateCurrentConfig(
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
      throw newInternalServerErrorException(PUT, uriInfo, timers, e);
    }
  }

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
    Timers timers = newTimers();

    // normalize the entity class
    entityClassCode = entityClassCode.trim().toUpperCase();

    this.checkEntityClassNotFound(POST, uriInfo, timers, entityClassCode);

    return this.postEntityTypes(entityClassCode,
                                true,
                                entityTypeCodes,
                                withRaw,
                                uriInfo,
                                entityTypesInBody,
                                timers);
  }

  @POST
  @Path("entity-types")
  public SzEntityTypesResponse addEntityTypes(
      @DefaultValue("ACTOR") @QueryParam("entityClass") String entityClassCode,
      @QueryParam("entityType") List<String> entityTypeCodes,
      @DefaultValue("false") @QueryParam("withRaw") boolean withRaw,
      @Context UriInfo uriInfo,
      String entityTypesInBody)
  {
    Timers timers = newTimers();

    // normalize the entity class
    if (entityClassCode != null) {
      entityClassCode = entityClassCode.trim().toUpperCase();
    }

    this.checkEntityClassInvalid(POST, uriInfo, timers, entityClassCode);

    return this.postEntityTypes(entityClassCode,
                                false,
                                entityTypeCodes,
                                withRaw,
                                uriInfo,
                                entityTypesInBody,
                                timers);
  }

  private SzEntityTypesResponse postEntityTypes(String        entityClassCode,
                                                boolean       enforceSameClass,
                                                List<String>  entityTypeCodes,
                                                boolean       withRaw,
                                                UriInfo       uriInfo,
                                                String        entityTypesInBody,
                                                Timers        timers)
  {
    SzApiProvider provider = SzApiProvider.Factory.getProvider();
    ensureConfigChangesAllowed(provider, POST, uriInfo, timers);

    Map<String, SzEntityType> map = new LinkedHashMap<>();

    // add the entity class codes from the query to the map
    for (String code : entityTypeCodes) {
      SzEntityType entityType
          = new SzEntityType(code,null, entityClassCode);
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
              throw newBadRequestException(
                  POST, uriInfo, timers,
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
        throw newBadRequestException(POST, uriInfo, timers, e);
      }

      // loop through the entity types and put them in the map
      for (SzEntityType entityType : entityTypes) {
        map.put(entityType.getEntityTypeCode(), entityType);
      }
    }

    // add the entity types
    return this.doAddEntityTypes(POST, map.values(), uriInfo, withRaw, timers);
  }

  /**
   * Internal method for adding one or more entity classes.
   *
   * @return An SzEntityClassesResponse with all the configured entity classes.
   */
  private SzEntityTypesResponse doAddEntityTypes(
      SzHttpMethod httpMethod,
      Collection<SzEntityType> entityTypes,
      UriInfo uriInfo,
      boolean withRaw,
      Timers timers)
  {
    SzApiProvider provider = SzApiProvider.Factory.getProvider();

    try {
      // get the engine API and the config API
      G2ConfigMgr configMgrApi  = provider.getConfigMgrApi();
      G2Config    configApi     = provider.getConfigApi();
      G2Engine    engineApi     = provider.getEngineApi();
      Set<String> createdSet    = new LinkedHashSet<>();

      if (configMgrApi == null) {
        throw newForbiddenException(
            httpMethod, uriInfo, timers, "Configuration changes not permitted.");
      }

      // loop until the provider has the data source code we are looking for
      enteringQueue(timers);
      String rawData = provider.executeInThread(() -> {
        exitingQueue(timers);

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
            callingNativeAPI(timers, "config", "load");
            configHandle = configApi.load(configJSON);
            calledNativeAPI(timers, "config", "load");

            if (configHandle <= 0) {
              throw newInternalServerErrorException(
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
                throw newBadRequestException(
                    httpMethod, uriInfo, timers,
                    "At least one entity type already exists, but "
                        + "with a different entity type ID.  specified=[ "
                        + entityType + " ], existing=[ " + existingET + " ]");
              }

              // check if it already exists with a different entity class
              if (existingET != null && classCode != null
                  && !classCode.equals(existingET.getEntityClassCode()))
              {
                throw newBadRequestException(
                    httpMethod, uriInfo, timers,
                    "At least one entity type already exists, but "
                        + "with a different entity class code.  specified=[ "
                        + entityType + " ], existing=[ " + existingET + " ]");
              }

              // check if the entity class code is not recognized
              if (!provider.getEntityClasses(classCode).contains(classCode)) {
                throw newBadRequestException(
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
              callingNativeAPI(timers, "config", "addEntityTypeV2");
              int returnCode = configApi.addEntityTypeV2(
                  configHandle, entityType.toNativeJson(), new StringBuffer());
              calledNativeAPI(timers, "config", "addEntityTypeV2");

              if (returnCode != 0) {
                throw newInternalServerErrorException(
                    httpMethod, uriInfo, timers, configApi);
              }

              createdSet.add(entityType.getEntityTypeCode());
            }

            if (createdSet.size() > 0) {
              this.updateCurrentConfig(
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
      throw newInternalServerErrorException(PUT, uriInfo, timers, e);
    }
  }

  private String getDefaultConfig(SzHttpMethod  httpMethod,
                                  UriInfo       uriInfo,
                                  G2ConfigMgr   configMgrApi,
                                  Timers        timers,
                                  Result<Long>  result)
  {
    synchronized (configMgrApi) {
      callingNativeAPI(timers, "configMgr", "getDefaultConfigID");
      int returnCode = configMgrApi.getDefaultConfigID(result);
      calledNativeAPI(timers, "configMgr", "getDefaultConfigID");
      // check the return code
      if (returnCode != 0) {
        throw newInternalServerErrorException(
            httpMethod, uriInfo, timers, configMgrApi);
      }

      long defaultConfigId = result.getValue();
      StringBuffer sb = new StringBuffer();

      // get the config
      callingNativeAPI(timers, "configMgr", "getConfig");
      returnCode = configMgrApi.getConfig(defaultConfigId, sb);
      calledNativeAPI(timers, "configMgr", "getConfig");
      if (returnCode != 0) {
        throw newInternalServerErrorException(
            httpMethod, uriInfo, timers, configMgrApi);
      }

      // get the JSON
      return sb.toString();
    }
  }

  private boolean updateCurrentConfig(SzHttpMethod httpMethod,
                                      UriInfo      uriInfo,
                                      G2Config     configApi,
                                      G2ConfigMgr  configMgrApi,
                                      Timers       timers,
                                      long         defaultConfigId,
                                      long         configHandle,
                                      String       configComment)
  {
    StringBuffer sb     = new StringBuffer();
    Result<Long> result = new Result<>();

    // convert the config to a JSON string
    callingNativeAPI(timers, "config", "save");
    int returnCode = configApi.save(configHandle, sb);
    if (returnCode != 0) {
      throw newInternalServerErrorException(
          PUT, uriInfo, timers, configApi);
    }
    calledNativeAPI(timers, "config", "save");

    String configJSON = sb.toString();

    // save the configuration
    obtainingLock(timers, "configMgrApi");
    synchronized (configMgrApi) {
      obtainedLock(timers, "configMgrApi");
      callingNativeAPI(timers, "configMgr", "addConfig");
      returnCode = configMgrApi.addConfig(configJSON, configComment, result);

      if (returnCode != 0) {
        throw newInternalServerErrorException(
            PUT, uriInfo, timers, configMgrApi);
      }
      calledNativeAPI(timers, "configMgr", "addConfig");

      // get the config ID for the newly saved config
      long newConfigId = result.getValue();

      callingNativeAPI(timers, "configMgr", "getDefaultConfigID");
      returnCode = configMgrApi.getDefaultConfigID(result);
      if (returnCode != 0) {
        throw newInternalServerErrorException(
            PUT, uriInfo, timers, configMgrApi);
      }
      calledNativeAPI(timers, "configMgr", "getDefaultConfigID");

      // check if the default configuration ID has changed
      if (!result.getValue().equals(defaultConfigId)) {
        System.out.println(
            "Concurrent configuration change detected.  Retrying...");
        return false;
      }

      callingNativeAPI(timers, "configMgr", "setDefaultConfigID");
      returnCode = configMgrApi.setDefaultConfigID(newConfigId);
      if (returnCode != 0) {
        throw newInternalServerErrorException(
            PUT, uriInfo, timers, configMgrApi);
      }
      calledNativeAPI(timers, "configMgr", "setDefaultConfigID");
    }
    return true;
  }

  private Map<String, SzDataSource> getDataSourcesMap(
      SzHttpMethod  httpMethod,
      UriInfo       uriInfo,
      G2Config      configApi,
      Timers        timers,
      long          configHandle)
  {
    StringBuffer sb = new StringBuffer();
    callingNativeAPI(timers, "config", "listDataSourcesV2");
    int returnCode = configApi.listDataSourcesV2(configHandle, sb);
    calledNativeAPI(timers, "config", "listDataSourcesV2");

    if (returnCode != 0) {
      throw newInternalServerErrorException(
          httpMethod, uriInfo, timers, configApi);
    }
    JsonObject jsonObject = JsonUtils.parseJsonObject(sb.toString());
    JsonArray jsonArray = jsonObject.getJsonArray("DATA_SOURCES");
    List<SzDataSource> dataSources
        = SzDataSource.parseDataSourceList(null, jsonArray);
    Map<String, SzDataSource> dataSourceMap = new LinkedHashMap<>();
    dataSources.forEach(dataSource -> {
      dataSourceMap.put(dataSource.getDataSourceCode(), dataSource);
    });

    return dataSourceMap;
  }

  private Map<String, SzEntityClass> getEntityClassesMap(
      SzHttpMethod  httpMethod,
      UriInfo       uriInfo,
      G2Config      configApi,
      Timers        timers,
      long          configHandle)
  {
    StringBuffer sb = new StringBuffer();
    callingNativeAPI(timers, "config", "listEntityClassesV2");
    int returnCode = configApi.listEntityClassesV2(configHandle, sb);
    calledNativeAPI(timers, "config", "listEntityClassesV2");

    if (returnCode != 0) {
      throw newInternalServerErrorException(
          httpMethod, uriInfo, timers, configApi);
    }
    JsonObject jsonObject = JsonUtils.parseJsonObject(sb.toString());
    JsonArray jsonArray = jsonObject.getJsonArray("ENTITY_CLASSES");
    List<SzEntityClass> entityClasses
        = SzEntityClass.parseEntityClassList(null, jsonArray);
    Map<String, SzEntityClass> entityClassMap = new LinkedHashMap<>();
    entityClasses.forEach(entityClass -> {
      entityClassMap.put(entityClass.getEntityClassCode(), entityClass);
    });

    return entityClassMap;
  }

  private Map<String, SzEntityType> getEntityTypesMap(
      SzHttpMethod  httpMethod,
      UriInfo       uriInfo,
      G2Config      configApi,
      Timers        timers,
      long          configHandle)
  {
    StringBuffer sb = new StringBuffer();
    callingNativeAPI(timers, "config", "listEntityTypesV2");
    int returnCode = configApi.listEntityTypesV2(configHandle, sb);
    calledNativeAPI(timers, "config", "listEntityTypesV2");

    if (returnCode != 0) {
      throw newInternalServerErrorException(
          httpMethod, uriInfo, timers, configApi);
    }
    JsonObject jsonObject = JsonUtils.parseJsonObject(sb.toString());
    JsonArray jsonArray = jsonObject.getJsonArray("ENTITY_TYPES");
    List<SzEntityType> entityTypes
        = SzEntityType.parseEntityTypeList(null, jsonArray);
    Map<String, SzEntityType> entityTypeMap = new LinkedHashMap<>();
    entityTypes.forEach(entityType -> {
      entityTypeMap.put(entityType.getEntityTypeCode(), entityType);
    });

    return entityTypeMap;
  }

  @GET
  @Path("attribute-types")
  public SzAttributeTypesResponse getAttributeTypes(
      @DefaultValue("false") @QueryParam("withInternal") boolean withInternal,
      @QueryParam("attributeClass")                      String  attributeClass,
      @QueryParam("featureType")                         String  featureType,
      @DefaultValue("false") @QueryParam("withRaw")      boolean withRaw,
      @Context                                           UriInfo uriInfo)
  {
    Timers timers = newTimers();
    SzApiProvider provider = SzApiProvider.Factory.getProvider();

    SzAttributeClass ac = null;
    if (attributeClass != null && attributeClass.trim().length() > 0) {
      try {
        ac = SzAttributeClass.valueOf(attributeClass.trim().toUpperCase());

      } catch (IllegalArgumentException e) {
        throw newBadRequestException(
            GET, uriInfo, timers, "Unrecognized attribute class: " + attributeClass);
      }
    }
    final SzAttributeClass attrClass = ac;
    final String featType
        = ((featureType != null && featureType.trim().length() > 0)
        ? featureType.trim() : null);

    try {
      enteringQueue(timers);
      JsonObject configRoot = provider.executeInThread(() -> {
        exitingQueue(timers);

        // get the engine API and the config API
        G2Engine engineApi = provider.getEngineApi();

        return getCurrentConfigRoot(GET, uriInfo, timers, engineApi);
      });
      
      processingRawData(timers);
      // get the array and construct the response
      JsonArray jsonArray = configRoot.getJsonArray("CFG_ATTR");

      List<SzAttributeType> attrTypes
          = SzAttributeType.parseAttributeTypeList(null, jsonArray);

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
      SzAttributeTypesResponse response
          = new SzAttributeTypesResponse(GET, 200, uriInfo, timers);

      response.setAttributeTypes(attrTypes);

      // if including raw data then add it
      if (withRaw) {
        processingRawData(timers);
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
        String rawData = JsonUtils.toJsonText(jsonArray);
        processedRawData(timers);
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

  @GET
  @Path("attribute-types/{attributeCode}")
  public SzAttributeTypeResponse getAttributeType(
      @PathParam("attributeCode")                   String  attributeCode,
      @DefaultValue("false") @QueryParam("withRaw") boolean withRaw,
      @Context                                      UriInfo uriInfo)
  {
    Timers timers = newTimers();
    SzApiProvider provider = SzApiProvider.Factory.getProvider();

    try {
      enteringQueue(timers);
      JsonObject configRoot = provider.executeInThread(() -> {
        exitingQueue(timers);
        // get the engine API and the config API
        G2Engine engineApi = provider.getEngineApi();

        JsonObject obj = getCurrentConfigRoot(GET, uriInfo, timers, engineApi);

        return obj;
      });

      processingRawData(timers);

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
        throw newNotFoundException(
            GET, uriInfo, timers, "Attribute code not recognized: " + attributeCode);
      }

      SzAttributeType attrType
          = SzAttributeType.parseAttributeType(null, jsonAttrType);

      SzAttributeTypeResponse response = new SzAttributeTypeResponse(
          GET, 200, uriInfo, timers, attrType);

      // if including raw data then add it
      if (withRaw) {
        String rawData = JsonUtils.toJsonText(jsonAttrType);

        response.setRawData(rawData);
      }
      processedRawData(timers);

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

  @GET
  @Path("configs/active")
  public SzConfigResponse getActiveConfig(@Context UriInfo uriInfo)
  {
    Timers timers = newTimers();
    SzApiProvider provider = SzApiProvider.Factory.getProvider();

    try {
      enteringQueue(timers);
      JsonObject configObject = provider.executeInThread(() -> {
        exitingQueue(timers);
        // get the engine API and the config API
        G2Engine engineApi = provider.getEngineApi();

        // export the config
        String config = exportConfig(GET, uriInfo, timers, engineApi);

        // parse the raw data
        processingRawData(timers);
        JsonObject configObj = JsonUtils.parseJsonObject(config);
        processedRawData(timers);

        return configObj;
      });

      processingRawData(timers);
      String rawData = JsonUtils.toJsonText(configObject);
      SzConfigResponse response = new SzConfigResponse(
          GET, 200, uriInfo, timers, rawData);
      processedRawData(timers);

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

  @GET
  @Path("configs/template")
  public SzConfigResponse getTemplateConfig(@Context UriInfo uriInfo)
  {
    Timers timers = newTimers();
    SzApiProvider provider = SzApiProvider.Factory.getProvider();

    try {
      enteringQueue(timers);
      JsonObject configObject = provider.executeInThread(() -> {
        exitingQueue(timers);
        // get the engine API and the config API
        G2Config configApi = provider.getConfigApi();

        // create the default config
        StringBuffer sb = new StringBuffer();
        callingNativeAPI(timers, "config", "create");
        long configId = configApi.create();
        if (configId <= 0L) {
          throw newInternalServerErrorException(
              GET, uriInfo, timers, configApi);
        }
        calledNativeAPI(timers, "config", "create");
        try {
          callingNativeAPI(timers, "config", "save");
          int returnCode = configApi.save(configId, sb);
          if (returnCode != 0) {
            throw newInternalServerErrorException(
                GET, uriInfo, timers, configApi);
          }
          calledNativeAPI(timers, "config", "save");
        } finally {
          callingNativeAPI(timers, "config", "close");
          configApi.close(configId);
          calledNativeAPI(timers, "config", "close");
        }

        String config = sb.toString();

        // parse the raw data
        processingRawData(timers);
        JsonObject configObj = JsonUtils.parseJsonObject(config);
        processedRawData(timers);

        return configObj;
      });

      processingRawData(timers);
      String rawData = JsonUtils.toJsonText(configObject);
      SzConfigResponse response = new SzConfigResponse(
          GET, 200, uriInfo, timers);
      response.setRawData(rawData);
      processedRawData(timers);

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
   * Exports the config using the specified {@link G2Engine} instance.
   */
  private static String exportConfig(SzHttpMethod   httpMethod,
                                     UriInfo        uriInfo,
                                     Timers         timers,
                                     G2Engine       engineApi)
  {
    StringBuffer sb = new StringBuffer();
    callingNativeAPI(timers, "engine", "exportConfig");
    int result = engineApi.exportConfig(sb);
    if (result != 0) {
      throw newInternalServerErrorException(httpMethod, uriInfo, timers, engineApi);
    }
    calledNativeAPI(timers, "engine", "exportConfig");
    return sb.toString();
  }

  /**
   * From an exported config, this pulls the <tt>"G2_CONFIG"</tt>
   * {@link JsonObject} from it.
   */
  private JsonObject getCurrentConfigRoot(SzHttpMethod  httpMethod,
                                          UriInfo       uriInfo,
                                          Timers        timers,
                                          G2Engine      engineApi)
  {
    // export the config
    String config = exportConfig(httpMethod, uriInfo, timers, engineApi);

    // parse the raw data
    processingRawData(timers);
    JsonObject configObj = JsonUtils.parseJsonObject(config);
    processedRawData(timers);
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
  private void checkEntityClassNotFound(SzHttpMethod  httpMethod,
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
    SzApiProvider provider = SzApiProvider.Factory.getProvider();
    Set<String> entityClasses = provider.getEntityClasses(entityClassCode);
    if (!entityClasses.contains(entityClassCode)) {
      throw newNotFoundException(
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
  private void checkEntityClassInvalid(SzHttpMethod   httpMethod,
                                       UriInfo        uriInfo,
                                       Timers         timers,
                                       String         entityClassCode)
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
    SzApiProvider provider = SzApiProvider.Factory.getProvider();
    Set<String> entityClasses = provider.getEntityClasses(entityClassCode);
    if (!entityClasses.contains(entityClassCode)) {
      throw newBadRequestException(
          httpMethod, uriInfo, timers,
          "The entity class code was not recognized: " + entityClassCode);
    }
  }
}
