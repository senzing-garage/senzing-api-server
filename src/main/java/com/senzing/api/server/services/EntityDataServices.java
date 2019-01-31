package com.senzing.api.server.services;

import com.senzing.api.model.*;
import com.senzing.api.server.SzApiServer;
import com.senzing.g2.engine.G2Engine;
import com.senzing.util.JsonUtils;

import javax.json.*;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriInfo;

import java.util.*;

import static com.senzing.api.model.SzHttpMethod.*;
import static com.senzing.api.server.services.ServicesUtil.*;

/**
 * Provides entity data related API services.
 */
@Path("/")
@Produces("application/json; charset=UTF-8")
public class EntityDataServices {
  private static final int DATA_SOURCE_NOT_FOUND_CODE = 27;

  private static final int RECORD_NOT_FOUND_CODE = 33;

  @POST
  @Path("data-sources/{dataSourceCode}/records")
  public SzLoadRecordResponse loadRecord(
      @PathParam("dataSourceCode")  String  dataSourceCode,
      @QueryParam("loadId")         String  loadId,
      @Context                      UriInfo uriInfo,
      String                                recordJsonData)
  {
    SzApiServer server = SzApiServer.getInstance();
    dataSourceCode = dataSourceCode.toUpperCase();

    final String dataSource = dataSourceCode;

    final String normalizedLoadId = normalizeString(loadId);

    return server.executeInThread(() -> {
      String recordText = ensureJsonFields(
        POST,
        uriInfo,
        recordJsonData,
        Collections.singletonMap("DATA_SOURCE", dataSource));

      try {
        // get the engine API and the config API
        G2Engine engineApi = server.getEngineApi();

        checkDataSource(POST, uriInfo, dataSource, server);

        StringBuffer sb = new StringBuffer();

        int result = engineApi.addRecordWithReturnedRecordID(dataSource,
                                                             sb,
                                                             recordText,
                                                             normalizedLoadId);
        if (result != 0) {
          throw newWebApplicationException(POST, uriInfo, engineApi);
        }

        String recordId = sb.toString().trim();

        // construct the response
        SzLoadRecordResponse response = new SzLoadRecordResponse(
            POST, 200, uriInfo, recordId);

        // return the response
        return response;

      } catch (WebApplicationException e) {
        throw e;

      } catch (Exception e) {
        throw ServicesUtil.newInternalServerErrorException(POST, uriInfo, e);
      }

    });
  }

  @PUT
  @Path("data-sources/{dataSourceCode}/records/{recordId}")
  public SzLoadRecordResponse loadRecord(
      @PathParam("dataSourceCode")  String  dataSourceCode,
      @PathParam("recordId")        String  recordId,
      @QueryParam("loadId")         String  loadId,
      @Context                      UriInfo uriInfo,
      String                                recordJsonData)
  {
    SzApiServer server = SzApiServer.getInstance();
    dataSourceCode = dataSourceCode.toUpperCase();

    final String dataSource = dataSourceCode;

    final String normalizedLoadId = normalizeString(loadId);

    return server.executeInThread(() -> {
      Map<String,String> map = new HashMap<>();
      map.put("DATA_SOURCE", dataSource);
      map.put("RECORD_ID", recordId);

      String recordText = ensureJsonFields(PUT,
                                           uriInfo,
                                           recordJsonData,
                                           map);

      try {
        // get the engine API and the config API
        G2Engine engineApi = server.getEngineApi();

        Set<String> dataSources = server.getDataSources();

        if (!dataSources.contains(dataSource)) {
          throw newNotFoundException(
              POST, uriInfo,
              "The specified data source is not recognized: " + dataSource);
        }

        StringBuffer sb = new StringBuffer();

        int result = engineApi.addRecord(dataSource,
                                         recordId,
                                         recordText,
                                         normalizedLoadId);
        if (result != 0) {
          throw newWebApplicationException(PUT, uriInfo, engineApi);
        }

        // construct the response
        SzLoadRecordResponse response = new SzLoadRecordResponse(
            PUT, 200, uriInfo, recordId);

        // return the response
        return response;

      } catch (WebApplicationException e) {
        throw e;

      } catch (Exception e) {
        throw ServicesUtil.newInternalServerErrorException(PUT, uriInfo, e);
      }

    });
  }

  @GET
  @Path("data-sources/{dataSourceCode}/records/{recordId}")
  public SzRecordResponse getRecord(
      @PathParam("dataSourceCode")                  String  dataSourceCode,
      @PathParam("recordId")                        String  recordId,
      @DefaultValue("false") @QueryParam("withRaw") boolean withRaw,
      @Context                                      UriInfo uriInfo)
  {
    SzApiServer server = SzApiServer.getInstance();
    dataSourceCode = dataSourceCode.toUpperCase();

    final String dataSource = dataSourceCode;
    return server.executeInThread(() -> {
      try {
        // get the engine API and the config API
        G2Engine engineApi = server.getEngineApi();

        StringBuffer sb = new StringBuffer();

        int result = engineApi.getRecord(dataSource, recordId, sb);
        if (result != 0) {
          throw newWebApplicationException(GET, uriInfo, engineApi);
        }
        // parse the raw data
        String rawData = sb.toString();
        JsonObject jsonObject = JsonUtils.parseJsonObject(rawData);

        SzEntityRecord entityRecord
            = SzEntityRecord.parseEntityRecord(null, jsonObject);

        // construct the response
        SzRecordResponse response = new SzRecordResponse(GET,
                                                         200,
                                                         uriInfo,
                                                         entityRecord);

        // if including raw data then add it
        if (withRaw) response.setRawData(rawData);

        // return the response
        return response;

      } catch (WebApplicationException e) {
        throw e;

      } catch (Exception e) {
        throw ServicesUtil.newInternalServerErrorException(GET, uriInfo, e);
      }
    });
  }

  @GET
  @Path("data-sources/{dataSourceCode}/records/{recordId}/entity")
  public SzEntityResponse geEntityByRecordId(
      @PathParam("dataSourceCode")                      String  dataSourceCode,
      @PathParam("recordId")                            String  recordId,
      @DefaultValue("false") @QueryParam("withRaw")     boolean withRaw,
      @DefaultValue("false") @QueryParam("withRelated") boolean withRelated,
      @Context                                          UriInfo uriInfo) {
    SzApiServer server = SzApiServer.getInstance();
    dataSourceCode = dataSourceCode.toUpperCase();

    final String dataSource = dataSourceCode;
    return server.executeInThread(() -> {
      try {
        // get the engine API and the config API
        G2Engine engineApi = server.getEngineApi();

        StringBuffer sb = new StringBuffer();

        SzEntityData entityData = null;

        // check if we want 1-degree relations as well -- if so we need to
        // find the network instead of a simple lookup
        if (withRelated) {
          // build the record IDs JSON to find the network
          JsonObjectBuilder builder1 = Json.createObjectBuilder();
          JsonArrayBuilder builder2 = Json.createArrayBuilder();
          JsonObjectBuilder builder3 = Json.createObjectBuilder();
          builder1.add("RECORD_ID", recordId);
          builder1.add("DATA_SOURCE", dataSource);
          builder2.add(builder1);
          builder3.add("RECORDS", builder2);
          String recordIds = JsonUtils.toJsonText(builder3);

          // set the other arguments
          final int maxDegrees = 1;
          final int buildOutDegrees = 1;
          final int maxEntityCount = 1000;

          // find the network and check the result
          int result = engineApi.findNetworkByRecordID(
              recordIds, maxDegrees, buildOutDegrees, maxEntityCount, sb);

          if (result != 0) {
            throw newWebApplicationException(GET, uriInfo, engineApi);
          }

          // organize all the entities into a map for lookup
          Map<Long, SzEntityData> dataMap
              = parseEntityDataList(sb.toString(), server);

          // find the entity ID matching the data source and record ID
          Long entityId = null;
          for (SzEntityData edata : dataMap.values()) {
            SzResolvedEntity resolvedEntity = edata.getResolvedEntity();
            // check if this entity is the one that was requested by record ID
            for (SzEntityRecord record : resolvedEntity.getRecords()) {
              if (record.getDataSource().equalsIgnoreCase(dataSource)
                  && record.getRecordId().equals(recordId)) {
                // found the entity ID for the record ID
                entityId = resolvedEntity.getEntityId();
              }
            }
          }

          // get the result entity data
          entityData = getAugmentedEntityData(entityId, dataMap, server);

        } else {
          // 1-degree relations are not required, so do a standard lookup
          int result = engineApi.getEntityByRecordID(dataSource, recordId, sb);

          checkEntityResult(result, sb.toString(), uriInfo, engineApi);

          // parse the result
          entityData = SzEntityData.parseEntityData(
              null,
              JsonUtils.parseJsonObject(sb.toString()),
              (f) -> server.getAttributeClassForFeature(f));
        }

        // construct the response
        return newEntityResponse(
            uriInfo, entityData, (withRaw ? sb.toString() : null));

      } catch (WebApplicationException e) {
        throw e;

      } catch (Exception e) {
        throw ServicesUtil.newInternalServerErrorException(GET, uriInfo, e);
      }
    });
  }

  @GET
  @Path("entities/{entityId}")
  public SzEntityResponse getEntityByEntityId(
      @PathParam("entityId")                            long    entityId,
      @DefaultValue("false") @QueryParam("withRaw")     boolean withRaw,
      @DefaultValue("false") @QueryParam("withRelated") boolean withRelated,
      @Context                                          UriInfo uriInfo)
  {
    SzApiServer server = SzApiServer.getInstance();

    return server.executeInThread(() -> {

      try {
        // get the engine API
        G2Engine engineApi = server.getEngineApi();

        StringBuffer sb = new StringBuffer();

        SzEntityData entityData = null;

        // check if we want 1-degree relations as well -- if so we need to
        // find the network instead of a simple lookup
        if (withRelated) {
          // build the record IDs JSON to find the network
          JsonObjectBuilder builder1 = Json.createObjectBuilder();
          JsonArrayBuilder builder2 = Json.createArrayBuilder();
          JsonObjectBuilder builder3 = Json.createObjectBuilder();
          builder1.add("ENTITY_ID", entityId);
          builder2.add(builder1);
          builder3.add("ENTITIES", builder2);
          String entityIds = JsonUtils.toJsonText(builder3);

          // set the other arguments
          final int maxDegrees = 1;
          final int buildOutDegrees = 1;
          final int maxEntityCount = 1000;

          // find the network and check the result
          int result = engineApi.findNetworkByEntityID(
              entityIds, maxDegrees, buildOutDegrees, maxEntityCount, sb);

          if (result != 0) {
            throw newWebApplicationException(GET, uriInfo, engineApi);
          }

          // organize all the entities into a map for lookup
          Map<Long, SzEntityData> dataMap
              = parseEntityDataList(sb.toString(), server);

          // get the result entity data
          entityData = getAugmentedEntityData(entityId, dataMap, server);

        } else {
          // 1-degree relations are not required, so do a standard lookup
          int result = engineApi.getEntityByEntityID(entityId, sb);

          checkEntityResult(result, sb.toString(), uriInfo, engineApi);

          // parse the result
          entityData = SzEntityData.parseEntityData(
              null,
              JsonUtils.parseJsonObject(sb.toString()),
              (f) -> server.getAttributeClassForFeature(f));
        }

        // construct the response
        return newEntityResponse(
            uriInfo, entityData, (withRaw ? sb.toString() : null));

      } catch (WebApplicationException e) {
        throw e;

      } catch (Exception e) {
        throw ServicesUtil.newInternalServerErrorException(GET, uriInfo, e);
      }
    });
  }

  @GET
  @Path("entities")
  public SzAttributeSearchResponse searchByAttributes(
      @QueryParam("attrs")                          String  attrs,
      @DefaultValue("false") @QueryParam("withRaw") boolean withRaw,
      @Context                                      UriInfo uriInfo)
  {
    SzApiServer server = SzApiServer.getInstance();

    return server.executeInThread(() -> {

      // check if no attributes
      if (attrs == null || attrs.trim().length() == 0) {
        throw newBadRequestException(
            GET, uriInfo,
            "Parameter missing or empty: \"attrs\".  "
            + "Search criteria attributes are required.");
      }

      try {
        // get the engine API
        G2Engine engineApi = server.getEngineApi();

        StringBuffer sb = new StringBuffer();

        SzEntityData entityData = null;

        int result = engineApi.searchByAttributes(attrs, sb);
        if (result != 0) {
          throw newInternalServerErrorException(GET, uriInfo, engineApi);
        }

        JsonObject jsonObject = JsonUtils.parseJsonObject(sb.toString());
        JsonArray jsonResults = jsonObject.getValue(
            "/SEARCH_RESPONSE/RESOLVED_ENTITIES").asJsonArray();

        // parse the result
        List<SzAttributeSearchResult> list
            = SzAttributeSearchResult.parseSearchResultList(
                null,
                jsonResults,
                (f) -> server.getAttributeClassForFeature(f));

        // construct the response
        SzAttributeSearchResponse response
            = new SzAttributeSearchResponse(GET, 200, uriInfo);

        response.setSearchResults(list);

        if (withRaw) {
          response.setRawData(sb.toString());
        }

        // return the response
        return response;

      } catch (WebApplicationException e) {
        throw e;

      } catch (Exception e) {
        throw ServicesUtil.newInternalServerErrorException(GET, uriInfo, e);
      }
    });
  }


  private static WebApplicationException newWebApplicationException(
      SzHttpMethod  httpMethod,
      UriInfo       uriInfo,
      G2Engine      engineApi)
  {
    int errorCode = engineApi.getLastExceptionCode();
    if (errorCode == DATA_SOURCE_NOT_FOUND_CODE
        || errorCode == RECORD_NOT_FOUND_CODE) {
      return newNotFoundException(GET, uriInfo, engineApi);
    }
    return newInternalServerErrorException(GET, uriInfo, engineApi);
  }

  /**
   *
   * @param entityId
   * @param dataMap
   * @param server
   * @return
   */
  private static SzEntityData getAugmentedEntityData(
      long                    entityId,
      Map<Long, SzEntityData> dataMap,
      SzApiServer             server)
  {
    // get the result entity data
    SzEntityData entityData = dataMap.get(entityId);

    // check if we can augment the related entities that were found
    // so they are not partial responses since they are part of the
    // entity network build-out
    List<SzRelatedEntity> relatedEntities
        = entityData.getRelatedEntities();

    // loop over the related entities
    for (SzRelatedEntity relatedEntity : relatedEntities) {
      // get the related entity data (should be present)
      SzEntityData relatedData = dataMap.get(relatedEntity.getEntityId());

      // just in case not present because of entity count limits
      if (relatedData == null) continue;

      // get the resolved entity for the related entity
      SzResolvedEntity related = relatedData.getResolvedEntity();

      // get the features and records
      Map<String, List<SzEntityFeature>> features
          = related.getFeatures();

      List<SzEntityRecord> records = related.getRecords();

      // summarize the records
      List<SzRecordSummary> summaries
          = SzResolvedEntity.summarizeRecords(records);

      // set the features and "data" fields
      relatedEntity.setFeatures(
          features, (f) -> server.getAttributeClassForFeature(f));

      // set the records and record summaries
      relatedEntity.setRecords(records);
      relatedEntity.setRecordSummaries(summaries);
      relatedEntity.setPartial(false);
    }

    return entityData;
  }

  /**
   *
   */
  private static Map<Long, SzEntityData> parseEntityDataList(
      String rawData, SzApiServer server)
  {
    // parse the raw response and extract the entities that were found
    JsonObject jsonObj = JsonUtils.parseJsonObject(rawData);
    JsonArray jsonArr = jsonObj.getJsonArray("ENTITIES");

    List<SzEntityData> list = SzEntityData.parseEntityDataList(
        null, jsonArr, (f) -> server.getAttributeClassForFeature(f));

    // organize all the entities into a map for lookup
    Map<Long, SzEntityData> dataMap = new LinkedHashMap<>();
    for (SzEntityData edata : list) {
      SzResolvedEntity resolvedEntity = edata.getResolvedEntity();
      dataMap.put(resolvedEntity.getEntityId(), edata);
    }

    return dataMap;
  }

  /**
   *
   */
  private static SzEntityResponse newEntityResponse(UriInfo       uriInfo,
                                                    SzEntityData  entityData,
                                                    String        rawData)
  {
    // construct the response
    SzEntityResponse response = new SzEntityResponse(GET,
                                                     200,
                                                     uriInfo,
                                                     entityData);

    // if including raw data then add it
    if (rawData != null) response.setRawData(rawData);

    // return the response
    return response;

  }

  /**
   *
   */
  private static void checkEntityResult(int       result,
                                        String    nativeJson,
                                        UriInfo   uriInfo,
                                        G2Engine  engineApi)
  {
    // check if failed to find result
    if (result != 0) {
      throw newWebApplicationException(GET, uriInfo, engineApi);
    }
    if (nativeJson.trim().length() == 0) {
      throw newNotFoundException(GET, uriInfo);
    }
  }

  /**
   *
   */
  private static String normalizeString(String text) {
    if (text == null) return null;
    if (text.trim().length() == 0) return null;
    return text.trim();
  }

  /**
   * Ensures the specified data source exists for the server and thows a
   * NotFoundException if not.
   *
   * @throws NotFoundException If the data source is not found.
   */
  private static void checkDataSource(SzHttpMethod  httpMethod,
                                      UriInfo       uriInfo,
                                      String        dataSource,
                                      SzApiServer   apiServer)
    throws NotFoundException
  {
    Set<String> dataSources = apiServer.getDataSources();

    if (!dataSources.contains(dataSource)) {
      throw newNotFoundException(
          POST, uriInfo,
          "The specified data source is not recognized: " + dataSource);
    }
  }

  /**
   * Ensures the JSON fields in the map are in the specified JSON text.
   * This is a utility method.
   */
  private static String ensureJsonFields(SzHttpMethod         httpMethod,
                                         UriInfo              uriInfo,
                                         String               jsonText,
                                         Map<String, String>  map)
  {
    try {
      JsonObject jsonObject = JsonUtils.parseJsonObject(jsonText);
      JsonObjectBuilder jsonBuilder = Json.createObjectBuilder(jsonObject);

      map.entrySet().forEach(entry -> {
        String key = entry.getKey();
        String val = entry.getValue();

        String jsonVal = JsonUtils.getString(jsonObject, key.toUpperCase());
        if (jsonVal == null) {
          jsonVal = JsonUtils.getString(jsonObject, key.toLowerCase());
        }
        if (jsonVal != null && jsonVal.trim().length() > 0) {
          if (!jsonVal.equalsIgnoreCase(val)) {
            throw ServicesUtil.newBadRequestException(
                httpMethod, uriInfo,
                key + " from path and from request body do not match.  "
                    + "fromPath=[ " + val + " ], fromRequestBody=[ "
                    + jsonVal + " ]");
          }
        } else {
          // we need to add the value for the key
          jsonBuilder.add(key, val);
        }
      });

      return JsonUtils.toJsonText(jsonBuilder);

    } catch (WebApplicationException e) {
      throw e;

    } catch (Exception e) {
      throw ServicesUtil.newBadRequestException(httpMethod,
                                                uriInfo,
                                                e.getMessage());
    }
  }
}
