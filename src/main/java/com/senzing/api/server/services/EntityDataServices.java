package com.senzing.api.server.services;

import com.senzing.api.model.*;
import com.senzing.api.server.SzApiServer;
import com.senzing.g2.engine.G2Engine;
import com.senzing.util.JsonUtils;

import javax.json.*;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;

import java.util.*;

import static com.senzing.api.model.SzHttpMethod.*;
import static com.senzing.api.model.SzFeatureInclusion.*;
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
    try {
      SzApiServer server = SzApiServer.getInstance();
      dataSourceCode = dataSourceCode.toUpperCase();

      final String dataSource = dataSourceCode;

      final String normalizedLoadId = normalizeString(loadId);

      String recordText = ensureJsonFields(
          POST,
          uriInfo,
          recordJsonData,
          Collections.singletonMap("DATA_SOURCE", dataSource));

      checkDataSource(POST, uriInfo, dataSource, server);

      StringBuffer sb = new StringBuffer();

      String recordId = server.executeInThread(() -> {
        // get the engine API and the config API
        G2Engine engineApi = server.getEngineApi();

        int result = engineApi.addRecordWithReturnedRecordID(dataSource,
                                                             sb,
                                                             recordText,
                                                             normalizedLoadId);
        if (result != 0) {
          throw newWebApplicationException(POST, uriInfo, engineApi);
        }

        return sb.toString().trim();
      });

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
    try {
      SzApiServer server = SzApiServer.getInstance();
      dataSourceCode = dataSourceCode.toUpperCase();

      final String dataSource = dataSourceCode;

      final String normalizedLoadId = normalizeString(loadId);

      Map<String,String> map = new HashMap<>();
      map.put("DATA_SOURCE", dataSource);
      map.put("RECORD_ID", recordId);

      String recordText = ensureJsonFields(PUT,
                                           uriInfo,
                                           recordJsonData,
                                           map);

      Set<String> dataSources = server.getDataSources();

      if (!dataSources.contains(dataSource)) {
        throw newNotFoundException(
            POST, uriInfo,
            "The specified data source is not recognized: " + dataSource);
      }

      server.executeInThread(() -> {
        // get the engine API
        G2Engine engineApi = server.getEngineApi();

        int result = engineApi.addRecord(dataSource,
                                         recordId,
                                         recordText,
                                         normalizedLoadId);
        if (result != 0) {
          throw newWebApplicationException(PUT, uriInfo, engineApi);
        }

        return recordId;
      });

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
  }

  @GET
  @Path("data-sources/{dataSourceCode}/records/{recordId}")
  public SzRecordResponse getRecord(
      @PathParam("dataSourceCode")                  String  dataSourceCode,
      @PathParam("recordId")                        String  recordId,
      @DefaultValue("false") @QueryParam("withRaw") boolean withRaw,
      @Context                                      UriInfo uriInfo)
  {
    try {
      SzApiServer server = SzApiServer.getInstance();
      dataSourceCode = dataSourceCode.toUpperCase();

      StringBuffer sb = new StringBuffer();

      final String dataSource = dataSourceCode;

      String rawData = server.executeInThread(() -> {
        // get the engine API
        G2Engine engineApi = server.getEngineApi();

        int result = engineApi.getRecord(dataSource, recordId, sb);
        if (result != 0) {
          throw newWebApplicationException(GET, uriInfo, engineApi);
        }

        return sb.toString();
      });

      // parse the raw data
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
  }

  @GET
  @Path("data-sources/{dataSourceCode}/records/{recordId}/entity")
  public SzEntityResponse geEntityByRecordId(
      @PathParam("dataSourceCode")                                String              dataSourceCode,
      @PathParam("recordId")                                      String              recordId,
      @DefaultValue("false") @QueryParam("withRaw")               boolean             withRaw,
      @DefaultValue("false") @QueryParam("withRelated")           boolean             withRelated,
      @DefaultValue("false") @QueryParam("forceMinimal")          boolean             forceMinimal,
      @DefaultValue("WITH_DUPLICATES") @QueryParam("featureMode") SzFeatureInclusion  featureMode,
      @Context                                                    UriInfo             uriInfo)
  {
    try {
      SzApiServer server = SzApiServer.getInstance();
      dataSourceCode = dataSourceCode.toUpperCase();

      final String dataSource = dataSourceCode;

      StringBuffer sb = new StringBuffer();

      SzEntityData entityData = null;

      int flags = getFlags(forceMinimal, featureMode);

      String rawData = null;

      // check if we want 1-degree relations as well -- if so we need to
      // find the network instead of a simple lookup
      if (withRelated && !forceMinimal) {
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

        rawData = server.executeInThread(() -> {
          // get the engine API and the config API
          G2Engine engineApi = server.getEngineApi();

          // find the network and check the result
          int result = engineApi.findNetworkByRecordIDV2(
              recordIds, maxDegrees, buildOutDegrees, maxEntityCount, flags, sb);

          if (result != 0) {
            throw newWebApplicationException(GET, uriInfo, engineApi);
          }

          return sb.toString();
        });

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
        rawData = server.executeInThread(() -> {
          // get the engine API and the config API
          G2Engine engineApi = server.getEngineApi();

          // 1-degree relations are not required, so do a standard lookup
          int result = engineApi.getEntityByRecordIDV2(dataSource, recordId, flags, sb);

          String engineJSON = sb.toString();
          checkEntityResult(result, engineJSON, uriInfo, engineApi);

          return engineJSON;
        });

        // parse the result
        entityData = SzEntityData.parseEntityData(
            null,
            JsonUtils.parseJsonObject(rawData),
            (f) -> server.getAttributeClassForFeature(f));
      }

      postProcessEntityData(entityData, forceMinimal, featureMode);

      // construct the response
      return newEntityResponse(
          uriInfo, entityData, (withRaw ? rawData : null));

    } catch (WebApplicationException e) {
      throw e;

    } catch (Exception e) {
      throw ServicesUtil.newInternalServerErrorException(GET, uriInfo, e);
    }
  }

  @GET
  @Path("entities/{entityId}")
  public SzEntityResponse getEntityByEntityId(
      @PathParam("entityId")                                      long                entityId,
      @DefaultValue("false") @QueryParam("withRaw")               boolean             withRaw,
      @DefaultValue("false") @QueryParam("withRelated")           boolean             withRelated,
      @DefaultValue("false") @QueryParam("forceMinimal")          boolean             forceMinimal,
      @DefaultValue("WITH_DUPLICATES") @QueryParam("featureMode") SzFeatureInclusion  featureMode,
      @Context                                                    UriInfo             uriInfo)
  {
    try {
      SzApiServer server = SzApiServer.getInstance();

      StringBuffer sb = new StringBuffer();

      SzEntityData entityData = null;

      String rawData = null;

      int flags = getFlags(forceMinimal, featureMode);

      // check if we want 1-degree relations as well -- if so we need to
      // find the network instead of a simple lookup
      if (withRelated && !forceMinimal) {
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

        rawData = server.executeInThread(() -> {
          // get the engine API
          G2Engine engineApi = server.getEngineApi();

          // find the network and check the result
          int result = engineApi.findNetworkByEntityIDV2(
              entityIds, maxDegrees, buildOutDegrees, maxEntityCount, flags, sb);

          if (result != 0) {
            throw newWebApplicationException(GET, uriInfo, engineApi);
          }
          return sb.toString();
        });

        // organize all the entities into a map for lookup
        Map<Long, SzEntityData> dataMap
            = parseEntityDataList(rawData, server);

        // get the result entity data
        entityData = getAugmentedEntityData(entityId, dataMap, server);

      } else {
        rawData = server.executeInThread(() -> {
          // get the engine API
          G2Engine engineApi = server.getEngineApi();

          // 1-degree relations are not required, so do a standard lookup
          int result = engineApi.getEntityByEntityIDV2(entityId, flags, sb);

          String engineJSON = sb.toString();

          checkEntityResult(result, engineJSON, uriInfo, engineApi);

          return engineJSON;
        });

        // parse the result
        entityData = SzEntityData.parseEntityData(
            null,
            JsonUtils.parseJsonObject(rawData),
            (f) -> server.getAttributeClassForFeature(f));
      }

      postProcessEntityData(entityData, forceMinimal, featureMode);

      // construct the response
      return newEntityResponse(
          uriInfo, entityData, (withRaw ? sb.toString() : null));

    } catch (WebApplicationException e) {
      throw e;

    } catch (Exception e) {
      throw ServicesUtil.newInternalServerErrorException(GET, uriInfo, e);
    }
  }

  @GET
  @Path("entities")
  public SzAttributeSearchResponse searchByAttributes(
      @QueryParam("attrs")                                        String              attrs,
      @DefaultValue("false") @QueryParam("forceMinimal")          boolean             forceMinimal,
      @DefaultValue("WITH_DUPLICATES") @QueryParam("featureMode") SzFeatureInclusion  featureMode,
      @DefaultValue("true") @QueryParam("withRelationships")      boolean             withRelationships,
      @DefaultValue("false") @QueryParam("withRaw")               boolean             withRaw,
      @Context                                                    UriInfo             uriInfo)
  {
    try {
      SzApiServer server = SzApiServer.getInstance();

      // check if no attributes
      if (attrs == null || attrs.trim().length() == 0) {
        // look for the "attr_" parameters
        MultivaluedMap<String,String> params= uriInfo.getQueryParameters(true);
        JsonObjectBuilder objBuilder = Json.createObjectBuilder();
        params.entrySet().forEach(e -> {
          String key = e.getKey().trim();
          if (!key.toLowerCase().startsWith("attr_")
              || key.length() <= ("attr_").length())
          {
            // skip this key since it is not of the expected format
            return;
          }
          String        jsonProp    = key.substring("attr_".length());
          List<String>  values      = e.getValue();
          String        firstValue  = values.get(0);
          JsonUtils.add(objBuilder, jsonProp, firstValue);
        });
        JsonObject jsonObject = null;
        try {
          jsonObject = objBuilder.build();
        } catch (Exception ignore) {
          // do nothing
        }
        if (jsonObject == null || jsonObject.size() == 0) {
          throw newBadRequestException(
              GET, uriInfo,
              "Parameter missing or empty: \"attrs\".  "
              + "Search criteria attributes are required.");
        }
        attrs = JsonUtils.toJsonText(jsonObject);
      }

      StringBuffer sb = new StringBuffer();

      int flags = getFlags(forceMinimal, featureMode, withRelationships);

      final String json = attrs;
      server.executeInThread(() -> {
        // get the engine API
        G2Engine engineApi = server.getEngineApi();

        int result = engineApi.searchByAttributesV2(json, flags, sb);
        if (result != 0) {
          throw newInternalServerErrorException(GET, uriInfo, engineApi);
        }
        return sb.toString();
      });

      JsonObject jsonObject = JsonUtils.parseJsonObject(sb.toString());
      JsonArray jsonResults = jsonObject.getValue(
        "/RESOLVED_ENTITIES").asJsonArray();

      // parse the result
      List<SzAttributeSearchResult> list
          = SzAttributeSearchResult.parseSearchResultList(
            null,
              jsonResults,
              (f) -> server.getAttributeClassForFeature(f));


      postProcessSearchResults(
          list, forceMinimal, featureMode, withRelationships);

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

  /**
   * Post-processes the search results according to the specified parameters.
   *
   * @param searchResults The {@link List} of {@link SzAttributeSearchResult}
   *                      to modify.
   *
   * @param forceMinimal Whether or not minimal format is forced.
   *
   * @param featureMode The {@link SzFeatureInclusion} describing how features
   *                    are retrieved.
   *
   * @param withRelationships Whether or not to include relationships.
   */
  private static void postProcessSearchResults(
      List<SzAttributeSearchResult>   searchResults,
      boolean                         forceMinimal,
      SzFeatureInclusion              featureMode,
      boolean                         withRelationships)
  {
    // check if we need to strip out duplicate features
    if (featureMode == REPRESENTATIVE) {
      stripDuplicateFeatureValues(searchResults);
    }

    // check if fields are going to be null if they would otherwise be set
    if (featureMode == NONE || forceMinimal) {
      setEntitiesPartial(searchResults);
    }
  }

  /**
   * Sets the partial flags for the resolved entity and related
   * entities in the {@link SzEntityData}.
   */
  private static void setEntitiesPartial(
      List<SzAttributeSearchResult> searchResults)
  {
    searchResults.forEach(e -> {
      e.setPartial(true);

      e.getRelatedEntities().forEach(e2 -> {
        e2.setPartial(true);
      });
    });
  }

  /**
   * Strips out duplicate feature values for each feature in the search
   * result entities of the specified {@link List} of {@link
   * SzAttributeSearchResult} instances.
   */
  private static void stripDuplicateFeatureValues(
      List<SzAttributeSearchResult> searchResults)
  {
    searchResults.forEach(e -> {
      ServicesUtil.stripDuplicateFeatureValues(e);

      e.getRelatedEntities().forEach(e2 -> {
        ServicesUtil.stripDuplicateFeatureValues(e2);
      });
    });
  }
}
