package com.senzing.api.services;

import com.senzing.api.model.*;
import com.senzing.g2.engine.G2Engine;
import com.senzing.util.JsonUtils;
import com.senzing.util.Timers;

import javax.json.*;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;

import java.util.*;

import static com.senzing.api.model.SzHttpMethod.*;
import static com.senzing.api.model.SzFeatureInclusion.*;
import static com.senzing.api.services.ServicesUtil.*;
import static com.senzing.g2.engine.G2Engine.*;

/**
 * Provides "why" API services.
 */
@Path("/")
@Produces("application/json; charset=UTF-8")
public class WhyServices {
  private static final int DATA_SOURCE_NOT_FOUND_CODE = 27;

  private static final int RECORD_NOT_FOUND_CODE = 33;

  private static final int ENTITY_ID_NOT_FOUND_CODE = 37;

  @GET
  @Path("data-sources/{dataSourceCode}/records/{recordId}/entity/why")
  public SzWhyEntityResponse whyEntityByRecordId(
      @PathParam("dataSourceCode")                                String              dataSourceCode,
      @PathParam("recordId")                                      String              recordId,
      @DefaultValue("false") @QueryParam("forceMinimal")          boolean             forceMinimal,
      @DefaultValue("WITH_DUPLICATES") @QueryParam("featureMode") SzFeatureInclusion  featureMode,
      @DefaultValue("true") @QueryParam("withFeatureStats")       boolean             withFeatureStats,
      @DefaultValue("true") @QueryParam("withDerivedFeatures")    boolean             withDerivedFeatures,
      @DefaultValue("false") @QueryParam("withRelationships")     boolean             withRelationships,
      @DefaultValue("false") @QueryParam("withRaw")               boolean             withRaw,
      @Context                                                    UriInfo             uriInfo)
  {
    Timers timers = newTimers();

    try {
      SzApiProvider provider = SzApiProvider.Factory.getProvider();
      dataSourceCode = dataSourceCode.toUpperCase();

      final String dataSource = dataSourceCode;

      StringBuffer sb = new StringBuffer();

      String rawData = null;

      int flags = getFlags(forceMinimal,
                           featureMode,
                           withFeatureStats,
                           withDerivedFeatures,
                           withRelationships);

      enteringQueue(timers);
      rawData = provider.executeInThread(() -> {
        exitingQueue(timers);

        // get the engine API and the config API
        G2Engine engineApi = provider.getEngineApi();

        callingNativeAPI(timers, "engine", "whyEntityByRecordID");

        // perform the "why" operation and check the result
        int result = engineApi.whyEntityByRecordIDV2(
            dataSource, recordId, flags, sb);

        calledNativeAPI(timers, "engine", "whyEntityByRecordID");

        if (result != 0) {
          throw newWebApplicationException(GET, uriInfo, timers, engineApi);
        }

        return sb.toString();
      });

      return createWhyEntityResponse(rawData,
                                     timers,
                                     uriInfo,
                                     withRaw,
                                     provider);

    } catch (ServerErrorException e) {
      e.printStackTrace();
      throw e;

    } catch (WebApplicationException e) {
      throw e;

    } catch (Exception e) {
      e.printStackTrace();
      throw ServicesUtil.newInternalServerErrorException(GET, uriInfo, timers, e);
    }
  }

  @GET
  @Path("entities/{entityId}/why")
  public SzWhyEntityResponse whyEntityByEntityId(
      @PathParam("entityId")                                      long                entityId,
      @DefaultValue("false") @QueryParam("withRelationships")     boolean             withRelationships,
      @DefaultValue("true") @QueryParam("withFeatureStats")       boolean             withFeatureStats,
      @DefaultValue("true") @QueryParam("withDerivedFeatures")    boolean             withDerivedFeatures,
      @DefaultValue("false") @QueryParam("forceMinimal")          boolean             forceMinimal,
      @DefaultValue("WITH_DUPLICATES") @QueryParam("featureMode") SzFeatureInclusion  featureMode,
      @DefaultValue("false") @QueryParam("withRaw")               boolean             withRaw,
      @Context                                                    UriInfo             uriInfo)
  {
    Timers timers = newTimers();

    try {
      SzApiProvider provider = SzApiProvider.Factory.getProvider();

      StringBuffer sb = new StringBuffer();

      String rawData = null;

      int flags = getFlags(forceMinimal,
                           featureMode,
                           withFeatureStats,
                           withDerivedFeatures,
                           withRelationships);

      enteringQueue(timers);
      rawData = provider.executeInThread(() -> {
        exitingQueue(timers);

        // get the engine API and the config API
        G2Engine engineApi = provider.getEngineApi();

        callingNativeAPI(timers, "engine", "whyEntityByEntityID");

        // perform the "why" operation and check the result
        int result = engineApi.whyEntityByEntityIDV2(entityId, flags, sb);

        calledNativeAPI(timers, "engine", "whyEntityByEntityID");

        if (result != 0) {
          throw newWebApplicationException(GET, uriInfo, timers, engineApi);
        }

        return sb.toString();
      });

      return createWhyEntityResponse(rawData,
                                     timers,
                                     uriInfo,
                                     withRaw,
                                     provider);

    } catch (ServerErrorException e) {
      e.printStackTrace();
      throw e;

    } catch (WebApplicationException e) {
      throw e;

    } catch (Exception e) {
      e.printStackTrace();
      throw ServicesUtil.newInternalServerErrorException(GET, uriInfo, timers, e);
    }
  }

  @GET
  @Path("why/records")
  public SzWhyRecordsResponse whyRecords(
      @QueryParam("dataSource1")                                  String              dataSourceCode1,
      @QueryParam("recordId1")                                    String              recordId1,
      @QueryParam("dataSource2")                                  String              dataSourceCode2,
      @QueryParam("recordId2")                                    String              recordId2,
      @DefaultValue("false") @QueryParam("forceMinimal")          boolean             forceMinimal,
      @DefaultValue("WITH_DUPLICATES") @QueryParam("featureMode") SzFeatureInclusion  featureMode,
      @DefaultValue("true") @QueryParam("withFeatureStats")       boolean             withFeatureStats,
      @DefaultValue("true") @QueryParam("withDerivedFeatures")    boolean             withDerivedFeatures,
      @DefaultValue("false") @QueryParam("withRelationships")     boolean             withRelationships,
      @DefaultValue("false") @QueryParam("withRaw")               boolean             withRaw,
      @Context                                                    UriInfo             uriInfo)
  {
    Timers timers = newTimers();

    try {
      SzApiProvider provider = SzApiProvider.Factory.getProvider();

      // check the parameters
      if (dataSourceCode1 == null || dataSourceCode1.trim().length() == 0) {
        throw newBadRequestException(
            GET, uriInfo, timers, "The dataSourceCode1 parameter is required.");
      }
      if (recordId1 == null || recordId1.trim().length() == 0) {
        throw newBadRequestException(
            GET, uriInfo, timers, "The recordId1 parameter is required.");
      }
      if (dataSourceCode2 == null || dataSourceCode2.trim().length() == 0) {
        throw newBadRequestException(
            GET, uriInfo, timers, "The dataSourceCode2 parameter is required.");
      }
      if (recordId2 == null || recordId2.trim().length() == 0) {
        throw newBadRequestException(
            GET, uriInfo, timers, "The recordId2 parameter is required.");
      }

      // normalize the data source parameters
      dataSourceCode1 = dataSourceCode1.trim().toUpperCase();
      dataSourceCode2 = dataSourceCode2.trim().toUpperCase();

      final String dataSource1 = dataSourceCode1;
      final String dataSource2 = dataSourceCode2;

      StringBuffer sb = new StringBuffer();

      String rawData = null;

      int flags = getFlags(forceMinimal,
                           featureMode,
                           withFeatureStats,
                           withDerivedFeatures,
                           withRelationships);

      enteringQueue(timers);
      rawData = provider.executeInThread(() -> {
        exitingQueue(timers);

        // get the engine API and the config API
        G2Engine engineApi = provider.getEngineApi();

        callingNativeAPI(timers, "engine", "whyRecords");

        // perform the "why" operation
        int result = engineApi.whyRecordsV2(
            dataSource1, recordId1, dataSource2, recordId2, flags, sb);

        calledNativeAPI(timers, "engine", "whyRecords");

        if (result != 0) {
          int errorCode = engineApi.getLastExceptionCode();
          if (errorCode == DATA_SOURCE_NOT_FOUND_CODE
              || errorCode == RECORD_NOT_FOUND_CODE)
          {
            throw newBadRequestException(GET, uriInfo, timers, engineApi);
          }
          throw newInternalServerErrorException(
              GET, uriInfo, timers, engineApi);
        }

        return sb.toString();
      });

      processingRawData(timers);
      // parse the result
      JsonObject  json        = JsonUtils.parseJsonObject(rawData);
      JsonArray   whyArray    = json.getJsonArray("WHY_RESULTS");
      JsonArray   entityArray = json.getJsonArray("ENTITIES");

      List<SzWhyRecordsResult> whyResults
          = SzWhyRecordsResult.parseWhyRecordsResultList(null, whyArray);

      if (whyResults.size() != 1) {
        throw new IllegalStateException(
            "Unexpected number of why results (" + whyResults.size()
            + ") for whyRecords() operation: dataSource1=[ " + dataSource1
            + " ], recordId1=[ " + recordId1 + " ], dataSource2=[ "
            + dataSource2 + " ], recordId2=[ " + recordId2 + " ]");
      }

      List<SzEntityData> entities = SzEntityData.parseEntityDataList(
          null, entityArray, (f) -> provider.getAttributeClassForFeature(f));
      processedRawData(timers);

      // construct the response
      SzWhyRecordsResponse response = new SzWhyRecordsResponse(
          GET, 200, uriInfo, timers);

      response.setWhyResult(whyResults.get(0));
      response.setEntities(entities);

      if (withRaw) {
        response.setRawData(rawData);
      }

      return response;

    } catch (ServerErrorException e) {
      e.printStackTrace();
      throw e;

    } catch (WebApplicationException e) {
      throw e;

    } catch (Exception e) {
      e.printStackTrace();
      throw ServicesUtil.newInternalServerErrorException(GET, uriInfo, timers, e);
    }
  }

  private static WebApplicationException newWebApplicationException(
      SzHttpMethod  httpMethod,
      UriInfo       uriInfo,
      Timers        timers,
      G2Engine      engineApi)
  {
    int errorCode = engineApi.getLastExceptionCode();
    if (errorCode == DATA_SOURCE_NOT_FOUND_CODE
        || errorCode == RECORD_NOT_FOUND_CODE
        || errorCode == ENTITY_ID_NOT_FOUND_CODE)
    {
      return newNotFoundException(httpMethod, uriInfo, timers, engineApi);
    }
    return newInternalServerErrorException(
        httpMethod, uriInfo, timers, engineApi);
  }

  private static SzWhyEntityResponse createWhyEntityResponse(
      String        rawData,
      Timers        timers,
      UriInfo       uriInfo,
      boolean       withRaw,
      SzApiProvider provider)
  {
    processingRawData(timers);
    // parse the result
    JsonObject  json        = JsonUtils.parseJsonObject(rawData);
    JsonArray   whyArray    = json.getJsonArray("WHY_RESULTS");
    JsonArray   entityArray = json.getJsonArray("ENTITIES");

    List<SzWhyEntityResult> whyResults
        = SzWhyEntityResult.parseWhyEntityResultList(null, whyArray);

    List<SzEntityData> entities = SzEntityData.parseEntityDataList(
        null, entityArray, (f) -> provider.getAttributeClassForFeature(f));
    processedRawData(timers);

    // construct the response
    SzWhyEntityResponse response = new SzWhyEntityResponse(
        GET, 200, uriInfo, timers);

    response.setWhyResults(whyResults);
    response.setEntities(entities);

    if (withRaw) {
      response.setRawData(rawData);
    }

    return response;

  }
}
