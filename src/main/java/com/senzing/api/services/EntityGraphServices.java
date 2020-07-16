package com.senzing.api.services;

import com.senzing.api.model.*;
import com.senzing.g2.engine.G2Engine;
import com.senzing.util.JsonUtils;
import com.senzing.util.Timers;

import javax.json.*;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriInfo;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static com.senzing.api.model.SzHttpMethod.GET;
import static com.senzing.api.services.ServicesUtil.*;
import static com.senzing.g2.engine.G2Engine.*;

/**
 * Provides entity graph related API services.
 */
@Path("/")
@Produces("application/json; charset=UTF-8")
public class EntityGraphServices {
  private static final int ENTITY_NOT_FOUND_CODE = 37;

  private static final int RECORD_NOT_FOUND_CODE = 33;

  @GET
  @Path("entity-paths")
  public SzEntityPathResponse getEntityPath(
      @QueryParam("from")                                         String              fromParam,
      @QueryParam("to")                                           String              toParam,
      @DefaultValue("3") @QueryParam("maxDegrees")                int                 maxDegrees,
      @QueryParam("x")                                            List<String>        avoidParam,
      @QueryParam("avoidEntities")                                String              avoidList,
      @DefaultValue("false") @QueryParam("forbidAvoided")         boolean             forbidAvoided,
      @QueryParam("s")                                            List<String>        sourcesParam,
      @DefaultValue("false") @QueryParam("forceMinimal")          boolean             forceMinimal,
      @DefaultValue("WITH_DUPLICATES") @QueryParam("featureMode") SzFeatureMode featureMode,
      @DefaultValue("false") @QueryParam("withFeatureStats")      boolean             withFeatureStats,
      @DefaultValue("false") @QueryParam("withInternalFeatures")   boolean             withInternalFeatures,
      @DefaultValue("false") @QueryParam("withRaw")               boolean             withRaw,
      @Context                                                    UriInfo             uriInfo)
  {
    Timers timers = newTimers();
    SzApiProvider provider = SzApiProvider.Factory.getProvider();

    SzEntityIdentifier        from;
    SzEntityIdentifier        to;
    Set<SzEntityIdentifier>   avoidEntities = null;
    List<String>              withSources   = null;
    try {
      if (fromParam == null) {
        throw newBadRequestException(
            GET, uriInfo, timers,
            "Parameter missing or empty: \"from\".  "
                + "The 'from' entity identifier is required.");
      }
      if (toParam == null) {
        throw newBadRequestException(
            GET, uriInfo, timers,
            "Parameter missing or empty: \"to\".  "
                + "The 'to' entity identifier is required.");
      }

      try {
        from = SzEntityIdentifier.valueOf(fromParam.trim());
      } catch (Exception e) {
        throw newBadRequestException(
            GET, uriInfo, timers,
            "Parameter is not formatted correctly: \"from\".");
      }

      try {
        to = SzEntityIdentifier.valueOf(toParam.trim());
      } catch (Exception e) {
        throw newBadRequestException(
            GET, uriInfo, timers,
            "Parameter is not formatted correctly: \"to\".");
      }

      // check for consistent from/to
      if (from.getClass() != to.getClass()) {
        throw newBadRequestException(
            GET, uriInfo, timers,
            "Entity identifiers must be consistent types.  from=" + from
                + ", to=" + to);
      }

      // check if we have entities to avoid (or forbid)
      if ((avoidParam != null && avoidParam.size() > 0)
          || (avoidList != null && avoidList.trim().length() > 0))
      {
        // parse the multi-valued parameters
        avoidEntities = parseEntityIdentifiers(
            avoidParam, "avoidEntities", GET, uriInfo, timers);

        // check if the avoid list is specified
        if (avoidList != null && avoidList.trim().length() > 0) {
          SzEntityIdentifiers ids = SzEntityIdentifiers.valueOf(avoidList);

          avoidEntities.addAll(ids.getIdentifiers());
        }

        if (!checkConsistent(avoidEntities)) {
          throw newBadRequestException(
              GET, uriInfo, timers,
              "Entity identifiers for avoided entities must be of "
                  + "consistent types: " + avoidEntities);
        }
      }

      if (avoidEntities == null || avoidEntities.size() == 0) {
        forbidAvoided = false;
      }

      if (sourcesParam != null && sourcesParam.size() > 0) {
        Set<String> dataSources = provider.getDataSources();
        withSources = new ArrayList<>(dataSources.size());

        for (String source : sourcesParam) {
          if (dataSources.contains(source)) {
            withSources.add(source);
          } else {
            throw newBadRequestException(
                GET, uriInfo, timers,
                "Unrecognized data source: " + source);
          }
        }
      }
      if (maxDegrees < 1) {
        throw newBadRequestException(
            GET, uriInfo, timers,
            "Max degrees must be greater than zero: " + maxDegrees);
      }
    } catch (WebApplicationException e) {
      throw e;
    } catch (Exception e) {
      throw newBadRequestException(GET, uriInfo, timers, e.getMessage());
    }

    final String encodedAvoid = (avoidEntities == null)
        ? null : nativeJsonEncodeEntityIds(avoidEntities);

    final List<String> originalSources = withSources;

    final String encodedSources = (withSources == null)
        ? null : nativeJsonEncodeDataSources(withSources);

    final int flags = (forbidAvoided ? 0 : G2_FIND_PATH_PREFER_EXCLUDE)
                    | getFlags(forceMinimal,
                               featureMode,
                               withFeatureStats,
                               withInternalFeatures,
                               true);

    try {
      enteringQueue(timers);
      String rawData = provider.executeInThread(() -> {
        exitingQueue(timers);

        // get the engine API and the config API
        G2Engine engineApi = provider.getEngineApi();

        StringBuffer responseDataBuffer = new StringBuffer();

        int result;
        if (from.getClass() == SzRecordId.class) {
          String source1 = ((SzRecordId) from).getDataSourceCode();
          String source2 = ((SzRecordId) to).getDataSourceCode();
          String id1 = ((SzRecordId) from).getRecordId();
          String id2 = ((SzRecordId) to).getRecordId();

          if (encodedAvoid == null && encodedSources == null) {
            callingNativeAPI(timers, "engine", "findPathByRecordIDV2");
            result = engineApi.findPathByRecordIDV2(source1,
                                                    id1,
                                                    source2,
                                                    id2,
                                                    maxDegrees,
                                                    flags,
                                                    responseDataBuffer);
            calledNativeAPI(timers, "engine", "findPathByRecordIDV2");

          } else if (encodedSources == null) {
            callingNativeAPI(timers, "engine", "findPathExcludingByRecordID");
            result = engineApi.findPathExcludingByRecordID(
                source1,
                id1,
                source2,
                id2,
                maxDegrees,
                (encodedAvoid != null ? encodedAvoid
                    : nativeJsonEncodeEntityIds(Collections.emptyList())),
                flags,
                responseDataBuffer);
            calledNativeAPI(timers, "engine", "findPathExcludingByRecordID");

          } else {
            callingNativeAPI(timers, "engine", "findPathIncludingSourceByRecordID");
            result = engineApi.findPathIncludingSourceByRecordID(
                source1,
                id1,
                source2,
                id2,
                maxDegrees,
                (encodedAvoid != null ? encodedAvoid
                    : nativeJsonEncodeEntityIds(Collections.emptyList())),
                (encodedSources != null ? encodedSources
                    : nativeJsonEncodeDataSources(Collections.emptyList())),
                flags,
                responseDataBuffer);
            calledNativeAPI(timers, "engine", "findPathIncludingSourceByRecordID");
          }
        } else {
          SzEntityId id1 = (SzEntityId) from;
          SzEntityId id2 = (SzEntityId) to;

          if (encodedAvoid == null && encodedSources == null) {
            callingNativeAPI(timers, "engine", "findPathByEntityIDV2");
            result = engineApi.findPathByEntityIDV2(id1.getValue(),
                                                  id2.getValue(),
                                                  maxDegrees,
                                                  flags,
                                                  responseDataBuffer);
            calledNativeAPI(timers, "engine", "findPathByEntityIDV2");

          } else if (encodedSources == null) {
            callingNativeAPI(timers, "engine", "findPathExcludingByEntityID");
            result = engineApi.findPathExcludingByEntityID(
                id1.getValue(),
                id2.getValue(),
                maxDegrees,
                (encodedAvoid != null ? encodedAvoid
                    : nativeJsonEncodeEntityIds(Collections.emptyList())),
                flags,
                responseDataBuffer);
            calledNativeAPI(timers, "engine", "findPathExcludingByEntityID");

          } else {
            callingNativeAPI(timers, "engine", "findPathIncludingSourceByEntityID");
            result = engineApi.findPathIncludingSourceByEntityID(
                id1.getValue(),
                id2.getValue(),
                maxDegrees,
                (encodedAvoid != null ? encodedAvoid
                    : nativeJsonEncodeEntityIds(Collections.emptyList())),
                (encodedSources != null ? encodedSources
                    : nativeJsonEncodeDataSources(Collections.emptyList())),
                flags,
                responseDataBuffer);
            calledNativeAPI(timers, "engine", "findPathIncludingSourceByEntityID");

          }
        }

        if (result != 0) {
          System.err.println("********* SOURCES: " + originalSources);
          System.err.println("********* ENCODED SOURCES: " + encodedSources);
          throw newWebApplicationException(GET, uriInfo, timers, engineApi);
        }

        // parse the raw data
        return responseDataBuffer.toString();
      });

      processingRawData(timers);
      JsonObject jsonObject = JsonUtils.parseJsonObject(rawData);
      SzEntityPathData entityPathData
          = SzEntityPathData.parseEntityPathData(
              jsonObject,
              provider::getAttributeClassForFeature);

      entityPathData.getEntities().forEach(e -> {
        postProcessEntityData(e, forceMinimal, featureMode);
      });

      processedRawData(timers);

      // construct the response
      SzEntityPathResponse response
          = new SzEntityPathResponse(GET,
                                     200,
                                     uriInfo,
                                     timers,
                                     entityPathData);

      // if including raw data then add it
      if (withRaw) response.setRawData(rawData);

      // return the response
      return response;

    } catch (WebApplicationException e) {
      throw e;

    } catch (Exception e) {
      throw ServicesUtil.newInternalServerErrorException(GET, uriInfo, timers, e);
    }
  }

  @GET
  @Path("entity-networks")
  public SzEntityNetworkResponse getEntityNetwork(
      @QueryParam("e")        List<String>  entitiesParam,
      @QueryParam("entities") String        entityList,
      @DefaultValue("3")      @QueryParam("maxDegrees")           int                 maxDegrees,
      @DefaultValue("1")      @QueryParam("buildOut")             int                 buildOut,
      @DefaultValue("1000")   @QueryParam("maxEntities")          int                 maxEntities,
      @DefaultValue("false")  @QueryParam("forceMinimal")         boolean             forceMinimal,
      @DefaultValue("WITH_DUPLICATES") @QueryParam("featureMode") SzFeatureMode featureMode,
      @DefaultValue("false") @QueryParam("withFeatureStats")      boolean             withFeatureStats,
      @DefaultValue("false") @QueryParam("withInternalFeatures")   boolean             withInternalFeatures,
      @DefaultValue("false")  @QueryParam("withRaw")              boolean             withRaw,
      @Context                                                    UriInfo             uriInfo)
  {
    Timers timers = newTimers();
    SzApiProvider provider = SzApiProvider.Factory.getProvider();

    Set<SzEntityIdentifier> entities;
    // check for consistent entity IDs
    try {
      if ((entitiesParam == null || entitiesParam.isEmpty())
          && ((entityList == null) || entityList.isEmpty()))
      {
        throw newBadRequestException(
            GET, uriInfo, timers,
            "One of the following parameters is required to specify at least "
                + "one entity: 'e' or 'entities'.  "
                + "At least one of 'e' or 'entities' parameter must specify at "
                + "least one entity identifier.");
      }

      entities = parseEntityIdentifiers(
          entitiesParam, "e", GET, uriInfo, timers);

      if (entityList != null && entityList.trim().length() > 0) {
        SzEntityIdentifiers ids = SzEntityIdentifiers.valueOf(entityList);

        entities.addAll(ids.getIdentifiers());
      }


      if (!checkConsistent(entities)) {
        throw newBadRequestException(
            GET, uriInfo, timers,
            "Entity identifiers for entities must be of consistent "
                + "types: " + entities);
      }

      if (maxDegrees < 1) {
        throw newBadRequestException(
            GET, uriInfo, timers,
            "Max degrees must be greater than zero: " + maxDegrees);
      }

      if (buildOut < 0) {
        throw newBadRequestException(
            GET, uriInfo, timers,
            "Build out must be zero or greater: " + buildOut);
      }

      if (maxEntities < 0) {
        throw newBadRequestException(
            GET, uriInfo, timers,
            "Max entities must be zero or greater: " + maxEntities);
      }

    } catch (WebApplicationException e) {
      throw e;
    } catch (Exception e) {
      e.printStackTrace();
      throw newBadRequestException(GET, uriInfo, timers, e.getMessage());
    }

    final String encodedEntityIds = (entities == null)
        ? null : nativeJsonEncodeEntityIds(entities);

    final int flags = getFlags(forceMinimal,
                               featureMode,
                               withFeatureStats,
                               withInternalFeatures,
                               true);
    try {
      enteringQueue(timers);
      String rawData = provider.executeInThread(() -> {
        exitingQueue(timers);

        // get the engine API and the config API
        G2Engine engineApi = provider.getEngineApi();

        StringBuffer sb = new StringBuffer();

        int result;

        if (entities.iterator().next().getClass() == SzRecordId.class) {
          callingNativeAPI(timers, "engine", "findNetworkByRecordIDV2");
          result = engineApi.findNetworkByRecordIDV2(
              encodedEntityIds,
              maxDegrees,
              buildOut,
              maxEntities,
              flags,
              sb);
          calledNativeAPI(timers, "engine", "findNetworkByRecordIDV2");

        } else {
          callingNativeAPI(timers, "engine", "findNetworkByEntityIDV2");
          result = engineApi.findNetworkByEntityIDV2(
              encodedEntityIds,
              maxDegrees,
              buildOut,
              maxEntities,
              flags,
              sb);
          calledNativeAPI(timers, "engine", "findNetworkByEntityIDV2");
        }

        if (result != 0) {
          throw newWebApplicationException(GET, uriInfo, timers, engineApi);
        }

        // parse the raw data
        return sb.toString();
      });

      processingRawData(timers);

      JsonObject jsonObject = JsonUtils.parseJsonObject(rawData);

      SzEntityNetworkData entityNetworkData
          = SzEntityNetworkData.parseEntityNetworkData(
              jsonObject,
              provider::getAttributeClassForFeature);

      entityNetworkData.getEntities().forEach(e -> {
        postProcessEntityData(e, forceMinimal, featureMode);
      });

      processedRawData(timers);

      // construct the response
      SzEntityNetworkResponse response
          = new SzEntityNetworkResponse(GET,
                                        200,
                                        uriInfo,
                                        timers,
                                        entityNetworkData);

      // if including raw data then add it
      if (withRaw) response.setRawData(rawData);

      // return the response
      return response;

    } catch (WebApplicationException e) {
      throw e;

    } catch (Exception e) {
      throw ServicesUtil.newInternalServerErrorException(GET, uriInfo, timers, e);
    }
  }

  /**
   * Checks if the entity ID's in the specified list are of a consistent type.
   *
   * @param entities The list of {@link SzEntityIdentifier} instances.
   */
  private static boolean checkConsistent(Set<SzEntityIdentifier> entities) {
    if (entities != null && !entities.isEmpty()) {
      Class idClass = null;
      for (SzEntityIdentifier id : entities) {
        if (idClass == null) {
          idClass = id.getClass();
        } else if (idClass != id.getClass()) {
          return false;
        }
      }
    }
    return true;
  }

  private static WebApplicationException newWebApplicationException(
      SzHttpMethod  httpMethod,
      UriInfo       uriInfo,
      Timers        timers,
      G2Engine      engineApi)
  {
    int errorCode = engineApi.getLastExceptionCode();
    if (errorCode == ENTITY_NOT_FOUND_CODE
        || errorCode == RECORD_NOT_FOUND_CODE) {
      return newBadRequestException(GET, uriInfo, timers, engineApi);
    }
    return newInternalServerErrorException(GET, uriInfo, timers, engineApi);
  }
}
