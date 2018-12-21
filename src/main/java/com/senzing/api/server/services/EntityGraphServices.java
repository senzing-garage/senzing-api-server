package com.senzing.api.server.services;

import com.senzing.api.model.*;
import com.senzing.api.server.SzApiServer;
import com.senzing.g2.engine.G2Engine;
import com.senzing.util.JsonUtils;

import javax.json.*;
import javax.ws.rs.*;
import java.util.List;
import java.util.Set;

import static com.senzing.api.model.SzHttpMethod.GET;
import static com.senzing.api.server.services.ServicesUtil.*;
import static com.senzing.g2.engine.G2Engine.*;

/**
 * Provides entity graph related API services.
 */
@Path("/")
@Produces("application/json; charset=UTF-8")
public class EntityGraphServices {
  @GET
  @Path("entity-paths")
  public SzEntityPathResponse getEntityPath(
      @QueryParam("from") String fromParam,
      @QueryParam("to") String toParam,
      @DefaultValue("3") @QueryParam("maxDegrees") int maxDegrees,
      @QueryParam("x") List<String> avoidParam,
      @DefaultValue("false") @QueryParam("forbidAvoided") boolean forbidAvoided,
      @QueryParam("s") List<String> sourcesParam,
      @DefaultValue("false") @QueryParam("withRaw") boolean withRaw)
  {
    SzApiServer server = SzApiServer.getInstance();

    return server.executeInThread(() -> {

      String selfLink = server.makeLink("/entity-paths");
      selfLink += "?maxDegrees=" + maxDegrees;

      if (fromParam != null) {
        selfLink += "&from=" + urlEncode(fromParam.toString());
      }

      if (toParam != null) {
        selfLink += "&to=" + urlEncode(toParam.toString());
      }

      if (forbidAvoided) {
        selfLink += "&forbidAvoided=" + forbidAvoided;
      }

      if (avoidParam != null && avoidParam.size() > 0) {
        selfLink += formatMultiValuedParam("&","x", avoidParam);
      }

      if (sourcesParam != null && sourcesParam.size() > 0) {
        selfLink += formatMultiValuedParam("&","s", sourcesParam);
      }

      if (withRaw) {
        selfLink += "?withRaw=true";
      }

      SzEntityIdentifier        from          = null;
      SzEntityIdentifier        to            = null;
      List<SzEntityIdentifier>  avoidEntities = null;
      List<String>              withSources   = null;
      try {
        if (fromParam == null) {
          throw newBadRequestException(
              GET, selfLink,
              "Parameter missing or empty: \"from\".  "
                  + "The 'from' entity identifier is required.");
        }
        if (toParam == null) {
          throw newBadRequestException(
              GET, selfLink,
              "Parameter missing or empty: \"to\".  "
                  + "The 'to' entity identifier is required.");
        }

        try {
          from = SzEntityIdentifier.valueOf(fromParam.trim());
        } catch (Exception e) {
          throw newBadRequestException(
              GET, selfLink,
              "Parameter is not formatted correctly: \"from\".");
        }

        try {
          to = SzEntityIdentifier.valueOf(toParam.trim());
        } catch (Exception e) {
          throw newBadRequestException(
              GET, selfLink,
              "Parameter is not formatted correctly: \"to\".");
        }

        // check for consistent from/to
        if (from.getClass() != to.getClass()) {
          throw newBadRequestException(
              GET, selfLink,
              "Entity identifiers must be consistent types.  from=" + from
                  + ", to=" + to);
        }

        if (avoidParam != null && avoidParam.size() > 0) {
          avoidEntities = parseEntityIdentifiers(
              avoidParam, "avoidEntities", GET, selfLink);

          if (!checkConsistent(avoidEntities)) {
            throw newBadRequestException(
                GET, selfLink,
                "Entity identifiers for avoided entities must be of "
                    + "consistent types: " + avoidEntities);
          }
        }

        if (sourcesParam != null && sourcesParam.size() > 0) {
          Set<String> dataSources = server.getDataSources();

          for (String source : withSources) {
            if (!dataSources.contains(source)) {
              throw newBadRequestException(
                  GET, selfLink,
                  "Unrecognized data source: " + source);
            }
          }
        }
        if (maxDegrees < 1) {
          throw newBadRequestException(
              GET, selfLink,
              "Max degrees must be greater than zero: " + maxDegrees);
        }
      } catch (WebApplicationException e) {
        throw e;
      } catch (Exception e) {
        throw newBadRequestException(GET, selfLink, e.getMessage());
      }

      try {
        // get the engine API and the config API
        G2Engine engineApi = server.getEngineApi();

        StringBuffer sb = new StringBuffer();

        int result = 0;
        if (from.getClass() == SzRecordId.class) {
          String source1 = ((SzRecordId) from).getDataSourceCode();
          String source2 = ((SzRecordId) to).getDataSourceCode();
          String id1 = ((SzRecordId) from).getRecordId();
          String id2 = ((SzRecordId) to).getRecordId();

          if (avoidEntities == null && withSources == null) {
            result = engineApi.findPathByRecordID(source1,
                                                  id1,
                                                  source2,
                                                  id2,
                                                  maxDegrees,
                                                  sb);
          } else if (withSources == null) {
            result = engineApi.findPathExcludingByRecordID(
                source1,
                id1,
                source2,
                id2,
                maxDegrees,
                nativeJsonEncodeEntityIds(avoidEntities),
                (forbidAvoided ? 0 : G2_FIND_PATH_PREFER_EXCLUDE),
                sb);

          } else {
            result = engineApi.findPathIncludingSourceByRecordID(
                source1,
                id1,
                source2,
                id2,
                maxDegrees,
                nativeJsonEncodeEntityIds(avoidEntities),
                nativeJsonEncodeDataSources(withSources),
                (forbidAvoided ? 0 : G2_FIND_PATH_PREFER_EXCLUDE),
                sb);
          }
        } else {
          SzEntityId id1 = (SzEntityId) from;
          SzEntityId id2 = (SzEntityId) to;

          if (avoidEntities == null && withSources == null) {
            result = engineApi.findPathByEntityID(id1.getValue(),
                                                  id2.getValue(),
                                                  maxDegrees,
                                                  sb);
          } else if (withSources == null) {
            result = engineApi.findPathExcludingByEntityID(
                id1.getValue(),
                id2.getValue(),
                maxDegrees,
                nativeJsonEncodeEntityIds(avoidEntities),
                (forbidAvoided ? 0 : G2_FIND_PATH_PREFER_EXCLUDE),
                sb);

          } else {
            result = engineApi.findPathIncludingSourceByEntityID(
                id1.getValue(),
                id2.getValue(),
                maxDegrees,
                nativeJsonEncodeEntityIds(avoidEntities),
                nativeJsonEncodeDataSources(withSources),
                (forbidAvoided ? 0 : G2_FIND_PATH_PREFER_EXCLUDE),
                sb);
          }
        }

        if (result != 0) {
          throw newInternalServerErrorException(GET, selfLink, engineApi);
        }

        // parse the raw data
        String rawData = sb.toString();
        JsonObject jsonObject = JsonUtils.parseJsonObject(rawData);
        SzEntityPathData entityPathData
            = SzEntityPathData.parseEntityPathData(
                jsonObject,
                (f) -> server.getAttributeClassForFeature(f));

        // construct the response
        SzEntityPathResponse response
            = new SzEntityPathResponse(GET,
                                       200,
                                       selfLink,
                                       entityPathData);

        // if including raw data then add it
        if (withRaw) response.setRawData(rawData);

        // return the response
        return response;

      } catch (WebApplicationException e) {
        throw e;

      } catch (Exception e) {
        throw ServicesUtil.newInternalServerErrorException(GET, selfLink, e);
      }
    });
  }

  @GET
  @Path("entity-networks")
  public SzEntityNetworkResponse getEntityNetwork(
      @QueryParam("e") List<String> entitiesParam,
      @DefaultValue("5")      @QueryParam("maxDegrees") int     maxDegrees,
      @DefaultValue("1")      @QueryParam("buildOut")   int     buildOut,
      @DefaultValue("1000")   @QueryParam("buildOut")   int     maxEntities,
      @DefaultValue("false")  @QueryParam("withRaw")    boolean withRaw)
  {
    SzApiServer server = SzApiServer.getInstance();

    return server.executeInThread(() -> {
      String selfLink = server.makeLink("/entity-networks");
      selfLink += "?maxDegrees=" + maxDegrees;
      selfLink += "&buildOut=" + buildOut;
      selfLink += "&maxEntities=" + maxEntities;

      if (entitiesParam != null && entitiesParam.size() > 0) {
        selfLink += formatMultiValuedParam("&", "e", entitiesParam);
      }
      if (withRaw) {
        selfLink += "?withRaw=true";
      }

      List<SzEntityIdentifier> entities = null;
      // check for consistent entity IDs
      try {
        if (entitiesParam == null && entitiesParam.size() > 0) {
          throw newBadRequestException(
              GET, selfLink,
              "Parameter missing or empty: \"entities\".  "
                  + "One or more 'entities' entity identifiers are required.");
        }

        entities = parseEntityIdentifiers(
            entitiesParam, "e", GET, selfLink);

        if (!checkConsistent(entities)) {
          throw newBadRequestException(
              GET, selfLink,
              "Entity identifiers for entities must be of consistent "
              + "types: " + entities);
        }

        if (maxDegrees < 1) {
          throw newBadRequestException(
              GET, selfLink,
              "Max degrees must be greater than zero: " + maxDegrees);
        }

        if (buildOut < 1) {
          throw newBadRequestException(
              GET, selfLink,
              "Build out must be greater than zero: " + buildOut);
        }

        if (maxEntities < 1) {
          throw newBadRequestException(
              GET, selfLink,
              "Max entities must be greater than zero: " + maxEntities);
        }

      } catch (WebApplicationException e) {
        throw e;
      } catch (Exception e) {
        throw newBadRequestException(GET, selfLink, e.getMessage());
      }

      try {
        // get the engine API and the config API
        G2Engine engineApi = server.getEngineApi();

        StringBuffer sb = new StringBuffer();

        int result = 0;

        if (entities.iterator().next().getClass() == SzRecordId.class) {
          result = engineApi.findNetworkByRecordID(
              nativeJsonEncodeEntityIds(entities),
              maxDegrees,
              buildOut,
              maxEntities,
              sb);

        } else {
          result = engineApi.findNetworkByEntityID(
              nativeJsonEncodeEntityIds(entities),
              maxDegrees,
              buildOut,
              maxEntities,
              sb);
        }

        if (result != 0) {
          throw newInternalServerErrorException(GET, selfLink, engineApi);
        }

        // parse the raw data
        String rawData = sb.toString();
        JsonObject jsonObject = JsonUtils.parseJsonObject(rawData);

        SzEntityNetworkData entityNetworkData
            = SzEntityNetworkData.parseEntityNetworkData(
            jsonObject,
            (f) -> server.getAttributeClassForFeature(f));

        // construct the response
        SzEntityNetworkResponse response
            = new SzEntityNetworkResponse(GET,
                                          200,
                                          selfLink,
                                          entityNetworkData);

        // if including raw data then add it
        if (withRaw) response.setRawData(rawData);

        // return the response
        return response;

      } catch (WebApplicationException e) {
        throw e;

      } catch (Exception e) {
        throw ServicesUtil.newInternalServerErrorException(GET, selfLink, e);
      }
    });
  }

  /**
   * Checks if the entity ID's in the specified list are of a consistent type.
   *
   * @param entities The list of {@link SzEntityIdentifier} instances.
   */
  private static boolean checkConsistent(List<SzEntityIdentifier> entities)
  {
    int entityCount = 0;
    if (entities != null && entities.size() > 0) {
      entityCount = entities.size();
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
}
