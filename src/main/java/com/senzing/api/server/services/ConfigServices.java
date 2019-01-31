package com.senzing.api.server.services;

import com.senzing.api.model.*;
import com.senzing.api.server.SzApiServer;
import com.senzing.g2.engine.G2Config;
import com.senzing.g2.engine.G2Engine;
import com.senzing.util.JsonUtils;

import javax.json.*;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriInfo;

import java.util.List;
import java.util.Map;

import static com.senzing.api.model.SzHttpMethod.GET;
import static com.senzing.api.server.services.ServicesUtil.*;

/**
 * Provides config related API services.
 */
@Produces("application/json; charset=UTF-8")
@Path("/")
public class ConfigServices {
  @GET
  @Path("/data-sources")
  public SzDataSourcesResponse getDataSources(
      @DefaultValue("false") @QueryParam("withRaw") boolean withRaw,
      @Context                                      UriInfo uriInfo)
  {
    SzApiServer server = SzApiServer.getInstance();

    return server.executeInThread(() -> {
      Long configId = null;
      try {
        // get the engine API and the config API
        G2Engine engineApi = server.getEngineApi();
        G2Config configApi = server.getConfigApi();

        String config = exportConfig(GET, uriInfo, engineApi);

        // load into a config object by ID
        configId = configApi.load(config);

        if (configId < 0) {
          throw newInternalServerErrorException(GET, uriInfo, configApi);
        }

        // clear the string buffer to reuse it
        StringBuffer sb = new StringBuffer();
        sb.delete(0, sb.length());

        // list the data sources on the config
        configApi.listDataSources(configId, sb);

        // the response is the raw data
        String rawData = sb.toString();

        // parse the raw data
        JsonObject jsonObject = JsonUtils.parseJsonObject(rawData);

        // get the array and construct the response
        JsonArray jsonArray = jsonObject.getJsonArray("DSRC_CODE");
        SzDataSourcesResponse response
            = new SzDataSourcesResponse(GET, 200, uriInfo);

        for (JsonString jsonString : jsonArray.getValuesAs(JsonString.class)) {
          response.addDataSource(jsonString.getString());
        }

        // if including raw data then add it
        if (withRaw) response.setRawData(rawData);

        // return the response
        return response;

      } catch (WebApplicationException e) {
        throw e;

      } catch (Exception e) {
        throw newInternalServerErrorException(GET, uriInfo, e);

      } finally {
        if (configId != null) {
          server.getConfigApi().close(configId);
        }
      }
    });
  }

  @GET
  @Path("/attribute-types")
  public SzAttributeTypesResponse geAttributeTypes(
      @DefaultValue("false") @QueryParam("withInternal") boolean withInternal,
      @QueryParam("attributeClass")                      String  attributeClass,
      @QueryParam("featureType")                         String  featureType,
      @DefaultValue("false") @QueryParam("withRaw")      boolean withRaw,
      @Context                                           UriInfo uriInfo)
  {
    SzApiServer server = SzApiServer.getInstance();

    return server.executeInThread(() -> {
      SzAttributeClass ac = null;
      if (attributeClass != null && attributeClass.trim().length() > 0) {
        try {
          ac = SzAttributeClass.valueOf(attributeClass.trim().toUpperCase());

        } catch (IllegalArgumentException e) {
          throw newBadRequestException(
              GET, uriInfo, "Unrecognized attribute class: " + attributeClass);
        }
      }
      final SzAttributeClass attrClass = ac;
      final String featType
          = ((featureType != null && featureType.trim().length() > 0)
             ? featureType.trim() : null);

      try {
        // get the engine API and the config API
        G2Engine engineApi = server.getEngineApi();

        JsonObject configRoot = getCurrentConfigRoot(GET, uriInfo, engineApi);

        // get the array and construct the response
        JsonArray jsonArray = configRoot.getJsonArray("CFG_ATTR");

        List<SzAttributeType> attrTypes
            = SzAttributeType.parseAttributeTypeList(null, jsonArray);

        // check if filtering out internal attribute types
        if (!withInternal) {
          attrTypes.removeIf(attrType -> attrType.isInternal());
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

        // build the response
        SzAttributeTypesResponse response
            = new SzAttributeTypesResponse(GET, 200, uriInfo);

        response.setAttributeTypes(attrTypes);

        // if including raw data then add it
        if (withRaw) {
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

          response.setRawData(rawData);
        }

        // return the response
        return response;

      } catch (WebApplicationException e) {
        throw e;

      } catch (Exception e) {
        throw newInternalServerErrorException(GET, uriInfo, e);
      }
    });
  }

  @GET
  @Path("/attribute-types/{attributeCode}")
  public SzAttributeTypeResponse geAttributeType(
      @PathParam("attributeCode")                   String  attributeCode,
      @DefaultValue("false") @QueryParam("withRaw") boolean withRaw,
      @Context                                      UriInfo uriInfo)
  {
    SzApiServer server = SzApiServer.getInstance();

    return server.executeInThread(() -> {
      try {
        // get the engine API and the config API
        G2Engine engineApi = server.getEngineApi();

        JsonObject configRoot = getCurrentConfigRoot(GET, uriInfo, engineApi);

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
              GET, uriInfo, "Attribute code not recognized: " + attributeCode);
        }

        SzAttributeType attrType
            = SzAttributeType.parseAttributeType(null, jsonAttrType);

        SzAttributeTypeResponse response = new SzAttributeTypeResponse(
            GET, 200, uriInfo, attrType);

        // if including raw data then add it
        if (withRaw) {
          String rawData = JsonUtils.toJsonText(jsonAttrType);

          response.setRawData(rawData);
        }

        // return the response
        return response;

      } catch (WebApplicationException e) {
        throw e;

      } catch (Exception e) {
        throw newInternalServerErrorException(GET, uriInfo, e);
      }
    });
  }

  /**
   * Exports the config using the specified {@link G2Engine} instance.
   */
  private static String exportConfig(SzHttpMethod   httpMethod,
                                     UriInfo        uriInfo,
                                     G2Engine       engineApi)
  {
    StringBuffer sb = new StringBuffer();
    int result = engineApi.exportConfig(sb);
    if (result != 0) {
      throw newInternalServerErrorException(httpMethod, uriInfo, engineApi);
    }
    return sb.toString();
  }

  /**
   * From an exported config, this pulls the <tt>"G2_CONFIG"</tt>
   * {@link JsonObject} from it.
   */
  private JsonObject getCurrentConfigRoot(SzHttpMethod  httpMethod,
                                          UriInfo       uriInfo,
                                          G2Engine      engineApi)
  {
    // export the config
    String config = exportConfig(httpMethod, uriInfo, engineApi);

    // parse the raw data
    JsonObject configObj = JsonUtils.parseJsonObject(config);
    return configObj.getJsonObject("G2_CONFIG");
  }
}
