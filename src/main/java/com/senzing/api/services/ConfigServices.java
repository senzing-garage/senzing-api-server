package com.senzing.api.services;

import com.senzing.api.model.*;
import com.senzing.g2.engine.G2Config;
import com.senzing.g2.engine.G2Engine;
import com.senzing.util.JsonUtils;
import com.senzing.util.Timers;

import javax.json.*;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriInfo;

import java.util.List;

import static com.senzing.api.model.SzHttpMethod.GET;
import static com.senzing.api.services.ServicesUtil.*;

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
    Timers timers = newTimers();
    SzApiProvider provider = SzApiProvider.Factory.getProvider();

    try {
      // get the engine API and the config API
      G2Engine engineApi = provider.getEngineApi();
      G2Config configApi = provider.getConfigApi();

      enteringQueue(timers);
      String rawData = provider.executeInThread(() -> {
        exitingQueue(timers);
        String config = exportConfig(GET, uriInfo, timers, engineApi);

        Long configId = null;
        try {
          // load into a config object by ID
          configId = configApi.load(config);

          if (configId < 0) {
            throw newInternalServerErrorException(GET, uriInfo, timers, configApi);
          }

          StringBuffer sb = new StringBuffer();

          // list the data sources on the config
          callingNativeAPI(timers, "config", "listDataSources");
          configApi.listDataSources(configId, sb);
          calledNativeAPI(timers, "config", "listDataSources");

          return sb.toString();

        } finally {
          if (configId != null) {
            provider.getConfigApi().close(configId);
          }
        }
      });

      processingRawData(timers);
      // parse the raw data
      JsonObject jsonObject = JsonUtils.parseJsonObject(rawData);

      // get the array and construct the response
      JsonArray jsonArray = jsonObject.getJsonArray("DSRC_CODE");
      SzDataSourcesResponse response
          = new SzDataSourcesResponse(GET, 200, uriInfo, timers);

      for (JsonString jsonString : jsonArray.getValuesAs(JsonString.class)) {
          response.addDataSource(jsonString.getString());
      }
      processedRawData(timers);

      // if including raw data then add it
      if (withRaw) response.setRawData(rawData);

      // return the response
      return response;

    } catch (WebApplicationException e) {
      throw e;

    } catch (Exception e) {
      throw newInternalServerErrorException(GET, uriInfo, timers, e);
    }
  }

  @GET
  @Path("/attribute-types")
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

    } catch (WebApplicationException e) {
      throw e;

    } catch (Exception e) {
      throw newInternalServerErrorException(GET, uriInfo, timers, e);
    }
  }

  @GET
  @Path("/attribute-types/{attributeCode}")
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

    } catch (WebApplicationException e) {
      throw e;

    } catch (Exception e) {
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
    timers.start("nativeAPI", "nativeAPI/exportConfig");
    int result = engineApi.exportConfig(sb);
    timers.pause("nativeAPI", "nativeAPI/exportConfig");
    if (result != 0) {
      throw newInternalServerErrorException(httpMethod, uriInfo, timers, engineApi);
    }
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
}
