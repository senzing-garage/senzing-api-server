package com.senzing.api.server.services;

import com.senzing.api.model.SzDataSourcesResponse;
import com.senzing.api.server.SzApiServer;
import com.senzing.g2.engine.G2Config;
import com.senzing.g2.engine.G2Engine;
import com.senzing.util.JsonUtils;

import javax.json.*;
import javax.ws.rs.*;

import static com.senzing.api.model.SzHttpMethod.GET;
import static com.senzing.api.server.services.ServicesUtil.*;

/**
 * Provides config related API services.
 */
@Path("/data-sources")
@Produces("application/json; charset=UTF-8")
public class ConfigServices {
  @GET
  public SzDataSourcesResponse getDataSources(
      @DefaultValue("false") @QueryParam("withRaw") boolean withRaw)
  {
    SzApiServer server = SzApiServer.getInstance();

    return server.executeInThread(() -> {
      String selfLink = server.makeLink("/data-sources");
      if (withRaw) {
        selfLink += "?withRaw=true";
      }

      Long configId = null;
      try {
        // get the engine API and the config API
        G2Engine engineApi = server.getEngineApi();
        G2Config configApi = server.getConfigApi();

        StringBuffer sb = new StringBuffer();

        // export the config
        int result = engineApi.exportConfig(sb);
        if (result != 0) {
          throw newInternalServerErrorException(GET, selfLink, engineApi);
        }

        // load into a config object by ID
        configId = configApi.load(sb.toString());

        if (configId < 0) {
          throw newInternalServerErrorException(GET, selfLink, configApi);
        }

        // clear the string buffer to reuse it
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
            = new SzDataSourcesResponse(GET, 200, selfLink);

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
        throw newInternalServerErrorException(GET, selfLink, e);

      } finally {
        if (configId != null) {
          server.getConfigApi().close(configId);
        }
      }
    });
  }
}
