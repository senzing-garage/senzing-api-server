package com.senzing.api.server.services;

import com.senzing.api.model.SzDataSourcesResponse;
import com.senzing.api.model.SzErrorResponse;
import com.senzing.api.server.SzApiServer;
import com.senzing.g2.engine.G2Config;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.ws.rs.*;
import javax.ws.rs.core.Response;
import java.io.StringReader;

import static com.senzing.api.model.SzHttpMethod.GET;

/**
 *
 */
@Path("/license")
@Produces("application/json; charset=UTF-8")
public class ConfigServices {
  @GET
  public SzDataSourcesResponse getDataSources(
      @DefaultValue("false") @QueryParam("withRaw") boolean withRaw)
  {
    String selfLink = SzApiServer.makeLink("/data-sources");
    if (withRaw) {
      selfLink += "?withRaw=true";
    }

    try {
      G2Config configApi = SzApiServer.getConfigApi();

/*
      String rawData = productApi.license();

      StringReader sr = new StringReader(rawData);
      JsonReader jsonReader = Json.createReader(sr);
      JsonObject jsonObject = jsonReader.readObject();
      SzLicenseInfo info = SzLicenseInfo.parseLicenseInfo(null, jsonObject);

      SzLicenseResponse response = new SzLicenseResponse(GET, selfLink);
      response.setData(info);
      if (withRaw) response.setRawData(rawData);
      return response;
*/
      return null;
    } catch (Exception e) {
      Response.ResponseBuilder builder = Response.status(500);
      builder.entity(new SzErrorResponse(GET, selfLink, e));
      throw new InternalServerErrorException(builder.build());
    }
  }
}
