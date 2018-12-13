package com.senzing.api.server.services;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.ws.rs.*;
import javax.ws.rs.core.Response;

import com.senzing.api.model.*;
import com.senzing.api.server.SzApiServer;
import com.senzing.g2.engine.G2Product;

import java.io.StringReader;

import static com.senzing.api.model.SzHttpMethod.*;

/**
 * Administration REST services.
 */
@Produces("application/json; charset=UTF-8")
public class AdminServices {
  /**
   * Generates a heartbeat response to affirnm the server is running.
   */
  @GET
  @Path("/heartbeat")
  public SzBasicResponse heartbeat() {
    return new SzBasicResponse(GET, SzApiServer.makeLink("/heartbeat"));
  }

  /**
   * Provides license information, optionally with raw data.
   */
  @GET
  @Path("/license")
  public SzLicenseResponse license(
      @DefaultValue("false") @QueryParam("withRaw") boolean withRaw)
  {
    String selfLink = SzApiServer.makeLink("/license");
    if (withRaw) {
      selfLink += "?withRaw=true";
    }

    try {
      G2Product productApi = SzApiServer.getProductApi();

      String rawData = productApi.license();

      StringReader sr = new StringReader(rawData);
      JsonReader jsonReader = Json.createReader(sr);
      JsonObject jsonObject = jsonReader.readObject();
      SzLicenseInfo info = SzLicenseInfo.parseLicenseInfo(null, jsonObject);

      SzLicenseResponse response = new SzLicenseResponse(GET, selfLink);
      response.setData(info);
      if (withRaw) response.setRawData(rawData);
      return response;

    } catch (Exception e) {
      Response.ResponseBuilder builder = Response.status(500);
      builder.entity(new SzErrorResponse(GET, selfLink, e));
      throw new InternalServerErrorException(builder.build());
    }
  }
}
