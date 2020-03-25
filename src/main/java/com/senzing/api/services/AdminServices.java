package com.senzing.api.services;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriInfo;

import com.senzing.api.model.*;
import com.senzing.g2.engine.G2ConfigMgr;
import com.senzing.g2.engine.G2Engine;
import com.senzing.g2.engine.G2Product;
import com.senzing.g2.engine.Result;
import com.senzing.util.Timers;

import java.io.StringReader;

import static com.senzing.api.model.SzHttpMethod.*;
import static com.senzing.api.services.ServicesUtil.*;

/**
 * Administration REST services.
 */
@Path("/")
@Produces("application/json; charset=UTF-8")
public class AdminServices {
  /**
   * Generates a heartbeat response to affirnm the provider is running.
   */
  @GET
  @Path("heartbeat")
  public SzBasicResponse heartbeat(@Context UriInfo uriInfo) {
    Timers timers = newTimers();
    return new SzBasicResponse(GET, 200, uriInfo, timers);
  }

  /**
   * Provides license information, optionally with raw data.
   */
  @GET
  @Path("license")
  public SzLicenseResponse license(
      @DefaultValue("false") @QueryParam("withRaw") boolean withRaw,
      @Context UriInfo uriInfo)
      throws WebApplicationException {
    Timers timers = newTimers();
    SzApiProvider provider = SzApiProvider.Factory.getProvider();

    try {
      enteringQueue(timers);
      String rawData = provider.executeInThread(() -> {
        exitingQueue(timers);
        G2Product productApi = provider.getProductApi();
        callingNativeAPI(timers, "product", "license");
        return productApi.license();
      });
      calledNativeAPI(timers, "product", "license");
      processingRawData(timers);

      StringReader sr = new StringReader(rawData);
      JsonReader jsonReader = Json.createReader(sr);
      JsonObject jsonObject = jsonReader.readObject();
      SzLicenseInfo info = SzLicenseInfo.parseLicenseInfo(null, jsonObject);
      processedRawData(timers);

      SzLicenseResponse response
          = new SzLicenseResponse(GET, 200, uriInfo, timers);
      response.setLicense(info);
      if (withRaw) response.setRawData(rawData);
      return response;

    } catch (ServerErrorException e) {
      e.printStackTrace();
      throw e;

    } catch (WebApplicationException e) {
      throw e;

    } catch (Exception e) {
      e.printStackTrace();
      throw newInternalServerErrorException(GET, uriInfo, timers, e);
    }
  }

  /**
   * Provides license information, optionally with raw data.
   */
  @GET
  @Path("version")
  public SzVersionResponse version(
      @DefaultValue("false") @QueryParam("withRaw") boolean withRaw,
      @Context UriInfo uriInfo)
      throws WebApplicationException {
    Timers timers = newTimers();
    SzApiProvider provider = SzApiProvider.Factory.getProvider();

    try {
      enteringQueue(timers);
      String rawData = provider.executeInThread(() -> {
        exitingQueue(timers);
        G2Product productApi = provider.getProductApi();
        callingNativeAPI(timers, "product", "version");
        return productApi.version();
      });
      calledNativeAPI(timers, "product", "version");
      processingRawData(timers);

      StringReader sr = new StringReader(rawData);
      JsonReader jsonReader = Json.createReader(sr);
      JsonObject jsonObject = jsonReader.readObject();
      SzVersionInfo info = SzVersionInfo.parseVersionInfo(null, jsonObject);
      processedRawData(timers);

      SzVersionResponse response
          = new SzVersionResponse(GET, 200, uriInfo, timers);
      response.setData(info);
      if (withRaw) response.setRawData(rawData);
      return response;

    } catch (ServerErrorException e) {
      e.printStackTrace();
      throw e;

    } catch (WebApplicationException e) {
      throw e;

    } catch (Exception e) {
      e.printStackTrace();
      throw newInternalServerErrorException(GET, uriInfo, timers, e);
    }
  }

  /**
   * Provides license information, optionally with raw data.
   */
  @GET
  @Path("server-info")
  public SzServerInfoResponse getServerInfo(@Context UriInfo uriInfo)
      throws WebApplicationException {
    Timers timers = newTimers();
    SzApiProvider provider = SzApiProvider.Factory.getProvider();

    G2ConfigMgr configMgrApi = provider.getConfigMgrApi();
    G2Engine engineApi = provider.getEngineApi();

    try {
      enteringQueue(timers);
      long activeConfigId = provider.executeInThread(() -> {
        exitingQueue(timers);
        Result<Long> result = new Result<>();

        callingNativeAPI(timers, "engine", "getActiveConfigID");
        long returnCode = engineApi.getActiveConfigID(result);
        if (returnCode != 0) {
          throw newInternalServerErrorException(
              GET, uriInfo, timers, engineApi);
        }
        calledNativeAPI(timers, "engine", "getActiveConfigID");

        return result.getValue();
      });

      SzServerInfo serverInfo = new SzServerInfo();
      serverInfo.setConcurrency(provider.getConcurrency());
      serverInfo.setDynamicConfig(configMgrApi != null);
      serverInfo.setReadOnly(provider.isReadOnly());
      serverInfo.setAdminEnabled(provider.isAdminEnabled());
      serverInfo.setActiveConfigId(activeConfigId);

      return new SzServerInfoResponse(
          GET, 200, uriInfo, timers, serverInfo);

    } catch (ServerErrorException e) {
      e.printStackTrace();
      throw e;

    } catch (WebApplicationException e) {
      throw e;

    } catch (Exception e) {
      e.printStackTrace();
      throw newInternalServerErrorException(GET, uriInfo, timers, e);
    }
  }
}
