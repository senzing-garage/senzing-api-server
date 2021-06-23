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

/**
 * Administration REST services.
 */
@Path("/")
@Produces("application/json; charset=UTF-8")
public class AdminServices implements ServicesSupport {
  /**
   * Generates a root-level response -- similar to heartbeat.
   */
  @GET
  @Path("")
  public SzBasicResponse root(@Context UriInfo uriInfo) {
    return this.heartbeat(uriInfo);
  }

  /**
   * Generates a heartbeat response to affirm the provider is running.
   */
  @GET
  @Path("heartbeat")
  public SzBasicResponse heartbeat(@Context UriInfo uriInfo) {
    Timers timers = this.newTimers();
    return newBasicResponse(uriInfo, timers);
  }
  /**
   * Creates a new response to the <tt>GET /heartbeat</tt> operation.
   *
   * @param uriInfo The {@link UriInfo} for the request.
   * @param timers The {@link Timers} for the operation.
   * @return The {@link SzBasicResponse} with the specified parameters.
   */
  protected SzBasicResponse newBasicResponse(UriInfo uriInfo,
                                             Timers  timers)
  {
    return SzBasicResponse.FACTORY.create(
        this.newMeta(GET, 200, timers), this.newLinks(uriInfo));
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
    Timers timers = this.newTimers();
    SzApiProvider provider = this.getApiProvider();

    try {
      this.enteringQueue(timers);
      String rawData = provider.executeInThread(() -> {
        this.exitingQueue(timers);
        G2Product productApi = provider.getProductApi();
        this.callingNativeAPI(timers, "product", "license");
        return productApi.license();
      });
      this.calledNativeAPI(timers, "product", "license");
      this.processingRawData(timers);

      StringReader sr = new StringReader(rawData);
      JsonReader jsonReader = Json.createReader(sr);
      JsonObject jsonObject = jsonReader.readObject();
      SzLicenseInfo info = this.parseLicenseInfo(jsonObject);
      this.processedRawData(timers);

      SzLicenseResponse response
          = this.newLicenseResponse(uriInfo, timers, info);
      if (withRaw) response.setRawData(rawData);
      return response;

    } catch (ServerErrorException e) {
      e.printStackTrace();
      throw e;

    } catch (WebApplicationException e) {
      throw e;

    } catch (Exception e) {
      e.printStackTrace();
      throw this.newInternalServerErrorException(GET, uriInfo, timers, e);
    }
  }

  /**
   * Method to parse the {@link SzLicenseInfo} from the RAW JSON.
   *
   * @param jsonObject The {@link JsonObject} describing the RAW JSON.
   *
   * @return The {@link SzLicenseInfo} that was parsed.
   */
  protected SzLicenseInfo parseLicenseInfo(JsonObject jsonObject) {
    return SzLicenseInfo.parseLicenseInfo(null, jsonObject);
  }

  /**
   * Creates a new {@link SzLicenseResponse} for the <tt>"GET /license"</tt>
   * operation.
   *
   * @param uriInfo The {@link UriInfo} for the request.
   * @param timers The {@link Timers} for the operation.
   * @param licenseInfo The {@link SzLicenseInfo} for the response.
   * @return The {@link SzLicenseResponse} with the specified parameters.
   */
  protected SzLicenseResponse newLicenseResponse(UriInfo       uriInfo,
                                                 Timers        timers,
                                                 SzLicenseInfo licenseInfo)
  {
    return SzLicenseResponse.FACTORY.create(
        this.newMeta(GET, 200, timers),
        this.newLinks(uriInfo),
        this.newLicenseResponseData(licenseInfo));
  }

  /**
   * Creates a new {@link SzLicenseResponseData} describing an {@link
   * SzLicenseInfo}.
   *
   * @param licenseInfo The {@link SzLicenseInfo} for the response.
   * @return The {@link SzLicenseResponseData} with the specified parameters.
   */
  protected SzLicenseResponseData newLicenseResponseData(
      SzLicenseInfo licenseInfo)
  {
    return SzLicenseResponseData.FACTORY.create(licenseInfo);
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
    Timers timers = this.newTimers();
    SzApiProvider provider = this.getApiProvider();

    try {
      this.enteringQueue(timers);
      String rawData = provider.executeInThread(() -> {
        this.exitingQueue(timers);
        G2Product productApi = provider.getProductApi();
        this.callingNativeAPI(timers, "product", "version");
        return productApi.version();
      });
      this.calledNativeAPI(timers, "product", "version");
      this.processingRawData(timers);

      StringReader sr = new StringReader(rawData);
      JsonReader jsonReader = Json.createReader(sr);
      JsonObject jsonObject = jsonReader.readObject();
      SzVersionInfo info = this.parseVersionInfo(jsonObject);
      this.processedRawData(timers);

      SzVersionResponse response = newVersionResponse(uriInfo, timers, info);

      if (withRaw) response.setRawData(rawData);
      return response;

    } catch (ServerErrorException e) {
      e.printStackTrace();
      throw e;

    } catch (WebApplicationException e) {
      throw e;

    } catch (Exception e) {
      e.printStackTrace();
      throw this.newInternalServerErrorException(GET, uriInfo, timers, e);
    }
  }

  /**
   * Method to parse the {@link SzVersionInfo} from the RAW JSON.
   *
   * @param jsonObject The {@link JsonObject} describing the RAW JSON.
   *
   * @return The {@link SzVersionInfo} that was parsed.
   */
  protected SzVersionInfo parseVersionInfo(JsonObject jsonObject) {
    return SzVersionInfo.parseVersionInfo(null, jsonObject);
  }

  /**
   * Creates a new {@link SzVersionResponse} for the <tt>"GET /version"</tt>
   * operation.
   *
   * @param uriInfo The {@link UriInfo} for the request.
   * @param timers The {@link Timers} for the operation.
   * @param versionInfo The {@link SzVersionInfo} for the response.
   * @return The {@link SzVersionResponse} with the specified parameters.
   */
  protected SzVersionResponse newVersionResponse(UriInfo       uriInfo,
                                                 Timers        timers,
                                                 SzVersionInfo versionInfo)
  {
    return SzVersionResponse.FACTORY.create(
        this.newMeta(GET, 200, timers),
        this.newLinks(uriInfo),
        versionInfo);
  }

  /**
   * Provides version information, optionally with raw data.
   */
  @GET
  @Path("server-info")
  public SzServerInfoResponse getServerInfo(@Context UriInfo uriInfo)
      throws WebApplicationException {
    Timers timers = this.newTimers();
    SzApiProvider provider = this.getApiProvider();

    G2Engine engineApi = provider.getEngineApi();

    try {
      this.enteringQueue(timers);
      long activeConfigId = provider.executeInThread(() -> {
        this.exitingQueue(timers);
        Result<Long> result = new Result<>();

        this.callingNativeAPI(timers, "engine", "getActiveConfigID");
        int returnCode = engineApi.getActiveConfigID(result);
        if (returnCode != 0) {
          throw newInternalServerErrorException(
              GET, uriInfo, timers, engineApi);
        }
        this.calledNativeAPI(timers, "engine", "getActiveConfigID");

        return result.getValue();
      });

      SzServerInfo serverInfo = newServerInfo(provider, activeConfigId);

      return this.newServerInfoResponse(uriInfo, timers, serverInfo);

    } catch (ServerErrorException e) {
      e.printStackTrace();
      throw e;

    } catch (WebApplicationException e) {
      throw e;

    } catch (Exception e) {
      e.printStackTrace();
      throw this.newInternalServerErrorException(GET, uriInfo, timers, e);
    }
  }

  /**
   * Creates a new instance of {@link SzServerInfo} with none of its properties
   * set (uninitialized).
   *
   * @return The newly created instance of {@link SzServerInfo}.
   */
  protected SzServerInfo newServerInfo() {
    return SzServerInfo.FACTORY.create();
  }

  /**
   * Creates a new instance of {@link SzServerInfo} and configures it using
   * the specified {@link SzApiProvider} and active configuration ID.  This
   * method calls {@link #newServerInfo()} to create the new instance before
   * configuring it.
   *
   * @param provider The {@link SzApiProvider} to use to configure the
   *                 server info.
   * @param activeConfigId The active configuration ID, or <tt>null</tt> if not
   *                       available.
   * @return The new instance of {@link SzServerInfo} configured according to
   *         the specified parameters.
   */
  protected SzServerInfo newServerInfo(SzApiProvider  provider,
                                       Long           activeConfigId)
  {
    SzServerInfo serverInfo = this.newServerInfo();

    G2ConfigMgr configMgrApi = provider.getConfigMgrApi();
    serverInfo.setConcurrency(provider.getConcurrency());
    serverInfo.setDynamicConfig(configMgrApi != null);
    serverInfo.setReadOnly(provider.isReadOnly());
    serverInfo.setAdminEnabled(provider.isAdminEnabled());
    serverInfo.setActiveConfigId(activeConfigId);
    serverInfo.setWebSocketsMessageMaxSize(
        provider.getWebSocketsMessageMaxSize());
    serverInfo.setInfoQueueConfigured(
        provider.hasInfoSink());
    return serverInfo;
  }

  /**
   * Creates a new {@link SzServerInfoResponse} for the
   * <tt>"GET /server-info"</tt> operation.
   *
   * @param uriInfo The {@link UriInfo} for the request.
   * @param timers The {@link Timers} for the operation.
   * @param serverInfo The {@link SzServerInfo} for the response.
   * @return The {@link SzServerInfoResponse} with the specified parameters.
   */
  protected SzServerInfoResponse newServerInfoResponse(
      UriInfo       uriInfo,
      Timers        timers,
      SzServerInfo  serverInfo)
  {
    return SzServerInfoResponse.FACTORY.create(
        this.newMeta(GET, 200, timers),
        this.newLinks(uriInfo),
        serverInfo);
  }
}
