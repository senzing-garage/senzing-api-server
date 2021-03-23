package com.senzing.api.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.senzing.api.BuildInfo;
import com.senzing.util.JsonUtils;

import javax.json.JsonArray;
import javax.json.JsonObject;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Describes the version information associated with the API Server, the
 * Senzing REST API Specification that is implemented and the native Senzing
 * API.
 */
public class SzVersionInfo {
  /**
   * The date-time pattern for the build number.
   */
  private static final String BUILD_NUMBER_PATTERN = "yyyy_MM_dd__HH_mm";

  /**
   * The time zone used for the time component of the build number.
   */
  private static final ZoneId BUILD_ZONE = ZoneId.of("America/Los_Angeles");

  /**
   * The {@link DateTimeFormatter} for interpretting the build number as a
   * LocalDateTime instance.
   */
  private static final DateTimeFormatter BUILD_NUMBER_FORMATTER
      = DateTimeFormatter.ofPattern(BUILD_NUMBER_PATTERN);

  /**
   * The version of the REST API Server implementation.
   */
  private String apiServerVersion = null;

  /**
   * The version of the REST API that is implemented.
   */
  private String restApiVersion = null;

  /**
   * The version for the underlying runtime native Senzing API
   */
  private String nativeApiVersion = null;

  /**
   * The build version for the underlying runtime native Senzing API.
   */
  private String nativeApiBuildVersion = null;

  /**
   * The build number for the underlying runtime native Senzing API.
   */
  private String nativeApiBuildNumber = null;

  /**
   * The build date associated with the underlying runtime native API.
   */
  @JsonFormat(shape   = JsonFormat.Shape.STRING,
              pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'",
              locale  = "en_GB")
  private Date nativeApiBuildDate = null;

  /**
   * The configuration compatibility version for the underlying runtime
   * native Senzing API.
   */
  private String configCompatibilityVersion = null;

  /**
   * Default constructor.
   */
  public SzVersionInfo() {
    this.apiServerVersion = BuildInfo.MAVEN_VERSION;
    this.restApiVersion   = BuildInfo.REST_API_VERSION;
  }

  /**
   * Gets the version of the REST API Server implementation.
   *
   * @return The version of the REST API Server implementation.
   */
  public String getApiServerVersion() {
    return this.apiServerVersion;
  }

  /**
   * Private setter for the version of the REST API Server implementation.
   * This is used for JSON serialization -- otherwise the version cannot be
   * normally set (it is inferred).
   *
   * @param apiServerVersion The version of the REST API Server implementation.
   */
  private void setApiServerVersion(String apiServerVersion) {
    this.apiServerVersion = apiServerVersion;
  }

  /**
   * Gets the version of the REST API Specification that is implemented.
   *
   * @return The version of the REST API Specification that is implemented.
   */
  public String getRestApiVersion() {
    return this.restApiVersion;
  }

  /**
   * Private setter for the REST API version implemented by the REST API
   * Server implementation.  This is used for JSON serialization -- otherwise
   * the version cannot be normally set (it is inferred).
   *
   * @param restApiVersion The version of the REST API Specification that is
   *                       implemented.
   */
  private void setRestApiVersion(String restApiVersion) {
    this.restApiVersion = restApiVersion;
  }

  /**
   * Gets the version for the underlying runtime native Senzing API.
   *
   * @return The version for the underlying runtime native Senzing API.
   */
  public String getNativeApiVersion() {
    return this.nativeApiVersion;
  }

  /**
   * Sets the version for the underlying runtime native Senzing API.
   *
   * @param nativeApiVersion Sets the version for the underlying runtime
   *                         native Senzing API.
   */
  public void setNativeApiVersion(String nativeApiVersion) {
    this.nativeApiVersion = nativeApiVersion;
  }

  /**
   * Gets the build version for the underlying runtime native Senzing API.
   *
   * @return The build version for the underlying runtime native Senzing API.
   */
  public String getNativeApiBuildVersion() {
    return this.nativeApiBuildVersion;
  }

  /**
   * Sets the build version for the underlying runtime native Senzing API.
   *
   * @param nativeApiBuildVersion The build version for the underlying runtime
   *                              native Senzing API.
   */
  public void setNativeApiBuildVersion(String nativeApiBuildVersion) {
    this.nativeApiBuildVersion = nativeApiBuildVersion;
  }

  /**
   * Gets the build number for the underlying runtime native Senzing API.
   *
   * @return The build number for the underlying runtime native Senzing API.
   */
  public String getNativeApiBuildNumber() {
    return this.nativeApiBuildNumber;
  }

  /**
   * Sets the build number for the underlying runtime native Senzing API.
   *
   * @param nativeApiBuildNumber The build number for the underlying runtime
   *                             native Senzing API.
   */
  public void setNativeApiBuildNumber(String nativeApiBuildNumber) {
    this.nativeApiBuildNumber = nativeApiBuildNumber;
  }

  /**
   * Gets the build date for the underlying runtime native Senzing API.
   *
   * @return The build date for the underlying runtime native Senzing API.
   */
  public Date getNativeApiBuildDate() {
    return this.nativeApiBuildDate;
  }

  /**
   * Sets the build date for the underlying runtime native Senzing API.
   *
   * @param nativeApiBuildDate The build date for the underlying runtime
   *                           native Senzing API.
   */
  public void setNativeApiBuildDate(Date nativeApiBuildDate) {
    this.nativeApiBuildDate = nativeApiBuildDate;
  }

  /**
   * Gets the configuration compatibility version for the underlying runtime
   * native Senzing API.
   *
   * @return The configuration compatibility version for the underlying runtime
   *         native Senzing API.
   */
  public String getConfigCompatibilityVersion() {
    return this.configCompatibilityVersion;
  }

  /**
   * Sets the configuration compatibility version for the underlying runtime
   * native Senzing API.
   *
   * @param configCompatibilityVersion The configuration compatibility version
   *                                   for the underlying runtime native
   *                                   Senzing API.
   */
  public void setConfigCompatibilityVersion(String configCompatibilityVersion) {
    this.configCompatibilityVersion = configCompatibilityVersion;
  }

  /**
   * Parses a JSON array of the engine API JSON to create or populate a
   * {@link List} of {@link SzVersionInfo} instances.
   *
   * @param list The {@link List} to populate or <tt>null</tt> if a new
   *             {@link List} should be created.
   *
   * @param jsonArray The {@link JsonArray} of {@link JsonObject} instances
   *                  to parse from the engine API.
   *
   * @return An unmodifiable view of the specified (or newly created) {@link
   *         List} of {@link SzVersionInfo} instances.
   */
  public static List<SzVersionInfo> parseVersionInfoList(
      List<SzVersionInfo>   list,
      JsonArray             jsonArray)
  {
    if (list == null) {
      list = new ArrayList<SzVersionInfo>(jsonArray.size());
    }
    for (JsonObject jsonObject : jsonArray.getValuesAs(JsonObject.class)) {
      list.add(parseVersionInfo(null, jsonObject));
    }
    return list;
  }

  /**
   * Parses the engine API JSON to create an instance of {@link SzVersionInfo}.
   *
   * @param info The {@link SzVersionInfo} object to initialize or <tt>null</tt>
   *             if a new one should be created.
   *
   * @param jsonObject The {@link JsonObject} to parse from the engine API.
   *
   * @return The specified (or newly created) {@link SzVersionInfo}
   */
  public static SzVersionInfo parseVersionInfo(SzVersionInfo info,
                                               JsonObject     jsonObject)
  {
    if (info == null) info = new SzVersionInfo();

    String nativeVersion = JsonUtils.getString(jsonObject, "VERSION");
    String buildVersion  = JsonUtils.getString(jsonObject, "BUILD_VERSION");
    String buildNumber   = JsonUtils.getString(jsonObject, "BUILD_NUMBER");

    JsonObject compatVersion
        = JsonUtils.getJsonObject(jsonObject, "COMPATIBILITY_VERSION");

    String configCompatVersion = JsonUtils.getString(compatVersion,
                                                     "CONFIG_VERSION");

    Date buildDate = null;
    if (buildNumber != null && buildNumber.length() > 0) {
      LocalDateTime localDateTime = LocalDateTime.parse(buildNumber,
                                                        BUILD_NUMBER_FORMATTER);
      ZonedDateTime zonedDateTime = localDateTime.atZone(BUILD_ZONE);
      buildDate = Date.from(zonedDateTime.toInstant());
    }

    info.setApiServerVersion(BuildInfo.MAVEN_VERSION);
    info.setRestApiVersion(BuildInfo.REST_API_VERSION);
    info.setConfigCompatibilityVersion(configCompatVersion);
    info.setNativeApiVersion(nativeVersion);
    info.setNativeApiBuildDate(buildDate);
    info.setNativeApiBuildVersion(buildVersion);
    info.setNativeApiBuildNumber(buildNumber);

    return info;
  }
}
