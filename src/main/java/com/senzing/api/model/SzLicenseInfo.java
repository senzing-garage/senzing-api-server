package com.senzing.api.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.senzing.util.JsonUtils;

import javax.json.JsonArray;
import javax.json.JsonObject;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

public class SzLicenseInfo {
  /**
   * The customer associated with the license.
   */
  private String customer = null;

  /**
   * The constract associated with the license.
   */
  private String contract = null;

  /**
   * The license type associated with the license.
   */
  private String licenseType = null;

  /**
   * The license level associated with the license.
   */
  private String licenseLevel = null;

  /**
   * The billing string associated with the license.
   */
  private String billing = null;

  /**
   * The issuance date associated with the license.
   */
  @JsonFormat(shape   = JsonFormat.Shape.STRING,
              pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'",
              locale  = "en_GB")
  private Date issuanceDate = null;

  /**
   * The expiration date associated with the license.
   */
  @JsonFormat(shape   = JsonFormat.Shape.STRING,
              pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'",
              locale  = "en_GB")
  private Date expirationDate = null;

  /**
   * The record limit associated with the license.
   */
  private long recordLimit = 0L;

  /**
   * Default constructor.
   */
  public SzLicenseInfo() {
    // do nothing
  }

  /**
   * Gets the customer string associated with the license.
   *
   * @return The customer string associated with the license.
   */
  public String getCustomer() {
    return customer;
  }

  /**
   * Sets the customer string associated with the license.
   *
   * @param customer The customer string associated with the license.
   */
  public void setCustomer(String customer) {
    this.customer = customer;
  }

  /**
   * Gets the contract string associated with the license.
   *
   * @return The contract string associated with the license.
   */
  public String getContract() {
    return contract;
  }

  /**
   * Sets the contract string associated with the license.
   *
   * @param contract The contract string associated with the license.
   */
  public void setContract(String contract) {
    this.contract = contract;
  }

  /**
   * Gets the license type associated with the license.
   *
   * @return The license type associated with the license.
   */
  public String getLicenseType() {
    return licenseType;
  }

  /**
   * Sets the license type associated with the license.
   *
   * @param licenseType The license type associated with the license.
   */
  public void setLicenseType(String licenseType) {
    this.licenseType = licenseType;
  }

  /**
   * Gets the license level associated with the license.
   *
   * @return The license level associated with the license.
   */
  public String getLicenseLevel() {
    return licenseLevel;
  }

  /**
   * Sets the license level associated with the licenese.
   *
   * @param licenseLevel The license level associated with the license.
   */
  public void setLicenseLevel(String licenseLevel) {
    this.licenseLevel = licenseLevel;
  }

  /**
   * Gets the billing string associated with the license.
   *
   * @return The billing string associated with the license.
   */
  public String getBilling() {
    return billing;
  }

  /**
   * Sets the billing string associated with the license.
   *
   * @param billing The billing string associated with the license.
   */
  public void setBilling(String billing) {
    this.billing = billing;
  }

  /**
   * Gets the issuance {@link Date} associated with the license.
   *
   * @return The issuance date associated with the license.
   */
  public Date getIssuanceDate() {
    return issuanceDate;
  }

  /**
   * Sets the issuance {@link Date} associated with the license.
   *
   * @param issuanceDate The issuance {@link Date} to be associated with the
   *                     license.
   */
  public void setIssuanceDate(Date issuanceDate) {
    this.issuanceDate = issuanceDate;
  }

  /**
   * Gets the expiration {@link Date} associated with the license.
   *
   * @return The expiration {@link Date} associated with the license.
   */
  public Date getExpirationDate() {
    return expirationDate;
  }

  /**
   * Sets the expiration date associated with the license.
   *
   * @param expirationDate The expiration date associated with the license.
   */
  public void setExpirationDate(Date expirationDate) {
    this.expirationDate = expirationDate;
  }

  /**
   * Gets the record limit associated with the license.
   *
   * @return The record limit associated with the license.
   */
  public long getRecordLimit() {
    return recordLimit;
  }

  /**
   * Sets the record limit associated with the license.
   *
   * @param recordLimit The record limit associated with the license.
   */
  public void setRecordLimit(long recordLimit) {
    this.recordLimit = recordLimit;
  }

  /**
   * Parses a JSON array of the engine API JSON to create or populate a
   * {@link List} of {@link SzLicenseInfo} instances.
   *
   * @param list The {@link List} to populate or <tt>null</tt> if a new
   *             {@link List} should be created.
   *
   * @param jsonArray The {@link JsonArray} of {@link JsonObject} instances
   *                  to parse from the engine API.
   *
   * @return An unmodifiable view of the specified (or newly created) {@link
   *         List} of {@link SzLicenseInfo} instances.
   */
  public static List<SzLicenseInfo> parseLicenseInfoList(
      List<SzLicenseInfo>   list,
      JsonArray             jsonArray)
  {
    if (list == null) {
      list = new ArrayList<SzLicenseInfo>(jsonArray.size());
    }
    for (JsonObject jsonObject : jsonArray.getValuesAs(JsonObject.class)) {
      list.add(parseLicenseInfo(null, jsonObject));
    }
    return list;
  }

  /**
   * Parses the engine API JSON to create an instance of {@link SzLicenseInfo}.
   *
   * @param info The {@link SzLicenseInfo} object to initialize or <tt>null</tt>
   *             if a new one should be created.
   *
   * @param jsonObject The {@link JsonObject} to parse from the engine API.
   *
   * @return The specified (or newly created) {@link SzLicenseInfo}
   */
  public static SzLicenseInfo parseLicenseInfo(SzLicenseInfo  info,
                                               JsonObject     jsonObject)
  {
    if (info == null) info = new SzLicenseInfo();

    String customer     = JsonUtils.getString(jsonObject, "customer");
    String contract     = JsonUtils.getString(jsonObject, "contract");
    String issueDate    = JsonUtils.getString(jsonObject, "issueDate");
    String licenseType  = JsonUtils.getString(jsonObject, "licenseType");
    String licenseLevel = JsonUtils.getString(jsonObject, "licenseLevel");
    String billing      = JsonUtils.getString(jsonObject, "billing");
    String expireDate   = JsonUtils.getString(jsonObject, "expireDate");
    Long   recordLimit  = JsonUtils.getLong(jsonObject, "recordLimit");

    ZoneId defaultZone = ZoneId.systemDefault();

    Date issuanceDate = null;
    if (issueDate != null && issueDate.length() > 0) {
      LocalDate localDate = LocalDate.parse(issueDate);
      LocalDateTime localDateTime = localDate.atStartOfDay();
      Instant instant = localDateTime.atZone(defaultZone).toInstant();
      issuanceDate = Date.from(instant);
    }

    Date expirationDate = null;
    if (expireDate != null && expireDate.length() > 0) {
      LocalDate localDate = LocalDate.parse(expireDate);
      LocalDateTime localDateTime = localDate.atTime(23,59,59);
      Instant instant = localDateTime.atZone(defaultZone).toInstant();
      expirationDate = Date.from(instant);
    }

    info.setCustomer(customer);
    info.setContract(contract);
    info.setIssuanceDate(issuanceDate);
    info.setExpirationDate(expirationDate);
    info.setLicenseType(licenseType);
    info.setLicenseLevel(licenseLevel);
    info.setBilling(billing);
    if (recordLimit != null) {
      info.setRecordLimit(recordLimit);
    }

    return info;
  }
}
