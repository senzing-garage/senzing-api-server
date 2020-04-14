package com.senzing.api.model;

import com.senzing.util.JsonUtils;

import javax.json.JsonArray;
import javax.json.JsonObject;
import java.util.ArrayList;
import java.util.List;

/**
 * Describes the details of an entity feature value, optionally including
 * statistics if they have been requested.
 */
public class SzEntityFeatureDetail {
  /**
   * The internal ID for the feature value.
   */
  private Long internalId;

  /**
   * The feature value.
   */
  private String featureValue;

  /**
   * The {@link SzEntityFeatureStatistics} describing the statistics for the
   * feature value.  This may be <tt>null</tt> if the statistics were not
   * requested.
   */
  private SzEntityFeatureStatistics statistics;

  /**
   * Default constructor.
   */
  public SzEntityFeatureDetail() {
    this.internalId   = null;
    this.featureValue = null;
    this.statistics   = null;
  }

  /**
   * Gets the internal ID for the feature value.
   *
   * @return The internal ID for the feature value.
   */
  public Long getInternalId() {
    return internalId;
  }

  /**
   * Sets the internal ID for the feature value.
   *
   * @param internalId The internal ID for the feature value.
   */
  public void setInternalId(Long internalId) {
    this.internalId = internalId;
  }

  /**
   * Gets the actual feature value.
   *
   * @return The actual feature value.
   */
  public String getFeatureValue() {
    return featureValue;
  }

  /**
   * Sets the actual feature value.
   *
   * @param featureValue The actual feature value.
   */
  public void setFeatureValue(String featureValue) {
    this.featureValue = featureValue;
  }

  /**
   * Gets the {@link SzEntityFeatureStatistics} describing the statistics for
   * the feature value.  This returns <tt>null</tt> if the statistics were not
   * requested.
   *
   * @return The {@link SzEntityFeatureStatistics} describing the statistics
   *         for the feature value, or <tt>null</tt> if the statistics were not
   *         requested.
   */
  public SzEntityFeatureStatistics getStatistics() {
    return statistics;
  }

  /**
   * Sets the {@link SzEntityFeatureStatistics} describing the statistics for
   * the feature value.  Set this to <tt>null</tt> if the statistics were not
   * requested.
   *
   * @param statistics The {@link SzEntityFeatureStatistics} describing the
   *                   statistics for the feature value, or <tt>null</tt> if
   *                   the statistics were not requested.
   */
  public void setStatistics(SzEntityFeatureStatistics statistics) {
    this.statistics = statistics;
  }

  @Override
  public String toString() {
    return "SzEntityFeatureDetail{" +
        "internalId=" + internalId +
        ", featureValue='" + featureValue + '\'' +
        ", statistics=" + statistics +
        '}';
  }

  /**
   * Parses the native API Senzing JSON to populate (or create and populate) an
   * instance of {@link SzEntityFeatureDetail}.
   *
   * @param detail The {@link SzEntityFeatureDetail} to populate, or
   *               <tt>null</tt> if a new instance should be created.
   *
   * @param jsonObject The {@link JsonObject} to parse.
   *
   * @return The {@link SzEntityFeatureDetail} that was parsed.
   */
  public static SzEntityFeatureDetail parseEntityFeatureDetail(
      SzEntityFeatureDetail detail,
      JsonObject            jsonObject)
  {
    if (detail == null) detail = new SzEntityFeatureDetail();

    Long    internalId  = JsonUtils.getLong(jsonObject, "LIB_FEAT_ID");
    String  value       = JsonUtils.getString(jsonObject,  "FEAT_DESC");

    SzEntityFeatureStatistics statistics
        = SzEntityFeatureStatistics.parseEntityFeatureStatistics(jsonObject);

    detail.setInternalId(internalId);
    detail.setFeatureValue(value);
    detail.setStatistics(statistics);

    return detail;
  }

  /**
   * Parses the native API Senzing JSON to populate (or create and populate) a
   * {@link List} of {@link SzEntityFeatureDetail} instances.
   *
   * @param list The {@link List} of {@link SzEntityFeatureDetail} to populate,
   *             or <tt>null</tt> if a new list should be created.
   *
   * @param jsonArray The {@link JsonArray} of {@link JsonObject} instances to
   *                  parse.
   *
   * @return The {@link SzEntityFeatureDetail} that was parsed.
   */
  public static List<SzEntityFeatureDetail> parseEntityFeatureDetailList(
      List<SzEntityFeatureDetail> list,
      JsonArray                   jsonArray)
  {
    if (jsonArray == null) return null;

    if (list == null) {
      list = new ArrayList<>(jsonArray.size());
    }

    for (JsonObject jsonObject : jsonArray.getValuesAs(JsonObject.class)) {
      list.add(parseEntityFeatureDetail(null, jsonObject));
    }

    return list;
  }
}
