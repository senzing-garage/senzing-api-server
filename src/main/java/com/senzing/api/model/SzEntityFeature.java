package com.senzing.api.model;

import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonValue;
import java.util.*;

/**
 * Describes a feature for an entity.
 */
public class SzEntityFeature {
  /**
   * The primary value for the feature.
   */
  private String primaryValue;

  /**
   * The usage type associated with the feature.
   */
  private String usageType;

  /**
   * The list of duplicate values.
   */
  private List<String> duplicateValues;

  /**
   * Default constructor.
   */
  public SzEntityFeature() {
    this.primaryValue     = null;
    this.usageType        = null;
    this.duplicateValues  = new LinkedList<>();
  }

  /**
   * Gets the primary value for the feature.
   *
   * @return The primary value for the feature.
   */
  public String getPrimaryValue() {
    return primaryValue;
  }

  /**
   * Sets the primary value for the feature.
   *
   * @param primaryValue The primary value for the feature.
   */
  public void setPrimaryValue(String primaryValue) {
    this.primaryValue = primaryValue;
  }

  /**
   * Gets the usage type for the feature.
   *
   * @return The usage type for the feature.
   */
  public String getUsageType() {
    return usageType;
  }

  /**
   * Sets the usage type for the feature.
   *
   * @param usageType The usage type for the feature.
   */
  public void setUsageType(String usageType) {
    this.usageType = usageType;
  }

  /**
   * Returns the list of duplicate values for the entity.
   *
   * @return The list of duplicate values for the entity.
   */
  public List<String> getDuplicateValues() {
    return Collections.unmodifiableList(this.duplicateValues);
  }

  /**
   * Sets the duplicate values list for the entity.
   *
   * @param duplicateValues The list of duplicate values.
   */
  public void setDuplicateValues(List<String> duplicateValues) {
    this.duplicateValues.clear();
    if (duplicateValues != null) {
      this.duplicateValues.addAll(duplicateValues);
    }
  }

  /**
   * Adds to the duplicate value list for the record.
   *
   * @param value The duplicate value to add to the duplicate value list.
   */
  public void addDuplicateValue(String value)
  {
    this.duplicateValues.add(value);
  }

  /**
   * Parses a list of entity features from a {@link JsonArray} describing a
   * JSON array in the Senzing native API format for entity features and
   * populates the specified {@link List} or creates a new {@link List}.
   *
   * @param list The {@link List} of {@link SzEntityFeature} instances to
   *             populate, or <tt>null</tt> if a new {@link List}
   *             should be created.
   *
   * @param jsonArray The {@link JsonArray} describing the JSON in the
   *                  Senzing native API format.
   *
   * @return The populated (or created) {@link List} of {@link SzEntityFeature}
   *         instances.
   */
  public static List<SzEntityFeature> parseEntityFeatureList(
      List<SzEntityFeature> list,
      JsonArray             jsonArray)
  {
    if (list == null) {
      list = new ArrayList<SzEntityFeature>(jsonArray.size());
    }
    for (JsonObject jsonObject : jsonArray.getValuesAs(JsonObject.class)) {
      list.add(parseEntityFeature(null, jsonObject));
    }
    return list;
  }

  /**
   * Parses the entity feature from a {@link JsonObject} describing JSON
   * for the Senzing native API format for an entity feature and populates
   * the specified {@link SzEntityFeature} or creates a new instance.
   *
   * @param feature The {@link SzEntityFeature} instance to populate, or
   *                <tt>null</tt> if a new instance should be created.
   *
   * @param jsonObject The {@link JsonObject} describing the JSON in the
   *                   Senzing native API format.
   *
   * @return The populated (or created) {@link SzEntityFeature}.
   */
  public static SzEntityFeature parseEntityFeature(SzEntityFeature  feature,
                                                   JsonObject       jsonObject)
  {
    if (feature == null) feature = new SzEntityFeature();

    String featureDesc = jsonObject.getString("FEAT_DESC");
    long   libFeatId   = jsonObject.getJsonNumber("LIB_FEAT_ID").longValue();
    String usageType   = jsonObject.getString("UTYPE_CODE", null);

    feature.setPrimaryValue(featureDesc);
    feature.setUsageType(usageType);

    JsonArray featureValues = jsonObject.getJsonArray("FEAT_DESC_VALUES");

    for (JsonValue value : featureValues) {
      JsonObject valueObj = value.asJsonObject();
      long valueId = valueObj.getJsonNumber("LIB_FEAT_ID").longValue();
      if (valueId == libFeatId) continue;
      String desc = valueObj.getString("FEAT_DESC");
      feature.addDuplicateValue(desc);
    }

    return feature;
  }

  @Override
  public String toString() {
    return "SzEntityFeature{" +
        "primaryValue='" + primaryValue + '\'' +
        ", usageType='" + usageType + '\'' +
        ", duplicateValues=" + duplicateValues +
        '}';
  }
}
