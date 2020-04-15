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
   * The internal ID of the primary feature value.
   */
  private Long primaryId;

  /**
   * The primary value for the feature.
   */
  private String primaryValue;

  /**
   * The usage type associated with the feature.
   */
  private String usageType;

  /**
   * The set of duplicate values.
   */
  private Set<String> duplicateValues;

  /**
   * The {@link List} of {@link SzEntityFeatureDetail} instances describing
   * the details of each of the clustered feature values for this feature.
   */
  private List<SzEntityFeatureDetail> featureDetails;

  /**
   * Default constructor.
   */
  public SzEntityFeature() {
    this.primaryId        = null;
    this.primaryValue     = null;
    this.usageType        = null;
    this.duplicateValues  = new LinkedHashSet<>();
    this.featureDetails   = new LinkedList<>();
  }

  /**
   * Gets the internal ID for the primary feature value.
   *
   * @return The internal ID for the primary feature value.
   */
  public Long getPrimaryId() {
    return this.primaryId;
  }

  /**
   * Sets the internal ID for the primary feature value.
   *
   * @param primaryId The internal ID for the primary feature value.
   */
  public void setPrimaryId(Long primaryId) {
    this.primaryId = primaryId;
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
   * Returns the <b>unmodifiable</b> {@link Set} of duplicate values for the
   * entity.
   *
   * @return The <b>unmodifiable</b> {@link Set} of duplicate values for the
   *         entity.
   */
  public Set<String> getDuplicateValues() {
    return Collections.unmodifiableSet(this.duplicateValues);
  }

  /**
   * Sets the duplicate values list for the entity.
   *
   * @param duplicateValues The list of duplicate values.
   */
  public void setDuplicateValues(Collection<String> duplicateValues) {
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
   * Gets the <b>unmodifiable</b> {@link List} of {@link SzEntityFeatureDetail}
   * instances describing the details of each of the clustered feature values
   * for this feature.
   *
   * @return The <b>unmodifiable</b> {@link List} of {@link
   *         SzEntityFeatureDetail} instances describing the details of each of
   *         the clustered feature values for this feature.
   */
  public List<SzEntityFeatureDetail> getFeatureDetails() {
    return Collections.unmodifiableList(this.featureDetails);
  }

  /**
   * Sets the {@link List} of {@link SzEntityFeatureDetail} instances describing
   * the details of each of the clustered feature values for this feature.
   *
   * @param details The {@link Collection} of {@linkSzEntityFeatureDetail}
   *                instances describing the details of each of the clustered
   *                feature values for this feature.
   */
  public void setFeatureDetails(Collection<SzEntityFeatureDetail> details) {
    this.featureDetails.clear();
    if (details != null) {
      this.featureDetails.addAll(details);
    }
  }

  /**
   * Adds the specified {@link SzEntityFeatureDetail} instance to the {@link
   * List} of feature details.
   *
   * @param featureDetail The {@link SzEntityFeatureDetail} instance to add to
   *                      the list of feature details.
   */
  public void addFeatureDetail(SzEntityFeatureDetail featureDetail) {
    this.featureDetails.add(featureDetail);
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

    feature.setPrimaryId(libFeatId);
    feature.setPrimaryValue(featureDesc);
    feature.setUsageType(usageType);

    JsonArray featureValues = jsonObject.getJsonArray("FEAT_DESC_VALUES");

    List<SzEntityFeatureDetail> details
        = SzEntityFeatureDetail.parseEntityFeatureDetailList(
            null, featureValues);

    for (SzEntityFeatureDetail detail: details) {
      long valueId = detail.getInternalId();
      if (valueId != libFeatId) {
        feature.addDuplicateValue(detail.getFeatureValue());
      }
      feature.addFeatureDetail(detail);
    }

    return feature;
  }

  @Override
  public String toString() {
    return "SzEntityFeature{" +
        "primaryId=" + primaryId +
        ", primaryValue='" + primaryValue + '\'' +
        ", usageType='" + usageType + '\'' +
        ", duplicateValues=" + duplicateValues +
        '}';
  }
}
