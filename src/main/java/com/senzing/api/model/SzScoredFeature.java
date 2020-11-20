package com.senzing.api.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.senzing.util.JsonUtils;

import javax.json.JsonObject;
import java.util.Objects;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.*;

/**
 * Describes a feature value that has been scored.
 */
public class SzScoredFeature {
  /**
   * The feature ID for the scored feature.
   */
  private Long featureId;

  /**
   * The feature type for the scored feature.
   */
  private String featureType;

  /**
   * The feature value for the scored feature.
   */
  private String featureValue;

  /**
   * The usage type for the scored feature.
   */
  private String usageType;

  /**
   * Constructs with the specified parameters.
   */
  public SzScoredFeature(Long   featureId,
                         String featureType,
                         String featureValue,
                         String usageType)
  {
    this.featureId    = featureId;
    this.featureType  = featureType;
    this.featureValue = featureValue;
    this.usageType    = usageType;
  }

  /**
   * Default constructor.
   */
  public SzScoredFeature() {
    this.featureId    = null;
    this.featureType  = null;
    this.featureValue = null;
    this.usageType    = null;
  }

  /**
   * Gets the feature ID for the scored feature.
   *
   * @return The feature ID for the scored feature.
   */
  public Long getFeatureId() {
    return this.featureId;
  }

  /**
   * Sets the feature ID for the scored feature.
   *
   * @param featureId The feature ID for the scored feature.
   */
  public void setFeatureId(Long featureId) {
    this.featureId = featureId;
  }

  /**
   * Gets the feature type for the scored feature.
   *
   * @return The feature type for the scored feature.
   */
  public String getFeatureType() {
    return this.featureType;
  }

  /**
   * Sets the feature type for the scored feature.
   *
   * @param featureType The feature type for the scored feature.
   */
  public void setFeatureType(String featureType) {
    this.featureType = featureType;
  }

  /**
   * Gets the feature value for the scored feature.
   *
   * @return The feature value for the scored feature.
   */
  public String getFeatureValue() {
    return this.featureValue;
  }

  /**
   * Sets the feature value for the scored feature.
   *
   * @param featureValue The feature value for the scored feature.
   */
  public void setFeatureValue(String featureValue) {
    this.featureValue = featureValue;
  }

  /**
   * Gets the usage type for the scored feature.
   *
   * @return The usage type for the scored feature.
   */
  @JsonInclude(NON_EMPTY)
  public String getUsageType() {
    return this.usageType;
  }

  /**
   * Sets the usage type for the scored feature.
   *
   * @param usageType The usage type for the scored feature.
   */
  public void setUsageType(String usageType) {
    this.usageType = usageType;
  }


  @Override
  public boolean equals(Object object) {
    if (this == object) return true;
    if (object == null || getClass() != object.getClass()) return false;
    SzScoredFeature that = (SzScoredFeature) object;
    return Objects.equals(this.getFeatureId(), that.getFeatureId()) &&
        Objects.equals(this.getFeatureType(), that.getFeatureType()) &&
        Objects.equals(this.getFeatureValue(), that.getFeatureValue()) &&
        Objects.equals(this.getUsageType(), that.getUsageType());
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.getFeatureId(),
                        this.getFeatureType(),
                        this.getFeatureValue(),
                        this.getUsageType());
  }

  @Override
  public String toString() {
    return "SzScoredFeature{" +
        "featureId=" + this.getFeatureId() +
        ", featureType='" + this.getFeatureType() + '\'' +
        ", featureValue='" + this.getFeatureValue() + '\'' +
        ", usageType='" + this.getUsageType() + '\'' +
        '}';
  }

  /**
   * Parses the native API JSON to build an instance of {@link
   * SzScoredFeature}.
   *
   * @param jsonObject The {@link JsonObject} describing the perspective using
   *                   the native API JSON format.
   *
   * @param prefix The prefix to apply to the native JSON keys.
   *
   * @param featureType The feature type for the {@link SzCandidateKey}
   *                    instances.
   *
   * @return The created instance of {@link SzWhyPerspective}.
   */
  public static SzScoredFeature parseScoredFeature(JsonObject jsonObject,
                                                   String     prefix,
                                                   String     featureType)
  {
    Long featureId = JsonUtils.getLong(jsonObject, prefix + "FEAT_ID");

    String value = (jsonObject.containsKey(prefix + "FEAT"))
      ? jsonObject.getString(prefix + "FEAT")
      : JsonUtils.getString(jsonObject, prefix + "FEAT_DESC");

    String usage = (jsonObject.containsKey(prefix + "FEAT_USAGE_TYPE"))
      ? jsonObject.getString(prefix + "FEAT_USAGE_TYPE")
      : JsonUtils.getString(jsonObject,prefix + "FEAT_UTYPE_CODE");

    SzScoredFeature result = new SzScoredFeature();

    result.setFeatureId(featureId);
    result.setFeatureType(featureType);
    result.setFeatureValue(value);
    result.setUsageType(usage);

    return result;
  }

}
