package com.senzing.api.model;

import com.senzing.util.JsonUtils;

import javax.json.JsonArray;
import javax.json.JsonObject;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Described two features that matched each other to create a relationship
 * (typically a disclosed relationship).
 */
public class SzRelatedFeatures {
  /**
   * The feature belonging to the first entity.
   */
  private SzScoredFeature feature1;

  /**
   * The feature belonging to the second entity.
   */
  private SzScoredFeature feature2;

  /**
   * Default constructor.
   */
  public SzRelatedFeatures() {
    this.feature1 = null;
    this.feature2 = null;
  }

  /**
   * Gets the relationship feature belonging to the first entity that was
   * matched to create the relationship.
   *
   * @return The relationship feature belonging to the first entity that was
   *         matched to create the relationship.
   */
  public SzScoredFeature getFeature1() {
    return this.feature1;
  }

  /**
   * Sets the relationship feature belonging to the first entity that was
   * matched to create the relationship.
   *
   * @param feature The relationship feature belonging to the first entity that
   *                was matched to create the relationship.
   */
  public void setFeature1(SzScoredFeature feature) {
    this.feature1 = feature;
  }

  /**
   * Gets the relationship feature belonging to the second entity that was
   * matched to create the relationship.
   *
   * @return The relationship feature belonging to the first entity that was
   *         matched to create the relationship.
   */
  public SzScoredFeature getFeature2() {
    return this.feature2;
  }

  /**
   * Sets the relationship feature belonging to the second entity that was
   * matched to create the relationship.
   *
   * @param feature The relationship feature belonging to the first entity that
   *                was matched to create the relationship.
   */
  public void setFeature2(SzScoredFeature feature) {
    this.feature2 = feature;
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) return true;
    if (object == null || getClass() != object.getClass()) return false;
    SzRelatedFeatures that = (SzRelatedFeatures) object;
    return Objects.equals(this.getFeature1(), that.getFeature1()) &&
        Objects.equals(this.getFeature2(), that.getFeature2());
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.getFeature1(), this.getFeature2());
  }

  @Override
  public String toString() {
    return "SzRelatedFeatures{" +
        "feature1=" + this.getFeature1() +
        ", feature2=" + this.getFeature2() +
        '}';
  }

  /**
   * Parses the native API JSON to build an instance of {@link
   * SzRelatedFeatures}.
   *
   * @param jsonObject The {@link JsonObject} describing the features using
   *                   the native API JSON format.
   *
   * @param featureType The feature tyoe for the first feature.
   *
   * @return The created instance of {@link SzRelatedFeatures}.
   */
  public static SzRelatedFeatures parseRelatedFeatures(JsonObject jsonObject,
                                                       String     featureType)
  {
    SzScoredFeature feature = SzScoredFeature.parseScoredFeature(
        jsonObject, "", featureType);

    String linkedType = JsonUtils.getString(jsonObject, "LINKED_FEAT_TYPE");

    SzScoredFeature linkedFeature = SzScoredFeature.parseScoredFeature(
        jsonObject, "LINKED_", linkedType);

    SzRelatedFeatures result = new SzRelatedFeatures();

    result.setFeature1(feature);
    result.setFeature2(linkedFeature);

    return result;
  }

  /**
   * Parses the native API JSON to build an instance of {@link
   * SzRelatedFeatures}.
   *
   * @param jsonArray The {@link JsonArray} describing the features list using
   *                  the native API JSON format.
   *
   * @param featureType The feature tyoe for the first feature.
   *
   * @return The created instance of {@link SzRelatedFeatures}.
   */
  public static List<SzRelatedFeatures> parseRelatedFeatures(
      JsonArray jsonArray, String featureType) {
    return parseRelatedFeatures(null, jsonArray, featureType);
  }

  /**
   * Parses the native API JSON to build an instance of {@link
   * SzRelatedFeatures}.
   *
   * @param jsonArray The {@link JsonArray} describing the features list using
   *                  the native API JSON format.
   *
   * @param featureType The feature tyoe for the first feature.
   *
   * @return The created instance of {@link SzRelatedFeatures}.
   */
  public static List<SzRelatedFeatures> parseRelatedFeatures(
      List<SzRelatedFeatures> list,  JsonArray jsonArray, String featureType)
  {
    if (list == null) {
      list = new ArrayList<>(jsonArray.size());
    }

    for (JsonObject jsonObject : jsonArray.getValuesAs(JsonObject.class)) {
      list.add(parseRelatedFeatures(jsonObject, featureType));
    }

    return list;
  }

}
