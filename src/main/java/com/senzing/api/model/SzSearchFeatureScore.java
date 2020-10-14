package com.senzing.api.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.senzing.util.JsonUtils;

import javax.json.JsonArray;
import javax.json.JsonObject;
import java.util.ArrayList;
import java.util.List;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_EMPTY;

/**
 * Describes the scoring between {@link SzScoredFeature} instances.
 */
public class SzSearchFeatureScore {
  /**
   * The feature type of the features being scored.
   */
  private String featureType;

  /**
   * The inbound feature value as a {@link String}.
   */
  private String inboundFeature;

  /**
   * The feature value that was a candidate match for the inbound feature as a
   * {@link String}.
   */
  private String candidateFeature;

  /**
   * The integer score between the two feature values (typically from 0 and 100)
   */
  private Integer score;

  /**
   * The optional name scoring details.
   */
  private SzNameScoring nameScoringDetails;

  /**
   * Default constructor.
   */
  public SzSearchFeatureScore() {
    this.featureType        = null;
    this.inboundFeature     = null;
    this.candidateFeature   = null;
    this.score              = null;
    this.nameScoringDetails = null;
  }

  /**
   * Gets the feature type for the features being scored.
   *
   * @return The feature type for the features being scored.
   */
  public String getFeatureType() {
    return featureType;
  }

  /**
   * Sets the feature type for the features being scored.
   *
   * @param featureType The feature type for the features being scored.
   */
  public void setFeatureType(String featureType) {
    this.featureType = featureType;
  }

  /**
   * Gets the inbound feature value as a {@link String}.
   *
   * @return The inbound feature value as a {@link String}.
   */
  public String getInboundFeature() {
    return this.inboundFeature;
  }

  /**
   * Sets the inbound feature value as a {@link String}.
   *
   * @param inboundFeature The inbound feature value as a {@link String}.
   */
  public void setInboundFeature(String inboundFeature) {
    this.inboundFeature = inboundFeature;
  }

  /**
   * Gets the feature value of the candidate match for the inbound feature as
   * a {@link String}.
   *
   * @return The feature value of the candidate match for the inbound feature as
   *         a {@link String}.
   */
  public String getCandidateFeature() {
    return this.candidateFeature;
  }

  /**
   * Sets the feature value that describes the candidate match for the inbound
   * feature.
   *
   * @param candidateFeature The feature value that describes the candidate
   *                         match for the inbound feature.
   */
  public void setCandidateFeature(String candidateFeature) {
    this.candidateFeature = candidateFeature;
  }

  /**
   * Gets the integer score between the two feature values (typically from 0
   * and 100).  If the score has not been explicitly set, but the {@linkplain
   * #setNameScoringDetails(SzNameScoring) name scoring details} have been set
   * then this returns {@link SzNameScoring#asFullScore()}.
   *
   * @return The integer score between the two feature values (typically from 0
   *         and 100).
   */
  public Integer getScore() {
    if (this.score != null) return this.score;
    if (this.nameScoringDetails != null) {
      return this.nameScoringDetails.asFullScore();
    }
    return null;
  }

  /**
   * Sets the integer score between the two feature values (typically from 0
   * and 100).
   *
   * @param score The integer score between the two feature values (typically
   *              from 0 and 100).
   */
  public void setScore(Integer score) {
    this.score = score;
  }

  /**
   * Gets the name scoring details if any exist.  This method returns
   * <tt>null</tt> if the scored feature was not a name.
   *
   * @return The name scoring details, or <tt>null</tt> if the scored feature
   *         was not a name.
   */
  @JsonInclude(NON_EMPTY)
  public SzNameScoring getNameScoringDetails() {
    return this.nameScoringDetails;
  }

  /**
   * Sets the name scoring details if any exist.  Set the value to <tt>null</tt>
   * if the scored feature was not a name.
   *
   * @param scoring The {@link SzNameScoring} describing the name-scoring
   *                details.
   */
  public void setNameScoringDetails(SzNameScoring scoring) {
    this.nameScoringDetails = scoring;
  }

  /**
   * Parses the {@link SzSearchFeatureScore} from a {@link JsonObject}
   * describing JSON for the Senzing native API format for a feature score to
   * create a new instance.
   *
   * @param jsonObject The {@link JsonObject} to parse.
   *
   * @param featureType The feature type for the {@link SzSearchFeatureScore}.
   *
   * @return The {@link SzSearchFeatureScore} that was created.
   */
  public static SzSearchFeatureScore parseFeatureScore(JsonObject jsonObject,
                                                       String     featureType)
  {
    Integer score     = JsonUtils.getInteger(jsonObject, "FULL_SCORE");
    String  inbound   = JsonUtils.getString(jsonObject, "INBOUND_FEAT");
    String  candidate = JsonUtils.getString(jsonObject, "CANDIDATE_FEAT");

    SzNameScoring nameScoring = null;
    if (score == null || featureType.equalsIgnoreCase("NAME")) {
      nameScoring = SzNameScoring.parseNameScoring(jsonObject);
      if (score == null && nameScoring != null) {
        score = nameScoring.asFullScore();
      }
    }

    SzSearchFeatureScore result = new SzSearchFeatureScore();

    result.setFeatureType(featureType);
    result.setInboundFeature(inbound);
    result.setCandidateFeature(candidate);
    result.setScore(score);
    result.setNameScoringDetails(nameScoring);

    return result;
  }

  /**
   * Parses and populates a {@link List} of {@link SzSearchFeatureScore} instances
   * from a {@link JsonArray} of {@link JsonObject} instances describing JSON
   * for the Senzing native API format for feature scores to create new
   * instances.
   *
   * @param jsonArray The {@link JsonArray} to parse.
   *
   * @param featureType The feature type for the {@link SzSearchFeatureScore}
   *                    instances.
   *
   * @return The {@link List} of {@link SzSearchFeatureScore} instances that were
   *         populated.
   */
  public static List<SzSearchFeatureScore> parseFeatureScoreList(
      JsonArray   jsonArray,
      String      featureType)
  {
    return parseFeatureScoreList(null, jsonArray, featureType);
  }

  /**
   * Parses and populates a {@link List} of {@link SzSearchFeatureScore} instances
   * from a {@link JsonArray} of {@link JsonObject} instances describing JSON
   * for the Senzing native API format for candidate keys to create new
   * instances.
   *
   * @param list The {@link List} to populate or <tt>null</tt> if a new {@link
   *             List} should be created.
   *
   * @param jsonArray The {@link JsonArray} to parse.
   *
   * @param featureType The feature type for the {@link SzSearchFeatureScore}
   *                    instances.
   *
   * @return The {@link List} of {@link SzSearchFeatureScore} instances that were
   *         populated.
   */
  public static List<SzSearchFeatureScore> parseFeatureScoreList(
      List<SzSearchFeatureScore>  list,
      JsonArray             jsonArray,
      String                featureType)
  {
    if (list == null) {
      list = new ArrayList<>(jsonArray.size());
    }

    for (JsonObject jsonObject : jsonArray.getValuesAs(JsonObject.class)) {
      list.add(parseFeatureScore(jsonObject, featureType));
    }

    return list;
  }

}
