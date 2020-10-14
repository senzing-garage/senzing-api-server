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
public class SzFeatureScore {
  /**
   * The feature type of the features being scored.
   */
  private String featureType;

  /**
   * The inbound feature described as an {@link SzScoredFeature}.
   */
  private SzScoredFeature inboundFeature;

  /**
   * The feature that was a candidate match for the inbound feature (also
   * described as an {@link SzScoredFeature}).
   */
  private SzScoredFeature candidateFeature;

  /**
   * The integer score between the two feature values (typically from 0 and 100)
   */
  private Integer score;

  /**
   * The optional name scoring details.
   */
  private SzNameScoring nameScoringDetails;

  /**
   * The {@link SzScoringBucket} describing the meaning of the score.
   */
  private SzScoringBucket scoringBucket;

  /**
   * The {@link SzScoringBehavior} describing the scoring behavior for the
   * features.
   */
  private SzScoringBehavior scoringBehavior;

  /**
   * Default constructor.
   */
  public SzFeatureScore() {
    this.featureType        = null;
    this.inboundFeature     = null;
    this.candidateFeature   = null;
    this.score              = null;
    this.nameScoringDetails = null;
    this.scoringBucket      = null;
    this.scoringBehavior    = null;
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
   * Gets the inbound feature described as an {@link SzScoredFeature}.
   *
   * @return The inbound feature described as an {@link SzScoredFeature}.
   */
  public SzScoredFeature getInboundFeature() {
    return inboundFeature;
  }

  /**
   * Sets the inbound feature described as an {@link SzScoredFeature}.
   *
   * @param inboundFeature The inbound feature described as an {@link
   *                       SzScoredFeature}.
   */
  public void setInboundFeature(SzScoredFeature inboundFeature) {
    this.inboundFeature = inboundFeature;
  }

  /**
   * Gets the {@link SzScoredFeature} that describes the candidate match for
   * the inbound feature.
   *
   * @return The {@link SzScoredFeature} that describes the candidate match for
   *         the inbound feature.
   */
  public SzScoredFeature getCandidateFeature() {
    return candidateFeature;
  }

  /**
   * Sets the {@link SzScoredFeature} that describes the candidate match for
   * the inbound feature.
   *
   * @param candidateFeature The {@link SzScoredFeature} that describes the
   *                         candidate match for the inbound feature.
   */
  public void setCandidateFeature(SzScoredFeature candidateFeature) {
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
   * Gets the {@link SzScoringBucket} describing the meaning of the score.
   *
   * @return The {@link SzScoringBucket} describing the meaning of the score.
   */
  public SzScoringBucket getScoringBucket() {
    return scoringBucket;
  }

  /**
   * Sets the {@link SzScoringBucket} describing the meaning of the score.
   *
   * @param scoringBucket The {@link SzScoringBucket} describing the meaning
   *                      of the score.
   */
  public void setScoringBucket(SzScoringBucket scoringBucket) {
    this.scoringBucket = scoringBucket;
  }

  /**
   * Gets the {@link SzScoringBehavior} describing the scoring behavior for the
   * features.
   *
   * @return The {@link SzScoringBehavior} describing the scoring behavior for
   *         the features.
   */
  public SzScoringBehavior getScoringBehavior() {
    return scoringBehavior;
  }

  /**
   * Sets the {@link SzScoringBehavior} describing the scoring behavior for the
   * features.
   *
   * @param scoringBehavior The {@link SzScoringBehavior} describing the scoring
   *                        behavior for the features.
   */
  public void setScoringBehavior(SzScoringBehavior scoringBehavior) {
    this.scoringBehavior = scoringBehavior;
  }

  /**
   * Parses the {@link SzFeatureScore} from a {@link JsonObject} describing JSON
   * for the Senzing native API format for a feature score to create a new
   * instance.
   *
   * @param jsonObject The {@link JsonObject} to parse.
   *
   * @param featureType The feature type for the {@link SzFeatureScore}.
   *
   * @return The {@link SzFeatureScore} that was created.
   */
  public static SzFeatureScore parseFeatureScore(JsonObject jsonObject,
                                                 String     featureType)
  {
    SzScoredFeature inboundFeature = SzScoredFeature.parseScoredFeature(
        jsonObject, "INBOUND_", featureType);

    SzScoredFeature candidateFeature = SzScoredFeature.parseScoredFeature(
        jsonObject, "CANDIDATE_", featureType);

    String  bucket    = JsonUtils.getString(jsonObject, "SCORE_BUCKET");
    String  behavior  = JsonUtils.getString(jsonObject, "SCORE_BEHAVIOR");
    Integer score     = JsonUtils.getInteger(jsonObject, "FULL_SCORE");

    SzNameScoring nameScoring = null;
    if (score == null || featureType.equalsIgnoreCase("NAME")) {
      nameScoring = SzNameScoring.parseNameScoring(jsonObject);
      if (score == null && nameScoring != null) {
        score = nameScoring.asFullScore();
      }
    }

    SzScoringBucket scoringBucket = null;
    try {
      scoringBucket = SzScoringBucket.valueOf(bucket);
    } catch (Exception e) {
      System.err.println("FAILED TO PARSE SCORE_BUCKET: " + bucket);
      e.printStackTrace();
    }
    SzScoringBehavior scoringBehavior = null;
    try {
      scoringBehavior = SzScoringBehavior.parse(behavior);
    } catch (Exception e) {
      System.err.println("FAILED TO PARSE SCORE_BEHAVIOR: " + behavior);
      e.printStackTrace();
    }

    SzFeatureScore result = new SzFeatureScore();

    result.setFeatureType(featureType);
    result.setInboundFeature(inboundFeature);
    result.setCandidateFeature(candidateFeature);
    result.setScore(score);
    result.setNameScoringDetails(nameScoring);
    result.setScoringBehavior(scoringBehavior);
    result.setScoringBucket(scoringBucket);

    return result;
  }

  /**
   * Parses and populates a {@link List} of {@link SzFeatureScore} instances
   * from a {@link JsonArray} of {@link JsonObject} instances describing JSON
   * for the Senzing native API format for feature scores to create new
   * instances.
   *
   * @param jsonArray The {@link JsonArray} to parse.
   *
   * @param featureType The feature type for the {@link SzFeatureScore}
   *                    instances.
   *
   * @return The {@link List} of {@link SzFeatureScore} instances that were
   *         populated.
   */
  public static List<SzFeatureScore> parseFeatureScoreList(
      JsonArray   jsonArray,
      String      featureType)
  {
    return parseFeatureScoreList(null, jsonArray, featureType);
  }

  /**
   * Parses and populates a {@link List} of {@link SzFeatureScore} instances
   * from a {@link JsonArray} of {@link JsonObject} instances describing JSON
   * for the Senzing native API format for candidate keys to create new
   * instances.
   *
   * @param list The {@link List} to populate or <tt>null</tt> if a new {@link
   *             List} should be created.
   *
   * @param jsonArray The {@link JsonArray} to parse.
   *
   * @param featureType The feature type for the {@link SzFeatureScore}
   *                    instances.
   *
   * @return The {@link List} of {@link SzFeatureScore} instances that were
   *         populated.
   */
  public static List<SzFeatureScore> parseFeatureScoreList(
      List<SzFeatureScore>  list,
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
