package com.senzing.api.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.senzing.util.JsonUtils;

import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonValue;
import java.util.*;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_EMPTY;
import static com.senzing.api.model.SzMatchLevel.*;

/**
 * The match info describing why two entities (or records) resolve or
 * relate to one another.
 */
public class SzMatchInfo {
  /**
   * The why key indicating the components of the match (similar to the
   * match key).
   */
  private String whyKey;

  /**
   * The match level describing how the two records resolve against each other.
   */
  private SzMatchLevel matchLevel;

  /**
   * The resolution rule that triggered the match.
   */
  private String resolutionRule;

  /**
   * The {@link Map} of {@link String} feature type keys to {@link List}
   * values of {@link SzCandidateKey} instances.
   */
  private Map<String, List<SzCandidateKey>> candidateKeys;

  /**
   * The {@link Map} of {@link String} feature type keys to <b>unmodifiable</b>
   * {@link List} values of {@link SzCandidateKey} instances.
   */
  private Map<String, List<SzCandidateKey>> candidateKeyViews;

  /**
   * The {@link Map} of {@link String} feature type keys to {@link List}
   * values of {@link SzFeatureScore} instances.
   */
  private Map<String, List<SzFeatureScore>> featureScores;

  /**
   * The {@link Map} of {@link String} feature type keys to <b>unmodifiable</b>
   * {@link List} values of {@link SzFeatureScore} instances.
   */
  private Map<String, List<SzFeatureScore>> featureScoreViews;

  /**
   * Default constructor.
   */
  public SzMatchInfo() {
    this.whyKey             = null;
    this.matchLevel         = null;
    this.resolutionRule     = null;
    this.candidateKeys      = new LinkedHashMap<>();
    this.candidateKeyViews  = new LinkedHashMap<>();
    this.featureScores      = new LinkedHashMap<>();
    this.featureScoreViews  = new LinkedHashMap<>();
  }

  /**
   * Gets the why key indicating the components of the match (similar to the
   * match key).
   *
   * @return The why key indicating the components of the match.
   */
  public String getWhyKey() {
    return this.whyKey;
  }

  /**
   * Sets the why key indicating the components of the match (similar to the
   * match key).
   *
   * @param whyKey The why key indicating the components of the match.
   */
  public void setWhyKey(String whyKey) {
    this.whyKey = whyKey;
  }

  /**
   * Returns the {@link SzMatchLevel} describing how the records resolve
   * against each other.
   *
   * @return The {@link SzMatchLevel} describing how the records resolve
   *         against each other.
   */
  public SzMatchLevel getMatchLevel() {
    return this.matchLevel;
  }

  /**
   * Sets the {@link SzMatchLevel} describing how the records resolve
   * against each other.
   *
   * @param matchLevel The {@link SzMatchLevel} describing how the records
   *                   resolve against each other.
   */
  public void setMatchLevel(SzMatchLevel matchLevel) {
    this.matchLevel = matchLevel;
  }

  /**
   * Gets the resolution rule that triggered the match.
   *
   * @return The resolution rule that triggered the match.
   */
  @JsonInclude(NON_EMPTY)
  public String getResolutionRule() {
    return this.resolutionRule;
  }

  /**
   * Sets the resolution rule that triggered the match.
   *
   * @param resolutionRule The resolution rule that triggered the match.
   */
  public void setResolutionRule(String resolutionRule) {
    this.resolutionRule = resolutionRule;
  }

  /**
   * Gets the <b>unmodifiable</b> {@link Map} of {@link String} feature type
   * keys to <b>unmodifiable</b> {@link List} values containing instances of
   * {@link SzCandidateKey} describing the candidate keys for that type.
   *
   * @return The <b>unmodifiable</b> {@link Map} of {@link String} feature type
   *         keys to <b>unmodifiable</b> {@link List} values containing
   *         instances of {@link SzCandidateKey} describing the candidate keys
   *         for that type.
   */
  @JsonInclude(NON_EMPTY)
  public Map<String, List<SzCandidateKey>> getCandidateKeys() {
    if (this.candidateKeyViews.size() == 0) return null;
    return Collections.unmodifiableMap(this.candidateKeyViews);
  }

  /**
   * Adds the specified {@link SzCandidateKey} to this instance.
   *
   * @param candidateKey The {@link SzCandidateKey} to add.
   */
  public void addCandidateKey(SzCandidateKey candidateKey) {
    String                featureType = candidateKey.getFeatureType();
    List<SzCandidateKey>  list        = this.candidateKeys.get(featureType);

    // check if the list does not exist
    if (list == null) {
      list = new LinkedList<>();
      List<SzCandidateKey> listView = Collections.unmodifiableList(list);

      this.candidateKeys.put(featureType, list);
      this.candidateKeyViews.put(featureType, listView);
    }

    // add to the list
    list.add(candidateKey);
  }

  /**
   * Private setter for JSON marshalling.  The specified {@link Map} will be
   * copied.
   *
   * @param candidateKeys The {@link Map} of {@link String} feature type
   *                      keys to {@link SzCandidateKey} values.
   */
  private void setCandidateKeys(Map<String, List<SzCandidateKey>> candidateKeys)
  {
    this.candidateKeys.clear();
    this.candidateKeyViews.clear();
    if (candidateKeys == null) return;
    candidateKeys.entrySet().forEach(entry -> {
      String                featureType = entry.getKey();
      List<SzCandidateKey>  list        = entry.getValue();

      List<SzCandidateKey> listCopy = new LinkedList<>();
      List<SzCandidateKey> listView = Collections.unmodifiableList(listCopy);
      listCopy.addAll(list);

      this.candidateKeys.put(featureType, listCopy);
      this.candidateKeyViews.put(featureType, listView);
    });
  }

  /**
   * Gets the <b>unmodifiable</b> {@link Map} of {@link String} feature type
   * keys to <b>unmodifiable</b> {@link List} values containing instances of
   * {@link SzFeatureScore} describing the feature scores for that type.
   *
   * @return The <b>unmodifiable</b> {@link Map} of {@link String} feature type
   *         keys to <b>unmodifiable</b> {@link List} values contianing
   *         instances of {@link SzFeatureScore} describing the feature scores
   *         for that type.
   */
  @JsonInclude(NON_EMPTY)
  public Map<String, List<SzFeatureScore>> getFeatureScores() {
    if (this.featureScoreViews.size() == 0) return null;
    return Collections.unmodifiableMap(this.featureScoreViews);
  }

  /**
   * Adds the specified {@link SzFeatureScore} to this instance.
   *
   * @param featureScore The {@link SzFeatureScore} to add.
   */
  public void addFeatureScore(SzFeatureScore featureScore) {
    String                featureType = featureScore.getFeatureType();
    List<SzFeatureScore>  list        = this.featureScores.get(featureType);

    // check if the list does not exist
    if (list == null) {
      list = new LinkedList<>();
      List<SzFeatureScore> listView = Collections.unmodifiableList(list);

      this.featureScores.put(featureType, list);
      this.featureScoreViews.put(featureType, listView);
    }

    // add to the list
    list.add(featureScore);
  }

  /**
   * Private setter for JSON marshalling.  The specified {@link Map} will be
   * copied.
   *
   * @param featureScores The {@link Map} of {@link String} feature type
   *                      keys to {@link SzFeatureScore} values.
   */
  private void setFeatureScores(Map<String, List<SzFeatureScore>> featureScores)
  {
    this.featureScores.clear();
    this.featureScoreViews.clear();
    if (featureScores == null) return;
    featureScores.entrySet().forEach(entry -> {
      String                featureType = entry.getKey();
      List<SzFeatureScore>  list        = entry.getValue();

      List<SzFeatureScore> listCopy = new LinkedList<>();
      List<SzFeatureScore> listView = Collections.unmodifiableList(listCopy);
      listCopy.addAll(list);

      this.featureScores.put(featureType, listCopy);
      this.featureScoreViews.put(featureType, listView);
    });
  }

  /**
   * Parses the native API JSON to build an instance of {@link SzMatchInfo}.
   *
   * @param jsonObject The {@link JsonObject} describing the match info using
   *                   the native API JSON format.
   *
   * @return The created instance of {@link SzMatchInfo}.
   */
  public static SzMatchInfo parseMatchInfo(JsonObject jsonObject)
  {
    SzMatchInfo matchInfo = new SzMatchInfo();

    String whyKey   = JsonUtils.getString(jsonObject, "WHY_KEY");
    if (whyKey != null && whyKey.trim().length() == 0) whyKey = null;

    SzMatchLevel matchLevel = NO_MATCH;
    String matchLevelCode
        = JsonUtils.getString(jsonObject, "MATCH_LEVEL_CODE");
    if (matchLevelCode != null && matchLevelCode.trim().length() > 0) {
      matchLevel = SzMatchLevel.valueOf(matchLevelCode);
    }

    String ruleCode = JsonUtils.getString(jsonObject, "WHY_ERRULE_CODE");
    if (ruleCode != null && ruleCode.trim().length() == 0) ruleCode = null;

    JsonObject candidateKeysObject
        = JsonUtils.getJsonObject(jsonObject, "CANDIDATE_KEYS");

    Map<String, List<SzCandidateKey>> candidateKeyMap
        = new LinkedHashMap<>();

    candidateKeysObject.entrySet().forEach(entry -> {
      String featureType = entry.getKey();

      JsonValue jsonValue = entry.getValue();

      JsonArray jsonArray = jsonValue.asJsonArray();

      List<SzCandidateKey> candidateKeys
          = SzCandidateKey.parseCandidateKeyList(jsonArray, featureType);

      candidateKeyMap.put(featureType,  candidateKeys);
    });

    JsonObject featureScoresObject
        = JsonUtils.getJsonObject(jsonObject, "FEATURE_SCORES");

    Map<String, List<SzFeatureScore>> featureScoreMap
        = new LinkedHashMap<>();

    featureScoresObject.entrySet().forEach(entry -> {
      String featureType = entry.getKey();

      JsonValue jsonValue = entry.getValue();

      JsonArray jsonArray = jsonValue.asJsonArray();

      List<SzFeatureScore> featureScores
          = SzFeatureScore.parseFeatureScoreList(jsonArray, featureType);

      featureScoreMap.put(featureType, featureScores);
    });

    SzMatchInfo result = new SzMatchInfo();

    result.setWhyKey(whyKey);
    result.setMatchLevel(matchLevel);
    result.setResolutionRule(ruleCode);
    result.setCandidateKeys(candidateKeyMap);
    result.setFeatureScores(featureScoreMap);

    return result;
  }

}
