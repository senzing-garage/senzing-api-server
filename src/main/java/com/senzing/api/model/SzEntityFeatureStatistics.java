package com.senzing.api.model;

import com.senzing.util.JsonUtils;

import javax.json.JsonObject;

/**
 * Describes the entity resolution statistics for the feature value.
 */
public class SzEntityFeatureStatistics {
  /**
   * Indicates if the feature is used for finding candidates during entity
   * resolution.
   */
  private boolean usedForCandidates;

  /**
   * Indicates if the feature is used for scoring during entity resolution.
   */
  private Boolean usedForScoring;

  /**
   * The number of entities having this feature value.
   */
  private Long entityCount;

  /**
   * Indicates if this feature value is no longer being used to find candidates
   * because too many entities share the same value.
   */
  private Boolean candidateCapReached;

  /**
   * Indicates if this feature value is no longer being used in entity
   * scoring because too many entities share the same value.
   */
  private Boolean scoringCapReached;

  /**
   * Indicates if this value was suppressed in favor of a more complete value.
   */
  private Boolean suppressed;

  /**
   * Default constructor.
   */
  public SzEntityFeatureStatistics() {
    // do nothing
  }

  /**
   * Checks if the feature is used for finding candidates during entity
   * resolution.
   *
   * @return <tt>true</tt> if used for finding candidates during entity
   *         resolution, otherwise <tt>false</tt>.
   */
  public Boolean isUsedForCandidates() {
    return usedForCandidates;
  }

  /**
   * Sets whether or not the feature is used for finding candidates during
   * entity resolution.
   *
   * @param usedForCandidates <tt>true</tt> if used for finding candidates
   *                          during entity resolution, otherwise
   *                          <tt>false</tt>.
   */
  public void setUsedForCandidates(Boolean usedForCandidates) {
    this.usedForCandidates = usedForCandidates;
  }

  /**
   * Checks if the feature is used for scoring during entity resolution.
   *
   * @return <tt>true</tt> if used for scoring during entity resolution,
   *         otherwise <tt>false</tt>.
   */
  public Boolean isUsedForScoring() {
    return usedForScoring;
  }

  /**
   * Sets whether or not the feature is used for scoring during entity
   * resolution.
   *
   * @param usedForScoring <tt>true</tt> if used for scoring during entity
   *                       resolution, otherwise <tt>false</tt>.
   */
  public void setUsedForScoring(Boolean usedForScoring) {
    this.usedForScoring = usedForScoring;
  }

  /**
   * Gets the number of entities having this feature value.
   *
   * @return The number of entities having this feature value.
   */
  public Long getEntityCount() {
    return entityCount;
  }

  /**
   * Sets the number of entities having this feature value.
   *
   * @param entityCount The number of entities having this feature value.
   */
  public void setEntityCount(Long entityCount) {
    this.entityCount = entityCount;
  }

  /**
   * Checks if this feature value is no longer being used to find candidates
   * because too many entities share the same value.
   *
   * @return <tt>true</tt> if this feature value is no longer being used to
   *         find candidates because too many entities share the same value,
   *         otherwise <tt>false</tt>.
   */
  public Boolean isCandidateCapReached() {
    return candidateCapReached;
  }

  /**
   * Sets whether or not this feature value is no longer being used to find
   * candidates because too many entities share the same value.
   *
   * @param candidateCapReached <tt>true</tt> if this feature value is no longer
   *                            being used to find candidates because too many
   *                            entities share the same value, otherwise
   *                            <tt>false</tt>.
   */
  public void setCandidateCapReached(Boolean candidateCapReached) {
    this.candidateCapReached = candidateCapReached;
  }

  /**
   * Checks if this feature value is no longer being used in entity scoring
   * because too many entities share the same value.
   *
   * @return <tt>true</tt> if this feature value is no longer being used in
   *         entity scoring because too many entities share the same value,
   *         otherwise <tt>false</tt>.
   */
  public Boolean isScoringCapReached() {
    return scoringCapReached;
  }

  /**
   * Sets whether or not this feature value is no longer being used in entity
   * scoring because too many entities share the same value.
   *
   * @param scoringCapReached <tt>true</tt> if this feature value is no longer
   *                          being used in entity scoring because too many
   *                          entities share the same value, otherwise
   *                          <tt>false</tt>.
   */
  public void setScoringCapReached(Boolean scoringCapReached) {
    this.scoringCapReached = scoringCapReached;
  }

  /**
   * Checks if this value was suppressed in favor of a more complete value.
   *
   * @return <tt>true</tt> if this value was suppressed in favor of a more
   *         complete value, otherwise <tt>false</tt>.
   */
  public Boolean isSuppressed() {
    return suppressed;
  }

  /**
   * Sets whether or not this value was suppressed in favor of a more complete
   * value.
   *
   * @param suppressed <tt>true</tt> if this value was suppressed in favor of a
   *                   more complete value, otherwise <tt>false</tt>.
   */
  public void setSuppressed(Boolean suppressed) {
    this.suppressed = suppressed;
  }

  @Override
  public String toString() {
    return "SzEntityFeatureStatistics{" +
        "usedForCandidates=" + usedForCandidates +
        ", usedForScoring=" + usedForScoring +
        ", entityCount=" + entityCount +
        ", candidateCapReached=" + candidateCapReached +
        ", scoringCapReached=" + scoringCapReached +
        ", suppressed=" + suppressed +
        '}';
  }

  /**
   * Parses the native API Senzing JSON to create an instance of
   * {@link SzEntityFeatureStatistics} and returns <tt>null</tt> if none of
   * the statistics are available.
   *
   * @param jsonObject The {@link JsonObject} to parse.
   *
   * @return The {@link SzEntityFeatureStatistics} that was parsed.
   */
  public static SzEntityFeatureStatistics parseEntityFeatureStatistics(
      JsonObject            jsonObject)
  {
    Boolean candidateUse = getBoolean(jsonObject, "USED_FOR_CAND");
    Boolean scoringUse   = getBoolean(jsonObject, "USED_FOR_SCORING");
    Long    entityCount  = JsonUtils.getLong(jsonObject, "ENTITY_COUNT");
    Boolean candidateCap = getBoolean(jsonObject, "CANDIDATE_CAP_REACHED");
    Boolean scoringCap   = getBoolean(jsonObject, "SCORING_CAP_REACHED");
    Boolean suppressed   = getBoolean(jsonObject, "SUPPRESSED");

    // check if we have no stats
    if (candidateUse == null && scoringUse == null && entityCount == null
        && candidateCap == null && scoringCap == null && suppressed == null) {
      return null;
    }

    SzEntityFeatureStatistics statistics = new SzEntityFeatureStatistics();
    statistics.setUsedForCandidates(candidateUse);
    statistics.setUsedForScoring(scoringUse);
    statistics.setEntityCount(entityCount);
    statistics.setCandidateCapReached(candidateCap);
    statistics.setScoringCapReached(scoringCap);
    statistics.setSuppressed(suppressed);

    return statistics;
  }

  /**
   * Gets a {@link Boolean} value that is designated as <tt>"Y"</tt> for
   * <tt>true</tt> and <tt>"N"</tt> for <tt>false</tt>.
   *
   * @param jsonObject The {@link JsonObject} to obtain the value from.
   * @param key The property key to obtain the value for.
   * @return {@link Boolean#TRUE} if <tt>true</tt>, {@link Boolean#FALSE} if
   *         <tt>false</tt> and <tt>null</tt> if missing, <tt>null</tt> or
   *         empty string.
   */
  private static Boolean getBoolean(JsonObject jsonObject, String key) {
    String text = JsonUtils.getString(jsonObject, key);
    if (text == null || text.trim().length() == 0) return null;
    text = text.trim();
    return text.equals("Y");
  }

}
