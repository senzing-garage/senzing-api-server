package com.senzing.api.model;

import javax.json.JsonObject;
import java.util.Optional;
import java.util.function.Function;

public abstract class SzBaseRelatedEntity extends SzResolvedEntity {
  /**
   * The match level.
   */
  private int matchLevel;

  /**
   * The full name score.
   */
  private Integer fullNameScore;

  /**
   * The match score.
   */
  private Integer matchScore;

  /**
   * Whether or not the relationship is ambiguous.
   */
  private boolean ambiguous;

  /**
   * The match key for the relationship.
   */
  private String matchKey;

  /**
   * The resolution rule code.
   */
  private String resolutionRuleCode;

  /**
   * The ref score.
   */
  private int refScore;

  /**
   * Whether or not this related entity is partially populated.
   */
  private boolean partial;

  /**
   * Default constructor.
   */
  public SzBaseRelatedEntity() {
    this.matchLevel         = 0;
    this.fullNameScore      = 0;
    this.matchScore         = 0;
    this.ambiguous          = false;
    this.matchKey           = null;
    this.resolutionRuleCode = null;
    this.refScore           = 0;
    this.partial            = true;
  }

  /**
   * Gets the underlying match level from the entity resolution between the
   * entities.
   *
   * @return The underlying match level from the entity resolution between the
   *         entities.
   */
  public int getMatchLevel() {
    return this.matchLevel;
  }

  /**
   * Sets the underlying match level from the entity resolution between the
   * entities.
   *
   * @param matchLevel The underlying match level from the entity resolution
   *                   between the entities.
   */
  public void setMatchLevel(int matchLevel) {
    this.matchLevel = matchLevel;
  }

  /**
   * Gets the underlying full name score from the entity resolution between
   * the entities.
   *
   * @return The underlying full name score from the entity resolution between
   *         the entities.
   */
  public Integer getFullNameScore() {
    return this.fullNameScore;
  }

  /**
   * Sets the underlying full name score from the entity resolution between
   * the entities.
   *
   * @param fullNameScore The underlying full name score from the entity
   *                      resolution between the entities.
   */
  public void setFullNameScore(Integer fullNameScore) {
    this.fullNameScore = fullNameScore;
  }

  /**
   * Gets the underlying match score from the entity resolution between
   * the entities.
   *
   * @return The underlying match score from the entity resolution between
   *         the entities.
   */
  public Integer getMatchScore() {
    return this.matchScore;
  }

  /**
   * Sets the underlying match score from the entity resolution between
   * the entities.
   *
   * @param matchScore The underlying match score from the entity resolution
   *                   between the entities.
   */
  public void setMatchScore(Integer matchScore) {
    this.matchScore = matchScore;
  }

  /**
   * Checks whether or not the relationship between the entities is an
   * ambiguous possible match.
   *
   * @return <tt>true</tt> if the relationship is an ambiguous possible match,
   *         or <tt>false</tt> if not disclosed.
   */
  public boolean isAmbiguous() {
    return this.ambiguous;
  }

  /**
   * Sets whether or not the relationship between the entities is an
   * ambiguous possible match.
   *
   * @param ambiguous <tt>true</tt> if the relationship is an ambiguous
   *                  possible match, or <tt>false</tt> if not disclosed.
   */
  public void setAmbiguous(boolean ambiguous) {
    this.ambiguous = ambiguous;
  }

  /**
   * Gets the underlying match key from the entity resolution between
   * the entities.
   *
   * @return The underlying match key from the entity resolution between
   *         the entities.
   */
  public String getMatchKey() {
    return matchKey;
  }

  /**
   * Sets the underlying match key from the entity resolution between
   * the entities.
   *
   * @param matchKey The underlying match key from the entity resolution
   *                 between the entities.
   */
  public void setMatchKey(String matchKey) {
    this.matchKey = matchKey;
  }

  /**
   * Gets the underlying resolution rule code from the entity resolution
   * between the entities.
   *
   * @return The underlying resolution rule code from the entity resolution
   *         between the entities.
   */
  public String getResolutionRuleCode() {
    return resolutionRuleCode;
  }

  /**
   * Sets the underlying resolution rule code from the entity resolution
   * between the entities.
   *
   * @param code The underlying resolution rule code from the entity resolution
   *             between the entities.
   */
  public void setResolutionRuleCode(String code) {
    this.resolutionRuleCode = code;
  }

  /**
   * Gets the underlying ref score from the entity resolution between
   * the entities.
   *
   * @return The underlying ref score from the entity resolution between
   * the entities.
   */
  public int getRefScore() {
    return this.refScore;
  }

  /**
   * Sets the underlying ref score from the entity resolution between
   * the entities.
   *
   * @param refScore The underlying ref score from the entity resolution between
   *                 the entities.
   */
  public void setRefScore(int refScore) {
    this.refScore = refScore;
  }

  /**
   * Checks whether or not the related entity data is only partially populated.
   * If partially populated then it will not have complete features or records
   * and the record summaries will be missing the top record IDs.
   *
   * @return <tt>true</tt> if the related entity data is only partially
   *         populated, otherwise <tt>false</tt>.
   */
  public boolean isPartial() {
    return this.partial;
  }

  /**
   * Sets whether or not the related entity data is only partially populated.
   * If partially populated then it will not have complete features or records
   * and the record summaries will be missing the top record IDs.
   *
   * @param partial <tt>true</tt> if the related entity data is only partially
   *                populated, otherwise <tt>false</tt>.
   */
  public void setPartial(boolean partial) {
    this.partial = partial;
  }

  /**
   * Parses the entity feature from a {@link JsonObject} describing JSON
   * for the Senzing native API format for an entity feature and populates
   * the specified {@link SzBaseRelatedEntity} or creates a new instance.
   *
   * @param entity The {@link SzBaseRelatedEntity} instance to populate, or
   *               <tt>null</tt> if a new instance should be created.
   *
   * @param jsonObject The {@link JsonObject} describing the JSON in the
   *                   Senzing native API format.
   *
   * @param featureToAttrClassMapper Mapping function to map feature names to
   *                                 attribute classes.
   *
   * @return The populated (or created) {@link SzBaseRelatedEntity}.
   */
  protected static SzBaseRelatedEntity parseBaseRelatedEntity(
      SzBaseRelatedEntity     entity,
      JsonObject              jsonObject,
      Function<String,String> featureToAttrClassMapper)
  {
    Function<String,String> mapper = featureToAttrClassMapper;
    SzResolvedEntity.parseResolvedEntity(entity, jsonObject, mapper);

    int     matchLevel  = jsonObject.getJsonNumber("MATCH_LEVEL").intValue();
    int     refScore    = jsonObject.getJsonNumber("REF_SCORE").intValue();
    String  matchKey    = jsonObject.getString("MATCH_KEY");
    String  ruleCode    = jsonObject.getString("ERRULE_CODE");
    boolean partial     = (!jsonObject.containsKey("FEATURES")
                           || !jsonObject.containsKey("RECORDS"));

    Optional<Integer> nameScore = Optional.ofNullable(
        jsonObject.getJsonObject("MATCH_SCORES"))
          .map(o -> o.getJsonArray("NAME"))
          .map(a -> a.stream().map(v -> v.asJsonObject().getInt("GNR_FN")))
          .flatMap(s -> s.max(Integer::compareTo));

    Optional<Integer> matchScore
        = Optional.ofNullable(jsonObject.getValue("/MATCH_SCORE"))
          .map(o -> {
            switch(o.getValueType()) {
              case NUMBER:
                return jsonObject.getJsonNumber("MATCH_SCORE").intValue();
              case STRING:
                return Integer.parseInt(jsonObject.getString("MATCH_SCORE"));
              default:
                return null;
            }
          });

    entity.setMatchScore(matchScore.orElse(null));
    entity.setMatchLevel(matchLevel);
    entity.setMatchKey(matchKey);
    entity.setResolutionRuleCode(ruleCode);
    entity.setRefScore(refScore);

    if (jsonObject.containsKey("IS_AMBIGUOUS")) {
      boolean ambiguous = jsonObject.getInt("IS_AMBIGUOUS") != 0;
      entity.setAmbiguous(ambiguous);
    }

    entity.setFullNameScore(nameScore.orElse(null));
    entity.setPartial(partial);

    // iterate over the feature map
    return entity;
  }

  @Override
  public String toString() {
    return "SzRelatedEntity{" +
        super.toString() +
        ", matchLevel=" + matchLevel +
        ", fullNameScore=" + fullNameScore +
        ", matchScore=" + matchScore +
        ", ambiguous=" + ambiguous +
        ", matchKey='" + matchKey + '\'' +
        ", resolutionRuleCode='" + resolutionRuleCode + '\'' +
        ", refScore=" + refScore +
        ", partial=" + partial +
        '}';
  }
}
