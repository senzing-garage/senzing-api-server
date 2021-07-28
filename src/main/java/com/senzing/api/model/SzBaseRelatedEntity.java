package com.senzing.api.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.senzing.util.JsonUtils;

import javax.json.JsonObject;
import java.util.Optional;
import java.util.function.Function;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;

/**
 * Describes the base features for a related entity.
 */
public interface SzBaseRelatedEntity extends SzResolvedEntity {
  /**
   * Gets the underlying match level from the entity resolution between the
   * entities.
   *
   * @return The underlying match level from the entity resolution between the
   *         entities.
   */
  @JsonInclude(NON_NULL)
  Integer getMatchLevel();

  /**
   * Sets the underlying match level from the entity resolution between the
   * entities.
   *
   * @param matchLevel The underlying match level from the entity resolution
   *                   between the entities.
   */
  void setMatchLevel(Integer matchLevel);

  /**
   * Gets the underlying match score from the entity resolution between
   * the entities.
   *
   * @return The underlying match score from the entity resolution between
   *         the entities.
   */
  @JsonInclude(NON_NULL)
  Integer getMatchScore();

  /**
   * Sets the underlying match score from the entity resolution between
   * the entities.
   *
   * @param matchScore The underlying match score from the entity resolution
   *                   between the entities.
   */
  void setMatchScore(Integer matchScore);

  /**
   * Gets the underlying match key from the entity resolution between
   * the entities.
   *
   * @return The underlying match key from the entity resolution between
   *         the entities.
   */
  @JsonInclude(NON_NULL)
  String getMatchKey();

  /**
   * Sets the underlying match key from the entity resolution between
   * the entities.
   *
   * @param matchKey The underlying match key from the entity resolution
   *                 between the entities.
   */
  void setMatchKey(String matchKey);

  /**
   * Gets the underlying resolution rule code from the entity resolution
   * between the entities.
   *
   * @return The underlying resolution rule code from the entity resolution
   *         between the entities.
   */
  @JsonInclude(NON_NULL)
  String getResolutionRuleCode();

  /**
   * Sets the underlying resolution rule code from the entity resolution
   * between the entities.
   *
   * @param code The underlying resolution rule code from the entity resolution
   *             between the entities.
   */
  void setResolutionRuleCode(String code);

  /**
   * Gets the underlying ref score from the entity resolution between
   * the entities.
   *
   * @return The underlying ref score from the entity resolution between
   *         the entities.
   */
  @JsonInclude(NON_NULL)
  Integer getRefScore();

  /**
   * Sets the underlying ref score from the entity resolution between
   * the entities.
   *
   * @param refScore The underlying ref score from the entity resolution between
   *                 the entities.
   */
  void setRefScore(Integer refScore);

  /**
   * Parses the entity feature from a {@link JsonObject} describing JSON
   * for the Senzing native API format for an entity feature and populates
   * the specified {@link SzBaseRelatedEntity} or creates a new instance.
   *
   * @param entity The {@link SzBaseRelatedEntity} instance to populate, (this
   *               cannot be <tt>null</tt>).
   *
   * @param jsonObject The {@link JsonObject} describing the JSON in the
   *                   Senzing native API format.
   *
   * @param featureToAttrClassMapper Mapping function to map feature names to
   *                                 attribute classes.
   *
   * @return The populated (or created) {@link SzBaseRelatedEntity}.
   *
   * @throws NullPointerException If the specified entity or JSON object is
   *                              <tt>null</tt>.
   */
  static SzBaseRelatedEntity parseBaseRelatedEntity(
      SzBaseRelatedEntity     entity,
      JsonObject              jsonObject,
      Function<String,String> featureToAttrClassMapper)
    throws NullPointerException
  {
    Function<String,String> mapper = featureToAttrClassMapper;

    // check if we have a MATCH_INFO object and if so use it for match information
    JsonObject matchInfo = JsonUtils.getJsonObject(jsonObject,"MATCH_INFO");

    // check if we have a RESOLVED_ENTITY object and if so use it for other fields
    JsonObject entityObject = JsonUtils.getJsonObject(jsonObject, "ENTITY");
    JsonObject resolvedObject
        = JsonUtils.getJsonObject(entityObject, "RESOLVED_ENTITY");
    if (resolvedObject != null) {
      jsonObject = resolvedObject;
    }

    SzResolvedEntity.parseResolvedEntity(entity, jsonObject, mapper);

    // if no match info, then assume the data is in the base object
    if (matchInfo == null) matchInfo = jsonObject;

    Integer matchLevel  = JsonUtils.getInteger(matchInfo, "MATCH_LEVEL");
    Integer refScore    = JsonUtils.getInteger(matchInfo, "REF_SCORE");
    String  matchKey    = JsonUtils.getString(matchInfo, "MATCH_KEY");
    String  ruleCode    = JsonUtils.getString(matchInfo,"ERRULE_CODE");
    boolean partial     = (!jsonObject.containsKey("FEATURES")
                           || !jsonObject.containsKey("RECORDS")
                           || (matchLevel == null)
                           || (refScore == null)
                           || (matchKey == null)
                           || (ruleCode == null));

    final JsonObject matchObject = matchInfo;
    Optional<Integer> matchScore = readMatchScore(matchObject);

    if (!matchScore.isPresent()) partial = true;
    entity.setMatchScore(matchScore.orElse(null));
    entity.setMatchLevel(matchLevel);
    entity.setMatchKey(matchKey);
    entity.setResolutionRuleCode(ruleCode);
    entity.setRefScore(refScore);

    entity.setPartial(partial);

    // iterate over the feature map
    return entity;
  }

  /**
   * Reads a MATCH_SCORE field from native Senzing JSON response.
   *
   * @param jsonObject The {@link JsonObject} to read the value from.
   *
   * @return The {@link Optional<Integer>} representing the match score.
   */
  static Optional<Integer> readMatchScore(JsonObject jsonObject) {
    return Optional.ofNullable(JsonUtils.getJsonValue(jsonObject, "MATCH_SCORE"))
        .map(o -> {
          switch (o.getValueType()) {
            case NUMBER:
              return jsonObject.getJsonNumber("MATCH_SCORE").intValue();
            case STRING:
              // check for empty string
              if (jsonObject.getString("MATCH_SCORE").trim().length() == 0) {
                // empty string is the same as null
                return null;
              } else {
                // if not an empty string then parse as an integer
                return Integer.parseInt(jsonObject.getString("MATCH_SCORE"));
              }
            default:
              return null;
          }
        });
  }
}
