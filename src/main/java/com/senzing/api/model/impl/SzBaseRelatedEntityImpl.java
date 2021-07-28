package com.senzing.api.model.impl;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.senzing.api.model.SzBaseRelatedEntity;
import com.senzing.api.model.SzResolvedEntity;
import com.senzing.util.JsonUtils;

import javax.json.JsonObject;
import java.util.Optional;
import java.util.function.Function;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;

/**
 * Provides a default implementation of the {@link SzBaseRelatedEntity}
 * functionality.
 */
public abstract class SzBaseRelatedEntityImpl
    extends SzResolvedEntityImpl implements SzBaseRelatedEntity
{
  /**
   * The match level.
   */
  private Integer matchLevel;

  /**
   * The match score.
   */
  private Integer matchScore;

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
  private Integer refScore;

  /**
   * Default constructor.
   */
  public SzBaseRelatedEntityImpl() {
    this.matchLevel         = null;
    this.matchScore         = null;
    this.matchKey           = null;
    this.resolutionRuleCode = null;
    this.refScore           = null;
  }

  /**
   * Gets the underlying match level from the entity resolution between the
   * entities.
   *
   * @return The underlying match level from the entity resolution between the
   *         entities.
   */
  @JsonInclude(NON_NULL)
  public Integer getMatchLevel() {
    return this.matchLevel;
  }

  /**
   * Sets the underlying match level from the entity resolution between the
   * entities.
   *
   * @param matchLevel The underlying match level from the entity resolution
   *                   between the entities.
   */
  public void setMatchLevel(Integer matchLevel) {
    this.matchLevel = matchLevel;
  }

  /**
   * Gets the underlying match score from the entity resolution between
   * the entities.
   *
   * @return The underlying match score from the entity resolution between
   *         the entities.
   */
  @JsonInclude(NON_NULL)
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
   * Gets the underlying match key from the entity resolution between
   * the entities.
   *
   * @return The underlying match key from the entity resolution between
   *         the entities.
   */
  @JsonInclude(NON_NULL)
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
  @JsonInclude(NON_NULL)
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
   *         the entities.
   */
  @JsonInclude(NON_NULL)
  public Integer getRefScore() {
    return this.refScore;
  }

  /**
   * Sets the underlying ref score from the entity resolution between
   * the entities.
   *
   * @param refScore The underlying ref score from the entity resolution between
   *                 the entities.
   */
  public void setRefScore(Integer refScore) {
    this.refScore = refScore;
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName() + "{" +
        "matchLevel=" + matchLevel +
        ", matchScore=" + matchScore +
        ", matchKey='" + matchKey + '\'' +
        ", resolutionRuleCode='" + resolutionRuleCode + '\'' +
        ", refScore=" + refScore +
        ", super=" + super.toString() +
        '}';
  }
}
