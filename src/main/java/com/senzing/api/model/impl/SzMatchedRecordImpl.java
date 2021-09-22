package com.senzing.api.model.impl;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.senzing.api.model.SzMatchedRecord;

/**
 * Provides a default implementation of {@link SzMatchedRecord}.
 */
@JsonDeserialize
public class SzMatchedRecordImpl extends SzEntityRecordImpl
    implements SzMatchedRecord
{
  /**
   * The match level.
   */
  private Integer matchLevel;

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
  public SzMatchedRecordImpl() {
    super();
    this.matchKey           = null;
    this.resolutionRuleCode = null;
    this.matchLevel         = null;
    this.refScore           = null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Integer getMatchLevel() {
    return matchLevel;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setMatchLevel(Integer matchLevel) {
    this.matchLevel = matchLevel;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getMatchKey() {
    return matchKey;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setMatchKey(String matchKey) {
    this.matchKey = matchKey;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getResolutionRuleCode() {
    return resolutionRuleCode;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setResolutionRuleCode(String resolutionRuleCode) {
    this.resolutionRuleCode = resolutionRuleCode;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Integer getRefScore() {
    return refScore;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setRefScore(Integer refScore) {
    this.refScore = refScore;
  }

  @Override
  public String toString() {
    return "SzMatchedRecord{" + this.fieldsToString() + "}";
  }

  @Override
  protected String fieldsToString() {
    return super.fieldsToString() +
        ", matchLevel=" + matchLevel +
        ", matchKey='" + matchKey + '\'' +
        ", resolutionRuleCode='" + resolutionRuleCode + '\'' +
        ", refScore=" + refScore;
  }
}
