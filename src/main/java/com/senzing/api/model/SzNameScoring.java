package com.senzing.api.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.senzing.util.JsonUtils;

import javax.json.JsonObject;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_EMPTY;

/**
 * Describes the various scoring values between two name feature values.
 */
public class SzNameScoring {
  /**
   * The full name score, or <tt>null</tt> if no full name score.
   */
  private Integer fullNameScore;

  /**
   * The surname score, or <tt>null</tt> if no surname score.
   */
  private Integer surnameScore;

  /**
   * The given name score, or <tt>null</tt> if no given name score.
   */
  private Integer givenNameScore;

  /**
   * The generation match score, or <tt>null</tt> if no generation match score.
   */
  private Integer generationScore;

  /**
   * The organization name score, or <tt>null</tt> if no organization name
   * score.
   */
  private Integer orgNameScore;

  /**
   * Default constructor.
   */
  public SzNameScoring() {
    this.fullNameScore    = null;
    this.surnameScore     = null;
    this.givenNameScore   = null;
    this.generationScore  = null;
    this.orgNameScore     = null;
  }

  /**
   * Gets the full name score if one exists.  This method returns <tt>null</tt>
   * if there is no full name score.
   *
   * @return The full name score, or <tt>null</tt> if there is no full name
   *         score.
   */
  @JsonInclude(NON_EMPTY)
  public Integer getFullNameScore() {
    return fullNameScore;
  }

  /**
   * Sets the full name score if one exists.  Set the value to <tt>null</tt>
   * if there is no full name score.
   *
   * @param score The full name score, or <tt>null</tt> if there is no full
   *              name score.
   */
  public void setFullNameScore(Integer score) {
    this.fullNameScore = score;
  }

  /**
   * Gets the surname score if one exists.  This method returns <tt>null</tt>
   * if there is no surname score.
   *
   * @return The surname score, or <tt>null</tt> if there is no surname score.
   */
  @JsonInclude(NON_EMPTY)
  public Integer getSurnameScore() {
    return surnameScore;
  }

  /**
   * Sets the surname score if one exists.  Set the value to <tt>null</tt>
   * if there is no surname score.
   *
   * @param score The surname score, or <tt>null</tt> if there is no surname
   *              score.
   */
  public void setSurnameScore(Integer score) {
    this.surnameScore = score;
  }

  /**
   * Gets the given name score if one exists.  This method returns <tt>null</tt>
   * if there is no given name score.
   *
   * @return The given name score, or <tt>null</tt> if there is no given name
   *         score.
   */
  @JsonInclude(NON_EMPTY)
  public Integer getGivenNameScore() {
    return givenNameScore;
  }

  /**
   * Sets the given name score if one exists.  Set the value to <tt>null</tt>
   * if there is no given name score.
   *
   * @param score The given name score, or <tt>null</tt> if there is no
   *              given name score.
   */
  public void setGivenNameScore(Integer score) {
    this.givenNameScore = score;
  }

  /**
   * Gets the generation match score if one exists.  This method returns
   * <tt>null</tt> if there is no generation match score.
   *
   * @return The generation match score, or <tt>null</tt> if there is no
   *         generation match score.
   */
  @JsonInclude(NON_EMPTY)
  public Integer getGenerationScore() {
    return generationScore;
  }

  /**
   * Sets the generation match score if one exists.  Set the value to
   * <tt>null</tt> if there is no generation match score.
   *
   * @param score The generation match score, or <tt>null</tt> if there is no
   *              generation match score.
   */
  public void setGenerationScore(Integer score) {
    this.generationScore = score;
  }

  /**
   * Gets the organization name score if one exists.  This method returns
   * <tt>null</tt> if there is no organization name score.
   *
   * @return The organization name score, or <tt>null</tt> if there is no
   *         organization name score.
   */
  @JsonInclude(NON_EMPTY)
  public Integer getOrgNameScore() {
    return orgNameScore;
  }

  /**
   * Sets the organization name score if one exists.  Set the value to
   * <tt>null</tt> if there is no organization name score.
   *
   * @param score The organization name score, or <tt>null</tt> if there is no
   *              organization name score.
   */
  public void setOrgNameScore(Integer score) {
    this.orgNameScore = score;
  }

  @Override
  public String toString() {
    return "SzNameScoring{" +
        "fullNameScore=" + fullNameScore +
        ", surnameScore=" + surnameScore +
        ", givenNameScore=" + givenNameScore +
        ", generationScore=" + generationScore +
        ", orgNameScore=" + orgNameScore +
        '}';
  }

  /**
   * Converts the value into the best value for a "full score".  This looks for
   * values in the follow fields and returns them if not <tt>null</tt> in the
   * following order of precedence:
   * <ol>
   *   <li>{@link #getOrgNameScore()}</li>
   *   <li>{@link #getFullNameScore()}</li>
   *   <li>{@link #getSurnameScore()}</li>
   *   <li>{@link #getGivenNameScore()}</li>
   * </ol>
   *
   * @return The "best" {@link Integer} value for the full score.
   */
  public Integer asFullScore() {
    if (this.orgNameScore != null)    return this.orgNameScore;
    if (this.fullNameScore != null)   return this.fullNameScore;
    if (this.surnameScore != null)    return this.surnameScore;
    if (this.givenNameScore != null)  return this.givenNameScore;
    return null;
  }

  /**
   * Parses the {@link SzNameScoring} from a {@link JsonObject} describing JSON
   * for the Senzing native API format for a name score to create a new
   * instance.
   *
   * @param jsonObject The {@link JsonObject} to parse.
   *
   * @return The {@link SzFeatureScore} that was created.
   */
  public static SzNameScoring parseNameScoring(JsonObject jsonObject) {
    Integer fnScore  = JsonUtils.getInteger(jsonObject, "GNR_FN");
    Integer snScore  = JsonUtils.getInteger(jsonObject, "GNR_SN");
    Integer gnScore  = JsonUtils.getInteger(jsonObject, "GNR_GN");
    Integer genScore = JsonUtils.getInteger(jsonObject, "GENERATION_MATCH");
    Integer orgScore = JsonUtils.getInteger(jsonObject, "GNR_ON");

    // check if there are no name scoring values (we do this before converting
    // negative values to null -- checking for missing JSON properties)
    if ((fnScore == null) && (snScore == null) && (gnScore == null)
        && (genScore == null) && (orgScore == null)) {
      return null;
    }

    // check for negative values and set to null if found
    if (fnScore < 0)  fnScore = null;
    if (snScore < 0)  snScore = null;
    if (gnScore < 0)  gnScore = null;
    if (genScore < 0) genScore = null;
    if (orgScore < 0) orgScore = null;

    // construct the result
    SzNameScoring result = new SzNameScoring();

    // set the score values
    result.setFullNameScore(fnScore);
    result.setSurnameScore(snScore);
    result.setGivenNameScore(gnScore);
    result.setGenerationScore(genScore);
    result.setOrgNameScore(orgScore);

    // return the result
    return result;
  }
}
