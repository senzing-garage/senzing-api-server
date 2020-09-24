package com.senzing.api.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.senzing.util.JsonUtils;

import javax.json.JsonArray;
import javax.json.JsonObject;
import java.util.*;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_EMPTY;

/**
 * Describes an entity record that has matched another record in a resolved
 * entity.
 */
public class SzMatchedRecord extends SzEntityRecord {
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
  public SzMatchedRecord() {
    super();
    this.matchKey           = null;
    this.resolutionRuleCode = null;
    this.matchLevel         = null;
    this.matchScore         = null;
    this.refScore           = null;
  }

  /**
   * Gets the match level for how this record matched against the first record
   * in the resolved entity.
   *
   * @return The match level for how this record matched against the first record
   *         in the resolved entity.
   */
  @JsonInclude(NON_EMPTY)
  public Integer getMatchLevel() {
    return matchLevel;
  }

  /**
   * Sets the match level for how this record matched against the first record
   * in the resolved entity.
   *
   * @param matchLevel The match level for how this record matched against the
   *                   first record in the resolved entity.
   */
  public void setMatchLevel(Integer matchLevel) {
    this.matchLevel = matchLevel;
  }

  /**
   * Gets the match score for how this record matched against the first record
   * in the resolved entity.
   *
   * @return The match score for how this record matched against the first
   *         record in the resolved entity.
   */
  @JsonInclude(NON_EMPTY)
  public Integer getMatchScore() {
    return matchScore;
  }

  /**
   * Sets the match score for how this record matched against the first record
   * in the resolved entity.
   *
   * @param matchScore The match score for how this record matched against the
   *                   first record in the resolved entity.
   */
  public void setMatchScore(Integer matchScore) {
    this.matchScore = matchScore;
  }

  /**
   * Gets the match key for how this record matched against the first record
   * in the resolved entity.
   *
   * @return The match key for how this record matched against the first record
   *         in the resolved entity.
   */
  @JsonInclude(NON_EMPTY)
  public String getMatchKey() {
    return matchKey;
  }

  /**
   * Sets the match key for how this record matched against the first record
   * in the resolved entity.
   *
   * @param matchKey The match key for how this record matched against the
   *                 first record in the resolved entity.
   */
  public void setMatchKey(String matchKey) {
    this.matchKey = matchKey;
  }

  /**
   * Gets the resolution rule code for how this record matched against the
   * first record in the resolved entity.
   *
   * @return The resolution rule code for how this record matched against the
   *         first record in the resolved entity.
   */
  @JsonInclude(NON_EMPTY)
  public String getResolutionRuleCode() {
    return resolutionRuleCode;
  }

  /**
   * Sets the resolution rule code for how this record matched against the
   * first record in the resolved entity.
   *
   * @param resolutionRuleCode The resolution rule code for how this record
   *                           matched against the first record in the
   *                           resolved entity.
   */
  public void setResolutionRuleCode(String resolutionRuleCode) {
    this.resolutionRuleCode = resolutionRuleCode;
  }

  /**
   * Gets the ref score for how this record matched against the
   * first record in the resolved entity.
   *
   * @return The ref score for how this record matched against the
   *         first record in the resolved entity.
   */
  @JsonInclude(NON_EMPTY)
  public Integer getRefScore() {
    return refScore;
  }

  /**
   * Sets the ref score for how this record matched against the first record
   * in the resolved entity.
   *
   * @param refScore The ref score for how this record matched against the
   *                 first record in the resolved entity.
   */
  public void setRefScore(Integer refScore) {
    this.refScore = refScore;
  }

  /**
   * Parses the native JSON to construct/populate a {@link List}
   * of {@link SzMatchedRecord} instances.
   *
   * @param list The {@link List} to populate or <tt>null</tt> if a new {@link
   *             List} should be created.
   * @param jsonArray The {@link JsonArray} describing the native JSON array.
   *
   * @return The specified (or constructed) {@link List} of {@link
   *         SzMatchedRecord} instances.
   */
  public static List<SzMatchedRecord> parseMatchedRecordList(
      List<SzMatchedRecord>  list,
      JsonArray             jsonArray)
  {
    for (JsonObject jsonObject : jsonArray.getValuesAs(JsonObject.class)) {
      if (list == null) {
        list = new ArrayList<SzMatchedRecord>(jsonArray.size());
      }
      list.add(parseMatchedRecord(null, jsonObject));
    }
    if (list != null) {
      list = Collections.unmodifiableList(list);
    } else {
      list = Collections.emptyList();
    }
    return list;
  }

  /**
   * Parses the native API JSON to build an populate or create an instance of
   * {@link SzMatchedRecord}.
   *
   * @param record The {@link SzMatchedRecord} to populate or <tt>null</tt> if
   *               a new instance should be created.
   *
   * @param jsonObject The {@link JsonObject} describing the record using the
   *                   native API JSON format.
   *
   * @return The populated (or created) instance of {@link SzMatchedRecord}
   */
  public static SzMatchedRecord parseMatchedRecord(SzMatchedRecord record,
                                                   JsonObject     jsonObject)
  {
    if (record == null) record = new SzMatchedRecord();

    // populate from the base class
    parseEntityRecord(record, jsonObject);

    // now get the match fields
    Optional<Integer> matchScore = SzBaseRelatedEntity.readMatchScore(jsonObject);

    String  matchKey    = JsonUtils.getString(jsonObject, "MATCH_KEY");
    Integer matchLevel  = JsonUtils.getInteger(jsonObject, "MATCH_LEVEL");
    Integer refScore    = JsonUtils.getInteger(jsonObject, "REF_SCORE");
    String  ruleCode    = JsonUtils.getString(jsonObject,"ERRULE_CODE");

    record.setMatchScore(matchScore.orElse(null));
    record.setMatchKey(matchKey);
    record.setMatchLevel(matchLevel);
    record.setRefScore(refScore);
    record.setResolutionRuleCode(ruleCode);

    return record;
  }

  @Override
  public String toString() {
    return "SzMatchedRecord{" + this.fieldsToString() + "}";
  }

  @Override
  protected String fieldsToString() {
    return super.fieldsToString() +
        ", matchLevel=" + matchLevel +
        ", matchScore=" + matchScore +
        ", matchKey='" + matchKey + '\'' +
        ", resolutionRuleCode='" + resolutionRuleCode + '\'' +
        ", refScore=" + refScore;
  }

}
