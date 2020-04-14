package com.senzing.api.model;

import com.senzing.util.JsonUtils;

import javax.json.JsonArray;
import javax.json.JsonObject;
import java.util.ArrayList;
import java.util.List;

/**
 * Describes why an entity resolved.
 */
public class SzWhyRecordsResult {
  /**
   * The {@link SzWhyPerspective} identifying and describing the perspective
   * from the first record.
   */
  private SzWhyPerspective perspective1;

  /**
   * The {@link SzWhyPerspective} identifying and describing the perspective
   * from the second record.
   */
  private SzWhyPerspective perspective2;

  /**
   * The {@link SzMatchInfo} providing the details of the result.
   */
  private SzMatchInfo matchInfo;

  /**
   * Default constructor.
   */
  public SzWhyRecordsResult() {
    this.perspective1 = null;
    this.perspective2 = null;
    this.matchInfo    = null;
  }

  /**
   * Gets the {@link SzWhyPerspective} identifying and describing the
   * perspective for this why result from the first record.
   *
   * @return The {@link SzWhyPerspective} identifying and describing the
   *         perspective for this why result from the first record.
   */
  public SzWhyPerspective getPerspective1() {
    return this.perspective1;
  }

  /**
   * Sets the {@link SzWhyPerspective} identifying and describing the
   * perspective for this why result from the first record.
   *
   * @param perspective The {@link SzWhyPerspective} identifying and describing
   *                    the perspective for this why result from the first
   *                    record.
   */
  public void setPerspective1(SzWhyPerspective perspective) {
    this.perspective1 = perspective;
  }

  /**
   * Gets the {@link SzWhyPerspective} identifying and describing the
   * perspective for this why result from the first record.
   *
   * @return The {@link SzWhyPerspective} identifying and describing the
   *         perspective for this why result from the first record.
   */
  public SzWhyPerspective getPerspective2() {
    return this.perspective2;
  }

  /**
   * Sets the {@link SzWhyPerspective} identifying and describing the
   * perspective for this why result from the first record.
   *
   * @param perspective The {@link SzWhyPerspective} identifying and describing
   *                    the perspective for this why result from the first
   *                    record.
   */
  public void setPerspective2(SzWhyPerspective perspective) {
    this.perspective2 = perspective;
  }

  /**
   * Gets the {@link SzMatchInfo} providing the details of the result.
   *
   * @return The {@link SzMatchInfo} providing the details of the result.
   */
  public SzMatchInfo getMatchInfo() {
    return matchInfo;
  }

  /**
   * Sets the {@link SzMatchInfo} providing the details of the result.
   *
   * @param matchInfo The {@link SzMatchInfo} providing the details of the
   *                  result.
   */
  public void setMatchInfo(SzMatchInfo matchInfo) {
    this.matchInfo = matchInfo;
  }

  /**
   * Parses the native API JSON to build an instance of {@link
   * SzWhyRecordsResult}.
   *
   * @param jsonObject The {@link JsonObject} describing the why entity result
   *                   using the native API JSON format.
   *
   * @return The created instance of {@link SzWhyRecordsResult}.
   */
  public static SzWhyRecordsResult parseWhyRecordsResult(JsonObject jsonObject)
  {
    SzWhyPerspective perspective1
        = SzWhyPerspective.parseWhyPerspective(jsonObject);

    SzWhyPerspective perspective2
        = SzWhyPerspective.parseWhyPerspective(jsonObject, "_2");

    JsonObject infoJson = JsonUtils.getJsonObject(jsonObject, "MATCH_INFO");

    SzMatchInfo matchInfo
        = SzMatchInfo.parseMatchInfo(infoJson);

    SzWhyRecordsResult result = new SzWhyRecordsResult();
    result.setPerspective1(perspective1);
    result.setPerspective2(perspective2);
    result.setMatchInfo(matchInfo);

    return result;
  }

  /**
   * Parses the native API JSON array to populate a list of {@link
   * SzWhyRecordsResult} instances.
   *
   * @param list The {@link List} of {@link SzWhyRecordsResult} instances to
   *             populate or <tt>null</tt> if a new list should be created.
   *
   * @param jsonArray The {@link JsonArray} of {@link JsonObject} instances to
   *                  be parsed as instances of {@link SzWhyRecordsResult}.
   *
   * @return The {@link List} of {@link SzWhyRecordsResult} instances that was
   *         populated.
   */
  public static List<SzWhyRecordsResult> parseWhyRecordsResultList(
      List<SzWhyRecordsResult>  list,
      JsonArray                 jsonArray)
  {
    if (list == null) {
      list = new ArrayList<>(jsonArray.size());
    }

    for (JsonObject jsonObject: jsonArray.getValuesAs(JsonObject.class)) {
      list.add(parseWhyRecordsResult(jsonObject));
    }

    return list;
  }
}
