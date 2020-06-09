package com.senzing.api.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.senzing.util.JsonUtils;

import javax.json.JsonArray;
import javax.json.JsonObject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;

/**
 * Describes why an entity resolved.
 */
public class SzWhyEntityResult {
  /**
   * The {@link SzWhyPerspective} identifying and describing the perspective
   * for this why result.
   */
  private SzWhyPerspective perspective;

  /**
   * The {@link SzMatchInfo} providing the details of the result.
   */
  private SzMatchInfo matchInfo;

  /**
   * Default constructor.
   */
  public SzWhyEntityResult() {
    this.perspective  = null;
    this.matchInfo    = null;
  }

  /**
   * Gets the {@link SzWhyPerspective} identifying and describing the perspective
   * for this why result.
   *
   * @return The {@link SzWhyPerspective} identifying and describing the perspective
   *         for this why result.
   */
  @JsonInclude(NON_NULL)
  public SzWhyPerspective getPerspective() {
    return this.perspective;
  }

  /**
   * Sets the {@link SzWhyPerspective} identifying and describing the
   * perspective for this why result.
   *
   * @param perspective The {@link SzWhyPerspective} identifying and describing
   *                    the perspective for this why result.
   */
  public void setPerspective(SzWhyPerspective perspective) {
    this.perspective = perspective;
  }

  /**
   * Gets the {@link SzMatchInfo} providing the details of the result.
   *
   * @return The {@link SzMatchInfo} providing the details of the result.
   */
  @JsonInclude(NON_NULL)
  public SzMatchInfo getMatchInfo() {
    return this.matchInfo;
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
   * SzWhyEntityResult}.
   *
   * @param jsonObject The {@link JsonObject} describing the why entity result
   *                   using the native API JSON format.
   *
   * @return The created instance of {@link SzWhyEntityResult}.
   */
  public static SzWhyEntityResult parseWhyEntityResult(JsonObject jsonObject)
  {
    SzWhyPerspective perspective
        = SzWhyPerspective.parseWhyPerspective(jsonObject);

    JsonObject infoJson = JsonUtils.getJsonObject(jsonObject, "MATCH_INFO");

    SzMatchInfo matchInfo
        = SzMatchInfo.parseMatchInfo(infoJson);

    SzWhyEntityResult result = new SzWhyEntityResult();
    result.setPerspective(perspective);
    result.setMatchInfo(matchInfo);

    return result;
  }

  /**
   * Parses the native API JSON array to populate a list of {@link
   * SzWhyEntityResult} instances.
   *
   * @param list The {@link List} of {@link SzWhyEntityResult} instances to
   *             populate or <tt>null</tt> if a new list should be created.
   *
   * @param jsonArray The {@link JsonArray} of {@link JsonObject} instances to
   *                  be parsed as instances of {@link SzWhyEntityResult}.
   *
   * @return The {@link List} of {@link SzWhyEntityResult} instances that was
   *         populated.
   */
  public static List<SzWhyEntityResult> parseWhyEntityResultList(
      List<SzWhyEntityResult> list,
      JsonArray               jsonArray)
  {
    if (list == null) {
      list = new ArrayList<>(jsonArray.size());
    }

    for (JsonObject jsonObject: jsonArray.getValuesAs(JsonObject.class)) {
      list.add(parseWhyEntityResult(jsonObject));
    }

    return list;
  }
}
