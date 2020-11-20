package com.senzing.api.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.senzing.util.JsonUtils;

import javax.json.JsonArray;
import javax.json.JsonObject;
import java.util.ArrayList;
import java.util.List;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;

/**
 * Describes why an entity resolved.
 */
public class SzWhyEntitiesResult {
  /**
   * The entity ID for the first entity.
   */
  private Long entityId1;

  /**
   * The entity ID for the second entity.
   */
  private Long entityId2;

  /**
   * The {@link SzMatchInfo} providing the details of the result.
   */
  private SzMatchInfo matchInfo;

  /**
   * Default constructor.
   */
  public SzWhyEntitiesResult() {
    this.entityId1  = null;
    this.entityId2  = null;
    this.matchInfo  = null;
  }

  /**
   * Gets the entity ID of the first entity.
   *
   * @return The entity ID of the first entity.
   */
  @JsonInclude(NON_NULL)
  public Long getEntityId1() {
    return this.entityId1;
  }

  /**
   * Sets the entity ID of the first entity.
   *
   * @param entityId The entity ID of the first entity.
   */
  public void setEntityId1(Long entityId) {
    this.entityId1 = entityId;
  }

  /**
   * Gets the entity ID of the second entity.
   *
   * @return The entity ID of the second entity.
   */
  @JsonInclude(NON_NULL)
  public Long getEntityId2() {
    return this.entityId2;
  }

  /**
   * Sets the entity ID of the second entity.
   *
   * @param entityId The entity ID of the second entity.
   */
  public void setEntityId2(Long entityId) {
    this.entityId2 = entityId;
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
   * SzWhyEntitiesResult}.
   *
   * @param jsonObj The {@link JsonObject} describing the why entity result
   *                using the native API JSON format.
   *
   * @return The created instance of {@link SzWhyEntitiesResult}.
   */
  public static SzWhyEntitiesResult parseWhyEntitiesResult(JsonObject jsonObj)
  {
    Long entityId1 = JsonUtils.getLong(jsonObj, "ENTITY_ID");
    Long entityId2 = JsonUtils.getLong(jsonObj, "ENTITY_ID_2");

    JsonObject infoJson = JsonUtils.getJsonObject(jsonObj, "MATCH_INFO");

    SzMatchInfo matchInfo
        = SzMatchInfo.parseMatchInfo(infoJson);

    SzWhyEntitiesResult result = new SzWhyEntitiesResult();
    result.setEntityId1(entityId1);
    result.setEntityId2(entityId2);
    result.setMatchInfo(matchInfo);

    return result;
  }

  /**
   * Parses the native API JSON array to populate a list of {@link
   * SzWhyEntitiesResult} instances.
   *
   * @param list The {@link List} of {@link SzWhyEntitiesResult} instances to
   *             populate or <tt>null</tt> if a new list should be created.
   *
   * @param jsonArray The {@link JsonArray} of {@link JsonObject} instances to
   *                  be parsed as instances of {@link SzWhyEntitiesResult}.
   *
   * @return The {@link List} of {@link SzWhyEntitiesResult} instances that was
   *         populated.
   */
  public static List<SzWhyEntitiesResult> parseWhyEntitiesResultList(
      List<SzWhyEntitiesResult> list,
      JsonArray               jsonArray)
  {
    if (list == null) {
      list = new ArrayList<>(jsonArray.size());
    }

    for (JsonObject jsonObject: jsonArray.getValuesAs(JsonObject.class)) {
      list.add(parseWhyEntitiesResult(jsonObject));
    }

    return list;
  }
}
