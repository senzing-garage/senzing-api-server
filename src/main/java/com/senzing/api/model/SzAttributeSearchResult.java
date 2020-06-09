package com.senzing.api.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.senzing.util.JsonUtils;

import javax.json.JsonArray;
import javax.json.JsonObject;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;

import static com.senzing.api.model.SzAttributeSearchResultType.*;
import static com.fasterxml.jackson.annotation.JsonInclude.Include.*;

public class SzAttributeSearchResult extends SzBaseRelatedEntity {
  /**
   * The search result type.
   */
  private SzAttributeSearchResultType resultType;

  /**
   * The entities related to the resolved entity.
   */
  private List<SzRelatedEntity> relatedEntities;

  /**
   * Default constructor.
   */
  public SzAttributeSearchResult() {
    this.resultType = null;
    this.relatedEntities = new LinkedList<>();
  }

  /**
   * Gets the {@link SzRelationshipType} describing the type of relation.
   *
   * @return The {@link SzRelationshipType} describing the type of relation.
   */
  public SzAttributeSearchResultType getResultType() {
    return this.resultType;
  }

  /**
   * Sets the {@link SzAttributeSearchResultType} describing the type of
   * relation.
   *
   * @param resultType The {@link SzAttributeSearchResultType} describing the
   *                   type of relation.
   */
  public void setResultType(SzAttributeSearchResultType resultType) {
    this.resultType = resultType;
  }

  /**
   * Gets the {@link List} of {@linkplain SzRelatedEntity related entities}.
   *
   * @return The {@link List} of {@linkplain SzRelatedEntity related entities}.
   */
  @JsonInclude(NON_EMPTY)
  public List<SzRelatedEntity> getRelatedEntities() {
    return this.relatedEntities;
  }

  /**
   * Sets the {@link List} of {@linkplain SzRelatedEntity related entities}.
   *
   * @param relatedEntities The {@link List} of {@linkplain SzRelatedEntity
   *                        related entities}.
   */
  public void setRelatedEntities(List<SzRelatedEntity> relatedEntities) {
    this.relatedEntities.clear();
    if (relatedEntities != null) {
      this.relatedEntities.addAll(relatedEntities);
    }
  }

  /**
   * Adds the specified {@link SzRelatedEntity}
   */
  public void addRelatedEntity(SzRelatedEntity relatedEntity) {
    if (relatedEntity != null) {
      this.relatedEntities.add(relatedEntity);
    }
  }

  /**
   * Parses a list of resolved entities from a {@link JsonArray} describing a
   * JSON array in the Senzing native API format for entity features and
   * populates the specified {@link List} or creates a new {@link List}.
   *
   * @param list The {@link List} of {@link SzAttributeSearchResult} instances to
   *             populate, or <tt>null</tt> if a new {@link List}
   *             should be created.
   *
   * @param jsonArray The {@link JsonArray} describing the JSON in the
   *                  Senzing native API format.
   *
   * @param featureToAttrClassMapper Mapping function to map feature names to
   *                                 attribute classes.
   *
   * @return The populated (or created) {@link List} of {@link
   *         SzAttributeSearchResult} instances.
   */
  public static List<SzAttributeSearchResult> parseSearchResultList(
      List<SzAttributeSearchResult> list,
      JsonArray                     jsonArray,
      Function<String,String>       featureToAttrClassMapper)
  {
    if (list == null) {
      list = new ArrayList<>(jsonArray.size());
    }
    for (JsonObject jsonObject : jsonArray.getValuesAs(JsonObject.class)) {
      list.add(parseSearchResult(null,
                                 jsonObject,
                                 featureToAttrClassMapper));
    }
    return list;
  }

  /**
   * Parses the entity feature from a {@link JsonObject} describing JSON
   * for the Senzing native API format for an entity feature and populates
   * the specified {@link SzAttributeSearchResult} or creates a new instance.
   *
   * @param entity The {@link SzAttributeSearchResult} instance to populate, or
   *               <tt>null</tt> if a new instance should be created.
   *
   * @param jsonObject The {@link JsonObject} describing the JSON in the
   *                   Senzing native API format.
   *
   * @param featureToAttrClassMapper Mapping function to map feature names to
   *                                 attribute classes.
   *
   * @return The populated (or created) {@link SzAttributeSearchResult}.
   */
  public static SzAttributeSearchResult parseSearchResult(
      SzAttributeSearchResult entity,
      JsonObject              jsonObject,
      Function<String,String> featureToAttrClassMapper)
  {
    if (entity == null) entity = new SzAttributeSearchResult();

    Function<String,String> mapper = featureToAttrClassMapper;

    SzBaseRelatedEntity.parseBaseRelatedEntity(entity, jsonObject, mapper);

    JsonObject entityObject = JsonUtils.getJsonObject(jsonObject, "ENTITY");
    if (entityObject == null) {
      entityObject = jsonObject;
    }
    JsonArray relatedArray = JsonUtils.getJsonArray(entityObject,
                                                    "RELATED_ENTITIES");

    List<SzRelatedEntity> relatedEntities = null;
    if (relatedArray != null) {
      relatedEntities = SzRelatedEntity.parseRelatedEntityList(null,
                                                               relatedArray,
                                                               mapper);
    }

    SzAttributeSearchResultType resultType = null;
    switch (entity.getMatchLevel()) {
      case 1:
        resultType = MATCH;
        break;
      case 2:
        resultType = POSSIBLE_MATCH;
        break;
      case 3:
        resultType = POSSIBLE_RELATION;
        break;
      case 4:
        resultType = NAME_ONLY_MATCH;
        break;
    }
    entity.setResultType(resultType);
    if (relatedEntities != null) {
      entity.setRelatedEntities(relatedEntities);
    }

    // iterate over the feature map
    return entity;
  }

  @Override
  public String toString() {
    return "SzAttributeSearchResult{" +
        super.toString() +
        ", resultType=" + resultType +
        ", relatedEntities=" + relatedEntities +
        '}';
  }
}
