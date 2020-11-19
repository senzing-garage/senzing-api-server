package com.senzing.api.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.senzing.util.JsonUtils;

import javax.json.JsonArray;
import javax.json.JsonObject;
import java.util.*;
import java.util.function.Function;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.*;

/**
 * Describes a resolved entity and its related entities.
 *
 */
public class SzEntityData {
  /**
   * The resolved entity.
   */
  private SzResolvedEntity resolvedEntity;

  /**
   * The entities related to the resolved entity.
   */
  private List<SzRelatedEntity> relatedEntities;

  /**
   * Default constructor.
   */
  public SzEntityData() {
    this.resolvedEntity = null;
    this.relatedEntities = new LinkedList<>();
  }

  /**
   * Gets the {@link SzResolvedEntity} describing the resolved entity.
   *
   * @return The {@link SzResolvedEntity} describing the resolved entity.
   */
  public SzResolvedEntity getResolvedEntity() {
    return resolvedEntity;
  }

  /**
   * Sets the {@link SzResolvedEntity} describing the resolved entity.
   *
   * @param resolvedEntity The {@link SzResolvedEntity} describing the
   *                       resolved entity.
   */
  public void setResolvedEntity(SzResolvedEntity resolvedEntity) {
    this.resolvedEntity = resolvedEntity;
  }

  /**
   * Gets the {@link List} of {@linkplain SzRelatedEntity related entities}.
   *
   * @return The {@link List} of {@linkplain SzRelatedEntity related entities}.
   */
  @JsonInclude(NON_EMPTY)
  public List<SzRelatedEntity> getRelatedEntities() {
    return Collections.unmodifiableList(this.relatedEntities);
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
   * Parses a list of entity data instances from a {@link JsonArray}
   * describing a JSON array in the Senzing native API format for entity
   * features and populates the specified {@link List} or creates a new
   * {@link List}.
   *
   * @param list The {@link List} of {@link SzEntityData} instances to
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
   *         SzEntityData} instances.
   */
  public static List<SzEntityData> parseEntityDataList(
      List<SzEntityData>      list,
      JsonArray               jsonArray,
      Function<String,String> featureToAttrClassMapper)
  {
    Function<String,String> mapper = featureToAttrClassMapper;

    if (list == null) {
      list = new ArrayList<>(jsonArray.size());
    }

    for (JsonObject jsonObject : jsonArray.getValuesAs(JsonObject.class)) {
      list.add(parseEntityData(null, jsonObject, mapper));
    }
    return list;
  }

  /**
   * Parses the entity data from a {@link JsonObject} describing JSON
   * for the Senzing native API format for an entity data and populates
   * the specified {@link SzEntityData} or creates a new instance.
   *
   * @param entityData The {@link SzEntityData} instance to populate, or
   *                   <tt>null</tt> if a new instance should be created.
   *
   * @param jsonObject The {@link JsonObject} describing the JSON in the
   *                   Senzing native API format.
   *
   * @param featureToAttrClassMapper Mapping function to map feature names to
   *                                 attribute classes.
   *
   * @return The populated (or created) {@link SzEntityData}.
   */
  public static SzEntityData parseEntityData(
      SzEntityData            entityData,
      JsonObject              jsonObject,
      Function<String,String> featureToAttrClassMapper)
  {
    if (entityData == null) entityData = new SzEntityData();

    Function<String,String> mapper = featureToAttrClassMapper;

    JsonObject resEntObj = jsonObject.getJsonObject("RESOLVED_ENTITY");

    SzResolvedEntity resolvedEntity
        = SzResolvedEntity.parseResolvedEntity(null, resEntObj, mapper);

    JsonArray relatedArray
        = JsonUtils.getJsonArray(jsonObject,"RELATED_ENTITIES");

    List<SzRelatedEntity> relatedEntities
        = SzRelatedEntity.parseRelatedEntityList(null,
                                                 relatedArray,
                                                 mapper);

    entityData.setResolvedEntity(resolvedEntity);
    entityData.setRelatedEntities(relatedEntities);

    return entityData;
  }

  @Override
  public String toString() {
    return "SzEntityData{" +
        "resolvedEntity=" + resolvedEntity +
        ", relatedEntities=" + relatedEntities +
        '}';
  }
}
