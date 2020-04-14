package com.senzing.api.model;

import com.senzing.util.JsonUtils;

import javax.json.JsonArray;
import javax.json.JsonObject;
import java.util.*;
import java.util.function.Function;

import static com.senzing.api.model.SzRelationshipType.*;

/**
 *
 */
public class SzRelatedEntity extends SzBaseRelatedEntity {
  /**
   * Whether or not the relationship is disclosed.
   */
  private boolean disclosed;

  /**
   * The relationship type.
   */
  private SzRelationshipType relationType;

  /**
   * Default constructor.
   */
  public SzRelatedEntity() {
    this.disclosed    = false;
    this.relationType = null;
  }

  /**
   * Checks whether or not the relationship between the entities is disclosed.
   *
   * @return <tt>true</tt> if the relationship is disclosed, or <tt>false</tt>
   *         if not disclosed.
   */
  public boolean isDisclosed() {
    return this.disclosed;
  }

  /**
   * Sets whether or not the relationship between the entities is disclosed.
   *
   * @param disclosed <tt>true</tt> if the relationship is disclosed, or
   *                  <tt>false</tt> if not disclosed.
   */
  public void setDisclosed(boolean disclosed) {
    this.disclosed = disclosed;
  }

  /**
   * Gets the {@link SzRelationshipType} describing the type of relation.
   *
   * @return The {@link SzRelationshipType} describing the type of relation.
   */
  public SzRelationshipType getRelationType() {
    return this.relationType;
  }

  /**
   * Sets the {@link SzRelationshipType} describing the type of relation.
   *
   * @param relationType The {@link SzRelationshipType} describing the type
   *                     of relation.
   */
  public void setRelationType(SzRelationshipType relationType) {
    this.relationType = relationType;
  }

  /**
   * Parses a list of resolved entities from a {@link JsonArray} describing a
   * JSON array in the Senzing native API format for entity features and
   * populates the specified {@link List} or creates a new {@link List}.
   *
   * @param list The {@link List} of {@link SzRelatedEntity} instances to
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
   *         SzRelatedEntity} instances.
   */
  public static List<SzRelatedEntity> parseRelatedEntityList(
      List<SzRelatedEntity>   list,
      JsonArray               jsonArray,
      Function<String,String> featureToAttrClassMapper)
  {
    if (list == null) {
      list = new ArrayList<>(jsonArray == null ? 0 : jsonArray.size());
    }

    if (jsonArray == null) return list;

    for (JsonObject jsonObject : jsonArray.getValuesAs(JsonObject.class)) {
      list.add(parseRelatedEntity(null,
                                  jsonObject,
                                  featureToAttrClassMapper));
    }
    return list;
  }

  /**
   * Parses the entity feature from a {@link JsonObject} describing JSON
   * for the Senzing native API format for an entity feature and populates
   * the specified {@link SzRelatedEntity} or creates a new instance.
   *
   * @param entity The {@link SzRelatedEntity} instance to populate, or
   *               <tt>null</tt> if a new instance should be created.
   *
   * @param jsonObject The {@link JsonObject} describing the JSON in the
   *                   Senzing native API format.
   *
   * @param featureToAttrClassMapper Mapping function to map feature names to
   *                                 attribute classes.
   *
   * @return The populated (or created) {@link SzRelatedEntity}.
   */
  public static SzRelatedEntity parseRelatedEntity(
      SzRelatedEntity         entity,
      JsonObject              jsonObject,
      Function<String,String> featureToAttrClassMapper)
  {
    if (entity == null) entity = new SzRelatedEntity();

    Function<String,String> mapper = featureToAttrClassMapper;
    SzBaseRelatedEntity.parseBaseRelatedEntity(entity, jsonObject, mapper);

    if (jsonObject.containsKey("IS_DISCLOSED")) {
      boolean disclosed = jsonObject.getInt("IS_DISCLOSED") != 0;
      entity.setDisclosed(disclosed);
    }

    if (entity.getMatchLevel() != null) {
      if (entity.isDisclosed()) {
        entity.setRelationType(DISCLOSED_RELATION);
      } else if (entity.getMatchLevel() == 2) {
        entity.setRelationType(POSSIBLE_MATCH);
      } else {
        entity.setRelationType(POSSIBLE_RELATION);
      }
    }

    // iterate over the feature map
    return entity;
  }

  @Override
  public String toString() {
    return "SzRelatedEntity{" +
        super.toString() +
        ", disclosed=" + disclosed +
        ", relationType=" + relationType +
        '}';
  }
}
