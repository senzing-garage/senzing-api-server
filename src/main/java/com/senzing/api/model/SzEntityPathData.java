package com.senzing.api.model;

import javax.json.JsonArray;
import javax.json.JsonObject;
import java.util.*;
import java.util.function.Function;

/**
 * Describes an entity path and the entities in the path.
 *
 */
public class SzEntityPathData {
  /**
   * The {@link SzEntityPath} describing the entity path.
   */
  private SzEntityPath entityPath;

  /**
   * The {@link List} of {@link SzEntityData} instances describing the entities
   * in the path.
   */
  private List<SzEntityData> entities;

  /**
   * Package-private default constructor.
   */
  SzEntityPathData() {
    this.entityPath = null;
    this.entities   = null;
  }

  /**
   * Constructs with the specified {@link SzEntityPath} and {@link
   * SzEntityData} instances describing the entities in the path.
   *
   * @param entityPath The {@link SzEntityPath} describing the entity path.
   *
   * @param entities The {@link List} of {@link SzEntityData} instances
   *                 describing the entities in the path.
   *
   * @throws IllegalArgumentException If the entities list is not consistent
   *                                  with the specified entity path.
   */
  public SzEntityPathData(SzEntityPath        entityPath,
                          List<SzEntityData>  entities)
    throws IllegalArgumentException
  {
    if (entityPath.getEntityIds().size() > 0
        && entities.size() != entityPath.getEntityIds().size()) {
      throw new IllegalArgumentException(
          "The specified entity path and entities list are not consistent.  "
          + "pathSize=[ " + entityPath.getEntityIds().size()
          + " ], entityCount=[ " + entities.size() + " ]");
    }

    // check the sets of entity IDs
    Set<Long> set1 = new HashSet<>();
    Set<Long> set2 = new HashSet<>();
    entities.forEach(e -> set1.add(e.getResolvedEntity().getEntityId()));
    set2.addAll(entityPath.getEntityIds());

    if ((set2.size() > 0)
        && (!set1.containsAll(set2) || !set2.containsAll(set1)))
    {
      throw new IllegalArgumentException(
          "The specified entity path and entities list have different "
          + "entity IDs.  pathEntities=[ " + set2 + " ], listEntities=[ "
          + set1 + " ]");
    }

    this.entityPath = entityPath;
    this.entities = Collections.unmodifiableList(new ArrayList<>(entities));
  }

  /**
   * Returns the {@link SzEntityPath} describing the entity path.
   *
   * @return The {@link SzEntityPath} describing the entity path.
   */
  public SzEntityPath getEntityPath() {
    return this.entityPath;
  }

  /**
   * Returns the {@link List} of {@link SzEntityData} instances describing
   * the entities in the path.
   *
   * @return The {@link List} of {@link SzEntityData} instances describing
   *         the entities in the path.
   */
  public List<SzEntityData> getEntities() {
    return this.entities;
  }

  @Override
  public String toString() {
    return "SzEntityPathData{" +
        "entityPath=" + entityPath +
        ", entities=" + entities +
        '}';
  }

  /**
   * Parses the entity path data from a {@link JsonObject} describing JSON
   * for the Senzing native API format for an entity feature and populates
   * the specified {@link SzEntityPathData} or creates a new instance.
   *
   * @param jsonObject The {@link JsonObject} describing the JSON in the
   *                   Senzing native API format.
   *
   * @param featureToAttrClassMapper Mapping function to map feature names to
   *                                 attribute classes.
   *
   * @return The populated (or created) {@link SzEntityPathData}.
   */
  public static SzEntityPathData parseEntityPathData(
      JsonObject              jsonObject,
      Function<String,String> featureToAttrClassMapper)
  {
    JsonArray pathArray = jsonObject.getJsonArray("ENTITY_PATHS");
    if (pathArray.size() == 0) return null;
    JsonObject pathObject = pathArray.get(0).asJsonObject();
    SzEntityPath entityPath = SzEntityPath.parseEntityPath(pathObject);

    JsonArray jsonArray = jsonObject.getJsonArray("ENTITIES");

    List<SzEntityData> dataList = SzEntityData.parseEntityDataList(
        null, jsonArray, featureToAttrClassMapper);

    return new SzEntityPathData(entityPath, dataList);
  }

}
