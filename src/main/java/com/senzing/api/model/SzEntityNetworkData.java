package com.senzing.api.model;

import javax.json.JsonArray;
import javax.json.JsonObject;
import java.util.*;
import java.util.function.Function;

/**
 * Describes an entity path and the entities in the path.
 *
 */
public class SzEntityNetworkData {
  /**
   * The {@link List} of {@link SzEntityPath} describing the entity paths.
   */
  private List<SzEntityPath> entityPaths;

  /**
   * The {@link List} of {@link SzEntityData} instances describing the entities
   * in the path.
   */
  private List<SzEntityData> entities;

  /**
   * Constructs with the specified {@link List} of {@link SzEntityPath}
   * instances and {@link List} of {@link SzEntityData} instances describing
   * the entities in the path.
   *
   * @param entityPaths The {@link List} of {@link SzEntityPath} instances
   *                    describing the entity paths.
   *
   * @param entities The {@link List} of {@link SzEntityData} instances
   *                 describing the entities in the path.
   *
   * @throws IllegalArgumentException If the entities list is not consistent
   *                                  with the specified entity paths.
   */
  public SzEntityNetworkData(List<SzEntityPath>  entityPaths,
                             List<SzEntityData>  entities)
    throws IllegalArgumentException
  {
    // check the sets of entity IDs
    Set<Long> set1 = new HashSet<>();
    Set<Long> set2 = new HashSet<>();
    entities.forEach(e -> set1.add(e.getResolvedEntity().getEntityId()));
    entityPaths.forEach(entityPath -> set2.addAll(entityPath.getEntityIds()));

    if (!set1.containsAll(set2)) {
      throw new IllegalArgumentException(
          "Some of the entities on the paths are not in included in the "
          + "enitty list.  pathEntities=[ " + set2 + " ], listEntities=[ "
          + set1 + " ]");
    }

    this.entityPaths = Collections.unmodifiableList(
        new ArrayList<>(entityPaths));
    this.entities = Collections.unmodifiableList(new ArrayList<>(entities));
  }

  /**
   * Returns the {@link List} of {@link SzEntityPath} instances describing
   * the entity paths.
   *
   * @return The {@link List} of {@link SzEntityPath} instances describing
   *         the entity paths.
   */
  public List<SzEntityPath> getEntityPaths() {
    return this.entityPaths;
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
    return "SzEntityNetworkData{" +
        "entityPaths=" + entityPaths +
        ", entities=" + entities +
        '}';
  }

  /**
   * Parses the entity feature from a {@link JsonObject} describing JSON
   * for the Senzing native API format for an entity feature and populates
   * the specified {@link SzEntityNetworkData} or creates a new instance.
   *
   * @param jsonObject The {@link JsonObject} describing the JSON in the
   *                   Senzing native API format.
   *
   * @param featureToAttrClassMapper Mapping function to map feature names to
   *                                 attribute classes.
   *
   * @return The populated (or created) {@link SzEntityNetworkData}.
   */
  public static SzEntityNetworkData parseEntityNetworkData(
      JsonObject              jsonObject,
      Function<String,String> featureToAttrClassMapper)
  {
    JsonArray jsonArray = jsonObject.getJsonArray("ENTITY_PATHS");
    List<SzEntityPath> entityPaths
        = SzEntityPath.parseEntityPathList(null, jsonArray);

    jsonArray = jsonObject.getJsonArray("ENTITIES");

    List<SzEntityData> dataList = SzEntityData.parseEntityDataList(
        null, jsonArray, featureToAttrClassMapper);

    return new SzEntityNetworkData(entityPaths, dataList);
  }

}
