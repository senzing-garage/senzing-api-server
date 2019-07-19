package com.senzing.api.model;

import com.senzing.util.JsonUtils;

import javax.json.JsonArray;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.json.JsonValue;
import java.util.*;
import java.util.function.Function;

/**
 * Represents a path between entities consisting of one or more entity IDs
 * identifying the entities in the path in order.
 */
public class SzEntityPath {
  /**
   * The starting entity ID for the path.
   */
  private long startEntityId;

  /**
   * The ending entity ID for the path.
   */
  private long endEntityId;

  /**
   * The {@link List} of entity IDs.
   */
  private List<Long> entityIds;

  /**
   * Package-private default constructor.
   */
  SzEntityPath() {
    this.startEntityId  = 0;
    this.endEntityId    = 0;
    this.entityIds      = null;
  }

  /**
   * Constructs with the specified list of entity IDs.
   *
   * @param startEntityId The starting entity ID for the path.
   *
   * @param endEntityId The ending entity ID for the path.
   *
   * @param entityIds The {@link List} of entity IDs, or an empty
   *                  {@link List} if there is no path between the entities.
   *
   * @throws IllegalArgumentException If the specified {@link List} contains
   *                                  duplicate entity IDs is empty.
   */
  public SzEntityPath(long        startEntityId,
                      long        endEntityId,
                      List<Long>  entityIds)
    throws IllegalArgumentException
  {
    this.startEntityId = startEntityId;
    this.endEntityId   = endEntityId;
    this.entityIds = Collections.unmodifiableList(new ArrayList<>(entityIds));
    if (this.entityIds.size() > 0 && this.entityIds.get(0) != startEntityId
        && this.entityIds.get(this.entityIds.size()-1) != endEntityId) {
      throw new IllegalArgumentException(
          "The specified entity IDs list does not start and end with the "
          + "specified starting and ending entity ID.  startEntityId=[ "
          + startEntityId + " ], endEntityId=[ " + endEntityId
          + " ], entityIDs=[ " + entityIds + " ]");
    }
  }

  /**
   * Returns the entity ID of the first entity in the path.
   *
   * @return The entity ID of the first entity in the path.
   */
  public long getStartEntityId() {
    return this.startEntityId;
  }

  /**
   * Returns the entity ID of the last entity in the path.
   *
   * @return The entity ID of the last entity in the path.
   */
  public long getEndEntityId() {
    return this.endEntityId;
  }

  /**
   * Returns the {@link List} of entity IDs identifying the entities in the
   * path in order of the path.
   *
   * @return The {@link List} of entity IDs identifying the entities in the
   *         path in order of the path.
   */
  public List<Long> getEntityIds() {
    return this.entityIds;
  }

  @Override
  public String toString() {
    return "SzEntityPath{" +
        "startEntityId=" + startEntityId +
        ", endEntityId=" + endEntityId +
        ", entityIds=" + entityIds +
        '}';
  }

  /**
   * Parses a list of entity path instances from a {@link JsonArray}
   * describing a JSON array in the Senzing native API format for entity
   * features and populates the specified {@link List} or creates a new
   * {@link List}.
   *
   * @param list The {@link List} of {@link SzEntityPath} instances to
   *             populate, or <tt>null</tt> if a new {@link List}
   *             should be created.
   *
   * @param jsonArray The {@link JsonArray} describing the JSON in the
   *                  Senzing native API format.
   *
   * @return The populated (or created) {@link List} of {@link
   *         SzEntityPath} instances.
   */
  public static List<SzEntityPath> parseEntityPathList(
      List<SzEntityPath>      list,
      JsonArray               jsonArray)
  {
    if (list == null) {
      list = new ArrayList<>(jsonArray.size());
    }
    for (JsonObject jsonObject : jsonArray.getValuesAs(JsonObject.class)) {
      list.add(parseEntityPath(jsonObject));
    }
    return list;
  }

  /**
   * Parses the entity feature from a {@link JsonObject} describing JSON
   * for the Senzing native API format for an entity feature and populates
   * the specified {@link SzEntityPath} or creates a new instance.
   *
   * @param jsonObject The {@link JsonObject} describing the JSON in the
   *                   Senzing native API format.
   *
   * @return The populated (or created) {@link SzEntityPath}.
   */
  public static SzEntityPath parseEntityPath(JsonObject jsonObject)
  {
    Long startId = JsonUtils.getLong(jsonObject, "START_ENTITY_ID");
    Long endId = JsonUtils.getLong(jsonObject, "END_ENTITY_ID");
    JsonArray entities = jsonObject.getJsonArray("ENTITIES");
    int count = entities.size();

    List<Long> list = new ArrayList<>(count);
    for (int index = 0; index < count; index++) {
      list.add(entities.getJsonNumber(index).longValue());
    }

    return new SzEntityPath(startId, endId, list);
  }

}
