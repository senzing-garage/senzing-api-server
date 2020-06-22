package com.senzing.api.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.senzing.util.JsonUtils;

import javax.json.JsonArray;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.json.JsonString;
import java.util.*;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_EMPTY;
import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;

/**
 * Describes the information associated with resolution of a record.
 */
public class SzResolutionInfo {
  /**
   * The data source for the focus record.
   */
  private String dataSource;

  /**
   * The record ID for the focus record.
   */
  private String recordId;

  /**
   * The {@link List} of {@link Long} entity ID's for the affected entities.
   */
  private Set<Long> affectedEntities;

  /**
   * The {@link List} of {@link SzFlaggedEntity} instances describing the
   * flagged entities.
   */
  private List<SzFlaggedEntity> flaggedEntities;

  /**
   * Default constructor.
   */
  public SzResolutionInfo() {
    this.dataSource       = null;
    this.recordId         = null;
    this.affectedEntities = new LinkedHashSet<>();
    this.flaggedEntities  = new LinkedList<>();
  }

  /**
   * Gets the data source for the focal record.
   *
   * @return The data source for the focal record.
   */
  @JsonInclude(NON_NULL)
  public String getDataSource() {
    return dataSource;
  }

  /**
   * Sets the data source for the focal record.
   *
   * @param dataSource The data source for the focal record.
   */
  public void setDataSource(String dataSource) {
    this.dataSource = dataSource;
  }

  /**
   * Gets the record ID for the focal record.
   *
   * @return The record ID for the focal record.
   */
  @JsonInclude(NON_NULL)
  public String getRecordId() {
    return recordId;
  }

  /**
   * Sets the record ID for the focal record.
   *
   * @param recordId The record ID for the focal record.
   */
  public void setRecordId(String recordId) {
    this.recordId = recordId;
  }

  /**
   * Get the <b>unmodifiable</b> {@link Set} of entity ID's for the affected
   * entities.
   *
   * @return The <b>unmodifiable</b> {@link Set} of entity ID's for the
   *         affected entities.
   */
  @JsonInclude(NON_EMPTY)
  public Set<Long> getAffectedEntities() {
    return Collections.unmodifiableSet(this.affectedEntities);
  }

  /**
   * Adds the specified entity ID to the {@Link Set} of affected entities.
   * If the specified value is <tt>null</tt> then this method does nothing.
   *
   * @param entityId The entity ID to add.
   */
  public void addAffectedEntity(Long entityId) {
    if (entityId != null) this.affectedEntities.add(entityId);
  }

  /**
   * Sets the {@link Set} of affected entity IDs to those in the specified
   * {@link Collection}.  If the specified parameter is <tt>null</tt> then no
   * entity IDs are added and any <tt>null</tt> values in the {@link Collection}
   * are ignored.
   *
   * @param affectedEntities
   */
  public void setAffectedEntities(Collection<Long> affectedEntities) {
    this.affectedEntities.clear();
    if (affectedEntities != null) {
      for (Long entityId : affectedEntities) {
        if (entityId != null) {
          this.affectedEntities.add(entityId);
        }
      }
    }
  }

  /**
   * Gets the <b>unmodifiable</b> {@link List} of {@link SzFlaggedEntity}
   * instances.
   *
   * @return The <b>unmodifiable</b> {@link List} of {@link SzFlaggedEntity}
   *         instances.
   */
  @JsonInclude(NON_EMPTY)
  public List<SzFlaggedEntity> getFlaggedEntities() {
    return Collections.unmodifiableList(this.flaggedEntities);
  }

  /**
   * Adds the specified {@link SzFlaggedEntity} to the {@link List} of flagged
   * entities for this instance.  This method does nothing if the specified
   * parameter is <tt>null</tt>.
   *
   * @param flaggedEntity The {@link SzFlaggedEntity} to add to the {@link List}
   */
  public void addFlaggedEntity(SzFlaggedEntity flaggedEntity) {
    if (flaggedEntity != null) this.flaggedEntities.add(flaggedEntity);
  }

  /**
   * Sets the {@link List} of flagged entities to those in the specified
   * {@link Collection} of {@link SzFlaggedEntity} instances.  If the specified
   * parameter is <tt>null</tt> then {@link List} of flagged entities is
   * cleared.  Any null instances in the specified {@link List} are ignored.
   *
   * @param flaggedEntities The {@link Collection} of {@link SzFlaggedEntity}
   *                        instances to add to the {@link List} of flagged
   *                        entities.
   */
  public void setFlaggedEntities(Collection<SzFlaggedEntity> flaggedEntities) {
    this.flaggedEntities.clear();
    if (flaggedEntities != null) {
      for (SzFlaggedEntity entity : flaggedEntities) {
        this.flaggedEntities.add(entity);
      }
    }
  }

  @Override
  public String toString() {
    return "SzResolutionInfo{" +
        "dataSource='" + dataSource + '\'' +
        ", recordId='" + recordId + '\'' +
        ", affectedEntities=" + affectedEntities +
        ", flaggedEntities=" + flaggedEntities +
        '}';
  }

  /**
   * Parses the resolution info from a {@link JsonObject} describing JSON
   * for the Senzing native API format for resolution info and populates
   * the specified {@link SzResolutionInfo} or creates a new instance.
   *
   * @param info The {@link SzResolutionInfo} instance to populate, or
   *             <tt>null</tt> if a new instance should be created.
   *
   * @param jsonObject The {@link JsonObject} describing the JSON in the
   *                   Senzing native API format.
   *
   * @return The populated (or created) {@link SzResolutionInfo}.
   */
  public static SzResolutionInfo parseResolutionInfo(
      SzResolutionInfo  info,
      JsonObject        jsonObject)
  {
    if (info == null) info = new SzResolutionInfo();

    info.setDataSource(JsonUtils.getString(jsonObject, "DATA_SOURCE"));
    info.setRecordId(JsonUtils.getString(jsonObject, "RECORD_ID"));

    JsonArray jsonArray = JsonUtils.getJsonArray(jsonObject,
                                                 "AFFECTED_ENTITIES");

    if (jsonArray != null) {
      for (JsonObject jsonObj : jsonArray.getValuesAs(JsonObject.class)) {
        info.addAffectedEntity(JsonUtils.getLong(jsonObj, "ENTITY_ID"));
      }
    }

    jsonArray = JsonUtils.getJsonArray(jsonObject, "INTERESTING_ENTITIES");
    if (jsonArray != null) {
      List<SzFlaggedEntity> flaggedEntities
          = SzFlaggedEntity.parseFlaggedEntityList(info.flaggedEntities,
                                                   jsonArray);
    }

    return info;
  }

}
