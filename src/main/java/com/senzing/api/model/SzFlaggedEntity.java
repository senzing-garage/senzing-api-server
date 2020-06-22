package com.senzing.api.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.senzing.util.JsonUtils;

import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonString;
import java.util.*;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;
import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_EMPTY;

/**
 * Describes an entity that was flagged during entity resolution.
 */
public class SzFlaggedEntity {
  /**
   * The entity ID of this entity.
   */
  private Long entityId;

  /**
   * The number of degrees that this entity is separated from the resolved
   * entity.
   */
  private Integer degrees;

  /**
   * The {@link Set} of {@link String} flags for this entity.
   */
  private Set<String> flags;

  /**
   * The {@link List} of {@link SzFlaggedRecord} instances.
   */
  private List<SzFlaggedRecord> sampleRecords;

  /**
   * Default constructor.
   */
  public SzFlaggedEntity() {
    this.entityId       = null;
    this.degrees        = null;
    this.flags          = new LinkedHashSet<>();
    this.sampleRecords  = new LinkedList<>();
  }

  /**
   * Gets the entity ID for the flagged entity.
   *
   * @return The entity ID for the flagged entity.
   */
  @JsonInclude(NON_NULL)
  public Long getEntityId() {
    return entityId;
  }

  /**
   * Sets the entity ID for the flagged entity.
   *
   * @param entityId The entity ID for the flagged entity.
   */
  public void setEntityId(Long entityId) {
    this.entityId = entityId;
  }

  /**
   * Gets the number of degrees of separation for the flagged entity.
   *
   * @return The number of degrees of separation for the flagged entity.
   */
  @JsonInclude(NON_NULL)
  public Integer getDegrees() {
    return degrees;
  }

  /**
   * Sets the number of degrees of separation for the flagged entity.
   *
   * @param degrees The number of degrees of separation for the flagged entity.
   */
  public void setDegrees(Integer degrees) {
    this.degrees = degrees;
  }

  /**
   * Gets the {@link Set} of {@link String} flags that were triggered for this
   * entity.
   *
   * @return The {@link Set} of {@link String} flags that were triggered for
   *         this entity.
   */
  @JsonInclude(NON_EMPTY)
  public Set<String> getFlags() {
    return Collections.unmodifiableSet(this.flags);
  }

  /**
   * Adds the specified {@link String} flag to the {@link Set} of flags
   * triggered by this flagged entity.
   *
   * @param flag The {@link String} flag to add to the {@link Set} of flags.
   */
  public void addFlag(String flag) {
    if (flag != null) this.flags.add(flag);
  }

  /**
   * Sets the {@link Set} of {@link String} flags that were triggered for this
   * entity.
   *
   * @return The {@link Set} of {@link String} flags that were triggered for
   *         this entity, or <tt>null</tt> if none.
   */
  public void setFlags(Set<String> flags) {
    this.flags.clear();
    if (flags != null) {
      for (String flag : flags) {
        if (flag != null) this.flags.add(flag);
      }
    }
  }

  /**
   * Gets the {@link List} of {@link SzFlaggedRecord} instances describing the
   * sample of records that were flagged for this entity.
   *
   * @return The {@link List} of {@link SzFlaggedRecord} instances describing
   *         the sample of records that were flagged for this entity.
   */
  @JsonInclude(NON_EMPTY)
  public List<SzFlaggedRecord> getSampleRecords() {
    return Collections.unmodifiableList(sampleRecords);
  }

  /**
   * Adds the specified {@link SzFlaggedRecord} to the {@link List} of sample
   * records for this instance.  If the specified record is <tt>null</tt> then
   * it is not added.
   *
   * @param sampleRecord The {@link SzFlaggedRecord} to add as a sample record.
   */
  public void addSampleRecord(SzFlaggedRecord sampleRecord) {
    if (sampleRecord != null) this.sampleRecords.add(sampleRecord);
  }

  /**
   * Sets the {@link List} of {@link SzFlaggedRecord} instances using those
   * contained in the specified {@link Collection}.
   *
   * @param sampleRecords The sample records to set, or <tt>null</tt> if none.
   */
  public void setSampleRecords(Collection<SzFlaggedRecord> sampleRecords) {
    this.sampleRecords.clear();
    if (sampleRecords != null) {
      for (SzFlaggedRecord record: sampleRecords) {
        if (record != null) this.sampleRecords.add(record);
      }
    }
  }

  @Override
  public String toString() {
    return "SzFlaggedEntity{" +
        "entityId=" + entityId +
        ", degrees=" + degrees +
        ", flags=" + flags +
        ", sampleRecords=" + sampleRecords +
        '}';
  }

  /**
   * Parses a list of flagged entities from a {@link JsonArray} describing a
   * JSON array in the Senzing native API format for flagged entity info and
   * populates the specified {@link List} or creates a new {@link List}.
   *
   * @param list      The {@link List} of {@link SzFlaggedEntity} instances to
   *                  populate, or <tt>null</tt> if a new {@link List}
   *                  should be created.
   * @param jsonArray The {@link JsonArray} describing the JSON in the
   *                  Senzing native API format.
   * @return The populated (or created) {@link List} of {@link
   *         SzFlaggedEntity} instances.
   */
  public static List<SzFlaggedEntity> parseFlaggedEntityList(
      List<SzFlaggedEntity> list,
      JsonArray             jsonArray)
  {
    if (list == null) {
      list = new ArrayList<>(jsonArray == null ? 0 : jsonArray.size());
    }

    if (jsonArray == null) return list;

    for (JsonObject jsonObject : jsonArray.getValuesAs(JsonObject.class)) {
      list.add(parseFlaggedEntity(null, jsonObject));
    }
    return list;
  }

  /**
   * Parses the flagged entity from a {@link JsonObject} describing JSON
   * for the Senzing native API format for flagged entity info and populates
   * the specified {@link SzFlaggedEntity} or creates a new instance.
   *
   * @param entity     The {@link SzFlaggedEntity} instance to populate, or
   *                   <tt>null</tt> if a new instance should be created.
   * @param jsonObject The {@link JsonObject} describing the JSON in the
   *                   Senzing native API format.
   * @return The populated (or created) {@link SzFlaggedEntity}.
   */
  public static SzFlaggedEntity parseFlaggedEntity(
      SzFlaggedEntity entity,
      JsonObject      jsonObject)
  {
    if (entity == null) entity = new SzFlaggedEntity();

    entity.setEntityId(JsonUtils.getLong(jsonObject, "ENTITY_ID"));
    entity.setDegrees(JsonUtils.getInteger(jsonObject, "DEGREES"));

    JsonArray jsonArray = JsonUtils.getJsonArray(jsonObject, "FLAGS");
    if (jsonArray != null) {
      for (JsonString flag : jsonArray.getValuesAs(JsonString.class)) {
        entity.addFlag(flag.getString());
      }
    }

    jsonArray = JsonUtils.getJsonArray(jsonObject, "SAMPLE_RECORDS");
    if (jsonArray != null) {
      List<SzFlaggedRecord> sampleRecords
          = SzFlaggedRecord.parseFlaggedRecordList(entity.sampleRecords,
                                                   jsonArray);
    }

    return entity;
  }

}
