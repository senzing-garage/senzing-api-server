package com.senzing.api.model;

import com.fasterxml.jackson.annotation.JsonInclude;

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
    if (flags != null) this.flags.addAll(flags);
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
   * Sets the {@link List} of {@link SzFlaggedRecord} instances using those
   * contained in the specified {@link Collection}.
   *
   * @param sampleRecords The sample records to set, or <tt>null</tt> if none.
   */
  public void setSampleRecords(Collection<SzFlaggedRecord> sampleRecords) {
    this.sampleRecords.clear();
    if (sampleRecords != null) this.sampleRecords.addAll(sampleRecords);
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
}
