package com.senzing.api.model;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_EMPTY;
import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;

/**
 * Describes a record for a flagged entity.
 */
public class SzFlaggedRecord {
  /**
   * The data source for the record.
   */
  private String dataSource;

  /**
   * The record ID for the record.
   */
  private String recordId;

  /**
   * The {@link Set} of flags for the record.
   */
  private Set<String> flags;

  /**
   * Default constructor.
   */
  public SzFlaggedRecord() {
    this.dataSource = null;
    this.recordId   = null;
    this.flags      = new LinkedHashSet<>();
  }

  /**
   * Gets the data source for the flagged record.
   *
   * @return The data source for the flagged record.
   */
  @JsonInclude(NON_NULL)
  public String getDataSource() {
    return dataSource;
  }

  /**
   * Sets the data source for the flagged record.
   *
   * @param dataSource The data source for the flagged record.
   */
  public void setDataSource(String dataSource) {
    this.dataSource = dataSource;
  }

  /**
   * Gets the record ID for the flagged record.
   *
   * @return The record ID for the flagged record.
   */
  @JsonInclude(NON_NULL)
  public String getRecordId() {
    return recordId;
  }

  /**
   * Sets the record ID for the flagged record.
   *
   * @param recordId The record ID for the flagged record.
   */
  public void setRecordId(String recordId) {
    this.recordId = recordId;
  }

  /**
   * Returns the {@link Set} of {@link String} flags that were flagged for
   * this record.
   *
   * @return The {@link Set} of {@link String} flags that were flagged for
   *         this record, or <tt>null</tt> if none.
   */
  @JsonInclude(NON_EMPTY)
  public Set<String> getFlags() {
    return Collections.unmodifiableSet(this.flags);
  }

  /**
   * Adds the specified {@link String} flag as one flagged for this record.
   *
   * @param flag The flag to add to this instance.
   */
  public void addFlag(String flag) {
    this.flags.add(flag);
  }

  /**
   * Sets the {@link String} flags that were flagged for this record.
   *
   * @param flags The {@link Collection} of {@link String} flags that were
   *              flagged for this record.
   */
  public void setFlags(Collection<String> flags) {
    this.flags.clear();
    if (flags != null) this.flags.addAll(flags);
  }

  @Override
  public String toString() {
    return "SzFlaggedRecord{" +
        "dataSource='" + dataSource + '\'' +
        ", recordId='" + recordId + '\'' +
        ", flags=" + flags +
        '}';
  }
}
