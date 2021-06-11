package com.senzing.api.model.impl;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.senzing.api.model.SzEntityType;
import java.util.Objects;

/**
 * Describes a entity type in its entirety.
 */
@JsonDeserialize
public class SzEntityTypeImpl implements SzEntityType {
  /**
   * The entity type code.
   */
  private String entityTypeCode;

  /**
   * The entity type ID.
   */
  private Integer entityTypeId;

  /**
   * The entity class code identifying the associated entity class.
   */
  private String entityClassCode;

  /**
   * Default constructor.
   */
  public SzEntityTypeImpl() {
    this.entityTypeCode = null;
    this.entityTypeId   = null;
    this.entityTypeCode = null;
  }

  /**
   * Constructs with only the entity type code, leaving the entity class code
   * and entity type ID as <tt>null</tt>.
   *
   * @param entityTypeCode The entity type code for the entity type.
   */
  public SzEntityTypeImpl(String entityTypeCode) {
    this(entityTypeCode, null, null);
  }

  /**
   * Constructs with the specified entity tfype code and entity type ID.
   *
   * @param entityTypeCode The entity type code for the entity type.
   * @param entityTypeId The entity type ID for the entity type.
   * @param entityClassCode The entity class code for the associated entity
   *                        class.
   */
  public SzEntityTypeImpl(String    entityTypeCode,
                          Integer   entityTypeId,
                          String    entityClassCode)
  {
    this.entityTypeCode   = entityTypeCode.toUpperCase().trim();
    this.entityTypeId     = entityTypeId;
    this.entityClassCode  = entityClassCode;
    if (this.entityClassCode != null) {
      this.entityClassCode = this.entityClassCode.toUpperCase().trim();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getEntityTypeCode() {
    return this.entityTypeCode;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setEntityTypeCode(String code) {
    this.entityTypeCode = code;
    if (this.entityTypeCode != null) {
      this.entityTypeCode = this.entityTypeCode.toUpperCase().trim();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Integer getEntityTypeId() {
    return this.entityTypeId;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setEntityTypeId(Integer entityTypeId) {
    this.entityTypeId = entityTypeId;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getEntityClassCode() {
    return this.entityClassCode;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setEntityClassCode(String code) {
    this.entityClassCode = code;
    if (this.entityClassCode != null) {
      this.entityClassCode = this.entityClassCode.toUpperCase().trim();
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SzEntityTypeImpl ec = (SzEntityTypeImpl) o;
    return Objects.equals(this.getEntityTypeCode(), ec.getEntityTypeCode())
        && Objects.equals(this.getEntityTypeId(), ec.getEntityTypeId())
        && Objects.equals(this.getEntityClassCode(), ec.getEntityClassCode());
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.getEntityTypeCode(),
                        this.getEntityTypeId(),
                        this.getEntityClassCode());
  }

  @Override
  public String toString() {
    return this.toJson();
  }
}
