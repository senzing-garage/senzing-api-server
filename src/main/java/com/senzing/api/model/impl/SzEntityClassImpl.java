package com.senzing.api.model.impl;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.senzing.api.model.SzEntityClass;
import com.senzing.api.model.SzEntityClassDescriptor;
import com.senzing.util.JsonUtilities;

import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Describes a entity class in its entirety.
 */
@JsonDeserialize
public class SzEntityClassImpl implements SzEntityClass {
  /**
   * The entity class code.
   */
  private String entityClassCode;

  /**
   * The entity class ID.
   */
  private Integer entityClassId;

  /**
   * Flag indicating if entities of this class resolve against each other.
   */
  private Boolean resolving;

  /**
   * Default constructor.
   */
  public SzEntityClassImpl() {
    this.entityClassCode  = null;
    this.entityClassId    = null;
    this.resolving        = null;
  }

  /**
   * Constructs with the specified entity class code leaving the entity
   * class ID and resolving flag <tt>null</tt>.
   *
   * @param entityClassCode The entity class code for the entity class.
   */
  public SzEntityClassImpl(String entityClassCode) {
    this(entityClassCode, null, null);
  }

  /**
   * Constructs with the specified entity class code, entity class ID
   * and flag indicating whether or not entities having entity types belonging
   * to this entity class will resolve against each other.
   *
   * @param entityClassCode The entity class code for the entity class.
   * @param entityClassId The entity class ID for the entity class.
   * @param resolving <tt>true</tt> if entities having entity types belonging
   *                  to this entity class will resolve against each other,
   *                  and <tt>false</tt> if they will not resolve.
   */
  public SzEntityClassImpl(String   entityClassCode,
                           Integer  entityClassId,
                           Boolean  resolving)
  {
    this.entityClassCode  = entityClassCode.toUpperCase().trim();
    this.entityClassId    = entityClassId;
    this.resolving        = resolving;
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

  /**
   * {@inheritDoc}
   */
  @Override
  public Integer getEntityClassId() {
    return this.entityClassId;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setEntityClassId(Integer entityClassId) {
    this.entityClassId = entityClassId;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Boolean isResolving() {
    return this.resolving;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setResolving(Boolean resolving) {
    this.resolving = resolving;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SzEntityClassImpl eclass = (SzEntityClassImpl) o;
    return Objects.equals(getEntityClassCode(), eclass.getEntityClassCode())
        && Objects.equals(this.getEntityClassId(), eclass.getEntityClassId())
        && Objects.equals(this.isResolving(), eclass.isResolving());
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.getEntityClassCode(),
                        this.getEntityClassId(),
                        this.isResolving());
  }

  @Override
  public String toString() {
    return this.toJson();
  }
}
