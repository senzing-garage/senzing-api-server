package com.senzing.api.model.impl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.senzing.api.model.SzEntityType;
import com.senzing.api.model.SzEntityTypesResponseData;

import java.util.*;

/**
 * Provides a default implementation of {@Link SzEntityTypesResponseData}.
 */
@JsonDeserialize
public class SzEntityTypesResponseDataImpl implements SzEntityTypesResponseData {
  /**
   * The map of {@link String} entity type codes to {@link SzEntityType}
   * instances.
   */
  private Map<String, SzEntityType> entityTypes;

  /**
   * Default constructor.
   */
  public SzEntityTypesResponseDataImpl() {
    this.entityTypes = new LinkedHashMap<>();
  }

  /**
   * Constructs with the specified {@link Collection} of {@link SzEntityType}
   * instances describing the entity types for this instance.
   *
   * @param entityTypes The {@link Collection} of {@link SzEntityType} instances
   *                    describing the entity types for this instance.
   */
  public SzEntityTypesResponseDataImpl(
      Collection<? extends SzEntityType> entityTypes)
  {
    this.entityTypes = new LinkedHashMap<>();
    if (entityTypes != null) {
      for (SzEntityType entityType: entityTypes) {
        this.entityTypes.put(entityType.getEntityTypeCode(), entityType);
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Set<String> getEntityTypes() {
    Set<String> set = this.entityTypes.keySet();
    return Collections.unmodifiableSet(set);
  }

  /**
   * Private setter for JSON deserialization.
   */
  @JsonProperty("entityTypes")
  private void setEntityTypes(Set<String> entityTypes) {
    Iterator<Map.Entry<String,SzEntityType>> iter
        = this.entityTypes.entrySet().iterator();

    // remove entries in the map that are not in the specified set
    while (iter.hasNext()) {
      Map.Entry<String,SzEntityType> entry = iter.next();
      if (!entityTypes.contains(entry.getKey())) {
        iter.remove();
      }
    }

    // add place-holder entries to the map for entity types in the set
    for (String entityType: entityTypes) {
      this.entityTypes.put(entityType, null);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Map<String, SzEntityType> getEntityTypeDetails() {
    return Collections.unmodifiableMap(this.entityTypes);
  }

  /**
   * Private setter for JSON deserialization.
   */
  @JsonProperty("entityTypeDetails")
  private void setEntityTypeDetails(Map<String, SzEntityType> details)
  {
    this.setEntityTypes(details == null ? null : details.values());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void addEntityType(SzEntityType entityType) {
    Objects.requireNonNull(
        entityType, "The specified entity type cannot be null");

    this.entityTypes.put(entityType.getEntityTypeCode(), entityType);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setEntityTypes(Collection<? extends SzEntityType> entityTypes) {
    this.entityTypes.clear();
    if (entityTypes != null) {
      for (SzEntityType entityType : entityTypes) {
        this.entityTypes.put(entityType.getEntityTypeCode(), entityType);
      }
    }
  }

}
