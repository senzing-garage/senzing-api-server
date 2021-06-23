package com.senzing.api.model.impl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.senzing.api.model.SzEntityClass;
import com.senzing.api.model.SzEntityClassesResponseData;

import java.util.*;

/**
 * Provides a default implementation of {@Link SzEntityClassesResponseData}.
 */
@JsonDeserialize
public class SzEntityClassesResponseDataImpl implements SzEntityClassesResponseData {
  /**
   * The map of {@link String} entity class codes to {@link SzEntityClass}
   * instances.
   */
  private Map<String, SzEntityClass> entityClasses;

  /**
   * Default constructor.
   */
  public SzEntityClassesResponseDataImpl() {
    this.entityClasses = new LinkedHashMap<>();
  }

  /**
   * Constructs with the specified {@link Collection} of {@link SzEntityClass}
   * instances describing the entity classes for this instance.
   *
   * @param entityClasses The {@link Collection} of {@link SzEntityClass} instances
   *                    describing the entity classes for this instance.
   */
  public SzEntityClassesResponseDataImpl(
      Collection<? extends SzEntityClass> entityClasses)
  {
    this.entityClasses = new LinkedHashMap<>();
    if (entityClasses != null) {
      for (SzEntityClass entityClass: entityClasses) {
        this.entityClasses.put(entityClass.getEntityClassCode(), entityClass);
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Set<String> getEntityClasses() {
    Set<String> set = this.entityClasses.keySet();
    return Collections.unmodifiableSet(set);
  }

  /**
   * Private setter for JSON deserialization.
   */
  @JsonProperty("entityClasses")
  private void setEntityClasses(Set<String> entityClasses) {
    Iterator<Map.Entry<String,SzEntityClass>> iter
        = this.entityClasses.entrySet().iterator();

    // remove entries in the map that are not in the specified set
    while (iter.hasNext()) {
      Map.Entry<String,SzEntityClass> entry = iter.next();
      if (!entityClasses.contains(entry.getKey())) {
        iter.remove();
      }
    }

    // add place-holder entries to the map for entity classes in the set
    for (String entityClass: entityClasses) {
      this.entityClasses.put(entityClass, null);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Map<String, SzEntityClass> getEntityClassDetails() {
    return Collections.unmodifiableMap(this.entityClasses);
  }

  /**
   * Private setter for JSON deserialization.
   */
  @JsonProperty("entityClassDetails")
  private void setEntityClassDetails(Map<String, SzEntityClass> details)
  {
    this.setEntityClasses(details == null ? null : details.values());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void addEntityClass(SzEntityClass entityClass) {
    Objects.requireNonNull(
        entityClass, "The specified entity class cannot be null");

    this.entityClasses.put(entityClass.getEntityClassCode(), entityClass);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setEntityClasses(Collection<? extends SzEntityClass> entityClasses) {
    this.entityClasses.clear();
    if (entityClasses != null) {
      for (SzEntityClass entityClass : entityClasses) {
        this.entityClasses.put(entityClass.getEntityClassCode(), entityClass);
      }
    }
  }

}
