package com.senzing.api.model.impl;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.senzing.api.model.*;
import com.senzing.util.JsonUtilities;

import javax.json.*;
import java.util.*;

/**
 * Used to represent a {@link List} of zero or more {@link
 * SzEntityClassDescriptor} instances.
 *
 */
@JsonDeserialize
public class SzEntityClassDescriptorsImpl implements SzEntityClassDescriptors {
  /**
   * The {@link List} of {@link SzEntityClassDescriptor} instances.
   */
  private List<SzEntityClassDescriptor> descriptors;

  /**
   * Constructs with no {@link SzEntityClassDescriptor} instances.
   */
  public SzEntityClassDescriptorsImpl() throws NullPointerException
  {
    this.descriptors = Collections.emptyList();
  }

  /**
   * Constructs with a single {@link SzEntityClassDescriptor} instance.
   *
   * @param identifier The single non-null {@link SzEntityClassDescriptor}
   *                   instance.
   *
   * @throws NullPointerException If the specified parameter is null.
   */
  public SzEntityClassDescriptorsImpl(SzEntityClassDescriptor identifier)
      throws NullPointerException
  {
    Objects.requireNonNull(identifier, "Identifier cannot be null.");
    this.descriptors = Collections.singletonList(identifier);
  }

  /**
   * Constructs with the specified {@link Collection} of {@link
   * SzEntityClassDescriptor} instances.  The specified {@link Collection} will
   * be copied.
   *
   * @param descriptors The non-null {@link Collection} of {@link
   *                    SzEntityClassDescriptor} instances.
   *
   * @throws NullPointerException If the specified parameter is null.
   */
  public SzEntityClassDescriptorsImpl(
      Collection<? extends SzEntityClassDescriptor> descriptors)
    throws NullPointerException
  {
    Objects.requireNonNull(descriptors, "Identifiers cannot be null.");
    this.descriptors = Collections.unmodifiableList(
        new ArrayList<>(descriptors));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<SzEntityClassDescriptor> getDescriptors() {
    return this.descriptors;
  }

  /**
   * Overridden to convert the {@link SzEntityClassDescriptorsImpl} instance to a
   * JSON array string.
   *
   * @return The JSON array string representation of this instance.
   *
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    String prefix = "";
    for (SzEntityClassDescriptor identifier : this.getDescriptors()) {
      sb.append(prefix).append(identifier.toString());
      prefix = ",";
    }
    sb.append("]");
    return sb.toString();
  }
}
