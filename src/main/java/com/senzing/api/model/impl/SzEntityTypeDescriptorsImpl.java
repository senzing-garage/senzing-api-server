package com.senzing.api.model.impl;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.senzing.api.model.*;

import java.util.*;

/**
 * Provides a default implementation of {@link SzEntityTypeDescriptors}.
 */
@JsonDeserialize
public class SzEntityTypeDescriptorsImpl implements SzEntityTypeDescriptors {
  /**
   * The {@link List} of {@link SzEntityTypeDescriptor} instances.
   */
  private List<SzEntityTypeDescriptor> descriptors;

  /**
   * Constructs with no {@link SzEntityTypeDescriptor} instances.
   */
  public SzEntityTypeDescriptorsImpl() throws NullPointerException
  {
    this.descriptors = Collections.emptyList();
  }

  /**
   * Constructs with a single {@link SzEntityTypeDescriptor} instance.
   *
   * @param identifier The single non-null {@link SzEntityTypeDescriptor}
   *                   instance.
   *
   * @throws NullPointerException If the specified parameter is null.
   */
  public SzEntityTypeDescriptorsImpl(SzEntityTypeDescriptor identifier)
      throws NullPointerException
  {
    Objects.requireNonNull(identifier, "Identifier cannot be null.");
    this.descriptors = Collections.singletonList(identifier);
  }

  /**
   * Constructs with the specified {@link Collection} of {@link
   * SzEntityTypeDescriptor} instances.  The specified {@link Collection} will
   * be copied.
   *
   * @param descriptors The non-null {@link Collection} of {@link
   *                    SzEntityTypeDescriptor} instances.
   *
   * @throws NullPointerException If the specified parameter is null.
   */
  public SzEntityTypeDescriptorsImpl(
      Collection<? extends SzEntityTypeDescriptor> descriptors)
    throws NullPointerException
  {
    Objects.requireNonNull(descriptors, "Identifiers cannot be null.");
    this.descriptors = Collections.unmodifiableList(
        new ArrayList<>(descriptors));
  }

  /**
   * Returns the unmodifiable {@link List} of {@link SzEntityTypeDescriptor}
   * instances that were specified.
   *
   * @return The unmodifiable {@link List} of {@link SzEntityTypeDescriptor}
   *         instances that were specified.
   */
  public List<SzEntityTypeDescriptor> getDescriptors() {
    return this.descriptors;
  }

  /**
   * Overridden to convert the {@link SzEntityTypeDescriptorsImpl} instance to a
   * JSON array string.
   *
   * @return The JSON array string representation of this instance.
   *
   */
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    String prefix = "";
    for (SzEntityTypeDescriptor identifier : this.getDescriptors()) {
      sb.append(prefix).append(identifier.toString());
      prefix = ",";
    }
    sb.append("]");
    return sb.toString();
  }
}
