package com.senzing.api.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.senzing.util.JsonUtils;

import javax.json.JsonArray;
import javax.json.JsonObject;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.Collections;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_EMPTY;
import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;

/**
 * Describes the perspective used in evaluating why an entity resolved or why
 * two records may or may not resolve.  The answer to "why" is dependent on
 * which "record" you are comparing against the other "records".  Internally,
 * it is not always based on "record" because multiple records that are
 * effectively identical collapse into a single perspective.
 */
public class SzWhyPerspective {
  /**
   * The internal ID uniquely identifying this perspective from others
   * in the complete "why" response.
   */
  private Long internalId;

  /**
   * The associated entity ID for the perspective.
   */
  private Long entityId;

  /**
   * The {@link Set} of {@link SzFocusRecordId} instances identifying the
   * effectively identical records that are being compared against the other
   * records.
   */
  private Set<SzFocusRecordId> focusRecords;

  /**
   * Default constructor.
   */
  public SzWhyPerspective() {
    this.internalId = null;
    this.entityId = null;
    this.focusRecords = new LinkedHashSet<>();
  }

  /**
   * Gets the internal ID uniquely identifying this perspective from others
   * in the complete "why" response.
   *
   * @return The internal ID uniquely identifying this perspective from others
   * in the complete "why" response.
   */
  @JsonInclude(NON_NULL)
  public Long getInternalId() {
    return this.internalId;
  }

  /**
   * Sets the internal ID uniquely identifying this perspective from others
   * in the complete "why" response.
   *
   * @param internalId The internal ID uniquely identifying this perspective
   *                   from others in the complete "why" response.
   */
  public void setInternalId(Long internalId) {
    this.internalId = internalId;
  }

  /**
   * Gets the associated entity ID for the perspective.
   *
   * @return The associated entity ID for the perspective.
   */
  @JsonInclude(NON_NULL)
  public Long getEntityId() {
    return this.entityId;
  }

  /**
   * Sets the associated entity ID for the perspective.
   *
   * @param entityId The associated entity ID for the perspective.
   */
  public void setEntityId(Long entityId) {
    this.entityId = entityId;
  }

  /**
   * Gets the <b>unmodifiable</b> {@link Set} of {@link SzFocusRecordId}
   * instances identifying the focus records for this perspective.
   *
   * @return The <b>unmodifiable</b> {@link Set} of {@link SzFocusRecordId}
   *         instances identifying the focus records for this perspective.
   */
  @JsonInclude(NON_EMPTY)
  public Set<SzFocusRecordId> getFocusRecords() {
    return Collections.unmodifiableSet(this.focusRecords);
  }

  /**
   * Adds the specified {@link SzFocusRecordId} to the {@link Set} of focus
   * records.
   *
   * @param focusRecord The {@link SzFocusRecordId} to add to the focus records.
   */
  public void addFocusRecord(SzFocusRecordId focusRecord) {
    this.focusRecords.add(focusRecord);
  }

  /**
   * Sets the {@link Set} of {@link SzFocusRecordId} instances identifying the
   * focus records for this perspective.
   *
   * @param focusRecords The {@link Set} of {@link SzFocusRecordId} instances
   *                     identifying the focus records for this perspective.
   */
  public void setFocusRecords(Collection<SzFocusRecordId> focusRecords) {
    this.focusRecords.clear();
    if (focusRecords != null) this.focusRecords.addAll(focusRecords);
  }

  /**
   * Parses the native API JSON to build an instance of {@link
   * SzWhyPerspective}.
   *
   * @param jsonObject The {@link JsonObject} describing the record using the
   *                   native API JSON format.
   *
   * @return The created instance of {@link SzWhyPerspective}.
   */
  public static SzWhyPerspective parseWhyPerspective(JsonObject jsonObject) {
    return parseWhyPerspective(jsonObject, "");
  }

  /**
   * Parses the native API JSON to build an instance of {@link
   * SzWhyPerspective}.
   *
   * @param jsonObject The {@link JsonObject} describing the perspective using
   *                   the native API JSON format.
   *
   * @param suffix The suffix to apply to the native JSON keys.
   *
   * @return The created instance of {@link SzWhyPerspective}.
   */
  public static SzWhyPerspective parseWhyPerspective(JsonObject jsonObject,
                                                     String     suffix)
  {
    Long internalId = JsonUtils.getLong(jsonObject,"INTERNAL_ID" + suffix);

    Long entityId = JsonUtils.getLong(jsonObject,"ENTITY_ID" + suffix);

    JsonArray jsonArr = jsonObject.getJsonArray("FOCUS_RECORDS" + suffix);

    Collection<SzFocusRecordId> focusRecords
        = SzFocusRecordId.parseFocusRecordIdList(jsonArr);

    SzWhyPerspective perspective = new SzWhyPerspective();

    perspective.setInternalId(internalId);
    perspective.setEntityId(entityId);
    perspective.setFocusRecords(focusRecords);

    return perspective;
  }

}
