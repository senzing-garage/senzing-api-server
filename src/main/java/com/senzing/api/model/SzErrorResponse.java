package com.senzing.api.model;

import com.senzing.g2.engine.G2Fallible;
import com.senzing.util.Timers;

import javax.ws.rs.core.UriInfo;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * Extends {@link SzBasicResponse} to create a response for when errors occur.
 */
public class SzErrorResponse extends SzBasicResponse {
  /**
   * The list of errors.
   */
  private List<SzError> errors;

  /**
   * Package-private default constructor.
   */
  SzErrorResponse() {
    this.errors = null;
  }

  /**
   * Constructs with the specified HTTP method and self link.
   *
   * @param meta The response meta data.
   *
   * @param links The links for the response.
   *
   */
  public SzErrorResponse(SzMeta meta, SzLinks links)
  {
    this(meta, links, (SzError) null);
  }

  /**
   * Constructs with the specified HTTP method and self link and the first
   * error.
   *
   * @param meta The response meta data.
   *
   * @param links The links for the response.
   *
   * @param firstError The {@link SzError} describing the first error.
   */
  public SzErrorResponse(SzMeta       meta,
                         SzLinks      links,
                         SzError      firstError)
  {
    super(meta, links);
    this.errors = new LinkedList<>();
    if (firstError != null) this.errors.add(firstError);
  }

  /**
   * Constructs with the specified HTTP method and self link and the first
   * error message.
   *
   * @param meta The response meta data.
   *
   * @param links The links for the response.
   *
   * @param firstError The error message for the first error.
   */
  public SzErrorResponse(SzMeta meta, SzLinks links, String firstError)
  {
    this(meta, links, firstError != null ? new SzError(firstError) : null);
  }

  /**
   * Constructs with the specified HTTP method and self link and the first
   * error.
   *
   * @param meta The response meta data.
   *
   * @param links The links for the response.
   *
   * @param firstError The {@link Throwable} describing the first error.
   */
  public SzErrorResponse(SzMeta       meta,
                         SzLinks      links,
                         Throwable    firstError)
  {
    this(meta, links, firstError != null ? new SzError(firstError) : null);
  }

  /**
   * Constructs with the specified HTTP method and self link and the first
   * error.
   *
   * @param meta The response meta data.
   *
   * @param links The links for the response.
   *
   * @param firstErrorFallible The {@link G2Fallible} from which to extract the
   *                           error code and exception message.
   */
  public SzErrorResponse(SzMeta     meta,
                         SzLinks    links,
                         G2Fallible firstErrorFallible)
  {
    this(meta,
         links,
         ((firstErrorFallible != null)
             ? new SzError(firstErrorFallible) : null));
  }

  /**
   * Add an error to this instance.
   *
   * @param error The non-null {@link SzError} describing the failure.
   */
  public void addError(SzError error) {
    if (error == null) return;
    this.errors.add(error);
  }

  /**
   * Returns an unmodifiable view of the errors associated with this instance.
   *
   * @return The {@link List} of {@link SzError} instances for the associated
   *         errors.
   */
  public List<SzError> getErrors() {
    return Collections.unmodifiableList(this.errors);
  }

  /**
   * Private method to set the errors during JSON deserialization.
   *
   * @param errors The {@link List} of {@link SzError} instances.
   */
  private void setErrors(List<SzError> errors) {
    this.errors = (errors == null) ? null : new ArrayList<>(errors);
  }
}
