package com.senzing.api.model;

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
   * Constructs with the specified HTTP method and self link.
   *
   * @param httpMethod The {@link SzHttpMethod} from the request.
   *
   * @param selfLink The self link from the request.
   */
  public SzErrorResponse(SzHttpMethod httpMethod,
                         String       selfLink)
  {
    this(httpMethod, selfLink, (SzError) null);
  }

  /**
   * Constructs with the specified HTTP method and self link and the first
   * error.
   *
   * @param httpMethod The {@link SzHttpMethod} from the request.
   *
   * @param selfLink The self link from the request.
   *
   * @param firstError The {@link SzError} describing the first error.
   */
  public SzErrorResponse(SzHttpMethod httpMethod,
                         String       selfLink,
                         SzError      firstError)
  {
    super(httpMethod, selfLink);
    this.errors = new LinkedList<>();
    if (firstError != null) this.errors.add(firstError);
  }

  /**
   * Constructs with the specified HTTP method and self link and the first
   * error.
   *
   * @param httpMethod The {@link SzHttpMethod} from the request.
   *
   * @param selfLink The self link from the request.
   *
   * @param firstError The {@link Throwable} describing the first error.
   */
  public SzErrorResponse(SzHttpMethod httpMethod,
                         String       selfLink,
                         Throwable    firstError)
  {
    this(httpMethod,
         selfLink,
         firstError != null ? new SzError(firstError) : null);
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
}
