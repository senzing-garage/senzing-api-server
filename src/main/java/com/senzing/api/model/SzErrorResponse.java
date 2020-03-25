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
   * @param httpMethod The {@link SzHttpMethod} from the request.
   *
   * @param httpStatusCode The HTTP response code.
   *
   * @param selfLink The self link from the request.
   *
   * @param timers The {@link Timers} object for the timings that were taken.
   *
   */
  public SzErrorResponse(SzHttpMethod httpMethod,
                         int          httpStatusCode,
                         String       selfLink,
                         Timers       timers)
  {
    this(httpMethod, httpStatusCode, selfLink, timers, (SzError) null);
  }

  /**
   * Constructs with the specified HTTP method and self link and the first
   * error.
   *
   * @param httpMethod The {@link SzHttpMethod} from the request.
   *
   * @param httpStatusCode The HTTP response code.
   *
   * @param selfLink The self link from the request.
   *
   * @param timers The {@link Timers} object for the timings that were taken.
   *
   * @param firstError The {@link SzError} describing the first error.
   */
  public SzErrorResponse(SzHttpMethod httpMethod,
                         int          httpStatusCode,
                         String       selfLink,
                         Timers       timers,
                         SzError      firstError)
  {
    super(httpMethod, httpStatusCode, selfLink, timers);
    this.errors = new LinkedList<>();
    if (firstError != null) this.errors.add(firstError);
  }

  /**
   * Constructs with the specified HTTP method and self link and the first
   * error message.
   *
   * @param httpMethod The {@link SzHttpMethod} from the request.
   *
   * @param httpStatusCode The HTTP response code.
   *
   * @param selfLink The self link from the request.
   *
   * @param timers The {@link Timers} object for the timings that were taken.
   *
   * @param firstError The error message for the first error.
   */
  public SzErrorResponse(SzHttpMethod httpMethod,
                         int          httpStatusCode,
                         String       selfLink,
                         Timers       timers,
                         String       firstError)
  {
    this(httpMethod,
         httpStatusCode,
         selfLink,
         timers,
         firstError != null ? new SzError(firstError) : null);
  }

  /**
   * Constructs with the specified HTTP method and self link and the first
   * error.
   *
   * @param httpMethod The {@link SzHttpMethod} from the request.
   *
   * @param httpStatusCode The HTTP response code.
   *
   * @param selfLink The self link from the request.
   *
   * @param timers The {@link Timers} object for the timings that were taken.
   *
   * @param firstError The {@link Throwable} describing the first error.
   */
  public SzErrorResponse(SzHttpMethod httpMethod,
                         int          httpStatusCode,
                         String       selfLink,
                         Timers       timers,
                         Throwable    firstError)
  {
    this(httpMethod,
         httpStatusCode,
         selfLink,
         timers,
         firstError != null ? new SzError(firstError) : null);
  }

  /**
   * Constructs with the specified HTTP method and self link and the first
   * error.
   *
   * @param httpMethod The {@link SzHttpMethod} from the request.
   *
   * @param httpStatusCode The HTTP response code.
   *
   * @param selfLink The self link from the request.
   *
   * @param timers The {@link Timers} object for the timings that were taken.
   *
   * @param firstErrorFallible The {@link G2Fallible} from which to extract the
   *                           error code and exception message.
   */
  public SzErrorResponse(SzHttpMethod httpMethod,
                         int          httpStatusCode,
                         String       selfLink,
                         Timers       timers,
                         G2Fallible   firstErrorFallible)
  {
    this(httpMethod,
         httpStatusCode,
         selfLink,
         timers,
         ((firstErrorFallible != null)
             ? new SzError(firstErrorFallible) : null));
  }

  /**
   * Constructs with the specified HTTP method and {@link UriInfo}.
   *
   * @param httpMethod The {@link SzHttpMethod} from the request.
   *
   * @param httpStatusCode The HTTP response code.
   *
   * @param uriInfo The {@link UriInfo} from the request.
   *
   * @param timers The {@link Timers} object for the timings that were taken.
   *
   */
  public SzErrorResponse(SzHttpMethod httpMethod,
                         int          httpStatusCode,
                         UriInfo      uriInfo,
                         Timers       timers)
  {
    this(httpMethod, httpStatusCode, uriInfo, timers, (SzError) null);
  }

  /**
   * Constructs with the specified HTTP method and {@link UriInfo} and the
   * first error.
   *
   * @param httpMethod The {@link SzHttpMethod} from the request.
   *
   * @param httpStatusCode The HTTP response code.
   *
   * @param uriInfo The {@link UriInfo} from the request.
   *
   * @param timers The {@link Timers} object for the timings that were taken.
   *
   * @param firstError The {@link SzError} describing the first error.
   */
  public SzErrorResponse(SzHttpMethod httpMethod,
                         int          httpStatusCode,
                         UriInfo      uriInfo,
                         Timers       timers,
                         SzError      firstError)
  {
    super(httpMethod, httpStatusCode, uriInfo, timers);
    this.errors = new LinkedList<>();
    if (firstError != null) this.errors.add(firstError);
  }

  /**
   * Constructs with the specified HTTP method and {@link UriInfo} and the
   * first error message.
   *
   * @param httpMethod The {@link SzHttpMethod} from the request.
   *
   * @param httpStatusCode The HTTP response code.
   *
   * @param uriInfo The {@link UriInfo} from the request.
   *
   * @param timers The {@link Timers} object for the timings that were taken.
   *
   * @param firstError The error message for the first error.
   */
  public SzErrorResponse(SzHttpMethod httpMethod,
                         int          httpStatusCode,
                         UriInfo      uriInfo,
                         Timers       timers,
                         String       firstError)
  {
    this(httpMethod,
         httpStatusCode,
         uriInfo,
         timers,
         firstError != null ? new SzError(firstError) : null);
  }

  /**
   * Constructs with the specified HTTP method and {@link UriInfo} and the first
   * error.
   *
   * @param httpMethod The {@link SzHttpMethod} from the request.
   *
   * @param httpStatusCode The HTTP response code.
   *
   * @param uriInfo The {@link UriInfo} from the request.
   *
   * @param timers The {@link Timers} object for the timings that were taken.
   *
   * @param firstError The {@link Throwable} describing the first error.
   */
  public SzErrorResponse(SzHttpMethod httpMethod,
                         int          httpStatusCode,
                         UriInfo      uriInfo,
                         Timers       timers,
                         Throwable    firstError)
  {
    this(httpMethod,
         httpStatusCode,
         uriInfo,
         timers,
         firstError != null ? new SzError(firstError) : null);
  }

  /**
   * Constructs with the specified HTTP method and {@link UriInfo} link and
   * the first error.
   *
   * @param httpMethod The {@link SzHttpMethod} from the request.
   *
   * @param httpStatusCode The HTTP response code.
   *
   * @param uriInfo The {@link UriInfo} from the request.
   *
   * @param timers The {@link Timers} object for the timings that were taken.
   *
   * @param firstErrorFallible The {@link G2Fallible} from which to extract the
   *                           error code and exception message.
   */
  public SzErrorResponse(SzHttpMethod httpMethod,
                         int          httpStatusCode,
                         UriInfo      uriInfo,
                         Timers       timers,
                         G2Fallible   firstErrorFallible)
  {
    this(httpMethod,
         httpStatusCode,
         uriInfo,
         timers,
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
