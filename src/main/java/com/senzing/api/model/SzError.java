package com.senzing.api.model;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * Describes an error that occurred.
 */
public class SzError {
  /**
   * The associated error code (if any)
   */
  private String code;

  /**
   * The associated error message.
   */
  private String message;

  /**
   * Default constructor.
   */
  public SzError() {
    this.code = null;
    this.message = null;
  }

  /**
   * Constructs with the specified error code and error message.
   *
   * @param code The error code to associate.
   *
   * @param message The message to associate.
   */
  public SzError(String code, String message) {
    this.code     = code;
    this.message  = message;
  }

  /**
   * Constructs with the specified error code and error message.
   *
   * @param t The {@link Throwable} that triggered the error.
   */
  public SzError(Throwable t) {
    this.code     = null;
    this.message  = formatThrowable(t);
  }

  /**
   * Formats a throwable into a message string.
   *
   * @param t The {@link Throwable} to format.
   */
  private static String formatThrowable(Throwable t)
  {
    StringWriter  sw = new StringWriter();
    PrintWriter   pw = new PrintWriter(sw);
    pw.println(t.getMessage());
    pw.println();
    t.printStackTrace(pw);
    pw.flush();
    return sw.toString();
  }

  /**
   * Gets the associated error code (if any).
   *
   * @return The associated error code, or <tt>null</tt> if none associated.
   */
  public String getCode() {
    return code;
  }

  /**
   * Sets the associated error code.  Set to <tt>null</tt> if none.
   *
   * @param code The associated error code or <tt>null</tt> if none.
   */
  public void setCode(String code) {
    this.code = code;
  }

  /**
   * Gets the associated message for the error.
   *
   * @return The associated error message.
   */
  public String getMessage() {
    return message;
  }

  /**
   * Sets the associated message for the error.
   *
   * @param message The message to associate.
   */
  public void setMessage(String message) {
    this.message = message;
  }

  /**
   * Produces a diagnostic {@link String} describing this instance.
   *
   * @return A diagnostic {@link String} describing this instance.
   */
  @Override
  public String toString() {
    return "SzError{" +
        "code='" + code + '\'' +
        ", message='" + message + '\'' +
        '}';
  }
}
