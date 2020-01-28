package com.senzing.util;

import com.senzing.g2.engine.G2Fallible;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Objects;

public class LoggingUtilities {
  /**
   * The last logged exception in this thread.
   */
  private static final ThreadLocal<Long> LAST_LOGGED_EXCEPTION
      = new ThreadLocal<>();

  /**
   * Private default constructor
   */
  private LoggingUtilities() {
    // do nothing
  }

  /**
   * Formats a multiline message.
   *
   * @param lines The lines to be formatted.
   */
  public static String multilineFormat(String... lines) {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    for (String line : lines) {
      pw.println(line);
    }
    pw.flush();
    return sw.toString();
  }


  /**
   * Formats an error message from a {@link G2Fallible} instance, including
   * the details (which may contain sensitive PII information) as part of
   * the result.
   *
   * @param operation The name of the operation from the native API interface
   *                  that was attempted but failed.
   * @param fallible The {@link G2Fallible} from which to extract the error
   *                 message.
   * @return The multi-line formatted log message.
   */
  public static String formatError(String operation, G2Fallible fallible)
  {
    return formatError(operation, fallible, true);
  }

  /**
   * Formats an error message from a {@link G2Fallible} instance, optionally
   * including the details (which may contain sensitive PII information) as
   * part of the result.
   *
   * @param operation The name of the operation from the native API interface
   *                  that was attempted but failed.
   * @param fallible The {@link G2Fallible} from which to extract the error
   *                 message.
   * @param includeDetails <tt>true</tt> to include the details of the failure
   *                       failure in the resultant message, and <tt>false</tt>
   *                       to exclude them (usually to avoid logging sensitive
   *                       information).
   * @return The multi-line formatted log message.
   */
  public static String formatError(String     operation,
                                   G2Fallible fallible,
                                   boolean    includeDetails)
  {
    int errorCode = fallible.getLastExceptionCode();
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);

    pw.println();
    pw.println("Operation Failed : " + operation);
    pw.println("Error Code       : " + errorCode);

    if (includeDetails) {
      String message = fallible.getLastException();
      pw.println("Reason           : " + message);
    }
    pw.println();
    pw.flush();
    return sw.toString();
  }

  /**
   * Logs an error message from a {@link G2Fallible} instance, including the
   * details (which may contain sensitive PII information) as part of the
   * result.
   *
   * @param operation The name of the operation from the native API interface
   *                  that was attempted but failed.
   * @param fallible The {@link G2Fallible} from which to extract the error
   *                 message.
   */
  public static void logError(String      operation,
                              G2Fallible  fallible)
  {
    logError(operation, fallible, true);
  }

  /**
   * Logs an error message from a {@link G2Fallible} instance, optionally
   * including the details (which may contain sensitive PII information) as
   * part of the result.
   *
   * @param operation The name of the operation from the native API interface
   *                  that was attempted but failed.
   * @param fallible The {@link G2Fallible} from which to extract the error
   *                 message.
   * @param includeDetails <tt>true</tt> to include the details of the failure
   *                       failure in the resultant message, and <tt>false</tt>
   *                       to exclude them (usually to avoid logging sensitive
   *                       information).
   */
  public static void logError(String      operation,
                              G2Fallible  fallible,
                              boolean     includeDetails)
  {
    String message = formatError(operation, fallible, includeDetails);
    System.err.println(message);
  }

  /**
   * Convert a throwable to a {@link Long} value so we don't keep a reference
   * to what could be a complex exception object.
   * @param t The throwable to convert.
   * @return The long hash representation to identify the throwable instance.
   */
  private static Long throwableToLong(Throwable t) {
    if (t == null) return null;
    if (t.getClass() == RuntimeException.class && t.getCause() != null) {
      t = t.getCause();
    }
    long hash1 = (long) System.identityHashCode(t);
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    t.printStackTrace(pw);
    pw.flush();
    long hash2 = (long) sw.toString().hashCode();
    return ((hash1 << 32) | hash2);
  }

  /**
   * Checks if the specified {@link Throwable} is the last logged exception
   * or if the class of specified {@link Throwable} is {@link RuntimeException}
   * and it has a {@linkplain Throwable#getCause()} then if the cause is the
   * last logged exception.  This is handy for telling if the exception has already been logged by a
   * deeper level of the stack trace.
   * @param t The {@link Throwable} to check.
   * @return <tt>true</tt> if it is the last logged exception, otherwise
   *         <tt>false</tt>.
   */
  public static boolean isLastLoggedException(Throwable t) {
    if (t == null) return false;
    if (LAST_LOGGED_EXCEPTION.get() == null) return false;
    long value = throwableToLong(t);
    return (LAST_LOGGED_EXCEPTION.get() == value);
  }

  /**
   * Sets the specified {@link Throwable} as the last logged exception.
   * This is handy for telling if the exception has already been logged by a
   * deeper level of the stack trace.
   *
   * @param t The {@link Throwable} to set as the last logged exception.
   */
  public static void setLastLoggedException(Throwable t) {
    LAST_LOGGED_EXCEPTION.set(throwableToLong(t));
  }

  /**
   * Sets the last logged exception and rethrows the specified exception.
   * @param t The {@link Throwable} describing the exception.
   * @throws T The specified exception.
   */
  public static <T extends Throwable> void setLastLoggedAndThrow(T t)
    throws T
  {
    setLastLoggedException(t);
    throw t;
  }

  /**
   * Conditionally logs to stderr the specified {@link Throwable} is <b>not</b> the {@linkplain
   * #isLastLoggedException(Throwable) last logged exception} and then rethrows
   * it.
   *
   * @param t The {@link Throwable} to log and throw.
   * @return Never returns anything since it always throws.
   */
  public static <T extends Throwable> T logOnceAndThrow(T t)
    throws T
  {
    if (!isLastLoggedException(t)) {
      t.printStackTrace();
    }
    setLastLoggedAndThrow(t);
    return null; // we never get here
  }
}
