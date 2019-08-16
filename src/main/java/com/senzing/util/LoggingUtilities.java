package com.senzing.util;

import com.senzing.g2.engine.G2Fallible;

import java.io.PrintWriter;
import java.io.StringWriter;

public class LoggingUtilities {
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

}
