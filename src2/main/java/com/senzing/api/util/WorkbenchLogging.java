package com.senzing.api.util;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class WorkbenchLogging {

  private static final ReentrantReadWriteLock DETAILED_LOGS_LOCK
      = new ReentrantReadWriteLock();

  private static boolean sensitiveLogs = false;

  private static File WORKING_DIRECTORY = null;

  private static File DETAILED_LOG_CHECK_FILE = null;

  private static boolean controlProcess = false;

  /**
   * Private default constructor
   */
  private WorkbenchLogging() {
    // do nothing
  }

  /**
   * Claims controller status for the process
   */
  public static void claimControllerStatus() {
    DETAILED_LOGS_LOCK.writeLock().lock();
    try {
      if (WorkbenchLogging.isInitialized()) {
        throw new IllegalStateException(
            "Cannot claim controller status after initialization.");
      }
      WorkbenchLogging.controlProcess = true;
    } finally {
      DETAILED_LOGS_LOCK.writeLock().unlock();
    }
  }

  /**
   * Returns true if the current logs have at any point during execution
   * of the application contained detailed log information which may be
   * sensitive.
   *
   * @return <tt>true</tt> if the produced log files may contain sensitive
   *         data.
   */
  public static boolean areLogsSensitive() {
    DETAILED_LOGS_LOCK.readLock().lock();
    try {
      return WorkbenchLogging.sensitiveLogs;

    } finally {
      DETAILED_LOGS_LOCK.readLock().unlock();
    }
  }

  public static boolean isProducingDetailedLogs() {
    boolean result = false;

    // check the value
    DETAILED_LOGS_LOCK.readLock().lock();
    try {
      if (WorkbenchLogging.isInitialized()) {
        result = DETAILED_LOG_CHECK_FILE.exists();
      }
    } finally {
      DETAILED_LOGS_LOCK.readLock().unlock();
    }

    // the sensitive flag only goes from false to true, so check if this
    // thread believes its value is still false, and if so then set to true
    if (result && !WorkbenchLogging.sensitiveLogs) {
      DETAILED_LOGS_LOCK.writeLock().lock();
      try {
        WorkbenchLogging.sensitiveLogs = true;
      } finally {
        DETAILED_LOGS_LOCK.writeLock().unlock();
      }
    }

    // return the result
    return result;
  }

  /**
   * Sets the flag to produce detailed logs or not.  This should only be called
   * by the process that controls the logging state.  Sub-processes should just
   * read the state typically.
   *
   * @param useDetailedLogs A <tt>boolean</tt> flag indicating if detailed logs
   *                        should be produced.
   */
  public static void setProducingDetailedLogs(boolean useDetailedLogs) {
    DETAILED_LOGS_LOCK.writeLock().lock();
    try {
      // only the control process can call this
      if (!WorkbenchLogging.controlProcess) {
        throw new IllegalStateException(
            "Attempt to set the detailed logs status from a process "
            + "that is not the controlling process.  Detailed logs control "
            + "is only available from the parent main process, "
            + "not from engine sub processes");
      }

      if (useDetailedLogs) {
        WorkbenchLogging.sensitiveLogs = true;
        try {
          DETAILED_LOG_CHECK_FILE.createNewFile();

        } catch (IOException e) {
          throw new RuntimeException(e);
        }

      } else {
        if (DETAILED_LOG_CHECK_FILE.exists()) {
          if (!DETAILED_LOG_CHECK_FILE.delete()) {
            System.err.println("FAILED TO DELETE TEMPORARY FILE: "
                                   + DETAILED_LOG_CHECK_FILE);
          }
        }
      }

    } finally {
      DETAILED_LOGS_LOCK.writeLock().unlock();
    }
  }

  /**
   * Wraps the specified text in text demarcating regions of the log that may
   * contain sensitive data.
   *
   * @param sensitiveText The text to wrap.
   * @return The wrapped text.
   */
  public static String detailedLogsWrap(String sensitiveText) {
    return detailedLogsWrap(0, sensitiveText);
  }

  /**
   * Wraps the specified text in text demarcating regions of the log that may
   * contain sensitive data.
   *
   * @param errorCode The non-sensitive error code.
   * @param sensitiveText The text to wrap.
   * @return The wrapped text.
   *
   */
  public static String detailedLogsWrap(int errorCode, String sensitiveText)
  {
    if (WorkbenchLogging.isProducingDetailedLogs()) {
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);
      if (errorCode != 0) {
        pw.println("ERROR: " + errorCode);
      }
      pw.println("/---------- BEGIN POSSIBLY SENSITIVE");
      pw.println(sensitiveText);
      pw.println("\\---------- END POSSIBLY SENSITIVE");
      pw.flush();
      return sw.toString();
    } else {
      return WorkbenchLogging.omittedMessage(errorCode);
    }
  }

  /**
   * Makes a log message stating that the specified
   */
  private static String omittedMessage(int errorCode) {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);

    pw.println();
    pw.println("ERROR: " + errorCode);
    pw.println("-- DETAILS OMITTED TO PREVENT POSSIBLY SENSITIVE DATA IN LOGS --");
    pw.println("-- ACTIVATE DETAILED APPLICATION LOGGING FOR MORE DETAILS     --");
    pw.println();
    pw.flush();
    return sw.toString();
  }

  /**
   * Checks if api logging has been initialized.
   * @return <tt>true</tt> if initialized, otherwise <tt>false</tt>
   */
  public static boolean isInitialized() {
    DETAILED_LOGS_LOCK.readLock().lock();
    try {
      return (WORKING_DIRECTORY != null);
    } finally {
      DETAILED_LOGS_LOCK.readLock().unlock();
    }
  }

  /**
   * Initializes api logging with the working directory to use.
   * This can be called only once.  The specified directory is used to determine
   * where temporary files may reside as well as being the top-level directory
   * for log files (though some log files may reside in sub directories of the
   * parent).
   *
   * @param workingDirectory The working directory
   */
  public static void initialize(File workingDirectory) {
    System.err.println("INITIALIZING LOGGING....");
    Objects.requireNonNull(workingDirectory, "Working Directory cannot be null");
    System.err.println("OBTAINING WRITE LOCK....");
    DETAILED_LOGS_LOCK.writeLock().lock();
    System.err.println("OBTAINED WRITE LOCK....");
    try {
      if (WORKING_DIRECTORY != null) {
        throw new IllegalStateException("Initialized WorkbenchLogging more than once.");
      }
      WORKING_DIRECTORY = workingDirectory;
      DETAILED_LOG_CHECK_FILE = new File(WORKING_DIRECTORY, "detailed-logging-active.tmp");
      sensitiveLogs = isProducingDetailedLogs();
    } finally {
      System.err.println("RELEASING WRITE LOCK....");
      DETAILED_LOGS_LOCK.writeLock().unlock();
      System.err.println("RELEASED WRITE LOCK.");
    }
  }
}
