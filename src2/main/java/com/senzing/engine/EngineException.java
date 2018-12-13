package com.senzing.engine;

import com.senzing.g2.engine.G2Fallible;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static com.senzing.engine.EngineLogging.*;

public class EngineException extends RuntimeException implements Serializable {
  public static final Integer LICENSE_EXCEEDED_ERROR_CODE = 9000;

  private static final Set<Integer> NON_CRITICAL_ERROR_CODES;

  static {
    Set<Integer> set = new HashSet<>();
    set.add(LICENSE_EXCEEDED_ERROR_CODE);
    NON_CRITICAL_ERROR_CODES = Collections.unmodifiableSet(set);
  }

  private int responseCode = 0;
  private EngineOperation operation = null;
  private boolean critical;
  private Integer errorCode = null;
  private Object additionalInfo = null;

  public EngineException(String msg) {
    super(msg);
    this.critical = false;
  }

  public EngineException(Throwable cause) {
    super(cause);
    this.critical = false;
  }

  public EngineException(String msg, Throwable cause) {
    super(msg, cause);
    this.critical = false;
  }

  public EngineException(G2Fallible g2Fallible) {
    super(getExceptionDetail(g2Fallible));
    this.errorCode = g2Fallible.getLastExceptionCode();
    g2Fallible.clearLastException();
    this.critical = false;
  }

  public EngineException(G2Fallible g2Fallible, Throwable cause) {
    super(getExceptionDetail(g2Fallible), cause);
    this.errorCode = g2Fallible.getLastExceptionCode();
    g2Fallible.clearLastException();
    this.critical = false;
  }

  public EngineException(String msg, G2Fallible g2Fallible) {
    super(buildMessage(null, 0, g2Fallible, msg));
    this.errorCode = g2Fallible.getLastExceptionCode();
    g2Fallible.clearLastException();
    this.critical = false;
  }

  public EngineException(String msg, G2Fallible g2Fallible, Throwable cause) {
    super(buildMessage(null, 0, g2Fallible, msg), cause);
    this.errorCode = g2Fallible.getLastExceptionCode();
    g2Fallible.clearLastException();
    this.critical = false;
  }

  public EngineException(EngineOperation op, int responseCode, String msg) {
    super(buildMessage(op, responseCode, null, msg));
    this.operation = op;
    this.responseCode = (op == null) ? 0 : responseCode;
    this.critical = checkCritical(op, responseCode, this.errorCode);
  }

  public EngineException(EngineOperation op, int responseCode, Throwable cause) {
    super(buildMessage(op, responseCode, null, null), cause);
    this.operation = op;
    this.responseCode = (op == null) ? 0 : responseCode;
    this.critical = checkCritical(op, responseCode, this.errorCode);
  }

  public EngineException(EngineOperation op, int responseCode, String msg, Throwable cause) {
    super(buildMessage(op, responseCode, null, msg), cause);
    this.operation = op;
    this.responseCode = (op == null) ? 0 : responseCode;
    this.critical = checkCritical(op, responseCode, this.errorCode);
  }

  public EngineException(EngineOperation op, int responseCode, G2Fallible g2Fallible) {
    super(buildMessage(op, responseCode, g2Fallible, null));
    this.operation = op;
    this.responseCode = (op == null) ? 0 : responseCode;
    this.errorCode = g2Fallible.getLastExceptionCode();
    g2Fallible.clearLastException();
    this.critical = checkCritical(op, responseCode, this.errorCode);
  }

  public EngineException(EngineOperation op, int responseCode, G2Fallible g2Fallible, Throwable cause) {
    super(buildMessage(op, responseCode, g2Fallible, null), cause);
    this.operation = op;
    this.responseCode = (op == null) ? 0 : responseCode;
    this.errorCode = g2Fallible.getLastExceptionCode();
    g2Fallible.clearLastException();
    this.critical = checkCritical(op, responseCode, this.errorCode);
  }

  public EngineException(EngineOperation op, int responseCode, String msg, G2Fallible g2Fallible) {
    super(buildMessage(op, responseCode, g2Fallible, msg));
    this.operation = op;
    this.responseCode = (op == null) ? 0 : responseCode;
    this.errorCode = g2Fallible.getLastExceptionCode();
    g2Fallible.clearLastException();
    this.critical = checkCritical(op, responseCode, this.errorCode);
  }

  public EngineException(EngineOperation op, int responseCode, String msg, G2Fallible g2Fallible, Throwable cause) {
    super(buildMessage(op, responseCode, g2Fallible, msg), cause);
    this.operation = op;
    this.responseCode = (op == null) ? 0 : responseCode;
    this.errorCode = g2Fallible.getLastExceptionCode();
    g2Fallible.clearLastException();
    this.critical = checkCritical(op, responseCode, this.errorCode);
  }

  public EngineOperation getOperation() {
    return this.operation;
  }

  /**
   * Returns the response code for the associated {@linkplain #getOperation()
   * operation}.  If the operation is <tt>null</tt> then this value is always
   * zero (0) and has no meaning, otherwise, it represents the value returned
   * from the engine for performing the associated operation.
   *
   * @return An integer response code from the engine.
   */
  public int getResponseCode() {
    return this.responseCode;
  }

  /**
   * Returns the error code obtained from the G2 engine or <tt>null</tt>
   * if no such error code was obtained.
   *
   * @return The error code obtained from the G2 engine or <tt>null</tt>
   *         if no such error code was obtained.
   */
  public Integer getErrorCode() { return this.errorCode; }

  /**
   * Gets the additional info associated with this exception.
   *
   * @return The additional info associated with this exception.
   */
  public Object getAdditionalInfo() {
    return this.additionalInfo;
  }

  /**
   * Sets the additional info associated with this exception.
   *
   * @param info The info to set for this exception.
   */
  public void setAdditionalInfo(Object info) {
    this.additionalInfo = info;
  }

  /**
   * Checks if the exception has a response code for an operation that should
   * indicate halting further resolution.
   *
   * @return <tt>true</tt> if critical, otherwise <tt>false</tt>.
   */
  public boolean isCritical() {
    return this.critical;
  }

  private static boolean checkCritical(EngineOperation  op,
                                       int              responseCode,
                                       Integer          errorCode)
  {
    if (op == null) return false;
    switch (op) {
      case PROCESS:
        switch (responseCode) {
          case -2: // license failure
            return true;
          default:
            return false;
        }
      case ADD_RECORD:
        switch (responseCode) {
          case -2:
            return !NON_CRITICAL_ERROR_CODES.contains(errorCode);
          default:
            return false;
        }
      default:
        return false;
    }
  }

  private static String buildMessage(EngineOperation  operation,
                                     int              responseCode,
                                     G2Fallible         g2Fallible,
                                     String           msg)
  {
    StringBuilder sb = new StringBuilder();
    String prefix = "";

    if (operation != null) {
      sb.append("[ " + operation + " RETURNED " + responseCode + " ]");
      if (msg != null || g2Fallible != null) {
        sb.append("\r\n");
      }
    }

    if (msg != null) {
      sb.append(msg);
      if (g2Fallible != null) {
        sb.append("\r\n\r\nCaused By:\r\n");
      }
    }

    if (g2Fallible != null) {
      sb.append(getExceptionDetail(g2Fallible));
    }

    return sb.toString();
  }

  private static String getExceptionDetail(G2Fallible g2Fallible) {
    String detail = "UNKNOWN CAUSE";
    try {
      int     code  = g2Fallible.getLastExceptionCode();
      String  msg   = g2Fallible.getLastException();
      if (code != 0 || (msg != null && msg.length() > 0)) {
        if (msg != null && msg.length() > 0) detail = msg;
        detail = formatErrorMessage(code, detail);
      }

    } catch (Exception e) {
      detail = detail + " (" + e.toString() + " )";
    }
    return detail;
  }
}
