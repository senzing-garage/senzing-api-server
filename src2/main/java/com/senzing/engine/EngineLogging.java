package com.senzing.engine;

public class EngineLogging {
  /**
   * Private default constructor.
   */
  private EngineLogging() {
    // do nothing
  }

  /**
   * Formats an error message.
   *
   * @param errorCode The error code.
   * @param detail The detailed error message.
   * @return The formatted error message.
   */
  public static String formatErrorMessage(int errorCode, String detail) {
    return ("ERROR: " + errorCode + "\r\n\r\n" + detail);
  }
}
