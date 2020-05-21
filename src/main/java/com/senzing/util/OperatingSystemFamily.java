package com.senzing.util;

public enum OperatingSystemFamily {
  /**
   * Microsoft Windows operating systems.
   */
  WINDOWS,

  /**
   * Apple Macintosh operating systems.
   */
  MAC_OS,

  /**
   * Unix, Linux and Linux-like operating systems.
   */
  UNIX;

  /**
   * Check to see if this is {@link #WINDOWS}.
   */
  public boolean isWindows() {
    return (this == WINDOWS);
  }

  /**
   * Check to see if this is {@link #MAC_OS}.
   */
  public boolean isMacOS() {
    return (this == MAC_OS);
  }

  /**
   * Check to see if this is {@link #UNIX}.
   */
  public boolean isUnix() {
    return (this == UNIX);
  }

  public static final OperatingSystemFamily RUNTIME_OS_FAMILY;

  static {
    try {
      OperatingSystemFamily osFamily = null;

      final String osName = System.getProperty("os.name");
      String lowerOSName = osName.toLowerCase().trim();
      if (lowerOSName.startsWith("windows")) {
        osFamily = WINDOWS;
      } else if (lowerOSName.startsWith("mac")
          || lowerOSName.indexOf("darwin") >= 0) {
        osFamily = MAC_OS;
      } else {
        osFamily = UNIX;
      }

      RUNTIME_OS_FAMILY = osFamily;
    } catch (Exception e) {
      e.printStackTrace();
      throw new ExceptionInInitializerError(e);
    }
  }
}
