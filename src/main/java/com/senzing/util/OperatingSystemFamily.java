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
}
