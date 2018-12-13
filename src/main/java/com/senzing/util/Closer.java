package com.senzing.util;

public class Closer {
  private Closer() {
    // do nothing
  }
  public static <T extends AutoCloseable> T close(T closeable) {
    if (closeable == null) return null;
    try {
      closeable.close();
    } catch (Exception ignore) {
      // ignore
    }
    return null;    
  }
}
