package com.senzing.engine;

public interface EngineLogger {
  void log();

  void log(String msg);

  void log(String msg, boolean newline);

  void log(Throwable t);
}
