package com.senzing.engine;

public interface Engine {
  EngineType getEngineType();

  boolean isOperationSupported(EngineOperation op);

  EngineResponse processRequest(EngineRequest request) throws EngineException;

  String getModuleName();

  String getInitFileName();

  boolean isVerboseLogging();

  boolean isDestroyed();

  void destroy() throws EngineException;

  void uninitialize() throws EngineException;
  
  void reinitialize() throws EngineException;

  EngineLogger getLogger();

  default void log() {
    EngineLogger logger = this.getLogger();
    if (logger == null) {
      System.err.println();
    } else {
      logger.log();
    }
  }

  default void log(String msg) {
    EngineLogger logger = this.getLogger();
    if (logger == null) {
      System.err.println(msg);
    } else {
      logger.log(msg);
    }
  }

  default void log(String msg, boolean newline) {
    EngineLogger logger = this.getLogger();
    if (logger == null) {
      if (newline) {
        System.err.println(msg);
      } else {
        System.err.print(msg);
      }
    } else {
      logger.log(msg, newline);
    }
  }

  default void log(Throwable t) {
    EngineLogger logger = this.getLogger();
    if (logger == null) {
      t.printStackTrace(System.err);
    } else {
      logger.log(t);
    }
  }
}
