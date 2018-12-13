package com.senzing.engine;

public class EngineRequestCancelledException extends RuntimeException {
  public EngineRequestCancelledException(EngineRequest request) {
    this("Engine request cancelled due to engine restart: " + request);
  }

  public EngineRequestCancelledException() {
    super();
  }

  public EngineRequestCancelledException(String message) {
    super(message);
  }

  public EngineRequestCancelledException(String message, Throwable cause) {
    super(message, cause);
  }

  public EngineRequestCancelledException(Throwable cause) {
    super(cause);
  }
}
