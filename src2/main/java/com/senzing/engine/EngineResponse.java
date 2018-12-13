package com.senzing.engine;

import java.io.Serializable;

public class EngineResponse implements Serializable {
  private long responseId;

  private Object result = null;

  private EngineOperation operation = null;

  private EnginePriority priority = null;

  private EngineException exception = null;

  private long engineDuration = -1L;

  private long enqueueTime = -1L;

  private long dequeueTime = -1L;

  private long reqSerializeDuration = -1L;

  private long preEngineDuration = -1L;

  private long serializeTime = -1L;

  private long deserializedTime = -1L;

  private int backlogSize = 0;

  private Long engineAuthId = null;

  public EngineResponse(EngineRequest request) {
    this.responseId           = request.getRequestId();
    this.operation            = request.getOperation();
    this.engineDuration       = request.getEngineDuration();
    this.preEngineDuration    = request.getPreEngineDuration();
    this.reqSerializeDuration = request.getSerializeDuration();
    this.backlogSize          = request.getBacklogSize();
    this.priority             = request.getPriority();
  }

  public EngineResponse(EngineRequest request, Object result)
  {
    this.responseId           = request.getRequestId();
    this.result               = result;
    this.operation            = request.getOperation();
    this.engineDuration       = request.getEngineDuration();
    this.preEngineDuration    = request.getPreEngineDuration();
    this.reqSerializeDuration = request.getSerializeDuration();
    this.backlogSize          = request.getBacklogSize();
    this.priority             = request.getPriority();
  }

  public EngineResponse(EngineRequest request, EngineException exception) {
    this.responseId           = request.getRequestId();
    this.exception            = exception;
    this.operation            = request.getOperation();
    this.engineDuration       = request.getEngineDuration();
    this.preEngineDuration    = request.getPreEngineDuration();
    this.reqSerializeDuration = request.getSerializeDuration();
    this.backlogSize          = request.getBacklogSize();
    this.priority             = request.getPriority();
  }

  /**
   * For engine startup failures (before any requests)
   * @param exception The exception
   */
  public EngineResponse(EngineException exception) {
    this.exception = exception;
  }

  public EngineOperation getOperation() {
    return this.operation;
  }

  public EnginePriority getPriority() {
    return this.priority;
  }

  public Long getEngineAuthenticationId() {
    return this.engineAuthId;
  }

  public void setEngineAuthenticationId(long authId) {
    this.engineAuthId = authId;
  }

  public int getBacklogSize() {
    return this.backlogSize;
  }

  public void markSerializing() {
    this.serializeTime = System.currentTimeMillis();
  }

  public void markDeserialized() {
    this.deserializedTime = System.currentTimeMillis();
  }

  public long getSerializeDuration() {
    return this.deserializedTime - this.serializeTime;
  }

  public long getRoundtripSerializeDuration() {
    return this.reqSerializeDuration + this.getSerializeDuration();
  }

  public void markEnqueueTime() {
    this.enqueueTime = System.currentTimeMillis();
  }

  public void markDequeueTime() {
    this.dequeueTime = System.currentTimeMillis();
  }

  public long getResponseQueueDuration() {
    return (this.dequeueTime - this.enqueueTime);
  }

  public long getEngineDuration() {
    return this.engineDuration;
  }

  public long getPreEngineDuration() {
    return this.preEngineDuration;
  }

  public long getResponseId() {
    return this.responseId;
  }

  public Object getResult() {
    return this.result;
  }

  public EngineException getException() {
    return this.exception;
  }

  public boolean isSuccessful() {
    return (this.exception == null);
  }

  public String toString() {
    return ((this.isSuccessful() ? "SUCCESS" : "FAILED")
            + "(" + this.getOperation() + "/ " + this.getResponseId() + ") --> "
            + (this.isSuccessful() ? this.getResult() : this.getException()));
  }
}
