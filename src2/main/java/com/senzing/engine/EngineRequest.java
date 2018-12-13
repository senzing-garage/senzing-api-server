package com.senzing.engine;
import java.io.Serializable;
import java.util.Map;
import java.util.HashMap;

import static com.senzing.engine.EnginePriority.*;

public class EngineRequest implements Serializable {
  private EngineOperation operation;

  private long requestId;

  private String message;

  private EnginePriority priority;

  private long createTime;

  private long engineStart;

  private long engineEnd;

  private long responseTime;

  private long serializeTime;

  private long deserializedTime;

  private int backlogSize;

  private Integer threadAffinity;

  private Map<String,Object> params = null;

  private Long engineAuthId = null;

  public EngineRequest(EngineOperation operation, long requestId) {
    this(operation, requestId, null, STANDARD);
  }

  public EngineRequest(EngineOperation  operation,
                       long             requestId,
                       EnginePriority   priority)
  {
    this(operation, requestId, null, priority);
  }

  public EngineRequest(EngineOperation  operation,
                       long             requestId,
                       String           message)
  {
    this(operation, requestId, message, STANDARD);
  }

  public EngineRequest(EngineOperation  operation,
                       long             requestId,
                       String           message,
                       EnginePriority   priority)
  {
    this.operation  = operation;
    this.requestId  = requestId;
    this.message    = message;
    this.priority   = priority;
    this.createTime = System.currentTimeMillis();
  }

  public boolean isRedo() {
    return (this.getPriority() == REDO);
  }

  public Long getEngineAuthenticationId() {
    return this.engineAuthId;
  }

  public void setEngineAuthenticationId(long authId) {
    this.engineAuthId = authId;
  }

  public void setParameter(String name, Object value) {
    synchronized (this) {
      if (this.params == null) {
        this.params = new HashMap<String,Object>();
      }
      this.params.put(name, value);
    }
  }

  public Object getParameter(String name) {
    synchronized (this) {
      if (this.params == null) return null;
      return this.params.get(name);
    }
  }

  public void recordBacklogSize(int backlogSize) {
    this.backlogSize = backlogSize;
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

  public long getDeserializedTime() {
    return this.deserializedTime;
  }

  public void markEngineStart() {
    this.engineStart = System.currentTimeMillis();
  }

  public void markEngineEnd() {
    this.engineEnd = System.currentTimeMillis();
  }

  public long getPreEngineDuration() {
    return this.engineStart - this.deserializedTime;
  }

  public long getEngineDuration() {
    return this.engineEnd - this.engineStart;
  }

  public long getSerializeDuration() {
    return this.deserializedTime - this.serializeTime;
  }

  public void markResponseTime() {
    this.responseTime = System.currentTimeMillis();
  }

  public long getResponseDuration() {
    return this.responseTime - this.createTime;
  }

  public EngineOperation getOperation() {
    return this.operation;
  }

  public long getRequestId() {
    return this.requestId;
  }

  public EnginePriority getPriority() {
    return this.priority;
  }

  public String getMessage() {
    return this.message;
  }

  public Integer getThreadAffinity() {
    return this.threadAffinity;
  }

  public void setThreadAffinity(Integer affinity) {
    this.threadAffinity = affinity;
  }

  public String toString() {
    return ("{ operation=[ " + this.getOperation() + " ], requestId=[ "
            + this.getRequestId() + " ], message=[ " + this.getMessage()
            + "]");
  }

  public String getStatsString() {
    return ("totalDuration=[ " + getResponseDuration() + " ], engineDuration=[ "
            + this.getEngineDuration() + " ], queueDuration=[ "
            + this.getPreEngineDuration() + " ]");
  }
}
