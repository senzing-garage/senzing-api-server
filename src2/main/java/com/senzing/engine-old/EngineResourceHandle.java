package com.senzing.api.engine;

import java.io.Serializable;

public class EngineResourceHandle implements Serializable {
  private long handleId;

  private long engineAuthId;

  public EngineResourceHandle(long handleId, long engineAuthId) {
    this.handleId     = handleId;
    this.engineAuthId = engineAuthId;
  }

  public long getHandleId() {
    return this.handleId;
  }

  public long getEngineId() {
    return this.engineAuthId;
  }

  public boolean equals(Object obj) {
    if (obj == null) return false;
    if (this == obj) return true;
    if (this.getClass() != obj.getClass()) return false;
    EngineResourceHandle erh = (EngineResourceHandle) obj;
    return ((this.getHandleId() == erh.getHandleId())
            && (this.getEngineId() == erh.getEngineId()));
  }

  public int hashCode() {
    long id1 = this.getHandleId();
    long id2 = this.getEngineId();
    return (int) (id1 ^ id2);
  }

  public String toString() {
    return this.getEngineId() + "." + this.getHandleId();
  }
}
