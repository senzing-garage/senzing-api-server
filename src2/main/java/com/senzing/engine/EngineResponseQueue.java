package com.senzing.engine;

import java.util.List;
import java.util.LinkedList;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.NoSuchElementException;

import static com.senzing.api.engine.EnginePriority.*;

public class EngineResponseQueue {
  /**
   *
   */
  private Map<EnginePriority, List<EngineResponse>> responseMap;

  /**
   *
   */
  private int totalCount;

  /**
   *
   */
  public EngineResponseQueue() {
    this.responseMap = new LinkedHashMap<EnginePriority,List<EngineResponse>>();
    for (EnginePriority priority : EnginePriority.values()) {
      this.responseMap.put(priority, new LinkedList<EngineResponse>());
    }
    this.totalCount = 0;
  }

  public synchronized void clear() {
    for (List<EngineResponse> list : this.responseMap.values()) {
      list.clear();
    }
  }

  public synchronized int getCount() {
    return this.totalCount;
  }

  public synchronized int getCount(EnginePriority priority) {
    if (priority == null) {
      return this.getCount();
    }
    List<EngineResponse> list = this.responseMap.get(priority);
    return list.size();
  }

  public synchronized void enqueue(EngineResponse response) {
    EnginePriority priority = response.getPriority();
    if (priority == null) priority = STANDARD;
    List<EngineResponse> responseList = this.responseMap.get(priority);
    responseList.add(response);
    this.totalCount++;
    this.notifyAll();
  }

  public synchronized EngineResponse dequeue()
    throws NoSuchElementException
  {
    for (List<EngineResponse> list : this.responseMap.values()) {
      if (list.size() > 0) {
        this.totalCount--;
        return list.remove(0);
      }
    }
    throw new NoSuchElementException(
      "Attempt to dequeue response when none enqueued.");
  }

  public synchronized EngineResponse dequeue(EnginePriority  priority)
    throws NoSuchElementException
  {
    if (priority == null) return this.dequeue();
    List<EngineResponse> list = this.responseMap.get(priority);
    if (list.size() > 0) {
      this.totalCount--;
      return list.remove(0);
    }
    throw new NoSuchElementException(
      "Attempt to dequeue response when none enqueued: " + priority);
  }

}
