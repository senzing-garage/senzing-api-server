package com.senzing.engine;

import java.util.Iterator;
import java.util.List;
import java.util.LinkedList;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.NoSuchElementException;

import static com.senzing.api.engine.EnginePriority.*;

public class EngineRequestQueue {
  /**
   *
   */
  private Map<EnginePriority, List<EngineRequest>> requestMap;

  /**
   *
   */
  private int totalCount;

  /**
   *
   */
  private boolean shutdownReceived;

  /**
   *
   */
  public EngineRequestQueue() {
    this.requestMap = new LinkedHashMap<EnginePriority,List<EngineRequest>>();
    for (EnginePriority priority : EnginePriority.values()) {
      this.requestMap.put(priority, new LinkedList<EngineRequest>());
    }
    this.totalCount = 0;
    this.shutdownReceived = false;
  }

  public synchronized void clear() {
    for (List<EngineRequest> list : this.requestMap.values()) {
      list.clear();
    }
  }

  public synchronized int getCount() {
    return this.totalCount;
  }

  public synchronized boolean isShutdown() {
    return this.shutdownReceived;
  }

  public synchronized int getApplicableCount(int threadAffinity) {
    int count = 0;
    for (List<EngineRequest> list : this.requestMap.values()) {
      for (EngineRequest request : list) {
        if (request == null) {
          count++;
          continue;
        }
        Integer affinity = request.getThreadAffinity();
        if (affinity == null || affinity.intValue() == threadAffinity) {
          count++;
          continue;
        }
      }
    }
    return count;
  }

  public synchronized boolean hasApplicableRequests(int threadAffinity) {
    for (List<EngineRequest> list : this.requestMap.values()) {
      for (EngineRequest request : list) {
        if (request == null) {
          return true;
        }
        Integer affinity = request.getThreadAffinity();
        if (affinity == null || affinity.intValue() == threadAffinity) {
          return true;
        }
      }
    }
    return false;
  }

  public synchronized int getCount(EnginePriority priority) {
    if (priority == null) {
      return this.getCount();
    }
    List<EngineRequest> list = this.requestMap.get(priority);
    return list.size();
  }

  public synchronized int getApplicableCount(EnginePriority priority,
                                             int            threadAffinity)
  {
    if (priority == null) {
      return this.getApplicableCount(threadAffinity);
    }

    int count = 0;
    for (EngineRequest request : this.requestMap.get(priority)) {
      if (request == null) {
        count++;
        continue;
      }
      Integer affinity = request.getThreadAffinity();
      if (affinity == null || affinity.intValue() == threadAffinity) {
        count++;
        continue;
      }
    }
    return count;
  }

  public synchronized boolean hasApplicableRequests(
      EnginePriority priority,
      int            threadAffinity)
  {
    if (priority == null) {
      return this.hasApplicableRequests(threadAffinity);
    }

    for (EngineRequest request : this.requestMap.get(priority)) {
      if (request == null) {
        return true;
      }
      Integer affinity = request.getThreadAffinity();
      if (affinity == null || affinity.intValue() == threadAffinity) {
        return true;
      }
    }
    return false;
  }

  public synchronized void enqueue(EngineRequest request) {
    if (request == null) {
      this.shutdownReceived = true;
      this.notifyAll();
      return;
    }
    EnginePriority priority = request.getPriority();
    if (priority == null) priority = STANDARD;
    List<EngineRequest> requestList = this.requestMap.get(priority);
    requestList.add(request);
    this.totalCount++;
    this.notifyAll();
  }

  private EngineRequest dequeue(List<EngineRequest> list,
                                int                 threadAffinity)
  {
    Iterator<EngineRequest> iter = list.iterator();
    while (iter.hasNext()) {
      EngineRequest request = iter.next();
      if (request == null) {
        iter.remove();
        this.totalCount--;
        return request;
      }
      Integer affinity = request.getThreadAffinity();
      if (affinity == null || affinity.intValue() == threadAffinity) {
        iter.remove();
        this.totalCount--;
        return request;
      }
    }
    return null;
  }

  public synchronized EngineRequest dequeue(int threadAffinity)
    throws NoSuchElementException
  {
    for (List<EngineRequest> list : this.requestMap.values()) {
      if (list.size() > 0) {
        int count = list.size();
        EngineRequest request = this.dequeue(list, threadAffinity);
        if (list.size() < count) return request;
      }
    }
    if (this.shutdownReceived) {
      return null;
    }
    throw new NoSuchElementException(
      "Attempt to dequeue request when none enqueued.");
  }

  public synchronized EngineRequest dequeue(EnginePriority  priority,
                                            int             threadAffinity)
    throws NoSuchElementException
  {
    if (priority == null) {
      return this.dequeue(threadAffinity);
    }

    List<EngineRequest> list = this.requestMap.get(priority);
    int count = list.size();
    EngineRequest request = this.dequeue(list, threadAffinity);
    if (list.size() < count) return request;

    if (this.shutdownReceived) {
      return null;
    }
    throw new NoSuchElementException(
      "Attempt to dequeue request when none enqueued: " + priority);
  }

}
