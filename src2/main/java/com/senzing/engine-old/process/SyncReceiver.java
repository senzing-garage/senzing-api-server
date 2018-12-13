package com.senzing.api.engine.process;

import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.LinkedList;

import com.senzing.api.engine.EngineRequest;
import com.senzing.api.engine.EngineResponse;
import static com.senzing.api.Workbench.*;

class SyncReceiver extends Thread implements Switchboard.ResponseHandler {
  private long projectId;
  private Switchboard switchboard;
  private Map<Long,EngineRequest> syncRequests;
  private Map<Long,EngineResponse> syncResponses;
  private boolean complete;
  private Object monitor;
  private EngineProcess engineProcess;
  private List<EngineResponse> responseQueue;

  SyncReceiver(EngineProcess             engineProcess,
               Switchboard               switchboard,
               Map<Long,EngineRequest>   syncRequests,
               Map<Long,EngineResponse>  syncResponses)
  {
    this.engineProcess        = engineProcess;
    this.projectId            = this.engineProcess.getProjectId();
    this.complete             = false;
    this.monitor              = new Object();
    this.switchboard          = switchboard;
    this.syncRequests         = syncRequests;
    this.syncResponses        = syncResponses;
    this.responseQueue        = new LinkedList<EngineResponse>();

    this.setName("SyncReceiver-" + this.projectId
                 + "-" + Math.abs(System.identityHashCode(engineProcess)));
    this.switchboard.registerHandler(this);

    this.start();
  }

  public int getPendingCount() {
    synchronized (this.syncRequests) {
      return this.syncRequests.size();
    }
  }

  public boolean isPending(long requestId) {
    synchronized (this.syncRequests) {
      return this.syncRequests.containsKey(requestId);
    }
  }

  public void responseReceived(EngineResponse response) {
    synchronized (this.responseQueue) {
      this.responseQueue.add(response);
      this.responseQueue.notifyAll();
    }
  }

  private void complete() {
    this.switchboard.unregisterHandler(this);
    synchronized (this.monitor) {
      this.complete = true;
      this.monitor.notifyAll();
    }
    Set<Long> pendingRequestIds = null;
    synchronized (this.syncRequests) {
      pendingRequestIds = new HashSet<Long>(this.syncRequests.keySet());
    }
    if (pendingRequestIds != null && pendingRequestIds.size() > 0) {
      synchronized (this.syncResponses) {
        for (Long id : pendingRequestIds) {
          if (!this.syncResponses.containsKey(id)) {
            this.syncResponses.put(id, null);
          }
        }
        this.syncResponses.notifyAll();
      }
    }
  }

  public boolean isCompleted() {
    synchronized (this.monitor) {
      return this.complete;
    }
  }

  public void run() {
    try {
      this.doRun();

    } catch (Exception e) {
      log(e);

    } finally {
      log("MARKING RECEIVER COMPLETE...");
      this.complete();
      log("RECEIVER COMPLETED.");
    }
  }

  public void doRun() throws Exception {
    while (!this.isCompleted() && !this.engineProcess.isShutdown()
           && !this.switchboard.isCompleted())
    {
      synchronized (this.syncRequests) {
        while (this.syncRequests.size() == 0 && !this.engineProcess.isShutdown()) {
          try {
            this.syncRequests.wait(5000L);
          } catch (Exception ignore) {
            log(ignore);
          }
        }
      }
      if (this.engineProcess.isShutdown()) continue;
      EngineResponse response = null;
      synchronized (this.responseQueue) {
        while (this.responseQueue.size() == 0
               && !this.isCompleted()
               && !this.engineProcess.isShutdown()
               && !this.switchboard.isCompleted())
        {
          try {
            this.responseQueue.wait(2000L);
          } catch (InterruptedException ignore) {
            log(ignore);
          }
        }
        if (this.responseQueue.size() == 0) continue;
        response = this.responseQueue.remove(0);
      }
      long responseId = response.getResponseId();
      EngineRequest request = null;
      synchronized (this.syncRequests) {
        request = this.syncRequests.remove(responseId);
        if (request == null) {
          log("RECEIVED UNEXPECTED SYNC RESPONSE: " + responseId);
        } else {
          this.syncRequests.notifyAll();
        }
      }
      synchronized (this.syncResponses) {
        this.syncResponses.put(responseId, response);
        this.syncResponses.notifyAll();
      }
    }
  }
}
