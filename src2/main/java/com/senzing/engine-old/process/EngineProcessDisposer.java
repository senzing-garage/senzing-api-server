package com.senzing.api.engine.process;

import java.util.List;
import java.util.Map;
import java.util.ArrayList;

import com.senzing.api.ServerError;
import com.senzing.api.util.StreamLogger;
import com.senzing.api.engine.EngineOperation;
import com.senzing.api.engine.EngineRequest;

import static java.util.concurrent.TimeUnit.*;
import static com.senzing.api.engine.EnginePriority.*;
import static com.senzing.api.Workbench.*;
import static com.senzing.api.ServerErrorLog.*;

class EngineProcessDisposer extends Thread {
  private DetachedConnector connector;
  private boolean terminate;
  private Process process;
  private StreamLogger outLogger;
  private StreamLogger errLogger;
  private EngineProcess owner;
  private String logName;
  private List<EngineOperation> destroyOperations;
  private Map<Long,EngineRequest> pendingRequests;

  EngineProcessDisposer(
    EngineProcess           owner,
    String                  logName,
    boolean                 terminate,
    DetachedConnector       connector,
    Process                 process,
    StreamLogger            outLogger,
    StreamLogger            errLogger,
    Map<Long,EngineRequest> pendingRequests,
    EngineOperation...      destroyOps)
  {
    this.owner      = owner;
    this.logName    = logName;
    this.terminate  = terminate;
    this.connector  = connector;
    this.process    = process;
    this.outLogger  = outLogger;
    this.errLogger  = errLogger;
    this.destroyOperations = new ArrayList<EngineOperation>(destroyOps.length);
    for (EngineOperation op : destroyOps) {
      this.destroyOperations.add(op);
    }
    log(this.logName + " DESTROY OPERATIONS: " + this.destroyOperations);
    this.setName("EngineProcessDisposer-" + this.owner.getProjectId() + "-"
                 + this.logName);
    this.start();
  }
  public void run() {
    try {
      int failureCount = 0;
      log("SENDING " + this.logName + " DESTROY OPERATIONS: " + this.destroyOperations);
      for (EngineOperation destroyOp : this.destroyOperations) {
        long rid = this.owner.getNextRequestId();
        log("SENDING " + destroyOp + " REQUEST TO " + this.logName + " ENGINE PROCESS");
        EngineRequest request = new EngineRequest(destroyOp, rid, DEFERRED);
        if (this.pendingRequests != null) {
          synchronized (this.pendingRequests) {
            this.pendingRequests.put(rid, request);
          }
        }
        try {
          this.connector.writeRequest(request);
        } catch (Exception e) {
          log(e);
          failureCount++;
        }
      }
      if (failureCount > 0) {
        this.process.destroy();
      }

      boolean destroyed = false;
      log("WAITING FOR " + this.logName + " ENGINE PROCESS TO SHUTDOWN....");
      while (!destroyed && this.process.isAlive()) {
        try {
          destroyed = this.process.waitFor(15, SECONDS);
        } catch (InterruptedException ignore) {
          // ignore
        }
        if (!destroyed) {
          log(this.logName + " ENGINE PROCESS DID NOT SHUTDOWN, FORCIBLY DESTROYING....");
          this.process.destroyForcibly();
          log(this.logName + " ENGINE PROCESS DESTROYED.");
        }
      }
      log(this.logName + " ENGINE PROCESS IS SHUTDOWN");
      if (this.outLogger != null) this.outLogger.complete();
      if (this.errLogger != null) this.errLogger.complete();
      if (this.connector != null) this.connector.close(terminate);

    } catch (Exception e) {
      log(e);
      ServerError se = new ServerError(false, "engine-stop", e);
      if (this.owner.getCurrentFileId() < 0L) {
        recordProjectError(this.owner.getProjectId(), se);
      } else {
        recordFileError(this.owner.getProjectId(),
                        this.owner.getCurrentFileId(), se);
      }
    }
  }
}
