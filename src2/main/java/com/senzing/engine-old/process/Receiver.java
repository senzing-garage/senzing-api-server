package com.senzing.api.engine.process;

import java.util.Map;
import java.util.List;
import java.util.Set;
import java.util.LinkedList;
import java.sql.SQLException;

import com.senzing.util.AccessToken;
import com.senzing.api.Project;
import com.senzing.api.ProjectFile;
import com.senzing.api.ObservedEntityId;
import com.senzing.api.Redo;
import com.senzing.api.ServerError;
import com.senzing.api.engine.EngineRequest;
import com.senzing.api.engine.EngineResponse;
import com.senzing.api.engine.EngineException;

import static com.senzing.api.Workbench.*;
import static com.senzing.api.ServerErrorLog.*;
import static com.senzing.api.engine.EngineOperation.*;
import static com.senzing.api.engine.EngineException.*;
import static com.senzing.api.engine.process.EngineProcess.*;

class Receiver extends Thread implements Switchboard.ResponseHandler {
  private ProjectFile file;
  private long projectId;
  private Switchboard switchboard;
  private Map<Long,EngineRequest> pendingRequests;
  private Map<Long,Redo> pendingRedos;
  private boolean complete;
  private boolean receiveCompleted;
  private boolean sendCompleted;
  private Object monitor;
  private ProjectFileDataAccess fileDataAccess;
  private ProjectDataAccess projectDataAccess;
  private int engineThreadCount;
  private boolean redoDisabled;
  private boolean recordsComplete;
  private boolean statsNeeded;
  private RedoDeleter redoDeleter;
  private EngineProcess engineProcess;
  private Indexer indexer;
  private List<EngineResponse> responseQueue;

  Receiver(EngineProcess           engineProcess,
           Indexer                 indexer,
           ProjectFile             file,
           Switchboard             switchboard,
           Map<Long,EngineRequest> pendingRequests,
           Map<Long,Redo>          pendingRedos,
           int                     engineThreadCount) {
    this.engineProcess        = engineProcess;
    this.indexer              = indexer;
    this.file                 = file;
    this.projectId            = this.file.getProjectId();
    this.redoDisabled         = false;
    this.complete             = false;
    this.recordsComplete      = false;
    this.sendCompleted        = false;
    this.receiveCompleted     = false;
    this.statsNeeded          = false;
    this.monitor              = new Object();
    this.switchboard          = switchboard;
    this.pendingRequests      = pendingRequests;
    this.pendingRedos         = pendingRedos;
    this.fileDataAccess       = new ProjectFileDataAccess();
    this.projectDataAccess    = new ProjectDataAccess();
    this.engineThreadCount    = engineThreadCount;
    this.redoDeleter          = new RedoDeleter(this);
    this.responseQueue        = new LinkedList<EngineResponse>();
    this.setName("Receiver-" + this.projectId
                 + "-" + Math.abs(System.identityHashCode(engineProcess)));

    this.switchboard.registerHandler(this);
    this.start();
  }

  public int getPendingCount() {
    synchronized (this.pendingRequests) {
      return this.pendingRequests.size();
    }
  }

  public boolean isPending(long requestId) {
    synchronized (this.pendingRequests) {
      return this.pendingRequests.containsKey(requestId);
    }
  }

  public void responseReceived(EngineResponse response) {
    synchronized (this.responseQueue) {
      this.responseQueue.add(response);
      this.responseQueue.notifyAll();
    }
  }

  protected boolean isResponseAvailable() {
    synchronized (this.responseQueue) {
      return this.responseQueue.size() > 0;
    }
  }

  public long getProjectId() {
    return this.projectId;
  }

  public void recordsCompleted() {
    synchronized (this.monitor) {
      this.recordsComplete = true;
    }
  }

  public boolean areRecordsCompleted() {
    synchronized (this.monitor) {
      return this.recordsComplete;
    }
  }

  public void sendCompleted() {
    synchronized (this.monitor) {
      this.sendCompleted = true;
      this.monitor.notifyAll();
    }
  }

  public boolean isSendCompleted() {
    synchronized (this.monitor) {
      return this.sendCompleted;
    }
  }

  private void receiveCompleted() {
    synchronized (this.monitor) {
      this.receiveCompleted = true;
      this.monitor.notifyAll();
    }
  }

  public boolean isReceiveCompleted() {
    synchronized (this.monitor) {
      return this.receiveCompleted;
    }
  }

  public boolean hasPendingRequests() {
    synchronized (this.pendingRequests) {
      return (this.pendingRequests.size() > 0);
    }
  }

  public int getPendingRequestCount() {
    synchronized (this.pendingRequests) {
      return this.pendingRequests.size();
    }
  }
  private EngineRequest removePendingRequest(long requestId) {
    EngineRequest result = null;
    synchronized (this.pendingRequests) {
      result = this.pendingRequests.remove(requestId);
      if (result != null) {
        this.pendingRequests.notifyAll();
      }
    }
    return result;
  }

  public Redo removePendingRedo(long requestId) {
    Redo result = null;
    synchronized (this.pendingRedos) {
      result = this.pendingRedos.remove(requestId);
      if (result != null) {
        this.pendingRedos.notifyAll();
      }
    }
    return result;
  }

  public Redo getPendingRedo(long requestId) {
    Redo result = null;
    synchronized (this.pendingRedos) {
      result = this.pendingRedos.get(requestId);
    }
    return result;
  }

  public int getPendingRedoCount() {
    synchronized (this.pendingRedos) {
      return (this.pendingRedos.size());
    }
  }

  private void complete() {
    this.switchboard.unregisterHandler(this);
    synchronized (this.monitor) {
      this.complete = true;
      this.monitor.notifyAll();
    }
  }

  public void signalStatsNeeded() {
    synchronized (this.monitor) {
      this.statsNeeded = true;
    }
  }

  public boolean checkStatsNeeded(boolean clear) {
    synchronized (this.monitor) {
      boolean result = this.statsNeeded;
      if (clear) this.statsNeeded = false;
      return result;
    }
  }

  public boolean isCompleted() {
    synchronized (this.monitor) {
      return this.complete;
    }
  }

  public void disableRedos() {
    synchronized (this.pendingRedos) {
      this.redoDisabled = false;
      this.pendingRedos.notifyAll();
    }
  }

  public boolean isRedoDisabled() {
    synchronized (this.pendingRedos) {
      return (!this.recordsComplete && this.redoDisabled);
    }
  }

  public void run() {
    try {
      this.doRun();

    } catch (Exception e) {
      log(e);
    } finally {
      try {
        log("WAITING FOR REDO DELETER TO COMPLETE....");
        this.redoDeleter.join();
        log("REDO DELETER COMPLETED.");
      } catch (Exception ignore) {
        ignore.printStackTrace();
      }
      log("MARKING RECEIVER COMPLETE...");
      this.complete();
      log("RECEIVER COMPLETED.");
    }
  }

  public void doRun() throws Exception {
    long  totalEngineTime = 0L;
    long  totalResponseTime = 0L;
    long  totalResQueueTime = 0L;
    long  totalSerialTime = 0L;
    long  totalPreEngTime = 0L;
    int   totalBacklogSize = 0;
    long  totalRedoEngineTime = 0L;
    long  totalRedoResponseTime = 0L;
    long  totalRedoResQueueTime = 0L;
    long  totalRedoSerialTime = 0L;
    long  totalRedoPreEngTime = 0L;
    int   totalRedoBacklogSize = 0;
    int   statResolvedCount = 0;
    int   statFailureCount = 0;
    int   statRedoCount = 0;
    int   resolvedCount = 0;
    int   suppressedCount = 0;
    int   failureCount  = 0;
    long  nextUpdateTime = 0L;
    long  updateTime    = 0L;
    int   lastResolved  = 0;
    int   fileRedoCount = 0;
    int   postAbortFailures = 0;
    long  startTime     = System.currentTimeMillis();
    boolean showRedoStats = false;
    String showRedoProp = System.getProperty("SHOW_REDO_BREAKDOWN");
    if (showRedoProp != null && showRedoProp.length() > 0) {
      try {
        showRedoStats = Boolean.valueOf(showRedoProp.toLowerCase());
      } catch (Exception ignore) {
        // do nothing
      }
    }
    nextUpdateTime = System.currentTimeMillis();
    this.updateFile(file,
                    resolvedCount,
                    failureCount,
                    lastResolved,
                    fileRedoCount,
                    startTime);

    lastResolved = resolvedCount;
    updateTime = nextUpdateTime;
    int statsMeter = 0;

    int windDownLogThreshold = Integer.MAX_VALUE;
    while (!this.engineProcess.isAborted() && !this.switchboard.isCompleted()
           && (!this.isSendCompleted() || (this.getPendingRequestCount() > 0)))
    {
      int pendingRequestCount = this.getPendingRequestCount();
      if (this.isSendCompleted() && pendingRequestCount < windDownLogThreshold) {
        windDownLogThreshold = pendingRequestCount - 100;
        log("AWAITING RESPONSES FOR " + pendingRequestCount + " REQUESTS");
      }
      EngineResponse response = null;
      EngineRequest req = null;
      long responseId;

      if (!this.isSendCompleted() || this.hasPendingRequests()) {
        synchronized (this.pendingRequests) {
          while (!this.engineProcess.isAborted()
                 && !this.isSendCompleted() && !this.hasPendingRequests())
          {
            try {
              this.pendingRequests.wait(1000L);
            } catch (Exception ignore) {
              // ignore exception
            }
          }
          if (this.engineProcess.isAborted()
              || (this.isSendCompleted() && !this.hasPendingRequests()))
          {
            continue;
          }
        }
        synchronized (this.responseQueue) {
          while (this.responseQueue.size() == 0
                 && !this.engineProcess.isAborted()
                 && !this.switchboard.isCompleted()
                 && (!this.isSendCompleted()
                     || (this.getPendingRequestCount() > 0)))
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
        nextUpdateTime = System.currentTimeMillis();
        responseId = response.getResponseId();

        req = this.removePendingRequest(responseId);
        if (req == null) {
          log("WARNING: RECEIVED RESPONSE FOR UNKNOWN REQUEST: "
              + responseId);
          continue;
        }

        // record the response time for the request
        req.markResponseTime();

        // check if this is a stats response
        if (req.getOperation() == STATS) {
          log("****** ENGINE STATS: " + response.getResult());
          log("****** ENGINE STATS REQUEST TIMING: " + req.getStatsString());
          continue;
        }

        if (req.getOperation() == ADD_RECORD && response.isSuccessful()) {
          String dataSource = (String) req.getParameter(DATA_SOURCE);
          String recordId   = (String) response.getResult();
          ObservedEntityId obsEntId
            = new ObservedEntityId(dataSource, recordId);
          // DEACTIVATE ELASTIC SEARCH
          //this.indexer.enqueueRecord(obsEntId);
        }

        // clean-up the redo if successful
        if (req.isRedo()) {
          Redo redo = this.getPendingRedo(responseId);
          if (redo != null) {
            this.redoDeleter.enqueue(responseId, redo);
          } else {
            log("WARNING: RECEIVED A REDO THAT WAS NOT PENDING: " + req);
          }
        }
      }

      if (req == null) continue;

      // increment the stats as appropriate
      if (!req.isRedo()) {
        totalEngineTime   += response.getEngineDuration();
        totalResQueueTime += response.getResponseQueueDuration();
        totalBacklogSize  += response.getBacklogSize();
        totalResponseTime += req.getResponseDuration();
        totalSerialTime   += response.getSerializeDuration();
        totalPreEngTime   += response.getPreEngineDuration();

        if (response.isSuccessful()) {
          resolvedCount++;
          statResolvedCount++;
        } else {
          if (!this.engineProcess.isLoadCancelled()) failureCount++;
          statFailureCount++;
        }

        if (((resolvedCount + failureCount) % STATS_INTERVAL) == 0) {
          statsMeter++;
        }

        if ((statsMeter > 0 && ! this.isResponseAvailable()) || (statsMeter > 10)) {
          statsMeter = 0;
          this.signalStatsNeeded();
          double actualRate = this.updateFile(file,
                                              resolvedCount,
                                              failureCount,
                                              lastResolved,
                                              fileRedoCount,
                                              updateTime);
          updateTime = nextUpdateTime;
          lastResolved = resolvedCount;

          double standardCount = (double) (statResolvedCount + statFailureCount);
          double allCount = (double) (standardCount + statRedoCount);

          double avgEngineTime        = ((double)totalEngineTime) / standardCount;
          double avgResQueueTime      = ((double)totalResQueueTime) / standardCount;
          double avgResponseTime      = ((double)totalResponseTime) / standardCount;
          double avgSerialTime        = ((double)totalSerialTime) / standardCount;
          double avgPreEngTime        = ((double)totalPreEngTime) / standardCount;
          double avgBacklogSize       = ((double)totalBacklogSize) / standardCount;

          double avgRedoEngineTime    = ((double)totalRedoEngineTime) / ((double) statRedoCount);
          double avgRedoResQueueTime  = ((double)totalRedoResQueueTime) / ((double) statRedoCount);
          double avgRedoResponseTime  = ((double)totalRedoResponseTime) / ((double) statRedoCount);
          double avgRedoSerialTime    = ((double)totalRedoSerialTime) / ((double) statRedoCount);
          double avgRedoPreEngTime    = ((double)totalRedoPreEngTime) /  ((double) statRedoCount);
          double avgRedoBacklogSize   = ((double)totalRedoBacklogSize) / ((double) statRedoCount);

          double avgTotalEngineTime   = ((double)(totalEngineTime+totalRedoEngineTime)) / standardCount;
          double avgTotalResQueueTime = ((double)(totalResQueueTime+totalRedoResQueueTime)) / standardCount;
          double avgTotalResponseTime = ((double)(totalResponseTime+totalRedoResponseTime)) / standardCount;
          double avgTotalSerialTime   = ((double)(totalSerialTime+totalRedoSerialTime)) / standardCount;
          double avgTotalPreEngTime   = ((double)(totalPreEngTime+totalRedoPreEngTime)) / standardCount;
          double avgTotalBacklogSize  = ((double)(totalBacklogSize+totalRedoBacklogSize)) / allCount;

          double threadAvgEngineTime = (avgTotalEngineTime/(double)this.engineThreadCount);
          double idealResolutionRate = 1000.0/threadAvgEngineTime;
          double rateOverhead        = idealResolutionRate - actualRate;
          double actualThreadAvgTime = 1000.0/actualRate;
          double actualAvgTime       = actualThreadAvgTime * (double)this.engineThreadCount;
          double timeOverhead        = actualAvgTime - avgTotalEngineTime;

          String elapsedTime = this.getElapsedTime(startTime);

          log();
          log("+-------------------------------------------------------------------------------------");
          log("| LAST RECORD BLOCK DETAIL STATISTICS (" + elapsedTime + " elapsed since start)");
          log("+-------------------------------------------------------------------------------------");
          if (showRedoStats) {
            log("| RECORD-ONLY TIMES (" + (statResolvedCount + statFailureCount) + " records): ");
            log("|      PRE-ENGINE TIME        : " + avgPreEngTime + " ms per record");
            log("|      ENGINE TIME            : " + avgEngineTime + " ms per record");
            log("|      RESPONSE QUEUE TIME    : " + avgResQueueTime + " ms per record");
            log("|      RESPONSE TRANSMIT TIME : " + avgSerialTime + " ms per record");
            log("|      FULL ROUNDTRIP TIME    : " + avgResponseTime + " ms per record");
            log("|");
            log("| REDO TIMES (" + statRedoCount + " redos): ");
            log("|      PRE-ENGINE TIME        : " + avgRedoPreEngTime + " ms per record");
            log("|      ENGINE TIME            : " + avgRedoEngineTime + " ms per record");
            log("|      RESPONSE QUEUE TIME    : " + avgRedoResQueueTime + " ms per record");
            log("|      RESONSE TRANSMIT TIME  : " + avgRedoSerialTime + " ms per record");
            log("|      FULL ROUNDTRIP TIME    : " + avgRedoResponseTime + " ms per record");
            log("|");
          } else {
            log("| RECORDS PROCESSED           : " + (statResolvedCount + statFailureCount) + " (" + (resolvedCount + failureCount) +" total)");
            log("| REDOS PROCESSED             : " + (statRedoCount) + " (" + fileRedoCount + " total)");
            log("| REQUESTS PROCESSED          : " + (statResolvedCount + statFailureCount + statRedoCount)
                + " (" + (resolvedCount+failureCount+fileRedoCount) + " total)");
            log("|");
          }
          log("| OVERALL TIMES (" + (statResolvedCount + statFailureCount) + " records):");
          log("|      TIME IN BACKLOG        : " + avgTotalPreEngTime + " ms per record");
          log("|      ENGINE TIME            : " + avgTotalEngineTime + " ms per record");
          log("|      RESPONSE QUEUE TIME    : " + avgTotalResQueueTime + " ms per record");
          log("|      RESPONSE TRANSMIT TIME : " + avgTotalSerialTime + " ms per record");
          log("|      FULL ROUNDTRIP TIME    : " + avgTotalResponseTime + " ms per record");
          log("|");
          log("+-------------------------------------------------------------------------------------");
          log("| ENGINE TIME / NUMBER ENGINE THREAD  : " + threadAvgEngineTime + " ms per record");
          log("| IDEAL RESOLUTION RATE (INCL REDOS)  : " + idealResolutionRate + " records per second");
          log("| ACTUAL RESOLUTION RATE              : " + actualRate + " records per second");
          log("| RESOLUTION RATE OVERHEAD            : " + rateOverhead + " records per second");
          log("| RESOLUTION TIME OVERHEAD            : " + timeOverhead + " ms per record");
          log("+-------------------------------------------------------------------------------------");
          if (showRedoStats) {
            log("| AVERAGE BACKLOG SIZE:");
            log("|      RECORD-ONLY   : " + avgBacklogSize);
            log("|      REDO          : " + avgRedoBacklogSize);
            log("|      OVERALL       : " + avgTotalBacklogSize);
          } else {
            log("| AVERAGE BACKLOG SIZE: " + avgTotalBacklogSize);
          }
          log("+-------------------------------------------------------------------------------------");
          log();

          totalEngineTime = 0L;
          totalResQueueTime = 0L;
          totalResponseTime = 0L;
          totalSerialTime = 0L;
          totalPreEngTime = 0L;
          totalBacklogSize = 0;
          totalRedoEngineTime = 0L;
          totalRedoResQueueTime = 0L;
          totalRedoResponseTime = 0L;
          totalRedoSerialTime = 0L;
          totalRedoPreEngTime = 0L;
          totalRedoBacklogSize = 0;
          statRedoCount = 0;
          statResolvedCount = 0;
          statFailureCount = 0;

        }
      } else {
        totalRedoEngineTime += response.getEngineDuration();
        totalRedoResQueueTime += response.getResponseQueueDuration();
        totalRedoBacklogSize += response.getBacklogSize();
        totalRedoResponseTime += req.getResponseDuration();
        totalRedoSerialTime += response.getSerializeDuration();
        totalRedoPreEngTime += response.getPreEngineDuration();
        fileRedoCount++;
        statRedoCount++;
      }

      if (response != null && !response.isSuccessful()) {
        log("UNSUCCESSFUL RESPONSE RECEIVED");
        if (!this.engineProcess.isAborted() && !this.engineProcess.isLoadCancelled())
        {
          EngineException ee = response.getException();
          log("UNSUCCESSFUL RESPONSE: ");
          log(ee);

          Integer errorCode = ee.getErrorCode();
          boolean licenseExceeded = LICENSE_EXCEEDED_ERROR_CODE.equals(errorCode);
          log("****** ERROR CODE: " + errorCode);
          if (licenseExceeded) {
            log("******* GOT LICENSE EXCEEDED ERROR!!!!");
            Long recordCount = (Long) ee.getAdditionalInfo();
            log("USED RECORD COUNT: " + recordCount);
            if (recordCount != null) {
              this.saveLicenseRecordCount(recordCount);
            }
          }

          ServerError se;
          if (ee.isCritical()) {
            // halt further resolution
            se = new ServerError(true, "engine-error", ee);
            Set<Long> pendingFileIds = this.engineProcess.getPendingFileIds();
            if (! pendingFileIds.contains(this.file.getId())) {
              pendingFileIds.add(this.file.getId());
            }
            for (Long fileId : pendingFileIds) {
              recordFileError(this.projectId, fileId, se);
            }
            log("*****");
            log("***** ABORTING AFTER CRITICAL FAILURE -- REDUCING FURTHER LOGGING OF FAILURES");
            log("*****");
            this.engineProcess.abort();
            log("****** FLAGGED ABORT");
            break;

          } else if (!licenseExceeded || !this.engineProcess.isLoadCancelled()) {
            // NOTE: if this error is "license exceeded" and we already recorded
            // this error, then no need to record it again, hence the condition
            // above
            String code = "failed-resolve";
            if (licenseExceeded) {
              code = "license-exceeded";
            }
            se = new ServerError(false, code, ee);
            recordFileError(this.projectId, this.file.getId(), se);
            if (licenseExceeded) {
              recordProjectError(this.projectId, se);
            }
          }

          if (licenseExceeded) {
            this.engineProcess.setLoadCancelled(true);
          }

        } else {
          postAbortFailures++;
          if (postAbortFailures % 100 == 0) {
            log("***** " + postAbortFailures + " FAILURES SINCE " + (this.engineProcess.isAborted() ? "ABORTING" : "CANCELLING LOAD"));
          }
        }
      }
    }
    if (postAbortFailures > 0) {
      log("***** " + postAbortFailures + " TOTAL FAILURES SINCE " + (this.engineProcess.isAborted() ? "ABORTING" : "CANCELLING LOAD"));
    }
    log("COMPLETED WITH FILE REQUESTS AND RESPONSES: " + this.getElapsedTime(startTime) + " elapsed");
    this.updateFile(file,
                    resolvedCount,
                    failureCount,
                    lastResolved,
                    fileRedoCount,
                    updateTime);

    updateTime = nextUpdateTime;
    lastResolved = resolvedCount;
    log("UPDATED FILE WITH FINAL STATISTICS");
    this.updateLicenseRecordCount();
    this.receiveCompleted();
  }

  private void updateLicenseRecordCount() {
    // update the record count after receiving
    if (!this.engineProcess.isAborted()
        && !this.engineProcess.isReinitializing())
    {
      try {
        log("GETTING STATS LOADED RECORD COUNT....");
        long recordCount = this.engineProcess.getStatsLoadedRecordCount();
        log("GOT STATS LOADED RECORD COUNT: " + recordCount);
        if (recordCount >= 0) {
          this.saveLicenseRecordCount(recordCount);
        }
      } catch (Exception e) {
        log(e);
        ServerError se = new ServerError(false, "failed-stats", e);
        recordFileError(this.projectId, this.file.getId(), se);
      }
    }
  }

  private void saveLicenseRecordCount(long recordCount) {
    try {
      AccessToken token = new AccessToken();
      log("UPDATING LICENSE USAGE INFORMATION FOR PROJECT....");
      Project proj = this.projectDataAccess.selectProject(this.projectId, token);
      if (recordCount <= 0) {
        log("CALCULATING RECORD COUNT FROM SOURCE FILES");
        recordCount = proj.getFiles().stream()
            .mapToInt(e -> e.getResolvedRecordCount())
            .sum();
      }
      proj.setValue("licenseRecordCount", recordCount, token);
      this.projectDataAccess.updateProject(proj);
      log("UPDATED LICENSE USAGE INFORMATION FOR PROJECT.");

    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private String getElapsedTime(long startTime) {
    long elapsed = System.currentTimeMillis() - startTime;
    String elapsedHours = "" + (elapsed / (1000*60*60));
    if (elapsedHours.length() == 1) elapsedHours = "0" + elapsedHours;
    elapsed = (elapsed % (1000*60*60));
    String elapsedMinutes = "" + (elapsed / (1000*60));
    if (elapsedMinutes.length() == 1) elapsedMinutes = "0" + elapsedMinutes;
    elapsed = (elapsed % (1000*60));
    String elapsedSeconds = "" + (elapsed / 1000);
    if (elapsedSeconds.length() == 1) elapsedSeconds = "0" + elapsedSeconds;
    return (elapsedHours + ":" + elapsedMinutes + ":" + elapsedSeconds);
  }

  private double updateFile(ProjectFile file,
                            int         resolvedCount,
                            int         failureCount,
                            int         lastResolved,
                            int         redoCount,
                            long        startTime)
  {
    try {
      AccessToken token = new AccessToken();
      log("RESOLVED / FAILURE COUNT / REDO COUNT: " + resolvedCount + " / " + failureCount + " / " + redoCount);
      long now = System.currentTimeMillis();
      long elapsed = now - startTime;
      double elapsedSeconds = ((double)elapsed) / 1000.0;
      double resolutionRate = (elapsedSeconds == 0.0)
        ? 0.0 : ((double)(resolvedCount-lastResolved)) / elapsedSeconds;

      file = this.fileDataAccess.selectFile(this.projectId, file.getId(), token);
      file.setValue("resolvedRecordCount", resolvedCount, token);
      file.setValue("failedRecordCount", failureCount, token);
      file.setValue("resolutionRate", resolutionRate, token);
      file.setValue("resolved", true, token);
      log("UPDATING RESOLUTION RATE: " + resolutionRate + " ( " + (resolvedCount-lastResolved) + " in " + elapsedSeconds + " seconds )");
      this.fileDataAccess.updateFile(file);

      return resolutionRate;

    } catch (SQLException e) {
      log(e);
      throw new RuntimeException(e);
    }
  }

}
