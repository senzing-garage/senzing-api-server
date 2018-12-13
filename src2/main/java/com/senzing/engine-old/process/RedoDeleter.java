package com.senzing.api.engine.process;

import java.sql.SQLException;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.Iterator;

import com.senzing.api.Redo;

import static com.senzing.api.Workbench.*;

class RedoDeleter extends Thread {
  private Receiver receiver;
  private Map<Long,Redo> pendingRedoDeletions;
  private RedoDataAccess redoDataAccess;

  RedoDeleter(Receiver receiver) {
    this.receiver             = receiver;
    this.pendingRedoDeletions = new LinkedHashMap<Long,Redo>();
    this.redoDataAccess       = new RedoDataAccess();
    this.setName("RedoDeleter-" + this.receiver.getProjectId());
    this.start();
  }

  public void enqueue(Long redoId, Redo redo) {
    synchronized (this.pendingRedoDeletions) {
      this.pendingRedoDeletions.put(redoId, redo);
      this.pendingRedoDeletions.notifyAll();
    }
  }

  public boolean hasPendingDeletions() {
    synchronized (this.pendingRedoDeletions) {
      return this.pendingRedoDeletions.size() > 0;
    }
  }

  public int getPendingDeletionsCount() {
    synchronized (this.pendingRedoDeletions) {
      return this.pendingRedoDeletions.size();
    }
  }

  public Redo removePendingDeletion(Long redoId) {
    Redo result = null;
    synchronized (this.pendingRedoDeletions) {
      result = this.pendingRedoDeletions.remove(redoId);
      if (result != null) {
        this.pendingRedoDeletions.notifyAll();
      }
    }
    return result;
  }

  public void run() {
    while (!this.receiver.isReceiveCompleted() || this.hasPendingDeletions()) {
      Map<Long,Redo> redosToDelete = null;
      synchronized (this.pendingRedoDeletions) {
        while (this.pendingRedoDeletions.size() == 0
               && !this.receiver.isReceiveCompleted()) {
          try {
            this.pendingRedoDeletions.wait(2000L);
          } catch (Exception ignore) {
            // ignore the exception
          }
        }

        if (this.pendingRedoDeletions.size() == 0) {
          continue;
        }

        redosToDelete = new LinkedHashMap<Long,Redo>();

        redosToDelete.putAll(this.pendingRedoDeletions);
      }

      long projectId = this.receiver.getProjectId();
      try (Connector conn = Connector.entityRepo(projectId)) {
        Iterator<Map.Entry<Long,Redo>> iter = redosToDelete.entrySet().iterator();
        boolean delaying = false;
        while (iter.hasNext() && !delaying) {
          Map.Entry<Long,Redo> entry = iter.next();
          long redoId = entry.getKey();
          Redo redo = entry.getValue();
          boolean success = false;
          long retrySleep = 50L;
          int retryCount = 0;
          int maxRetries = 10;

          while (!success && retryCount <= maxRetries) {
            try {
              boolean deleted
                = this.redoDataAccess.deleteRedo(conn, projectId, redo);

              // remove from the list if no exception
              success = true;
              iter.remove();
              this.receiver.removePendingRedo(redoId);
              this.removePendingDeletion(redoId);

              if (!deleted) {
                log("WARNING: DID NOT FIND REDO TO DELETE: " + redo);
              }

            } catch (SQLException e) {
              if (e.toString().indexOf("SQLITE_BUSY") >= 0) {
                // check if we have more pending redos
                log("**** SQLITE BUSY FOR REDO DELETION.");
                log("**** " + redosToDelete.size() + " ON DECK FOR DELETION");
                log("**** " + this.getPendingDeletionsCount() + " TOTAL PENDING FOR DELETION");
                log("SLEEPING FOR " + retrySleep + "ms, THEN RETRYING.");
                success = false;
                try {
                  Thread.sleep(retrySleep);
                } catch (InterruptedException ignore) {
                  // ignore the exception
                }
                retrySleep += (retrySleep/2);
                retryCount++;
                if (retryCount > maxRetries) {
                  log("TOO MANY RETRIES FOR DELETING REDO... DISABLING REDOS FOR NOW.");
                  log(e);
                  this.receiver.disableRedos();
                  delaying = true;
                }
              } else {
                log("RECEIVED UNRECOGNIZED SQL EXCEPTION ON DELETE... DISABLING REDOS FOR NOW.");
                log(e);
                this.receiver.disableRedos();
                delaying = true;
                break;
              }
            }
          }
        }
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }
  }
}
