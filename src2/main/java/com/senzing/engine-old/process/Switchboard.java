package com.senzing.api.engine.process;

import java.util.List;
import java.util.LinkedList;
import java.util.Iterator;

import com.senzing.api.engine.EngineResponse;

import static com.senzing.api.Workbench.*;

class Switchboard extends Thread {
  public static interface ResponseHandler {
    public int getPendingCount();
    public boolean isPending(long requestId);
    public void responseReceived(EngineResponse response);
  }

  private EngineProcess owner;

  private ServerSocketConnector connector;

  private List<ResponseHandler> handlers;

  private boolean completed;

  public Switchboard(EngineProcess          owner,
                     ServerSocketConnector  connector)
  {
    this.handlers       = new LinkedList<ResponseHandler>();
    this.completed      = false;
    this.owner          = owner;
    this.connector      = connector;
    this.setName("Switchboard-" + this.owner.getProjectId()
                 + "-" + Math.abs(System.identityHashCode(owner)));
    this.start();
  }

  public synchronized void registerHandler(ResponseHandler handler) {
    if (handler == null) return;
    for (ResponseHandler h : this.handlers) {
      if (h == handler) return;
    }
    this.handlers.add(handler);
    this.notifyAll();
  }

  public synchronized void unregisterHandler(ResponseHandler handler) {
    if (handler == null) return;
    Iterator<ResponseHandler> iter = this.handlers.iterator();
    while (iter.hasNext()) {
      ResponseHandler h = iter.next();
      if (h == handler) {
        iter.remove();
        break;
      }
    }
    this.notifyAll();
  }

  public synchronized boolean isCompleted() {
    return this.completed;
  }

  public synchronized void complete() {
    this.completed = true;
    this.notifyAll();
  }

  public void run() {
    try {
      this.doRun();
    } catch (Exception e) {
      log(e);
    } finally {
      this.complete();
    }
  }

  protected synchronized boolean pendingResponses() {
    for (ResponseHandler handler : this.handlers) {
      if (handler.getPendingCount() > 0) {
        return true;
      }
    }
    return false;
  }

  protected void doRun() {
    while (!this.isCompleted() && !this.owner.isShutdown()
           && !this.connector.isClosed())
    {
      synchronized (this) {
        boolean pending = this.pendingResponses();
        if (!pending) {
          try {
            this.wait(5000L);
          } catch (InterruptedException ignore) {
            log(ignore);
          }
        }
      }

      EngineResponse  response    = this.connector.readResponse();
      long            responseId  = response.getResponseId();
      ResponseHandler handler     = null;

      synchronized (this) {
        for (ResponseHandler h : this.handlers) {
          if (h.isPending(responseId)) {
            handler = h;
            break;
          }
        }
      }
      if (handler == null) {
        log("NO HANDLER FOR RESPONSE: " + response);
        continue;
      }
      handler.responseReceived(response);
    }
  }
}
