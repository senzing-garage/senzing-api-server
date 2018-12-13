package com.senzing.engine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import static com.senzing.api.engine.EngineMain.*;

public class EngineThread extends Thread {
  private List<Engine>          engines       = null;
  private EngineRequestQueue    requestQueue  = null;
  private EngineResponseQueue   responseQueue = null;
  private int                   affinity      = -1;

  public EngineThread(EngineRequestQueue    requestQueue,
                      EngineResponseQueue   responseQueue,
                      int                   affinity,
                      List<Engine>          engines)
  {
    this.requestQueue   = requestQueue;
    this.responseQueue  = responseQueue;
    this.engines        = new ArrayList<Engine>(engines.size());
    for (Engine engine : engines) {
      if (engine == null) continue;
      this.engines.add(engine);
    }
    this.affinity = affinity;
    this.setName("EngineThread-" + this.affinity);
  }

  public EngineThread(EngineRequestQueue    requestQueue,
                      EngineResponseQueue   responseQueue,
                      int                   affinity,
                      Engine...             engines)
  {
    this.requestQueue   = requestQueue;
    this.responseQueue  = responseQueue;
    this.engines        = new ArrayList<>(engines.length);
    for (Engine engine : engines) {
      if (engine == null) continue;
      this.engines.add(engine);
    }
    this.affinity = affinity;
    this.setName("EngineThread-" + this.affinity);
  }

  public void run() {
    try {
      this.doRun();
    } catch (Exception e) {
      log(e);
    } finally {
      log("Engine Thread Complete");
    }
  }

  public void doRun() throws IOException {
    long totalWait = 0L;
    long waitCount = 0L;
    long lastReported = System.currentTimeMillis();
    long maxPeriod = 60000L;
    long handledCount = 0;
    long affinityCount = 0;

    while (true) {
      EngineRequest request = null;
      synchronized (this.requestQueue) {
        if (this.requestQueue.getCount() > 0) this.requestQueue.notifyAll();
        while (request == null) {
          boolean found = false;
          if (this.requestQueue.hasApplicableRequests(this.affinity)) {
            try {
              request = this.requestQueue.dequeue(this.affinity);
              found = true;

            } catch (NoSuchElementException ignore) {
              // nothing to do here, no requests found
            }
          }

          if (!found && this.requestQueue.isShutdown()) {
            request = null;
            found = true;
          }

          if (found && request == null) break;

          if (!found) {
            try {
              long begin = System.currentTimeMillis();
              this.requestQueue.wait(10000L);
              long end = System.currentTimeMillis();
              long wait = end - begin;
              totalWait += wait;
              waitCount++;
              if (end > lastReported + maxPeriod) {
                log("EngineThread (" + this.getName()
                    + ") ALL-HANDLED / AFFINITY-HANDLED / AVG WAIT: "
                    + handledCount + " / " + affinityCount
                    + " / " + (totalWait / waitCount));
                lastReported = end;
                totalWait = 0L;
                waitCount = 0L;
              }
            } catch (InterruptedException ignore) {
              // ignore this exception
            }
          }
        }
      }

      // check for end of processing
      if (request == null) {
        log("GOT TERMINATING REQUEST: " + Thread.currentThread().getName());
        // ensure next thread gets notified of termination
        synchronized (this.requestQueue) {
          this.requestQueue.enqueue(null);
          this.requestQueue.notifyAll();
        }

        // break out of processing loop
        break;
      }

      // process the request
      EngineOperation operation = request.getOperation();
      long            requestId = request.getRequestId();
      EngineResponse  response  = null;
      try {
        for (Engine engine : this.engines) {
          if (engine.isDestroyed()) continue;
          if (engine.isOperationSupported(operation)) {
            response = engine.processRequest(request);
            handledCount++;
            if (request.getThreadAffinity() != null) {
              affinityCount++;
            }
            break;
          }
        }
        if (response == null) {
          throw new UnsupportedOperationException(
            "Unrecognized engine operation: " + request);
        }

      } catch (EngineException e) {
        log(e);
        log(request.getMessage());
        String msg = "Failed to process request: " + request;
        response = new EngineResponse(request, e);

      } catch (Exception e) {
        log(e);
        log(request.getMessage());
        String msg = "Failed to process request: " + request;
        EngineException ee = new EngineException(msg, e);
        response = new EngineResponse(request, ee);
      }

      synchronized (this.responseQueue) {
        response.markEnqueueTime();
        this.responseQueue.enqueue(response);
        this.responseQueue.notifyAll();
      }
    }
  }
}
