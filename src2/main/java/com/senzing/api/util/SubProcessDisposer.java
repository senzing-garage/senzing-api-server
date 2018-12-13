package com.senzing.api.util;

import java.io.IOException;

import static java.util.concurrent.TimeUnit.*;
import static com.senzing.api.Workbench.*;

public class SubProcessDisposer extends Thread {
  private Process       process;
  private StreamLogger  outLogger;
  private StreamLogger  errLogger;
  private String        logName;
  private boolean       forceDestroy;
  private Integer       processId;
  private boolean       completed = false;

  public SubProcessDisposer(
    String                  logName,
    Process                 process,
    Integer                 processId,
    boolean                 forceDestroy,
    StreamLogger            outLogger,
    StreamLogger            errLogger)
  {
    this.logName      = logName;
    this.process      = process;
    this.processId    = processId;
    this.forceDestroy = forceDestroy;
    this.outLogger    = outLogger;
    this.errLogger    = errLogger;
    this.setName("SubProcessDisposer-" + this.logName);
  }

  public synchronized boolean isCompleted() {
    return this.completed;
  }

  public void run() {
    try {
      boolean destroyed = false;
      if (this.forceDestroy) {
        log("FORCIBLY DESTROYING " + this.logName + " SUB-PROCESS"
            + ((this.processId != null) ? " (" + this.processId + ")" : "")
            + "...");
        this.process.destroyForcibly();
      } else {
        log("GRACEFULLY DESTROYING " + this.logName + " SUB-PROCESS"
            + ((this.processId != null) ? " (" + this.processId + ")" : "")
            + "...");
        this.process.destroy();
      }
      log("WAITING FOR " + this.logName + " SUB-PROCESS"
          + ((this.processId != null) ? " (" + this.processId + ")" : "")
          + " TO SHUTDOWN...");
      while (!destroyed && this.process.isAlive()) {
        try {
          destroyed = this.process.waitFor(10, SECONDS);
        } catch (InterruptedException ignore) {
          // ignore
        }
        if (!destroyed) {
          log(this.logName + " SUB-PROCESS"
              + ((this.processId != null) ? " (" + this.processId + ")" : "")
              + " DID NOT SHUTDOWN, FORCIBLY DESTROYING...");
          this.process.destroyForcibly();
          log(this.logName + " SUB-PROCESS"
              + ((this.processId != null) ? " (" + this.processId + ")" : "")
              + " DID NOT SHUTDOWN, FORCIBLY DESTROYED");
        }
      }

      if (this.processId != null) {
        Thread checker = new Thread(() -> {
          long now = System.currentTimeMillis();
          long deadline = now + 5000L;
          while (!this.isCompleted() && System.currentTimeMillis() < deadline) {
            try {
              Thread.sleep(500L);
            } catch (InterruptedException ignore) {
              log(ignore);
            }
          }
          Runtime runtime = Runtime.getRuntime();
          final String osName = System.getProperty("os.name");

          boolean windows = false;
          if (osName.toLowerCase().trim().startsWith("windows")) {
            windows = true;
          }
          if (this.isCompleted()) {
            return;
          }
          String pid = this.processId.toString();
          String[] cmdArray = (windows)
              ? new String[] { "taskkill", "/PID", pid }
              : new String[] { "kill", "-SIGTERM", pid };

          log(this.logName + " SUB-PROCESS"
              + ((this.processId != null) ? " (" + this.processId + ")" : "")
              + " SHUTDOWN STILL PENDING, DESTROYING BY PROCESS ID....");
          try {
            runtime.exec(cmdArray);
          } catch (IOException ignore) {
            log(ignore);
          }

          deadline = System.currentTimeMillis() + 5000L;
          while (!this.isCompleted() && System.currentTimeMillis() < deadline) {
            try {
              Thread.sleep(500L);
            } catch (InterruptedException ignore) {
              log(ignore);
            }
          }
          if (this.isCompleted()) {
            log(this.logName + " SUB-PROCESS"
                + ((this.processId != null) ? " (" + this.processId + ")" : "")
                + " SHUTDOWN BY PROCESS ID SUCCESSFUL.");
            return;
          }
          cmdArray = (windows)
            ? new String[] { "taskkill", "/PID", pid, "/F" }
            : new String[] { "kill", "-9", pid };

          log(this.logName + " SUB-PROCESS"
              + ((this.processId != null) ? " (" + this.processId + ")" : "")
              + " SHUTDOWN STILL PENDING, DESTROYING FORCIBLY BY PROCESS ID....");
          try {
            runtime.exec(cmdArray);
          } catch (IOException ignore) {
            log(ignore);
          }
          try {
            Thread.sleep(500L);
          } catch (InterruptedException ignore) {
            log(ignore);
          }
          if (this.isCompleted()) {
            log(this.logName + " SUB-PROCESS"
                + ((this.processId != null) ? " (" + this.processId + ")" : "")
                + " FORCED SHUTDOWN BY PROCESS ID SUCCESSFUL.");
          } else {
            log("UNABLE TO SHUT DOWN " + this.logName + " SUB-PROCESS"
                + ((this.processId != null) ? " (" + this.processId + ")" : "")
                + " BY PROCESS ID.");
          }
        });
        checker.start();
      }

      log("CLOSING STREAMS FOR " + this.logName + " SUB-PROCESS"
          + ((this.processId != null) ? " (" + this.processId + ")" : ""));
      if (this.outLogger != null) this.outLogger.complete();
      if (this.errLogger != null) this.errLogger.complete();
      log("CLOSED STREAMS FOR " + this.logName + " SUB-PROCESS"
          + ((this.processId != null) ? " (" + this.processId + ")" : ""));

      synchronized (this) {
        this.completed = true;
      }

    } catch (Exception e) {
      log(e);
    } finally {
      log(this.logName + " DISPOSER COMPLETING");
    }
  }
}
