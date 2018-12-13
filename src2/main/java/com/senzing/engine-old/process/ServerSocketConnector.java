package com.senzing.api.engine.process;

import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.Optional;

import com.senzing.util.Closer;
import com.senzing.api.LoggedServerErrorException;
import com.senzing.api.ServerError;
import com.senzing.api.ServerErrorLog;
import com.senzing.api.engine.EngineOperation;
import com.senzing.api.engine.EngineRequest;
import com.senzing.api.engine.EngineResponse;
import com.senzing.api.engine.EngineRequestCancelledException;

import static com.senzing.api.Workbench.*;
import static com.senzing.api.engine.process.EngineProcess.*;

class ServerSocketConnector extends Thread {
  private static final int MAX_ENGINE_RESTARTS = 0;

  private InetAddress         ipAddr;
  private ObjectOutputStream  requestStream;
  private ObjectInputStream   responseStream;
  private InputStream         rawResponseStream;
  private ServerSocket        serverSocket;
  private Socket              socket;
  private boolean             closed;
  private int                 writeCount;
  private int                 resetThreshold;
  private Process             clientProcess;
  private long                authenticationId;
  private long                projectId;
  private String              logName;
  private int                 restartCount = 0;
  private EngineProcess       engineProcess = null;

  ServerSocketConnector(EngineProcess engineProcess, String logName) {
    try {
      this.projectId      = engineProcess.getProjectId();
      this.engineProcess  = engineProcess;
      this.logName        = logName;
      this.ipAddr         = InetAddress.getLoopbackAddress();
      this.serverSocket   = new ServerSocket(0, 5, this.ipAddr);
      this.socket         = null;
      this.requestStream  = null;
      this.responseStream = null;
      this.closed         = false;
      this.writeCount     = 0;
      this.restartCount   = 0;
      this.resetThreshold = RESET_THRESHOLD_INCREMENT;
      this.serverSocket.setSoTimeout(10000);
      this.setName("ServerSocketConnector-" + this.projectId
                   + "-" + Math.abs(System.identityHashCode(engineProcess))
                   + "-" + this.logName);
      this.start();
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public synchronized void setClientProcess(Process process) {
    this.clientProcess = process;
  }

  public synchronized long getAuthenticationId() {
    return this.authenticationId;
  }

  public synchronized void setAuthenticationId(long authId) {
    this.authenticationId = authId;
  }

  private synchronized boolean isClientProcessDead() {
    return (this.clientProcess != null && !this.clientProcess.isAlive());
  }

  public int getPort() {
    return this.serverSocket.getLocalPort();
  }

  synchronized boolean waitForEngine() {
    waitForConnection();

    EngineResponse engineResponse = null;
    try {
      EngineRequest ping = new EngineRequest(EngineOperation.NO_OP, getRequestId());
      writeRequest(ping);
      engineResponse = readResponse();
    } catch (RuntimeException e) {
      // do nothing.  No Response received.
    }
    if (engineResponse != null && engineResponse.getException() != null) {
      ServerErrorLog.recordProjectError(projectId, new ServerError(true, engineResponse.getException()));
      throw new LoggedServerErrorException("Engine process died before connecting.", engineResponse.getException());
    }
    log("IS CLIENT PROCESS DEAD: " + this.isClientProcessDead());
    return (this.socket != null);
  }

  private Long getRequestId() {
    return Optional.ofNullable(engineProcess)
        .map(EngineProcess::getNextRequestId)
        .orElseThrow(() -> new IllegalStateException("EngineProcess cannot be null"));
  }

  private void waitForConnection() {
    final int maxRetries = 500;
    int tryCount = 0;
    while (this.socket == null && !this.isClosed() && tryCount < maxRetries && !this.isClientProcessDead()) {
      try {
        this.wait(600L);
      } catch (InterruptedException e) {
        // ignore
      }
      tryCount++;
    }
  }

  public void run() {
    int retries = 0;
    while (!this.isClosed()) {
      try {
        Socket socket = this.serverSocket.accept();
        log("ACCEPTED CONNECTION.");
        synchronized (this) {
          socket.setSoTimeout(0);
          socket.setKeepAlive(true);
          log("CONNECTED (timeout/keepalive): " + socket.getSoTimeout() + " / " + socket.getKeepAlive());
          ObjectOutputStream  reqStream     = new ObjectOutputStream(socket.getOutputStream());
          InputStream         rawRespStream = socket.getInputStream();
          ObjectInputStream   respStream    = new ObjectInputStream(rawRespStream);
          long authId = (Long) respStream.readUnshared();
          if (authId != this.authenticationId) {
            log("CONNECTION IGNORED FROM UNAUTHORIZED OR STALE ENGINE CLIENT");
            reqStream   = Closer.close(reqStream);
            respStream  = Closer.close(respStream);
            socket      = Closer.close(socket);
            continue;
          }
          this.socket = socket;
          this.requestStream = reqStream;
          this.rawResponseStream = rawRespStream;
          this.responseStream = respStream;
          this.writeCount = 0;
          this.resetThreshold = RESET_THRESHOLD_INCREMENT;
          this.notifyAll();
        }
      } catch (SocketTimeoutException e) {
        continue;

      } catch (Exception e) {
        log(e);
        retries++;
        if (retries > 0) this.pause(500L);
      }
      if (retries > 5) {
        log("NO LONGER ACCEPTING CONNECTIONS -- TOO MANY FAILURES");
        this.close();
      }
    }
    this.shutdown();
  }

  private synchronized void clearRestartCount() {
    this.restartCount = 0;
  }

  private synchronized void incrementRestartCount() {
    this.restartCount++;
  }

  private synchronized int getRestartCount() {
    return this.restartCount;
  }

  private synchronized boolean optionallyRestartEngine() {
    if (this.getRestartCount() < MAX_ENGINE_RESTARTS) {
      log();
      log("************************************************");
      log();
      log("RESTARTING ENGINE.  ATTEMPT #" + this.getRestartCount()
          + " OF AT MOST " + MAX_ENGINE_RESTARTS);
      log();
      log("************************************************");
      log();
      if (this.engineProcess.restartEngine() != null) {
        this.incrementRestartCount();
      }
      return true;
    }
    return false;
  }

  public synchronized boolean isClosed() {
    return this.closed;
  }

  public synchronized void close() {
    this.disconnect();
    this.closed = true;
  }

  private synchronized void shutdown() {
    this.close();
    this.serverSocket = Closer.close(this.serverSocket);
  }

  public synchronized DetachedConnector detach() {
    DetachedConnector result
      = new DetachedConnector(this.requestStream,
                              this.responseStream,
                              this.socket,
                              this);
    this.requestStream = null;
    this.responseStream = null;
    this.rawResponseStream = null;
    this.socket = null;
    return result;
  }

  public synchronized void disconnect() {
    if (this.requestStream != null) {
      synchronized (this.requestStream) {
        this.requestStream = Closer.close(this.requestStream);
      }
    }
    if (this.responseStream != null) {
      synchronized (this.responseStream) {
        this.responseStream = Closer.close(this.responseStream);
        this.rawResponseStream = null;
      }
    }
    this.socket = Closer.close(this.socket);
  }

  public synchronized boolean isResponseAvailable() {
    try {
      if (this.socket == null) return false;
      if (this.rawResponseStream == null) return false;
      return (this.rawResponseStream.available() > 0);
    } catch (Exception e) {
      log(e);
      return false;
    }
  }

  private void pause(long milliseconds) {
    try {
      Thread.sleep(milliseconds);
    } catch (InterruptedException ignore) {
      // ignore
    }
  }

  public EngineResponse readResponse() {
    boolean success = false;
    int tryCount = 0;
    EngineResponse result = null;
    Exception lastFailure = null;
    final int maxRetries = 30;
    ObjectInputStream responseStream = null;

    while (!success && tryCount < maxRetries && !this.isClosed() && !this.isClientProcessDead()) {
      synchronized (this) {
        responseStream = null;
        while (this.socket == null && !this.isClosed() && tryCount < maxRetries && !this.isClientProcessDead()) {
          try {
            this.wait(5000L);
          } catch (InterruptedException e) {
            // ignore
          }
          tryCount++;
        }
        if (this.socket == null) {
          log("FAILED TO ESTABLISH CONNECTION WITH CHILD");
          break;
        }
        responseStream = this.responseStream;
      }
      try {
        synchronized (responseStream) {
          result = (EngineResponse) responseStream.readUnshared();
          result.markDeserialized();
        }
        success = true;
        responseStream = null;
        this.clearRestartCount();

      } catch (Exception e) {
        log(e);
        synchronized (this) {
          if (this.responseStream == responseStream) {
            responseStream = null;
            this.disconnect();
          }
        }
        lastFailure = e;
        tryCount++;
        if (tryCount < maxRetries) {
          log("CONNECTION FAILED DURING READ -- REESTABLISHING");
          log("---> CAUSE: ", false);
          log(e);
        }
      }
    }
    if (!success) {
      if (lastFailure == null && this.isClientProcessDead()) {
        if (this.optionallyRestartEngine()) {
          try { Thread.sleep(5000L); } catch (InterruptedException ignore) {}
          return this.readResponse();

        } else {
          throw new RuntimeException(
            "Engine process failed to start or quit unexpectedly");
        }
      }
      throw new RuntimeException(lastFailure);
    }
    return result;
  }

  public void writeRequest(EngineRequest request) {
    boolean success = false;
    int tryCount = 0;
    Exception lastFailure = null;
    final int maxRetries = 30;
    ObjectOutputStream requestStream = null;
    Long beginAuthId = null;
    if (request.getOperation().isCancelledOnEngineRestart()) {
      beginAuthId = request.getEngineAuthenticationId();
      if (beginAuthId == null) {
        beginAuthId = this.authenticationId;
      }
    }
    if (beginAuthId != null && beginAuthId != this.authenticationId) {
      log("CANCELLED REQUEST DUE TO ENGINE RESTART: "
          + request.getOperation());
      throw new EngineRequestCancelledException(request);
    }
    while (!success && tryCount < maxRetries && !this.isClosed() && !this.isClientProcessDead()
           && (beginAuthId == null || beginAuthId == this.authenticationId)) {
      synchronized (this) {
        requestStream = null;
        while (this.socket == null && !this.isClosed() && tryCount < maxRetries
               && !this.isClientProcessDead()
               && (beginAuthId == null || beginAuthId == this.authenticationId))
        {
          try {
            this.wait(5000L);
          } catch (InterruptedException e) {
            // ignore
          }

          tryCount++;
        }
        if (beginAuthId != null && beginAuthId != this.authenticationId) {
          log("CANCELLED RETRY OF REQUEST DUE TO ENGINE RESTART: "
              + request.getOperation());
          throw new EngineRequestCancelledException(request);
        }
        if (this.socket == null) {
          log("FAILED TO ESTABLISH CONNECTION WITH CHILD");
          break;
        }
        requestStream = this.requestStream;
      }
      if (beginAuthId != null && beginAuthId != this.authenticationId) {
        log("CANCELLED RETRY OF REQUEST DUE TO ENGINE RESTART: "
            + request.getOperation());
        throw new EngineRequestCancelledException(request);
      }
      try {
        synchronized (requestStream) {
          request.markSerializing();
          requestStream.writeUnshared(request);
          requestStream.flush();
          this.writeCount++;
          if (this.writeCount > this.resetThreshold) {
            this.resetThreshold += RESET_THRESHOLD_INCREMENT;
            requestStream.reset();
          }
        }
        this.clearRestartCount();
        success = true;
        requestStream = null;

      } catch (Exception e) {
        synchronized (this) {
          if (requestStream == this.requestStream) {
            requestStream = null;
            this.disconnect();
          }
        }
        lastFailure = e;
        tryCount++;
        if (tryCount < maxRetries) {
          log("CONNECTION FAILED DURING WRITE -- REESTABLISHING");
          log("---> CAUSE: ", false);
          log(e);
        }
      }
    }
    if (!success) {
      if (lastFailure == null && this.isClientProcessDead()) {
        if (this.optionallyRestartEngine()) {
          try { Thread.sleep(5000L); } catch (InterruptedException ignore) {}
          this.writeRequest(request);

        } else {
          RuntimeException e = new RuntimeException(
            "Engine process failed to start or quit unexpectedly");
          ServerError se = new ServerError(true, e);
          ServerErrorLog.recordProjectError(this.projectId, se);
          this.close();
          throw e;
        }
      } else {
        throw new RuntimeException(lastFailure);
      }
    }
  }
}
