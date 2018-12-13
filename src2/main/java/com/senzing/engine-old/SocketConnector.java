package com.senzing.api.engine;

import com.senzing.util.Closer;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;

/**
 * Connector used by the Engine's process to exchange requests and responses with the api process.
 */
class SocketConnector {
  private static final int RESET_THRESHOLD_INCREMENT = 200;
  private static final int MAX_RETRIES = 10;

  private InetAddress         ipAddr;
  private int                 port;
  private ObjectInputStream   requestStream;
  private ObjectOutputStream  responseStream;
  private Socket              socket;
  private int                 writeCount;
  private int                 resetThreshold;
  private long                authId;

  SocketConnector(InetAddress ipAddr, int port, long authId) {
    this.ipAddr         = ipAddr;
    this.port           = port;
    this.socket         = null;
    this.requestStream  = null;
    this.responseStream = null;
    this.resetThreshold = RESET_THRESHOLD_INCREMENT;
    this.writeCount     = 0;
    this.authId         = authId;
  }

  public synchronized void close() {
    EngineMain.log("Closing Socket");
    if (this.requestStream != null) {
      synchronized (this.requestStream) {
        this.requestStream = Closer.close(this.requestStream);
      }
    }
    if (this.responseStream != null) {
      synchronized (this.responseStream) {
        this.responseStream = Closer.close(this.responseStream);
      }
    }
    this.socket = Closer.close(this.socket);
  }

  public synchronized void connect() {
    this.connect(30);
  }

  public synchronized void connect(int maxRetries) {
    EngineMain.log("Beginning Connection");
    this.close();
    int retries = 0;
    if (maxRetries < 0) maxRetries = 0;
    Exception lastFailure = null;
    this.close();
    if (this.socket != null) EngineMain.log("********** SOCKET IS NOT NULL AFTER CLOSE!!!!!");
    while (this.socket == null && retries <= maxRetries) {
      try {
        EngineMain.log("ATTEMPTING TO CONNECT....");
        if (retries > 0) this.sleep(1000L);
        this.socket = new Socket(this.ipAddr, this.port);
        this.socket.setSoTimeout(0);
        this.socket.setKeepAlive(true);
        EngineMain.log("CONNECTED (timeout/keepalive): " + this.socket.getSoTimeout() + " / " + this.socket.getKeepAlive());
        this.requestStream = new ObjectInputStream(this.socket.getInputStream());
        this.responseStream = new ObjectOutputStream(this.socket.getOutputStream());
        this.responseStream.writeUnshared(this.authId);
        this.responseStream.flush();
        this.writeCount = 0;
        this.resetThreshold = RESET_THRESHOLD_INCREMENT;

      } catch (Exception e) {
        EngineMain.log(e);
        lastFailure = e;
        retries++;
        if (retries <= maxRetries) {
          EngineMain.log("RETRYING: " + retries);
        }
        else {
          EngineMain.log("ERROR: FAILURE WHILE REESTABLISHING CONNECTION WITH WORKBENCH, final error: ", true);
          EngineMain.log(e);
        }
      }
    }
    if (this.socket == null) {
      this.requestStream = null;
      this.responseStream = null;
      throw new RuntimeException(lastFailure);
    }
  }

  private void sleep(long milliseconds) {
    try {
      Thread.sleep(milliseconds);
    } catch (InterruptedException ignore) {
      // ignore
    }
  }

  EngineRequest readRequest() {
    ObjectInputStream requestStream = null;
    EngineRequest result = null;
    boolean success = false;
    int tryCount = 0;
    Exception lastFailure = null;
    int maxRetries = 10;
    while (!success && tryCount < maxRetries) {
      try {
        requestStream = null;
        synchronized (this) {
          requestStream = this.requestStream;
        }
        if (requestStream == null) {
          throw new IllegalStateException(
            "Request stream is null -- not connected to parent process.");
        }
        synchronized (requestStream) {
          result = (EngineRequest) requestStream.readUnshared();
          result.markDeserialized();
        }
        success = true;
      } catch (Exception e) {
        lastFailure = e;
        if (tryCount > 0) this.sleep(500L);
        EngineMain.log("CONNECTION FAILED DURING READ -- REESTABLISHING");
        EngineMain.log("---> CAUSE: ", false);
        EngineMain.log(e);
        synchronized (this) {
          // its possible the requestStream was updated asynchronously so check
          // if the member request stream and local are the same, then reconnect
          if (this.requestStream == requestStream || this.requestStream == null) {
            EngineMain.log("ATTEMPTING TO RECONNECT....");
            requestStream = null;
            this.connect(5);
          } else {
            EngineMain.log("NEW CONNECTION ALREADY ESTABLISHED -- RETRY READ");
          }
        }
        tryCount++;
      }
    }
    if (!success) throw new RuntimeException(lastFailure);
    return result;
  }

  void writeResponse(EngineResponse response) {
    boolean success = false;
    int tryCount = 0;
    Exception lastFailure = null;
    ObjectOutputStream responseStream = null;
    if (response != null) response.setEngineAuthenticationId(this.authId);

    while (!success && tryCount <= MAX_RETRIES) {
      try {
        responseStream = null;
        synchronized (this) {
          responseStream = this.responseStream;
        }
        response.markSerializing();
        this.responseStream.writeUnshared(response);
        EngineOperation operation = response.getOperation();
        resetIfNeeded(operation != null && operation.isResetRequired());
        success = true;
      } catch (Exception e) {
        lastFailure = e;
        EngineMain.log("CONNECTION FAILED DURING WRITE -- REESTABLISHING");
        EngineMain.log("---> CAUSE: ", false);
        EngineMain.log(e);
        if (tryCount > 0) {
          this.sleep(500L);
        }
        responseStream = attemptReconnect(responseStream);
        tryCount++;
      }
    }
    if (!success) throw new RuntimeException(lastFailure);
  }

  void writeStartupError(Throwable throwable) {
    EngineResponse engineResponse = new EngineResponse(new EngineException(throwable));
    boolean success = false;
    int tryCount = 0;
    boolean firstFailure = true;

    while (!success && tryCount++ <= MAX_RETRIES) {
      engineResponse.markSerializing();
      try {
        if (this.responseStream != null) {
          this.responseStream.writeUnshared(engineResponse);
          resetIfNeeded(engineResponse.getOperation() != null && engineResponse.getOperation().isResetRequired());
          success = true;
        }
      } catch(IOException e) {
        EngineMain.log("ERROR: FAILURE TO NOTIFY WORKBENCH OF ENGINE ERROR -- REESTABLISHING");
        EngineMain.log("---> CAUSE: ", false);
        EngineMain.log(e);
        if (!firstFailure) {
          this.sleep(500L);
        }
        attemptReconnect(this.responseStream);
        firstFailure = false;
      }
    }
    if (!success) {
      EngineMain.log("ERROR: UNABLE TO INFORM WORKBENCH OF ENGINE ERROR:");
      EngineMain.log(throwable);
    }
  }

  private ObjectOutputStream attemptReconnect(ObjectOutputStream responseStream) {
    synchronized (this) {
      if (this.responseStream == responseStream || this.responseStream == null) {
        EngineMain.log("ATTEMPTING TO RECONNECT....");
        responseStream = null;
        this.connect(5);
      } else {
        EngineMain.log("NEW CONNECTION ALREADY ESTABLISHED -- RETRY WRITE");
      }
    }
    return responseStream;
  }

  private void resetIfNeeded(boolean resetRequired) throws IOException {
    this.responseStream.flush();
    this.writeCount++;
    if (this.writeCount > this.resetThreshold || resetRequired) {
      this.resetThreshold += RESET_THRESHOLD_INCREMENT;
      this.responseStream.reset();
    }
  }
}
