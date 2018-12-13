package com.senzing.api.engine.process;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.IOException;
import java.net.Socket;

import com.senzing.util.Closer;
import com.senzing.api.engine.EngineRequest;
import com.senzing.api.engine.EngineResponse;

class DetachedConnector {
  private ObjectOutputStream    requestStream;
  private ObjectInputStream     responseStream;
  private Socket                socket;
  private ServerSocketConnector connector;

  DetachedConnector(ObjectOutputStream    requestStream,
                    ObjectInputStream     responseStream,
                    Socket                socket,
                    ServerSocketConnector connector)
  {
    this.requestStream  = requestStream;
    this.responseStream = responseStream;
    this.socket         = socket;
    this.connector      = connector;
  }

  public EngineResponse readResponse()
    throws IOException, ClassNotFoundException
  {
    EngineResponse result = null;
    synchronized (this.responseStream) {
      result = (EngineResponse) this.responseStream.readUnshared();
      result.markDeserialized();
    }
    return result;
  }

  public void writeRequest(EngineRequest request) throws IOException {
    synchronized (this.requestStream) {
      request.markSerializing();
      this.requestStream.writeUnshared(request);
      this.requestStream.flush();
    }
  }

  public synchronized void close(boolean terminate) {
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
    if (terminate) {
      this.connector.close();
    }
  }
}
