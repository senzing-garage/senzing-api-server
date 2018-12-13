package com.senzing.api.engine;

class Responder extends Thread {
  private final EngineResponseQueue   responseQueue;
  private SocketConnector       connector;
  private boolean               complete;

  Responder(SocketConnector connector, EngineResponseQueue responseQueue) {
    this.responseQueue = responseQueue;
    this.connector = connector;
    this.complete = false;
    this.start();
  }

  public void complete() {
    synchronized (this.responseQueue) {
      this.complete = true;
      this.responseQueue.notifyAll();
    }
  }

  public boolean isComplete() {
    synchronized (this.responseQueue) {
      return this.complete;
    }
  }

  public void run() {
    try {
      EngineResponse response;
      // connect the socket and wait for the server to start
      while (!this.isComplete()) {
        response = null;
        synchronized(this.responseQueue) {
          while (response == null && !this.isComplete()) {
            if (this.responseQueue.getCount() > 0) {
              response = this.responseQueue.dequeue();
              response.markDequeueTime();
            } else {
              try {
                this.responseQueue.wait();
              } catch (Exception ignore) {
                // ignore interruptions
              }
            }
          }
        }
        if (!this.isComplete() && response != null) {
          this.connector.writeResponse(response);
        }
      }
    } catch (Exception e) {
      EngineMain.log(e);
    }
  }
}
