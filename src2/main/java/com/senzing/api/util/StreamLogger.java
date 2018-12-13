package com.senzing.api.util;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.IOException;

import static com.senzing.api.Workbench.*;
import static com.senzing.util.Closer.*;

public class StreamLogger extends Thread {
  private InputStreamReader reader = null;
  private boolean completed = false;
  private Object monitor = null;
  private char[] buffer = null;
  private StringBuilder stringBuilder = null;

  public StreamLogger(InputStream is) throws IOException {
    this.reader = new InputStreamReader(is);
    this.completed = false;
    this.monitor = new Object();
    this.buffer = new char[8192];
    this.stringBuilder = new StringBuilder(8192);
    this.start();
  }

  public void complete() {
    synchronized (this.monitor) {
      this.completed = true;
      this.reader = close(this.reader);
    }
  }

  public boolean isComplete() {
    synchronized (this.monitor) {
      return this.completed;
    }
  }
  public void run() {
    while (!this.isComplete()) {
      try {
        int readCount = this.reader.read(this.buffer);
        if (readCount > 0) {
          String text = this.stringBuilder.append(this.buffer, 0, readCount).toString();
          this.stringBuilder.delete(0, readCount);
          log(text, false);
        }
        if (readCount < 0) this.complete();

      } catch (Exception e) {
        this.complete();
        log(e);
      }
    }
  }
}
