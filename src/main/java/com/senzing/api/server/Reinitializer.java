package com.senzing.api.server;

import com.senzing.g2.engine.G2ConfigMgr;
import com.senzing.g2.engine.G2Engine;
import com.senzing.util.WorkerThreadPool;

import static com.senzing.util.LoggingUtilities.formatError;

/**
 * Background thread to periodically check to see if there is a new
 * configuration and update the configuration if so.
 */
class Reinitializer extends Thread {
  /**
   * The maximum number of errors before giving up on monitoring the active
   * configuration for changes.
   */
  static final int MAX_ERROR_COUNT = 5;

  /**
   * The G2 engine API used to monitor the active config.
   */
  private G2Engine engineApi;

  /**
   * The G2 configuration manager API used to monitor the default config.
   */
  private G2ConfigMgr configMgrApi;

  /**
   * The API server to reinitialize.
   */
  private SzApiServer apiServer;

  /**
   * Flag indciating if the thread should complete or continue monitoring.
   */
  private boolean complete;

  /**
   * Constructs with the {@link G2ConfigMgr} and {@link G2Engine} API
   * references.
   *
   * @param configMgrApi The {@link G2ConfigMgr} API.
   * @param engineApi The {@link G2Engine} API.
   * @param apiServer The {@link SzApiServer} to notify of reinitialization.
   */
  Reinitializer(G2ConfigMgr       configMgrApi,
                G2Engine          engineApi,
                SzApiServer       apiServer)
  {
    this.configMgrApi     = configMgrApi;
    this.engineApi        = engineApi;
    this.apiServer        = apiServer;
    this.complete         = false;
    this.start();
  }

  /**
   * Signals that this thread should complete execution.
   */
  synchronized void complete() {
    if (this.complete) return;
    this.complete = true;
    this.notifyAll();
  }

  /**
   * Checks if this thread has received the completion signal.
   * @return <tt>true</tt> if the completion signal has been received, otherwise
   *         <tt>false</tt>.
   */
  synchronized boolean isComplete() {
    return this.complete;
  }

  /**
   * The run method implemented to periodially check if the active configuration
   * ID differs from the default configuration ID and if so, reinitializes.
   */
  public void run() {
    try {
      int errorCount = 0;
      // loop until completed
      while (!this.isComplete()) {
        // check if we have reached the maximum error count
        if (errorCount > MAX_ERROR_COUNT) {
          System.err.println(
              "Giving up on monitoring active configuration after "
                  + errorCount + " failures");
          return;
        }

        // sleep for the delay period
        try {
          Thread.sleep(SzApiServer.CONFIG_REFRESH_PERIOD);
        } catch (InterruptedException e) {
          errorCount++;
        }

        Boolean result = this.apiServer.ensureConfigCurrent(true);
        if (result == null) {
          errorCount++;
          continue;
        }

        // reset the error count if we successfully reach this point
        errorCount = 0;
      }
    } catch (Exception e) {
      System.err.println(
          "Giving up on monitoring active configuration due to exception:");
      e.printStackTrace();
    }
  }
}
