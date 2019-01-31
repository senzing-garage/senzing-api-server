package com.senzing.api.server;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.util.component.LifeCycle;

import java.net.InetAddress;

class LifeCycleListener implements LifeCycle.Listener {
  private int httpPort;
  private InetAddress ipAddr;
  private Server jettyServer;
  private FileMonitor fileMonitor;

  public LifeCycleListener(Server       jettyServer,
                           int          httpPort,
                           InetAddress  ipAddress,
                           FileMonitor  fileMonitor)
  {
    this.httpPort = httpPort;
    this.ipAddr = ipAddress;
    this.jettyServer = jettyServer;
    this.fileMonitor = fileMonitor;
  }

  public void lifeCycleStarting(LifeCycle event) {
    System.out.println();
    if (this.httpPort != 0) {
      System.out.println("Starting Senzing REST API Server on port "
                             + this.httpPort + "....");
    } else {
      System.out.println(
          "Starting Senzing REST API Server with rotating port....");
    }
    System.out.println();
  }

  public void lifeCycleStarted(LifeCycle event) {
    int port = this.httpPort;
    if (port == 0) {
      port = ((ServerConnector)(jettyServer.getConnectors()[0])).getLocalPort();
    }
    System.out.println("Started Senzing REST API Server on port " + port + ".");
    System.out.println();
    System.out.println("Server running at:");
    System.out.println("http://" + this.ipAddr.getHostAddress() + ":" + port + "/");
    System.out.println();
    if (this.fileMonitor != null) {
      this.fileMonitor.signalReady();
    }
  }

  public void lifeCycleFailure(LifeCycle event, Throwable cause) {
    if (this.httpPort != 0) {
      System.err.println("Failed to start Senzing REST API Server on port "
                             + this.httpPort + ".");
    } else {
      System.err.println(
          "Failed to start Senzing REST API Server with rotating port.");
    }
    System.err.println();
    //System.err.println(cause);
  }

  public void lifeCycleStopping(LifeCycle event) {
    System.out.println("Stopping Senzing REST API Server....");
    System.out.println();
  }

  public void lifeCycleStopped(LifeCycle event) {
    System.out.println("Stopped Senzing REST API Server.");
    System.out.println();
  }
}
