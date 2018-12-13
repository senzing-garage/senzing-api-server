package com.senzing.api.engine;
import java.io.*;
import java.util.*;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.net.InetAddress;
import java.time.LocalDateTime;

import com.senzing.g2.engine.G2Product;
import com.senzing.g2.engine.G2ProductJNI;
import com.senzing.api.util.WorkbenchLogging;

import static com.senzing.api.engine.EngineOperation.*;

public class EngineMain {
  private static final int MAX_THREAD_COUNT = 8;
  private static PrintStream LOG_WRITER = null;
  private static final DateTimeFormatter DATE_TIME_FORMATTER =DateTimeFormatter.ofPattern("yyyyMMdd-kkmmss");

  protected static void log() {
    log(LocalDateTime.now().toString() + "ENGINE: ");
  }

  protected static void log(String msg) {
    log(LocalDateTime.now().toString() + " ENGINE: " + msg, true);
  }

  protected static synchronized void log(String msg, boolean newline) {
    try {
      if (newline) {
        if (LOG_WRITER != null) {
          LOG_WRITER.println(msg);
          LOG_WRITER.flush();
        } else {
          System.err.println(msg);
        }
      } else {
        if (LOG_WRITER != null) {
          LOG_WRITER.print(msg);
          LOG_WRITER.flush();
        } else {
          System.err.print(msg);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  protected static synchronized void log(Throwable t) {
    try {
      String timestamp = LocalDateTime.now().toString();
      if (LOG_WRITER != null) {
        LOG_WRITER.println(timestamp + " ENGINE: " + t.toString());
        t.printStackTrace(LOG_WRITER);
        LOG_WRITER.flush();
      } else {
        System.err.println(timestamp + " ENGINE: " + t.toString());
        t.printStackTrace(System.err);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) {
    PrintWriter opLogWriter = null;
    int     argIndex             = 0;

    Integer portNumber;
    Long authId;
    String moduleName;
    String iniFileName;
    boolean verboseLogging;
    File userDataDir;
    try {
      authId = Long.parseLong(args[argIndex++]);
      moduleName = args[argIndex++];
      iniFileName = args[argIndex++];
      verboseLogging = Boolean.valueOf(args[argIndex++]);
      portNumber = Integer.parseInt(args[argIndex++]);
      userDataDir = new File(args[argIndex++]);
    }
    catch(Exception e) {
      log(e);
      System.out.println("Invalid Command line arguments: " + e.getMessage());
      return;
    }

    InetAddress ipAddr = InetAddress.getLoopbackAddress();
    log("CREATING SOCKET CONNECTOR FOR: " + ipAddr + ":" + portNumber);
    SocketConnector connector = new SocketConnector(ipAddr, portNumber, authId);

    // connect to the parent process
    boolean hasParentConnection = true;
    try {
      connector.connect();
    } catch (Exception e) {
      log("FAILED INITIAL CONNECTION TO PARENT PROCESS");
      log(e);
      hasParentConnection = false;
    }

    boolean startupSuccessful = false;
    try {
      File projectDir = new File(userDataDir, moduleName);
      File logFile    = new File(projectDir, "engine.log");

      WorkbenchLogging.initialize(userDataDir);
      LOG_WRITER = new PrintStream(new FileOutputStream(logFile), true, "UTF-8");
      EngineLogger logger = createLogger();

      List<Engine> enginesToDestroy = new ArrayList<>(args.length - argIndex);
      List<EngineRequest> destroyRequests = new ArrayList<>(args.length - 5);

      List<Engine> engines = getEngines(args, argIndex, moduleName, iniFileName, verboseLogging, logger);

      logInstallInfo(moduleName, iniFileName, verboseLogging);
      EngineRequestQueue requestQueue  = new EngineRequestQueue();
      EngineResponseQueue responseQueue = new EngineResponseQueue();
      Responder responder = new Responder(connector, responseQueue);

      int threadCount = getThreadCount();
      List<EngineThread> threadList = startEngineThreads(requestQueue, responseQueue, engines, threadCount);

      Set<EngineOperation> logOpSet = new HashSet<>();
      String logOps = System.getProperty("LOG_OPERATIONS");
      if (logOps != null && logOps.trim().length() > 0) {
        logOps = logOps.trim();
        String[] opNames = logOps.split(",");
        for (String opName : opNames) {
          try {
            logOpSet.add(EngineOperation.valueOf(opName.trim()));
          } catch (Exception e) {
            log("UNRECOGNIZED OPERATION NAME FOR LOGGING: " + opName);
          }
        }

        try {
          ZonedDateTime now       = ZonedDateTime.now();
          String        suffix    = "-" + DATE_TIME_FORMATTER.format(now) + ".json";
          File          opLogFile = new File(moduleName + suffix);
          opLogWriter = new PrintWriter(new OutputStreamWriter(new FileOutputStream(opLogFile), "UTF-8"));
        } catch (IOException e) {
          log("***** FAILED TO OPEN LOG FILE FOR OPERATIONS....");
          log(e);
        }
      }

      int requestCount = 0;
      int redoCount = 0;
      int otherCount = 0;

      startupSuccessful = true;
      while (engines.size() > 0 && !responder.isComplete()) {
        EngineRequest request = getNextRequest(opLogWriter, connector, hasParentConnection, logOpSet);
        boolean reinitializing = false;
        boolean purging = false;
        synchronized (requestQueue) {
          if (request == null) {
            responder.complete();
            requestQueue.clear();

            printBanner("PARENT PROCESS UNREACHABLE");

            requestQueue.enqueue(null);
          }
          else if (request.getOperation() == REINITIALIZE || request.getOperation() == PURGE_AND_REINITIALIZE) {

            printBanner("RECEIVED REINITIALIZE REQUEST");
            log(" ATTEMPTING TO CLEAR REQUEST QUEUE FIRST....");
            log(" REQUEST QUEUE SIZE: " + requestQueue.getCount());

            // notify the threads to stop -- we will create engine threads
            requestQueue.enqueue(null);
            reinitializing = true;
            if (request.getOperation() == PURGE_AND_REINITIALIZE) {
              purging = true;
            }

          } else if (request.getOperation().isDestroyOperation()) {
            printBanner("RECEIVED DESTROY REQUEST: " + request.getOperation());
            markForDestruction(enginesToDestroy, destroyRequests, engines, request);

            // if all engines are now marked for destruction then notify
            // threads to shutdown
            if (engines.size() == 0) {
              log("NOTIFYING ALL THREADS TO SHUTDOWN...");
              requestQueue.enqueue(null);
            } else {
              log("DEFERRING THREAD SHUTDOWN.  ENGINES REMAIN: " + engines);
            }

          } else {
            Integer affinity = request.getThreadAffinity();
            if (affinity != null) {
              request.setThreadAffinity(Math.abs(affinity % threadCount));
            }
            if (redoCount+requestCount > 0 && requestQueue.getCount() == 0) {
              log("WARNING: Refilling empty request queue");
            }
            EngineOperation op = request.getOperation();
            if (request.isRedo()) {
              redoCount++;
            } else if (op == PROCESS || op == ADD_RECORD) {
              requestCount++;
            } else {
              otherCount++;
            }
            request.recordBacklogSize(requestQueue.getCount());
            requestQueue.enqueue(request);

            if ((requestCount+redoCount) % 2000 == 0) {
              log("RECIEVED SO FAR (STANDARD/REDO/OTHER): "
                  + requestCount + " / " + redoCount + " / " + otherCount);
              log("BACKLOG SIZE: " + requestQueue.getCount());
            }
            if ((requestCount+redoCount) % 20000 == 0) {
              System.gc();
            }
          }
          requestQueue.notifyAll();
        }

        // handle reinitializing
        if (reinitializing) {
          for (EngineThread thread : threadList) {
            thread.join();
          }
          log(" ALL ENGINE THREADS JOINED");
          threadList.clear();
          requestQueue = new EngineRequestQueue();

          log(" REQUEST QUEUE CLEARED.");
          List<Engine> reverseList = new ArrayList<>(engines);
          Collections.reverse(reverseList);

          // uninitialize
          log(" UNINITIALIZING ENGINES...");
          for (Engine engine : reverseList) {
            if (engine.getEngineType() == EngineType.RESOLVER && purging) {
              log(" - PURGING REPOSITORY VIA " + engine.getEngineType() + " ENGINE...");
              ((ResolutionEngine) engine).purgeRepository();
              log(" - PURGED REPOSITORY VIA " + engine.getEngineType() + " ENGINE.");
            }
            log(" - UNINITIALIZING " + engine.getEngineType() + " ENGINE...");
            engine.uninitialize();
            log(" - UNINITIALIZED " + engine.getEngineType() + " ENGINE.");
          }
          log(" UNINITIALIZED ENGINES.");

          // reinitialize
          log(" REINITIALIZING ENGINES...");
          for (Engine engine : engines) {
            log(" - REINITIALIZING " + engine.getEngineType() + " ENGINE...");
            engine.reinitialize();
            log(" - REINITIALIZED " + engine.getEngineType() + " ENGINE.");
          }
          log(" REINITIALIZED ENGINES.");

          for (int index = 0; index < threadCount; index++) {
            EngineThread engineThread = new EngineThread(requestQueue, responseQueue, index, engines);
            threadList.add(engineThread);
            engineThread.start();
          }

          EngineResponse response = new EngineResponse(request);
          synchronized (responseQueue) {
            response.markEnqueueTime();
            responseQueue.enqueue(response);
            responseQueue.notifyAll();
          }
        }
      }

      printBanner("JOINING THREADS");
      for (EngineThread engineThread : threadList) {
        engineThread.join();
      }

      printBanner("DESTROYING ENGINE(S)");
      Collections.reverse(engines);
      enginesToDestroy.addAll(engines);
      int destroyCount = enginesToDestroy.size();
      System.out.println(destroyRequests);
      for (int index = 0; index < destroyCount; index++) {
        Engine engine = enginesToDestroy.get(index);
        EngineRequest destroyRequest = (destroyRequests.size() > index)
                                     ? destroyRequests.get(index)
                                     : null;
        EngineResponse destroyResponse = destroy(engine, destroyRequest);
        if (destroyResponse != null) {
          log("******** ENQUEUEING DESTROY RESPONSE: " + engine.getEngineType());
          synchronized (responseQueue) {
            destroyResponse.markEnqueueTime();
            responseQueue.enqueue(destroyResponse);
            responseQueue.notifyAll();
          }
          log("******** ENQUEUED DESTROY RESPONSE: " + engine.getEngineType());
        }
      }
      printBanner("DESTROYED ENGINE(S)");

      Thread.sleep(3000L);
      responder.complete();
      responder.join();
      printBanner("EXITING: " + moduleName);
      System.exit(0);
    } catch (Throwable e) {
      log(e);
      if (hasParentConnection && !startupSuccessful) {
        connector.writeStartupError(e);
      }
    } finally {
      if (opLogWriter != null) {
        opLogWriter.close();
      }
    }
  }

  private static void markForDestruction(List<Engine> enginesToDestroy, List<EngineRequest> destroyRequests, List<Engine> engines, EngineRequest request) {
    EngineOperation op = request.getOperation();
    Iterator<Engine> iter = engines.iterator();
    while (iter.hasNext()) {
      Engine e = iter.next();
      if (e.isOperationSupported(op)) {
        enginesToDestroy.add(e);
        destroyRequests.add(request);
        iter.remove();
        break;
      }
    }
  }

  private static void printBanner(final String s) {
    int repeat = s.length();
    char[] buf = new char[repeat];
    for (int i = repeat - 1; i >= 0; i--) {
      buf[i] = '*';
    }
    String variableStarStrip = new String(buf);

    log();
    log(" *********" + variableStarStrip + "*********");
    log(" ******** " + s + " ********");
    log(" *********" + variableStarStrip + "*********");
    log();
  }

  private static List<EngineThread> startEngineThreads(EngineRequestQueue requestQueue, EngineResponseQueue responseQueue, List<Engine> engines, int threadCount) {
    List<EngineThread> threadList = new ArrayList<>(threadCount);
    for (int index = 0; index < threadCount; index++) {
      EngineThread engineThread = new EngineThread(requestQueue, responseQueue, index, engines);
      threadList.add(engineThread);
      engineThread.start();
    }
    return threadList;
  }

  private static EngineRequest getNextRequest(PrintWriter opLogWriter, SocketConnector connector, boolean hasParentConnection, Set<EngineOperation> logOpSet) {
    if (hasParentConnection) {
      try {
        EngineRequest request = connector.readRequest();
        if (logOpSet.contains(request.getOperation())) {
          opLogWriter.println(request.getMessage());
          opLogWriter.flush();
        }
        if (!request.isRedo() && request.getOperation() != ADD_RECORD) {
          log("***** READ REQUEST: " + request.getOperation());
        }
        return request;
      } catch (Exception e) {
        log(e);
      }
    }
    // if this fails then it means we tried to read the request multiple times and
    // failed to do so -- this also means we tried to reconnect to the parent process
    // and failed to do so -- time to shutdown
    log("SHUTTING DOWN AFTER FAILING TO REESTABLISH CONNECTION TO PARENT PROCESS");
    return null;
  }

  private static int getThreadCount() {
    Runtime runtime = Runtime.getRuntime();
    int coreCount = runtime.availableProcessors();

    int threadCount = Math.min((coreCount * 2) - 1, MAX_THREAD_COUNT);

    String concurrencyProp = System.getProperty("G2_ENGINE_CONCURRENCY");
    if (concurrencyProp != null && concurrencyProp.length() > 0) {
      int systemConcurrencyCount = Integer.parseInt(concurrencyProp);
      if (systemConcurrencyCount > 0) {
        threadCount = systemConcurrencyCount;
      }
      else {
        log("Invalid G2_ENGINE_CONCURRENCY System Property: " + systemConcurrencyCount);
      }
    }
    log("STARTING " + threadCount + " ENGINE THREADS ON " + coreCount + " LOGICAL CORES");
    return threadCount;
  }

  private static void logInstallInfo(String moduleName, String iniFileName, boolean verboseLogging) {
    G2Product productAPI = new G2ProductJNI();
    int initResult = productAPI.init(moduleName, iniFileName, verboseLogging);
    if (initResult == 0) {
      log();
      log("****** VERSION INFO:\r\n\r\n" + productAPI.version());
      log("****** LICENSE INFO:\r\n\r\n" + productAPI.license()
          + "\r\n\r\n");
      log("******");
    }
  }

  private static List<Engine> getEngines(String[] args, int argIndex, String moduleName, String iniFileName, boolean verboseLogging, EngineLogger logger) {
    List<Engine> engines = new ArrayList<>(args.length - argIndex);
    while (argIndex < args.length) {
      EngineType engineType = EngineType.valueOf(args[argIndex++]);
      log("OBTAINING AND INITIALIZING " + engineType + " ENGINE " + moduleName + " WITH " + iniFileName);
      switch (engineType) {
        case RESOLVER:
          engines.add(ResolutionEngine.getEngine(moduleName,
                                                 iniFileName,
                                                 verboseLogging,
                                                 logger));
          break;
        case AUDITOR:
          engines.add(AuditEngine.getEngine(moduleName,
                                            iniFileName,
                                            verboseLogging,
                                            logger));

          break;
        case QUERIST:
          {
            String  elasticSearchCluster = args[argIndex++];
            String  elasticSearchHost    = args[argIndex++];
            int     elasticSearchPort    = Integer.parseInt(args[argIndex++]);
            String  elasticSearchIndex   = args[argIndex++];

            engines.add(QueryEngine.getEngine(moduleName,
                                              iniFileName,
                                              verboseLogging,
                                              elasticSearchCluster,
                                              elasticSearchHost,
                                              elasticSearchPort,
                                              elasticSearchIndex,
                                              logger));
          }
          break;
        default:
          log("UNHANDLED ENGINE TYPE: " + engineType);
      }
    }
    return engines;
  }

  private static EngineLogger createLogger() {
    return new EngineLogger() {
          public void log() {
            EngineMain.log();
          }
          public void log(String s) {
            EngineMain.log(s);
          }
          public void log(String s, boolean n) {
            EngineMain.log(s, n);
          }
          public void log(Throwable t) {
            EngineMain.log(t);
          }
        };
  }

  private static EngineResponse destroy(Engine engine, EngineRequest destroyRequest) {
    EngineResponse destroyResponse;
    try {
      log("******** DESTROYING " + engine.getEngineType());
      engine.destroy();
      log("******** DESTROYED " + engine.getEngineType());
      destroyResponse = (destroyRequest != null)
                      ? new EngineResponse(destroyRequest)
                      : null;

    } catch (EngineException e) {
      destroyResponse = (destroyRequest != null)
                        ? new EngineResponse(destroyRequest, e)
                        : null;

    } catch (Exception e) {
      EngineException ee = new EngineException(
        "Failed to destroy engine: " + engine.getEngineType(), e);
      destroyResponse = (destroyRequest != null)
                        ? new EngineResponse(destroyRequest, ee)
                        : null;
    }
    return destroyResponse;
  }
}
