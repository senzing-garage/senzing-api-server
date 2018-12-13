package com.senzing.engine;

import com.senzing.g2.query.G2Query;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import static com.senzing.api.engine.EngineOperation.*;

public class QueryEngine implements Engine {
  private static QueryEngine current_instance = null;
  private static final ReentrantReadWriteLock MONITOR = new ReentrantReadWriteLock();
  private G2Query       g2Query         = null;
  private String        moduleName      = null;
  private String        iniFileName     = null;
  private boolean       verboseLogging  = false;
  private EngineLogger  logger          = null;
  private String        clusterName     = null;
  private String        hostName        = null;
  private int           portNumber      = 0;
  private String        indexName       = null;
  private boolean       initialized     = false;

  private Map<Long,ReentrantReadWriteLock> auditSessionMonitors
    = new HashMap<Long,ReentrantReadWriteLock>();

  private Map<Long,ReentrantReadWriteLock> auditReportMonitors
    = new HashMap<Long,ReentrantReadWriteLock>();

  private QueryEngine(G2Query       g2Query,
                      String        moduleName,
                      String        iniFileName,
                      boolean       verboseLogging,
                      String        clusterName,
                      String        hostName,
                      int           portNumber,
                      String        indexName,
                      EngineLogger  logger)
  {
    this.g2Query        = g2Query;
    this.moduleName     = moduleName;
    this.iniFileName    = iniFileName;
    this.verboseLogging = verboseLogging;
    this.clusterName    = clusterName;
    this.hostName       = hostName;
    this.portNumber     = portNumber;
    this.indexName      = indexName;
    this.logger         = logger;
    this.initialized    = true;
  }

  public EngineLogger getLogger() {
    return this.logger;
  }

  public static QueryEngine getEngine(String        moduleName,
                                      String        iniFileName,
                                      boolean       verboseLogging,
                                      String        clusterName,
                                      String        hostName,
                                      int           portNumber,
                                      String        indexName,
                                      EngineLogger  logger)
  {
    MONITOR.writeLock().lock();
    try {
      QueryEngine query = QueryEngine.current_instance;
      if (query != null) {
        if (moduleName.equals(query.getModuleName())
            && iniFileName.equals(query.getInitFileName())
            && (verboseLogging == query.isVerboseLogging())
            && (clusterName.equals(query.getClusterName()))
            && (hostName.equals(query.getHostName()))
            && (portNumber == query.getPortNumber())
            && (indexName.equals(query.getIndexName())))
        {
          return query;
        }
        throw new IllegalStateException(
          "Must destroy previous G2 engine query instance before obtaining "
          + "a new one.  previous=[ " + query + " ], moduleName=[ " + moduleName
          + " ], iniFileName=[ " + iniFileName + " ], verboseLogging=[ "
          + verboseLogging + " ], clusterName= " + clusterName
          + " ], hostName=[ " + hostName + " ], portNumber=[ " + portNumber
          + " ], indexName=[ " + indexName + " ]");
      }

      try {
        // obtain the class loader
        ClassLoader parent = Thread.currentThread().getContextClassLoader();
        if (parent == null) {
          parent = ClassLoader.getSystemClassLoader();
        }
        ClassLoader engineLoader = new EngineClassLoader(parent);

        // get the implementation class instance
        final String implClassName = "com.senzing.g2.query.G2QueryImp";
        Class implClass = engineLoader.loadClass(implClassName);
        final G2Query impl = (G2Query) implClass.newInstance();

        int result = impl.init(moduleName,
                               iniFileName,
                               verboseLogging,
                               clusterName,
                               hostName,
                               portNumber,
                               indexName);

        if (result != 0) {
          throw new EngineException("Failed to initialize engine audit: "
                                    + implClassName + " / " + impl);
        }

        query = new QueryEngine(impl,
                                moduleName,
                                iniFileName,
                                verboseLogging,
                                clusterName,
                                hostName,
                                portNumber,
                                indexName,
                                logger);

        QueryEngine.current_instance = query;
        return query;

      } catch (RuntimeException e) {
        throw e;

      } catch (Exception e) {
        throw new EngineException(e);
      }
    } finally {
      MONITOR.writeLock().unlock();
    }
  }

  public EngineType getEngineType() {
    return EngineType.QUERIST;
  }

  public boolean isOperationSupported(EngineOperation op) {
    return NO_OP == op || op.getEngineType() == this.getEngineType();
  }

  public EngineResponse processRequest(EngineRequest request) {
    EngineOperation op = request.getOperation();
    if (! this.isOperationSupported(op)) {
      throw new UnsupportedOperationException(
        "Unsupported Engine Operation for " + this.getEngineType() + ": " + op);
    }
    Object result = null;
    EngineResponse response = null;
    try {
      switch (op) {
        case NO_OP:
          return new EngineResponse(request);
        case QUERY_ENTITIES:
          request.markEngineStart();
          try {
            result = this.queryEntities(request.getMessage());
          } finally {
            request.markEngineEnd();
          }
          break;

        case QUERY_DESTROY:
          request.markEngineStart();
          try {
            this.destroy();
          } finally {
            request.markEngineEnd();
          }
          break;

        default:
          throw new IllegalStateException(
            "Failed to find handler for 'supported' operation.  engineType=[ "
            + this.getEngineType() + " ], operation=[ " + op + " ]");
      }

      response = new EngineResponse(request, result);

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

    return response;
  }

  public String getModuleName() {
    return this.moduleName;
  }

  public String getInitFileName() {
    return this.iniFileName;
  }

  public boolean isVerboseLogging() {
    return this.verboseLogging;
  }

  public String getClusterName() {
    return this.clusterName;
  }

  public String getHostName() {
    return this.hostName;
  }

  public int getPortNumber() {
    return this.portNumber;
  }

  public String getIndexName() {
    return this.indexName;
  }

  public String toString() {
    return ("G2QUERY=[ moduleName=[ " + this.getModuleName()
            + " ], iniFileName=[ " + this.getInitFileName()
            + " ], verboseLogging=[ " + this.isVerboseLogging()
            + " ], clusterName=[ " + this.getClusterName()
            + " ], hostName=[ " + this.getHostName()
            + " ], portNumber=[ " + this.getPortNumber()
            + " ], indexName=[ " + this.getIndexName()
            + " ] ]");
  }

  public boolean isDestroyed() {
    MONITOR.readLock().lock();
    try {
      return (this.g2Query == null);
    } finally {
      MONITOR.readLock().unlock();
    }
  }

  public void destroy() throws EngineException {
    MONITOR.writeLock().lock();
    try {
      this.assertNotDestroyed();
      if (this.initialized) {
        int result = this.g2Query.destroy();
        if (result != 0) {
          throw new EngineException("Failed to destroy engine audit.");
        }
      }
      this.g2Query = null;
      this.initialized = false;
      System.gc();
    } finally {
      MONITOR.writeLock().unlock();
    }
  }

  public void uninitialize() throws EngineException {
    MONITOR.writeLock().lock();
    try {
      this.assertNotDestroyed();

      // check if already uninitialized
      if (!this.initialized) return;

      int result = this.g2Query.destroy();
      if (result != 0) {
        throw new EngineException("Failed to uninitialize query engine.");
      }
      this.initialized = false;
      System.gc();
    } finally {
      MONITOR.writeLock().unlock();
    }
  }

  public void reinitialize() throws EngineException {
    MONITOR.writeLock().lock();
    try {
      this.assertNotDestroyed();
      this.assertNotInitialized();

      int result = this.g2Query.init(this.moduleName,
                                     this.iniFileName,
                                     this.verboseLogging,
                                     this.clusterName,
                                     this.hostName,
                                     this.portNumber,
                                     this.indexName);

      if (result != 0) {
        throw new EngineException("Failed to reinitialize query engine.");
      }
      this.initialized = true;

    } finally {
      MONITOR.writeLock().unlock();
    }
  }


  private void assertNotDestroyed() {
    if (this.g2Query == null) {
      throw new IllegalStateException("G2 Query Engine already destroyed.");
    }
  }

  private void assertInitialized() {
    this.assertNotDestroyed();
    if (!this.initialized) {
      throw new IllegalStateException("Query engine not initialized.");
    }
  }

  private void assertNotInitialized() {
    if (this.initialized) {
      throw new IllegalStateException("Query engine is already initialized.");
    }
  }

  public String queryEntities(String query) throws EngineException {
      MONITOR.readLock().lock();
      try {
        this.assertInitialized();
        StringBuffer sb = new StringBuffer();
        int returnCode = this.g2Query.queryEntities(query, sb);

        if (returnCode != 0) {
          throw new EngineException(
            QUERY_ENTITIES, returnCode, "Failed to query entities.");
        }

        return sb.toString();

      } finally {
        MONITOR.readLock().unlock();
      }
  }

}
