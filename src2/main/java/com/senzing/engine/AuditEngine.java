package com.senzing.engine;

import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.ArrayList;
import com.senzing.g2.engine.G2Audit;

import static com.senzing.engine.EngineOperation.*;
import static com.senzing.engine.EngineLogging.*;

import java.util.concurrent.locks.ReentrantReadWriteLock;

public class AuditEngine implements Engine {
  private static AuditEngine current_instance = null;
  private static final ReentrantReadWriteLock MONITOR = new ReentrantReadWriteLock();
  private G2Audit       g2Audit         = null;
  private String        moduleName      = null;
  private String        iniFileName     = null;
  private boolean       verboseLogging  = false;
  private EngineLogger  logger          = null;
  private boolean       initialized     = false;

  private Map<Long,ReentrantReadWriteLock> auditSessionMonitors
    = new HashMap<>();

  private Map<Long,ReentrantReadWriteLock> auditReportMonitors
    = new HashMap<>();

  private AuditEngine(G2Audit       g2Audit,
                      String        moduleName,
                      String        iniFileName,
                      boolean       verboseLogging,
                      EngineLogger  logger) {
    this.g2Audit        = g2Audit;
    this.moduleName     = moduleName;
    this.iniFileName    = iniFileName;
    this.verboseLogging = verboseLogging;
    this.logger         = logger;
    this.initialized    = true;
  }

  public EngineLogger getLogger() {
    return this.logger;
  }

  public static AuditEngine getEngine(String        moduleName,
                                      String        iniFileName,
                                      boolean       verboseLogging,
                                      EngineLogger  logger)
  {
    MONITOR.writeLock().lock();
    try {
      AuditEngine audit = AuditEngine.current_instance;
      if (audit != null) {
        if (moduleName.equals(audit.getModuleName())
            && iniFileName.equals(audit.getInitFileName())
            && (verboseLogging == audit.isVerboseLogging())) {
          return audit;
        }
        throw new IllegalStateException(
          "Must destroy previous G2 engine audit instance before obtaining "
          + "a new one.  previous=[ " + audit + " ], moduleName=[ " + moduleName
          + " ], iniFileName=[ " + iniFileName + " ], verboseLogging=[ "
          + verboseLogging + " ]");
      }

      try {
        // obtain the class loader
        ClassLoader parent = Thread.currentThread().getContextClassLoader();
        if (parent == null) {
          parent = ClassLoader.getSystemClassLoader();
        }
        ClassLoader engineLoader = new EngineClassLoader(parent);

        // get the implementation class instance
        final String implClassName = "com.senzing.g2.engine.G2AuditJNI";
        Class implClass = engineLoader.loadClass(implClassName);
        final G2Audit impl = (G2Audit) implClass.newInstance();

        int result = impl.init(moduleName, iniFileName, verboseLogging);
        if (result != 0) {
          throw new EngineException("Failed to initialize engine audit: "
                                    + implClassName + " / " + impl);
        }
        audit = new AuditEngine(impl,
                                moduleName,
                                iniFileName,
                                verboseLogging,
                                logger);

        AuditEngine.current_instance = audit;
        return audit;

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
    return EngineType.AUDITOR;
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
      this.g2Audit.clearLastException();
      switch (op) {
        case NO_OP:
          return new EngineResponse(request);
        case AUDIT_OPEN_SESSION:
          request.markEngineStart();
          try {
            result = this.openSession();
          } finally {
            request.markEngineEnd();
          }
          break;

        case AUDIT_CANCEL_SESSION:
          request.markEngineStart();
          try {
            long sessionId = (Long) request.getParameter(SESSION_ID);
            this.cancelSession(sessionId);
          } finally {
            request.markEngineEnd();
          }
          break;

        case AUDIT_CLOSE_SESSION:
          request.markEngineStart();
          try {
            long sessionId = (Long) request.getParameter(SESSION_ID);
            this.closeSession(sessionId);
          } finally {
            request.markEngineEnd();
          }
          break;

        case AUDIT_SUMMARY_DATA:
          request.markEngineStart();
          try {
            long sessionId = (Long) request.getParameter(SESSION_ID);
            result = this.getSummaryData(sessionId);
          } finally {
            request.markEngineEnd();
          }
          break;

        case AUDIT_USED_MATCH_KEYS:
          request.markEngineStart();
          try {
            long sessionId = (Long) request.getParameter(SESSION_ID);
            String fromDataSource = (String) request.getParameter(FROM_DATA_SOURCE);
            String toDataSource = (String) request.getParameter(TO_DATA_SOURCE);
            int matchLevel = (Integer) request.getParameter(MATCH_LEVEL);

            result = this.getUsedMatchKeys(sessionId,
                                           fromDataSource,
                                           toDataSource,
                                           matchLevel);
          } finally {
            request.markEngineEnd();
          }
          break;

        case AUDIT_USED_PRINCIPLES:
          request.markEngineStart();
          try {
            long sessionId = (Long) request.getParameter(SESSION_ID);
            String fromDataSource = (String) request.getParameter(FROM_DATA_SOURCE);
            String toDataSource = (String) request.getParameter(TO_DATA_SOURCE);
            int matchLevel = (Integer) request.getParameter(MATCH_LEVEL);

            result = this.getUsedPrinciples(sessionId,
                                            fromDataSource,
                                            toDataSource,
                                            matchLevel);
          } finally {
            request.markEngineEnd();
          }
          break;

        case AUDIT_OPEN_REPORT:
          request.markEngineStart();
          try {
            long sessionId = (Long) request.getParameter(SESSION_ID);
            String fromDataSource = (String) request.getParameter(FROM_DATA_SOURCE);
            String toDataSource = (String) request.getParameter(TO_DATA_SOURCE);
            int matchLevel = (Integer) request.getParameter(MATCH_LEVEL);

            result = this.openReport(sessionId,
                                     fromDataSource,
                                     toDataSource,
                                     matchLevel);

        } finally {
          request.markEngineEnd();
        }
        break;

        case AUDIT_FETCH_NEXT:
          request.markEngineStart();
          try {
            long sessionId = (Long) request.getParameter(SESSION_ID);
            long reportId = (Long) request.getParameter(REPORT_ID);
            int fetchCount = 1;
            Integer paramVal = (Integer) request.getParameter(FETCH_COUNT);
            if (paramVal != null) fetchCount = paramVal.intValue();
            result = this.fetchNext(sessionId, reportId, fetchCount);

        } finally {
          request.markEngineEnd();
        }
        break;

        case AUDIT_CLOSE_REPORT:
          request.markEngineStart();
          try {
            long sessionId = (Long) request.getParameter(SESSION_ID);
            long reportId = (Long) request.getParameter(REPORT_ID);

            this.closeReport(sessionId, reportId);

        } finally {
          request.markEngineEnd();
        }
        break;

        case AUDIT_DESTROY:
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

  public String toString() {
    return ("G2AUDIT=[ moduleName=[ " + this.getModuleName()
            + "], iniFileName=[ " + this.getInitFileName()
            + " ], verboseLogging=[ " + this.isVerboseLogging() + " ] ]");
  }

  public boolean isDestroyed() {
    MONITOR.readLock().lock();
    try {
      return (this.g2Audit == null);
    } finally {
      MONITOR.readLock().unlock();
    }
  }

  public void destroy() throws EngineException {
    MONITOR.writeLock().lock();
    try {
      this.assertNotDestroyed();
      if (this.initialized) {
        int result = this.g2Audit.destroy();
        if (result != 0) {
          throw new EngineException(
              "Failed to destroy engine audit.", this.g2Audit);
        }
      }
      this.g2Audit = null;
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
      if (!this.initialized) return;
      int result = this.g2Audit.destroy();
      if (result != 0) {
        throw new EngineException(
            "Failed to uninitialize/destroy engine audit.", this.g2Audit);
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

      int result = this.g2Audit.init(this.moduleName,
                                     this.iniFileName,
                                     this.verboseLogging);

      if (result != 0) {
        throw new EngineException(
            "Failed to uninitialize/destroy engine audit.", this.g2Audit);
      }
      this.initialized = true;
    } finally {
      MONITOR.writeLock().unlock();
    }
  }

  private void assertNotDestroyed() {
    if (this.g2Audit == null) {
      throw new IllegalStateException("G2 Audit Engine already destroyed.");
    }
  }

  private void assertInitialized() {
    this.assertNotDestroyed();
    if (!this.initialized) {
      throw new IllegalStateException("Audit engine not initialized.");
    }
  }

  private void assertNotInitialized() {
    if (this.initialized) {
      throw new IllegalStateException("Audit engine is already initialized.");
    }
  }

  public long openSession() throws EngineException {
      MONITOR.readLock().lock();
      try {
        this.assertInitialized();
        long sessionId = this.g2Audit.openSession();
        if (sessionId <= 0L) {
          throw new EngineException(
            AUDIT_OPEN_SESSION, (int)sessionId,
            "Failed to open audit session.", this.g2Audit);
        }
        this.createAuditSessionMonitor(sessionId);
        return sessionId;
      } finally {
        MONITOR.readLock().unlock();
      }
  }

  /**
   * Cancel the audit session.
   */
  public void cancelSession(long sessionId) {
    MONITOR.readLock().lock();
    try {
      this.assertInitialized();
      ReentrantReadWriteLock sessionMonitor = this.getAuditSessionMonitor(sessionId);
      sessionMonitor.readLock().lock();
      try {
        this.g2Audit.cancelSession(sessionId);

      } finally {
        sessionMonitor.readLock().unlock();
      }
    } finally {
      MONITOR.readLock().unlock();
    }
  }

  /**
   * Close the audit session.
   */
  public void closeSession(long sessionId) {
    MONITOR.readLock().lock();
    try {
      this.assertInitialized();
      ReentrantReadWriteLock sessionMonitor = this.getAuditSessionMonitor(sessionId);
      sessionMonitor.writeLock().lock();
      try {
        this.g2Audit.closeSession(sessionId);
        this.removeAuditSessionMonitor(sessionId);

      } finally {
        sessionMonitor.writeLock().unlock();
      }
    } finally {
      MONITOR.readLock().unlock();
    }
  }

  public String getSummaryData(long sessionId) {
    MONITOR.readLock().lock();
    try {
      this.assertInitialized();
      ReentrantReadWriteLock sessionMonitor = this.getAuditSessionMonitor(sessionId);
      sessionMonitor.readLock().lock();
      try {
        StringBuffer sb = new StringBuffer();
        int rc = this.g2Audit.getSummaryData(sessionId, sb);

        if (rc != 0) {
          throw new EngineException(
            AUDIT_SUMMARY_DATA, rc,
            "Failed to get summary data.", this.g2Audit);
        }

        return sb.toString();

      } finally {
        sessionMonitor.readLock().unlock();
      }
    } finally {
      MONITOR.readLock().unlock();
    }
  }

  public String getUsedMatchKeys(long   sessionId,
                                 String fromDataSource,
                                 String toDataSource,
                                 int    matchLevel)
  {
    MONITOR.readLock().lock();
    try {
      this.assertInitialized();
      ReentrantReadWriteLock sessionMonitor = this.getAuditSessionMonitor(sessionId);
      sessionMonitor.readLock().lock();
      try {
        StringBuffer sb = new StringBuffer();
        int rc = this.g2Audit.getUsedMatchKeys(
          sessionId, fromDataSource, toDataSource, matchLevel, sb);

        if (rc != 0) {
          throw new EngineException(
            AUDIT_USED_MATCH_KEYS, rc,
            "Failed to get used match keys.", this.g2Audit);
        }

        return sb.toString();

      } finally {
        sessionMonitor.readLock().unlock();
      }
    } finally {
      MONITOR.readLock().unlock();
    }
  }

  public String getUsedPrinciples(long   sessionId,
                                  String fromDataSource,
                                  String toDataSource,
                                  int    matchLevel)
  {
    MONITOR.readLock().lock();
    try {
      this.assertInitialized();
      ReentrantReadWriteLock sessionMonitor = this.getAuditSessionMonitor(sessionId);
      sessionMonitor.readLock().lock();
      try {

        StringBuffer sb = new StringBuffer();
        int rc = this.g2Audit.getUsedPrinciples(
          sessionId, fromDataSource, toDataSource, matchLevel, sb);

        if (rc != 0) {
          throw new EngineException(
            AUDIT_USED_PRINCIPLES, rc,
            "Failed to get used principles.", this.g2Audit);
        }

        return sb.toString();

      } finally {
        sessionMonitor.readLock().unlock();
      }
    } finally {
      MONITOR.readLock().unlock();
    }
  }

  public long openReport(long   sessionId,
                         String fromDataSource,
                         String toDataSource,
                         int    matchLevel)
  {
    MONITOR.readLock().lock();
    try {
      this.assertInitialized();
      ReentrantReadWriteLock sessionMonitor
        = this.getAuditSessionMonitor(sessionId);
      sessionMonitor.readLock().lock();
      try {
        long reportId = this.g2Audit.getAuditReport(
          sessionId, fromDataSource, toDataSource, matchLevel);

        if (reportId <= 0L) {
          throw new EngineException(
            AUDIT_OPEN_REPORT, (int)reportId,
            "Failed to generate audit report.", this.g2Audit);
        }

        this.createAuditReportMonitor(reportId);
        return reportId;

      } finally {
        sessionMonitor.readLock().unlock();
      }
    } finally {
      MONITOR.readLock().unlock();
    }
  }

  /**
   * Close the audit report.
   */
  public void closeReport(long sessionId, long reportId) {
    MONITOR.readLock().lock();
    try {
      this.assertInitialized();
      ReentrantReadWriteLock sessionMonitor = this.getAuditSessionMonitor(sessionId);
      sessionMonitor.readLock().lock();
      try {
        ReentrantReadWriteLock reportMonitor = this.getAuditReportMonitor(reportId);
        reportMonitor.writeLock().lock();
        try {
          this.g2Audit.closeReport(reportId);
          this.removeAuditReportMonitor(reportId);

        } finally {
          reportMonitor.writeLock().unlock();
        }
      } finally {
        sessionMonitor.readLock().unlock();
      }
    } finally {
      MONITOR.readLock().unlock();
    }
  }

  /**
   * Close the audit session.
   */
  public List<String> fetchNext(long sessionId, long reportId, int fetchCount)
  {
    MONITOR.readLock().lock();
    try {
      this.assertInitialized();
      ReentrantReadWriteLock sessionMonitor = this.getAuditSessionMonitor(sessionId);
      sessionMonitor.readLock().lock();
      try {
        ReentrantReadWriteLock reportMonitor = this.getAuditReportMonitor(reportId);
        reportMonitor.readLock().lock();
        try {
          ArrayList<String> result = new ArrayList<>(fetchCount);
          for (int index = 0; index < fetchCount; index++) {
            String line = this.g2Audit.fetchNext(reportId);
            if (line == null || line.trim().length() == 0) {
              log("GOT A BLANK LINE FROM FETCH-NEXT [ " + sessionId + ", " + reportId + " ]: " + line + " @ " + index);
              int exceptionCode = this.g2Audit.getLastExceptionCode();
              if (exceptionCode != 0) {
                String lastException = this.g2Audit.getLastException();
                lastException = formatErrorMessage(exceptionCode, lastException);
                log(lastException);
                EngineException e = new EngineException(
                    AUDIT_FETCH_NEXT, exceptionCode,
                    "Failed to fetch next record for audit report.  session=[ "
                        + sessionId + " ], report=[ " + reportId + " ], fetched=[ "
                        + index + " ]", this.g2Audit);
                this.g2Audit.clearLastException();
                throw e;
              }
              result.trimToSize();
              break;
            }
            // trim off line terminators
            line = line.trim();
            result.add(line);
          }
          return result;

        } finally {
          reportMonitor.readLock().unlock();
        }
      } finally {
        sessionMonitor.readLock().unlock();
      }
    } catch (RuntimeException e) {
      log(e);
      throw e;
    } catch (Exception e) {
      log(e);
      throw new RuntimeException(e);

    } finally {
      MONITOR.readLock().unlock();
    }
  }

  public static final boolean AUDIT_SESSIONS_SUPPORTED = false;
  private ReentrantReadWriteLock dummyAuditSessionMonitor
    = new ReentrantReadWriteLock();

  private ReentrantReadWriteLock createAuditSessionMonitor(long sessionId) {
    if (!AUDIT_SESSIONS_SUPPORTED) {
      return dummyAuditSessionMonitor;
    }
    synchronized (this.auditSessionMonitors) {
      if (this.auditSessionMonitors.containsKey(sessionId)) {
        throw new IllegalStateException(
          "Audit session already exists: " + sessionId);
      }
      ReentrantReadWriteLock monitor = new ReentrantReadWriteLock();
      this.auditSessionMonitors.put(sessionId, monitor);
      return monitor;
    }
  }

  private void removeAuditSessionMonitor(long sessionId) {
    if (!AUDIT_SESSIONS_SUPPORTED) {
      return;
    }
    synchronized (this.auditSessionMonitors) {
      ReentrantReadWriteLock monitor
        = this.auditSessionMonitors.remove(sessionId);
      if (monitor == null) {
        log("The specified audit session ID is invalid or the audit session "
            + "has been closed: " + sessionId);
      }
    }
  }

  private ReentrantReadWriteLock getAuditSessionMonitor(long sessionId)
    throws IllegalArgumentException
  {
    if (!AUDIT_SESSIONS_SUPPORTED) {
      return dummyAuditSessionMonitor;
    }
    synchronized (this.auditSessionMonitors) {
      ReentrantReadWriteLock monitor = this.auditSessionMonitors.get(sessionId);
      if (monitor == null) {
        throw new IllegalArgumentException(
          "The specified audit session ID is invalid or the audit session "
          + "has been closed: " + sessionId);
      }
      return monitor;
    }
  }

  private ReentrantReadWriteLock createAuditReportMonitor(long reportId) {
    synchronized (this.auditReportMonitors) {
      if (this.auditReportMonitors.containsKey(reportId)) {
        throw new IllegalStateException(
          "Audit report already exists: " + reportId);
      }
      ReentrantReadWriteLock monitor = new ReentrantReadWriteLock();
      this.auditReportMonitors.put(reportId, monitor);
      return monitor;
    }
  }

  private void removeAuditReportMonitor(long reportId) {
    synchronized (this.auditReportMonitors) {
      ReentrantReadWriteLock monitor
        = this.auditReportMonitors.remove(reportId);
      if (monitor == null) {
        log("The specified audit report ID is invalid or the audit session "
            + "has been closed: " + reportId);
      }
    }
  }

  private ReentrantReadWriteLock getAuditReportMonitor(long sessionId)
    throws IllegalArgumentException
  {
    synchronized (this.auditReportMonitors) {
      ReentrantReadWriteLock monitor = this.auditReportMonitors.get(sessionId);
      if (monitor == null) {
        throw new IllegalArgumentException(
          "The specified audit session ID is invalid or the audit session "
          + "has been closed: " + sessionId);
      }
      return monitor;
    }
  }

}
