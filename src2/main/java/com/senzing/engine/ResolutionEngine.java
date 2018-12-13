package com.senzing.engine;

import java.util.*;

import com.senzing.g2.engine.G2Engine;
import com.senzing.g2.engine.Result;
import com.senzing.api.util.WorkbenchLogging;

import static com.senzing.api.engine.EngineOperation.*;
import static com.senzing.api.engine.EngineException.LICENSE_EXCEEDED_ERROR_CODE;

import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ResolutionEngine implements Engine {
  private static ResolutionEngine current_instance = null;
  private static final ReentrantReadWriteLock MONITOR = new ReentrantReadWriteLock();
  private static final Integer DEFAULT_MAX_DEGREES = 0;
  private static final Integer DEFAULT_BUILD_OUT_DEGREES = 0;
  private static final Integer DEFAULT_MAX_ENTITY_COUNT = 1000;
  private G2Engine      g2Engine;
  private String        moduleName;
  private String        iniFileName;
  private boolean       verboseLogging;
  private EngineLogger  logger;
  private boolean       initialized;

  private ResolutionEngine(G2Engine     g2Engine,
                           String       moduleName,
                           String       iniFileName,
                           boolean      verboseLogging,
                           EngineLogger logger) {
    this.g2Engine       = g2Engine;
    this.moduleName     = moduleName;
    this.iniFileName    = iniFileName;
    this.verboseLogging = verboseLogging;
    this.logger         = logger;
    this.initialized    = true;
  }

  public EngineLogger getLogger() {
    return this.logger;
  }

  public EngineType getEngineType() {
    return EngineType.RESOLVER;
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
    EngineResponse response;
    try {
      this.g2Engine.clearLastException();
      switch (op) {
        case NO_OP:
          return new EngineResponse(request);
        case PROCESS:
          request.markEngineStart();
          try {
            this.process(request.getMessage());
          } finally {
            request.markEngineEnd();
          }
          break;

        case PRIME_ENGINE:
          request.markEngineStart();
          try {
            this.primeEngine();
          } finally {
            request.markEngineEnd();
          }
          break;

        case GET_ACTIVE_CONFIG_ID:
          request.markEngineStart();
          try {
            result = this.getActiveConfigId();
          } finally {
            request.markEngineEnd();
          }
          break;

        case EXPORT_CONFIG:
          request.markEngineStart();
          try {
            result = this.exportConfig();
          } finally {
            request.markEngineEnd();
          }
          break;

        case GET_REPOSITORY_LAST_MODIFIED:
          request.markEngineStart();
          try {
            result = this.getRepositoryLastModifiedTime();
          } finally {
            request.markEngineEnd();
          }
          break;

        case ADD_RECORD:
          request.markEngineStart();
          try {
            String dataSource = (String) request.getParameter(DATA_SOURCE);
            String recordId   = (String) request.getParameter(RECORD_ID);
            String loadId     = (String) request.getParameter(LOAD_ID);
            String record     = request.getMessage();
            result = this.addRecord(dataSource, recordId, record, loadId);

          } finally {
            request.markEngineEnd();
          }
          if (request.getEngineDuration() > 5000L) {
            System.out.println("LONG ADD_RECORD REQUEST DURATION: "
                                   + request.getEngineDuration()
                                   + "ms (requestID=[ " + request.getRequestId()
                                   + " ], recordID=[ " + result + " ])");
          }
          break;

        case GET_RECORD:
          request.markEngineStart();
          try {
            String dataSource = (String) request.getParameter(DATA_SOURCE);
            String recordId   = (String) request.getParameter(RECORD_ID);
            result = this.getRecord(dataSource, recordId);

          } finally {
            request.markEngineEnd();
          }
          break;

        case GET_ENTITY_BY_ENTITY_ID:
          request.markEngineStart();
          try {
            long entityId = (Long) request.getParameter(ENTITY_ID);
            result = this.getEntityByEntityId(entityId);

          } finally {
            request.markEngineEnd();
          }
          break;

        case GET_ENTITY_BY_RECORD_ID:
          request.markEngineStart();
          try {
            String dataSource = (String) request.getParameter(DATA_SOURCE);
            String recordId   = (String) request.getParameter(RECORD_ID);
            result = this.getEntityByRecordId(dataSource, recordId);

          } finally {
            request.markEngineEnd();
          }
          break;

        case FIND_NETWORK_BY_ENTITY_ID:
          request.markEngineStart();
          try {
            Collection<Long> entityIds = (Collection<Long>) request.getParameter(ENTITY_IDS);
            if (entityIds == null || entityIds.size() == 0) {
              throw new IllegalArgumentException(
                  "No entity IDs specified for operation: " + op);
            }
            Number maxDegrees = (Number) request.getParameter(MAX_DEGREES);
            if (maxDegrees == null || maxDegrees.intValue() < 0) {
              maxDegrees = DEFAULT_MAX_DEGREES;
            }
            Number buildOut = (Number) request.getParameter(BUILD_OUT_DEGREES);
            if (buildOut == null || buildOut.intValue() < 0) {
              buildOut = DEFAULT_BUILD_OUT_DEGREES;
            }
            Number maxEntities = (Number) request.getParameter(MAX_ENTITY_COUNT);
            if (maxEntities == null || maxEntities.intValue() < 1) {
              maxEntities = DEFAULT_MAX_ENTITY_COUNT;
            }
            Set<Long> entityIdSet = new HashSet<>(entityIds);
            result = this.findNetworkByEntityId(entityIdSet,
                                                maxDegrees.intValue(),
                                                buildOut.intValue(),
                                                maxEntities.intValue());

          } finally {
            request.markEngineEnd();
          }
          break;

        case SEARCH_ENTITIES:
          request.markEngineStart();
          try {
            result = searchByAttributes(request);
          } finally {
            request.markEngineEnd();
          }
          break;

        case STATS:
          request.markEngineStart();
          try {
            result = this.stats();
          } finally {
            request.markEngineEnd();
          }
          break;

        case DESTROY:
          request.markEngineStart();
          try {
            this.destroy();
          } finally {
            request.markEngineEnd();
          }
          break;

        case EXPORT_JSON_ENTITY_REPORT:
          request.markEngineStart();
          try {
            int matchLevelFlags = (Integer) request.getParameter(MATCH_LEVEL_FLAGS);
            int exportFlags = (Integer) request.getParameter(EXPORT_FLAGS);
            result = this.exportJSONEntityReport(matchLevelFlags, exportFlags);

          } finally {
            request.markEngineEnd();
          }
          break;

        case EXPORT_CSV_ENTITY_REPORT:
          request.markEngineStart();
          try {
            int matchLevelFlags = (Integer) request.getParameter(MATCH_LEVEL_FLAGS);
            int exportFlags = (Integer) request.getParameter(EXPORT_FLAGS);
            result = this.exportCSVEntityReport(matchLevelFlags, exportFlags);

          } finally {
            request.markEngineEnd();
          }
          break;

        case EXPORT_FETCH_NEXT:
          request.markEngineStart();
          try {
            long exportHandle = (Long) request.getParameter(EXPORT_HANDLE);
            int fetchCount = 1;
            Integer paramVal = (Integer) request.getParameter(FETCH_COUNT);
            if (paramVal != null) fetchCount = paramVal;
            result = this.exportFetchNext(exportHandle, fetchCount);

          } finally {
            request.markEngineEnd();
          }
          break;

        case CLOSE_EXPORT:
          request.markEngineStart();
          try {
            long exportHandle = (Long) request.getParameter(EXPORT_HANDLE);
            this.closeExport(exportHandle);

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

    } catch (Exception e) {
      log(e);
      log(request.getMessage());
      String msg = "Failed to process request: " + request;
      EngineException ee = (e instanceof EngineException)
        ? ((EngineException) e) : new EngineException(msg, e);
      response = new EngineResponse(request, ee);
    }

    return response;
  }

  public String searchByAttributes(EngineRequest request) {
    String result;
    MONITOR.readLock().lock();
    try {
      this.assertInitialized();
      StringBuffer searchResult = new StringBuffer();
      String searchCriteria = request.getMessage();
      int engineResult = g2Engine.searchByAttributes(searchCriteria, searchResult);
      if (engineResult != 0) {
        throw new EngineException("Failed to search by attributes, searchJson: " + searchCriteria, g2Engine);
      }
      result = searchResult.toString();
    }
    finally {
      MONITOR.readLock().unlock();
    }
    return result;
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
    return ("G2ENGINE=[ moduleName=[ " + this.getModuleName()
            + "], iniFileName=[ " + this.getInitFileName()
            + " ], verboseLogging=[ " + this.isVerboseLogging() + " ] ]");
  }

  public boolean isDestroyed() {
    MONITOR.readLock().lock();
    try {
      return (this.g2Engine == null);
    } finally {
      MONITOR.readLock().unlock();
    }
  }

  public void destroy() throws EngineException {
    MONITOR.writeLock().lock();
    try {
      this.assertNotDestroyed();
      if (this.initialized) {
        int result = this.g2Engine.destroy();
        if (result != 0) {
          throw new EngineException("Failed to destroy engine.",
                                    this.g2Engine);
        }
      }
      this.g2Engine = null;
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

      int result = this.g2Engine.destroy();
      if (result != 0) {
        throw new EngineException("Failed to uninitialize/destroy engine.",
                                  this.g2Engine);
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
      Result<Long> configId = new Result<>();

      int result = this.g2Engine.init(this.moduleName,
                                      this.iniFileName,
                                      this.verboseLogging,
                                      configId);
      if (result != 0) {
        throw new EngineException("Failed to reinitialize engine.",
                                  this.g2Engine);
      }
      this.initialized = true;

    } finally {
      MONITOR.writeLock().unlock();
    }
  }

  public void primeEngine() throws EngineException {
    MONITOR.readLock().lock();
    try {
      this.assertInitialized();
      log("PRIMING ENGINE....");
      int result = this.g2Engine.primeEngine();
      log("PRIMED ENGINE WITH RESULT: " + result);
      if (result != 0) {
        throw new EngineException(PRIME_ENGINE, result, "Failed to prime engine.",
                                  this.g2Engine);
      }
    } finally {
      MONITOR.readLock().unlock();
    }
  }

  public long getActiveConfigId() throws EngineException {
    MONITOR.readLock().lock();
    try {
      this.assertInitialized();
      Result<Long> configId = new Result<>();
      int returnCode = this.g2Engine.getActiveConfigID(configId);
      long result = configId.getValue();
      if (returnCode < 0) {
        throw new EngineException(
            GET_ACTIVE_CONFIG_ID,
            returnCode,
            "Failed to get active config ID.",
            this.g2Engine);
      }
      return result;

    } finally {
      MONITOR.readLock().unlock();
    }
  }

  public String exportConfig() throws EngineException {
    MONITOR.readLock().lock();
    try {
      this.assertInitialized();
      Result<Long> configId = new Result<>();
      StringBuffer sb = new StringBuffer();
      int returnCode = this.g2Engine.exportConfig(sb, configId);
      String result = sb.toString();
      if (returnCode < 0) {
        throw new EngineException(
            EXPORT_CONFIG,
            returnCode,
            "Failed to export currently active config.",
            this.g2Engine);
      }
      return result;

    } finally {
      MONITOR.readLock().unlock();
    }
  }

  public long getRepositoryLastModifiedTime() throws EngineException {
    MONITOR.readLock().lock();
    try {
      this.assertInitialized();
      Result<Long> time = new Result<>();
      int returnCode = this.g2Engine.getRepositoryLastModifiedTime(time);
      if (returnCode < 0) {
        throw new EngineException(
            GET_REPOSITORY_LAST_MODIFIED,
            returnCode,
            "Failed to get last repository modification time.",
            this.g2Engine);
      }
      long result = time.getValue();
      return result;
    } finally {
      MONITOR.readLock().unlock();
    }
  }


  public void process(String record) throws EngineException {
    MONITOR.readLock().lock();
    try {
      this.assertInitialized();
      int result = this.g2Engine.process(record);
      if (result != 0) {
        String msg = "Failed to process record.";
        if (WorkbenchLogging.isProducingDetailedLogs()) {
          msg = WorkbenchLogging.detailedLogsWrap(
              "Failed to process record: " + record);
        }
        throw new EngineException(PROCESS, result, msg, this.g2Engine);
      }
    } finally {
      MONITOR.readLock().unlock();
    }
  }

  public String addRecord(String dataSource,
                          String recordId,
                          String record,
                          String loadId)
      throws EngineException
  {
    MONITOR.readLock().lock();
    try {
      this.assertInitialized();
      int result;
      if (recordId == null) {
        StringBuffer sb = new StringBuffer();
        result = this.g2Engine.addRecordWithReturnedRecordID(dataSource,
                                                             sb,
                                                             record,
                                                             loadId);
        recordId = sb.toString();

      } else {
        result = this.g2Engine.addRecord(dataSource, recordId, record, loadId);
      }
      if (result != 0) {
        String msg = "Failed to add record.";
        if (WorkbenchLogging.isProducingDetailedLogs()) {
          msg = WorkbenchLogging.detailedLogsWrap(
              "Failed to add record: " + record);
        }

        EngineException e = new EngineException(ADD_RECORD,
                                                result,
                                                msg,
                                                this.g2Engine);

        if (LICENSE_EXCEEDED_ERROR_CODE == e.getErrorCode()) {
          String jsonText = this.stats();

          long count = EngineUtilities.parseLoadedRecordsFromStats(jsonText);

          e.setAdditionalInfo(count);
        }
        throw e;
      }
      return recordId;

    } catch (EngineException e) {
      log(e);
      throw e;

    } catch (Throwable t) {
      log(t);
      throw new EngineException(ADD_RECORD, 0, t.toString(), this.g2Engine);

    } finally {
      MONITOR.readLock().unlock();
    }
  }

  public String getRecord(String dataSource, String recordId) {
    MONITOR.readLock().lock();
    try {
      this.assertInitialized();
      StringBuffer sb = new StringBuffer();

      int result = this.g2Engine.getRecord(dataSource, recordId, sb);

      if (result != 0) {
        throw new EngineException(
          GET_RECORD,
          result,
          "Failed to get record: " + dataSource + " / " + recordId,
          this.g2Engine);
      }
      return sb.toString();

    } finally {
      MONITOR.readLock().unlock();
    }
  }

  public String getEntityByEntityId(long entityId) {
    MONITOR.readLock().lock();
    try {
      this.assertInitialized();
      StringBuffer sb = new StringBuffer();

      int result = this.g2Engine.getEntityByEntityID(entityId, sb);

      if (result != 0) {
        throw new EngineException(
          GET_ENTITY_BY_ENTITY_ID,
          result,
          "Failed to get entity by entity ID: " + entityId,
          this.g2Engine);
      }
      return sb.toString();

    } finally {
      MONITOR.readLock().unlock();
    }
  }

  public String findNetworkByEntityId(Set<Long> entityIds,
                                      int       maxDegrees,
                                      int       buildOutDegrees,
                                      int       maxEntityCount)
  {
    MONITOR.readLock().lock();
    try {
      this.assertInitialized();
      StringBuffer jsonBuffer = new StringBuffer("{\"ENTITIES\":[");
      String prefix = "";
      for (Long entityId : entityIds) {
        jsonBuffer.append(prefix);
        jsonBuffer.append("{\"ENTITY_ID\": \"");
        jsonBuffer.append(entityId);
        jsonBuffer.append("\"}");
        prefix = ",";
      }
      jsonBuffer.append("]}");

      StringBuffer sb = new StringBuffer();

      int result = this.g2Engine.findNetworkByEntityID(jsonBuffer.toString(),
                                                       maxDegrees,
                                                       buildOutDegrees,
                                                       maxEntityCount,
                                                       sb);

      if (result != 0) {
        throw new EngineException(
            FIND_NETWORK_BY_ENTITY_ID,
            result,
            "Failed to get entity by entity ID: " + entityIds,
            this.g2Engine);
      }
      return sb.toString();

    } finally {
      MONITOR.readLock().unlock();
    }

  }

  public String getEntityByRecordId(String dataSource, String recordId) {
    MONITOR.readLock().lock();
    try {
      this.assertInitialized();
      StringBuffer sb = new StringBuffer();

      int result = this.g2Engine.getEntityByRecordID(dataSource, recordId, sb);

      if (result != 0) {
        throw new EngineException(
          GET_ENTITY_BY_RECORD_ID,
          result,
          "Failed to get entity by record ID: " + dataSource + " / " + recordId,
          this.g2Engine);
      }
      return sb.toString();

    } finally {
      MONITOR.readLock().unlock();
    }
  }

  public void purgeRepository() throws EngineException {
    MONITOR.writeLock().lock();
    try {
      this.assertInitialized();
      int result = this.g2Engine.purgeRepository();
      if (result != 0) {
        throw new EngineException(PURGE_AND_REINITIALIZE, result,
                                  "Failed to purge repository.",
                                  this.g2Engine);
      }
    } finally {
      MONITOR.writeLock().unlock();
    }
  }

  public String stats() throws EngineException {
    MONITOR.readLock().lock();
    try {
      this.assertInitialized();
      String result = this.g2Engine.stats();
      if (result == null || result.length() == 0) {
        throw new EngineException("Failed to obtain stats.", this.g2Engine);
      }
      return result;
    } finally {
      MONITOR.readLock().unlock();
    }
  }

  public long exportJSONEntityReport(int matchLevelFlags, int exportFlags)
    throws EngineException
  {
    MONITOR.readLock().lock();
    long result;
    try {
      this.assertInitialized();

      int flags = matchLevelFlags | exportFlags;
      result = this.g2Engine.exportJSONEntityReport(flags);
      if (result < 0L) {
        throw new EngineException(EXPORT_JSON_ENTITY_REPORT,
                                  (int) result,
                                  "Failed to export JSON entity report.",
                                  this.g2Engine);
      }
    } finally {
      MONITOR.readLock().unlock();
    }
    return result;
  }

  public long exportCSVEntityReport(int matchLevelFlags, int exportFlags)
    throws EngineException
  {
    MONITOR.readLock().lock();
    long result;
    try {
      this.assertInitialized();

      int flags = matchLevelFlags | exportFlags;
      result = this.g2Engine.exportCSVEntityReport(flags);
      if (result < 0L) {
        throw new EngineException(EXPORT_CSV_ENTITY_REPORT,
                                  (int) result,
                                  "Failed to export CSV entity report.",
                                  this.g2Engine);
      }
    } finally {
      MONITOR.readLock().unlock();
    }
    return result;
  }

  public List<String> exportFetchNext(long exportHandle, int fetchCount)
      throws EngineException
  {
    MONITOR.readLock().lock();
    try {
      this.assertInitialized();
      ArrayList<String> result = new ArrayList<>(fetchCount);
      for (int index = 0; index < fetchCount; index++) {
        String line = this.g2Engine.fetchNext(exportHandle);
        if (line == null || line.trim().length() == 0) {
          result.trimToSize();
          break;
        }
        String suffix = line.substring(line.length()-2, line.length());
        if (suffix.equals("\r\n") || suffix.equals("\n\r")) {
          line = line.substring(0, line.length()-2);
        } else if (suffix.charAt(1) == '\r' || suffix.charAt(1) == '\n') {
          line = line.substring(0, line.length()-1);
        }
        result.add(line);
      }
      return result;

    } finally {
      MONITOR.readLock().unlock();
    }
  }

  public void closeExport(long exportHandle) throws EngineException {
    MONITOR.readLock().lock();
    try {
      this.assertInitialized();
      this.g2Engine.closeExport(exportHandle);
    } finally {
      MONITOR.readLock().unlock();
    }
  }

  private void assertNotDestroyed() {
    if (this.g2Engine == null) {
      throw new IllegalStateException("G2 Engine already destroyed.");
    }
  }

  private void assertInitialized() {
    this.assertNotDestroyed();
    if (!this.initialized) {
      throw new IllegalStateException("Engine not initialized.");
    }
  }

  private void assertNotInitialized() {
    if (this.initialized) {
      throw new IllegalStateException("Engine is already initialized.");
    }
  }

  public static ResolutionEngine getEngine(String       moduleName,
                                           String       iniFileName,
                                           boolean      verboseLogging,
                                           EngineLogger logger)
  {
    MONITOR.writeLock().lock();
    try {
      ResolutionEngine eng = ResolutionEngine.current_instance;
      if (eng != null) {
        if (moduleName.equals(eng.getModuleName())
            && iniFileName.equals(eng.getInitFileName())
            && (verboseLogging == eng.isVerboseLogging())) {
          return eng;
        }
        logger.log("Attempting to create a new engine before destroying the current engine.");
        throw new IllegalStateException(
          "Must destroy previous G2 engine before obtaining a new one.  "
          + "previous=[ " + eng + " ], moduleName=[ " + moduleName
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
        final String implClassName = "com.senzing.g2.engine.G2JNI";
        Class implClass = engineLoader.loadClass(implClassName);
        final G2Engine impl = (G2Engine) implClass.newInstance();
        Result<Long> configId = new Result<>();

        int result = impl.init(moduleName, iniFileName, verboseLogging, configId);
        if (result != 0) {
          throw new EngineException("Failed to initialize engine", impl);
        }
        eng = new ResolutionEngine(impl,
                                   moduleName,
                                   iniFileName,
                                   verboseLogging,
                                   logger);
        ResolutionEngine.current_instance = eng;
        return eng;

      } catch (RuntimeException e) {
        throw e;

      } catch (Exception e) {
        throw new EngineException(e);
      }
    } finally {
      MONITOR.writeLock().unlock();
    }
  }
}
