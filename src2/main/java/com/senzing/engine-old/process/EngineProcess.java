package com.senzing.api.engine.process;

import com.senzing.util.*;
import com.senzing.io.IOUtilities;
import com.senzing.api.*;
import com.senzing.api.engine.*;
import com.senzing.api.util.StreamLogger;
import com.senzing.api.upload.CSVUploadPeer;
import com.senzing.api.upload.StringDifferentiator;

import java.io.StringWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.charset.Charset;
import java.security.SecureRandom;
import java.security.MessageDigest;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonWriterFactory;
import javax.json.JsonValue;
import javax.json.JsonWriter;
import javax.json.JsonObjectBuilder;

import java.util.function.Predicate;

import com.senzing.api.util.WorkbenchConfig;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import static com.senzing.api.ServerErrorLog.*;
import static com.senzing.api.Workbench.*;
import static com.senzing.api.engine.EngineOperation.*;
import static com.senzing.api.engine.EnginePriority.*;
import static com.senzing.api.engine.process.EngineProcessState.*;
import static java.nio.file.StandardCopyOption.*;
import static com.senzing.api.ResolvedEntity.summarizeRecords;

@SuppressWarnings({"ResultOfMethodCallIgnored", "SynchronizationOnLocalVariableOrMethodParameter"})
public class EngineProcess extends Thread {
  public static final String MATCH_KEY = "MATCH_KEY";
  public static final String PRINCIPLE = "PRINCIPLE";

  static final int RESET_THRESHOLD_INCREMENT;
  static final int STATS_INTERVAL;
  private static final int REDO_THRESHOLD_INCREMENT;
  private static final int SINGLE_THREAD_THRESHOLD;
  private static final boolean ENGINE_VERBOSE_LOGGING;
  private static final int REDO_BLOCK_SIZE = 300;
  private static final int BLOCK_THRESHOLD;
  private static final int UNBLOCK_THRESHOLD;

  private static final SecureRandom PRNG = new SecureRandom();
  static final int REDO_DIVERGENCE_THRESHOLD = 50;
  static final int DEFAULT_BLOCK_THRESHOLD = 30000;
  static final int DEFAULT_UNBLOCK_THRESHOLD = 25000;
  static final int DEFAULT_REDO_THRESHOLD = 1000;
  static final int DEFAULT_RESET_THRESHOLD = 10000;
  static final int DEFAULT_STATS_INTERVAL = 1000;
  static final int DEFAULT_SINGLE_THREAD_THRESHOLD = 1000;

  static final Base64.Encoder BASE64_ENCODER = Base64.getUrlEncoder();

  private static final Map<String, Predicate<String>> CHECK_INVALID_PREDICATES;
  private static final Map<Long, EngineProcess> INSTANCE_MAP = new HashMap<>();
  private static final Map<Long, ProjectState> project_state_map = new HashMap<>();

  private static JsonWriterFactory WRITER_FACTORY
    = Json.createWriterFactory(Collections.emptyMap());

  static {
    int redo = DEFAULT_REDO_THRESHOLD;
    int stats = DEFAULT_STATS_INTERVAL;
    int reset = DEFAULT_RESET_THRESHOLD;
    int single = DEFAULT_SINGLE_THREAD_THRESHOLD;
    boolean verbose = false;
    Map<String, Predicate<String>> predicates = new HashMap<>();

    try {
      predicates.put("GENDER", EngineProcess::checkGenderInvalid);
      predicates.put("DATE_OF_BIRTH", EngineProcess::checkDateInvalid);

      String resetThreshold = System.getProperty("RESET_THRESHOLD");
      String redoThreshold = System.getProperty("REDO_THRESHOLD");
      String statsInterval = System.getProperty("STATS_INTERVAL");
      String singleThreadThreshold = System.getProperty("SINGLE_THREAD_THRESHOLD");
      String verboseLogging = System.getProperty("G2_ENGINE_VERBOSE");

      if (resetThreshold != null) {
        try {
          reset = Integer.parseInt(resetThreshold);
        } catch (Exception ignore) {
          ignore.printStackTrace();
        }
      }
      if (redoThreshold != null) {
        try {
          redo = Integer.parseInt(redoThreshold);
        } catch (Exception ignore) {
          ignore.printStackTrace();
        }
      }
      if (statsInterval != null) {
        try {
          stats = Integer.parseInt(statsInterval);
        } catch (Exception ignore) {
          ignore.printStackTrace();
        }
      }
      if (singleThreadThreshold != null) {
        try {
          single = Integer.parseInt(singleThreadThreshold);
        } catch (Exception ignore) {
          ignore.printStackTrace();
        }
      }
      if (verboseLogging != null) {
        try {
          verbose = Boolean.valueOf(verboseLogging);
        } catch (Exception ignore) {
          ignore.printStackTrace();
        }
      }

    } finally {
      RESET_THRESHOLD_INCREMENT = reset;
      REDO_THRESHOLD_INCREMENT = redo;
      STATS_INTERVAL = stats;
      SINGLE_THREAD_THRESHOLD = single;
      CHECK_INVALID_PREDICATES = Collections.unmodifiableMap(predicates);
      ENGINE_VERBOSE_LOGGING = verbose;
    }
  }

  static {
    int block = DEFAULT_BLOCK_THRESHOLD;
    int unblock = DEFAULT_UNBLOCK_THRESHOLD;
    try {
      String blockThreshold = System.getProperty("BLOCK_THRESHOLD");
      String unblockThreshold = System.getProperty("UNBLOCK_THRESHOLD");
      if (blockThreshold != null && blockThreshold.length() > 0) {
        try {
          block = Integer.parseInt(blockThreshold);
        } catch (Exception ignore) {
          ignore.printStackTrace();
        }
      }
      if (unblockThreshold != null && unblockThreshold.length() > 0) {
        try {
          unblock = Integer.parseInt(unblockThreshold);
        } catch (Exception ignore) {
          ignore.printStackTrace();
        }
      }
      if (block < 0) block = 0;
      if (unblock > block) {
        unblock = (block * 2) / 3;
        if (unblock < 0) unblock = 0;
      }
    } finally {
      BLOCK_THRESHOLD = block;
      UNBLOCK_THRESHOLD = unblock;
      System.out.println("USING BLOCK THRESHOLD   : " + BLOCK_THRESHOLD);
      System.out.println("USING UNBLOCK THRESHOLD : " + UNBLOCK_THRESHOLD);
    }
  }

  static {
    Workbench.registerShutdownHook(() -> {
        shutdownAllInstances();
      });
  }

  private static boolean checkGenderInvalid(String value) {
    if (value == null || value.length() == 0) {
      return true;
    }
    Map<String, String> genderMap = Workbench.getVariantTokens("GENDER");
    return genderMap != null && !genderMap.containsKey(value.trim().toUpperCase());
  }

  private static boolean checkDateInvalid(String value) {
    return value == null || value.isEmpty() || value.matches("^[-/0.]*");
  }

  private boolean isFieldValueInvalid(String attrCode, String value) {
    if (attrCode == null) {
      return false;
    }
    attrCode = attrCode.trim().toUpperCase();
    Predicate<String> predicate = CHECK_INVALID_PREDICATES.get(attrCode);
    return predicate != null && predicate.test(value);
  }

  private long projectId;
  private Project project;
  private Process process = null;
  private boolean engineRunning = false;
  private Set<String> dataSourceCodes;
  private final List<ProjectFile> fileQueue;
  private long nextRequestId = 0L;
  private long currentFileId = -1L;
  private final Map<Long, EngineRequest> pendingRequests;
  private final Map<Long, Redo> pendingRedos;
  private final Map<Long, EngineRequest> syncRequests;
  private final Map<Long, EngineResponse> syncResponses;
  private final Object monitor;
  private final Object requestIdMonitor;
  private final ProjectDataAccess projectDataAccess;
  private boolean aborted = false;
  private boolean loadCancelled = false;
  private ProjectFileDataAccess fileDataAccess;
  private RedoDataAccess redoDataAccess;
  private boolean completed = false;
  private boolean restartingEngine = false;

  private boolean restartRequired = false;

  private ServerSocketConnector connector;

  private Switchboard switchboard;

  private EngineResourceHandle auditSessionId = null;
  private EngineResourceHandle cancelledAuditSessionId = null;

  private final Map<EngineResourceHandle, Map<String, ReportReader>>
      reportReaders = new HashMap<>();

  private Indexer indexer;

  private String elasticSearchIndex;

  private SyncReceiver syncReceiver = null;

  private StreamLogger outLogger = null;
  private StreamLogger errLogger = null;

  private File engineLogFile;

  private File auditSummaryFile;

  private int engineThreadCount = 0;
  private long configTimestamp = -1L;
  private Thread reinitializeThread = null;
  private final Object abortMonitor = new Object();
  private ResultsSummary currentAuditSummary = null;
  private Exception auditFailure = null;
  private boolean auditWasCached = false;
  private boolean shutdown = false;

  private boolean pendingPurge = false;
  
  private boolean primingAuditCancelled = false;
  private int primingAuditSummary = 0;
  private int primingAuditDetails = 0;
  private final Object primingMonitor = new Object();
  private Thread primerThread = null;
  private final AccessToken accessToken;
  private final SendPrioritizer reportSender;

  ReentrantReadWriteLock auditSessionLock = new ReentrantReadWriteLock();
  private ReentrantReadWriteLock engineLock = new ReentrantReadWriteLock();


  private boolean isPrimingAudit() {
    synchronized (this.primingMonitor) {

      boolean result = (this.primerThread != null
                        || this.primingAuditSummary > 0
                        || this.primingAuditDetails > 0);

      /*
      if (result) {
        log("PRIMER THREAD NOT NULL / PRIMING SUMMARY / PRIMING DETAILS : "
                + (this.primerThread != null) + " / " + this.primingAuditSummary
                + " / " + this.primingAuditDetails);

        if (this.primerThread != null) {
          log("PRIMER THREAD NAME: " + this.primerThread.getName());
          log("PRIMER THREAD ALIVE: " + this.primerThread.isAlive());
        }
      }
      */

      return result;
    }
  }

  private void waitIfPrimingAudit(boolean cancelSession) {
    log("WAIT IF PRIMING AUDIT "
            + (cancelSession?"WITH CANCEL" : "WITHOUT CANCEL"));
    boolean cancelled = false;
    boolean cleanedUp = false;
    EngineResourceHandle sessionId = null;
    synchronized (this.primingMonitor) {
      while (this.isPrimingAudit()) {
        // check if we should cancel the session and it has not yet been
        // cancelled
        if (cancelSession && !cancelled) {
          try {
            sessionId = this.getAuditSessionId(false);
            if (sessionId != null) {
              this.cancelAuditSession(sessionId);
              cancelled = true;
            }
          } catch (Exception ignore) {
            log(ignore);
          }
        }

        // notify any report readers that they need to give up
        if (sessionId != null && !cleanedUp) {
          log("SESSION TO CANCEL: " + sessionId);

          final EngineResourceHandle staleSession = sessionId;
          Thread cleanupThread = new Thread(() -> {
            Map<String,ReportReader> reportReaders
                = this.getAuditReportReaders(staleSession);

            if (reportReaders != null) {
              List<ReportReader> list;
              synchronized (reportReaders) {
                list = new ArrayList<>(reportReaders.values());
              }

              list.forEach(reader -> {
                log("INVALIDATING REPORT READER: " + reader.getReportKey());
                try {
                  reader.invalidate();
                } catch (Exception ignore) {
                  log("EXCEPTION WHILE INVALIDATING REPORT READER:");
                  log(ignore);
                }
              });
            }
          });
          cleanupThread.start();
          cleanedUp = true;
        }

        // now wait for the session an audit priming to complete
        try {
          log("WAITING FOR AUDIT PRIMING TO COMPLETE....");
          this.primingMonitor.wait(1000L);
        } catch (InterruptedException ignore) {
          log(ignore);
        }
      }
      log("AUDIT PRIMING IS COMPLETE");
      if (cancelSession && cancelled && sessionId != null) {
        log("CLEANING UP AUDIT SESSION....");
        cleanupAuditSession(sessionId, true);
        log("CLEANED UP AUDIT SESSION.");
      }
    }
  }

  private ResultsSummary getCurrentAuditSummary() {
    synchronized (this.primingMonitor) {
      return this.currentAuditSummary;
    }
  }

  /**
   * Sets the current audit summary unless audit priming has been cancelled
   * because the audit session was cancelled.  This method returns the specified
   * audit summary if not cancelled or <tt>null</tt> if cancelled.  If the
   * audit session wa cancelled then the current audit summary is set to null.
   *
   * @param auditSummary The audit summary value that was retrieved.
   * @return The specified audit summary or <tt>null</tt> if cancelled.
   */
  private ResultsSummary setCurrentAuditSummary(ResultsSummary auditSummary) {
    synchronized (this.primingMonitor) {
      if (this.primingAuditCancelled) auditSummary = null;
      this.currentAuditSummary = auditSummary;
      this.primingMonitor.notifyAll();
      return this.currentAuditSummary;
    }
  }

  private boolean isPrimingAuditSummary() {
    synchronized (this.primingMonitor) {
      return (this.primingAuditSummary > 0);
    }
  }

  private boolean isPrimingAuditDetails() {
    synchronized (this.primingMonitor) {
      return (this.primingAuditDetails > 0);
    }
  }

  private void beginPrimingAuditSummary() {
    synchronized (this.primingMonitor) {
      this.currentAuditSummary = null;
      this.primingAuditSummary++;
      this.primingMonitor.notifyAll();
      if (this.primingAuditSummary == 1) {
        log("******** PRIMING AUDIT SUMMARY...");
      } else {
        log("******** PRIMING AUDIT SUMMARY ALREADY UNDERWAY");
      }
    }
  }

  private void endPrimingAuditSummary() {
    synchronized (this.primingMonitor) {
      this.primingAuditSummary--;
      this.primingMonitor.notifyAll();
      if (this.primingAuditSummary == 0) {
        log("******** AUDIT SUMMARY PRIMED");
      }
    }
  }

  void beginPrimingAuditDetails() {
    synchronized (this.primingMonitor) {
      this.primingAuditDetails++;
      this.primingMonitor.notifyAll();
      if (this.primingAuditDetails == 1) {
        log("******** PRIMING AUDIT DETAILS...");
      }
    }
  }

  void endPrimingAuditDetails() {
    synchronized (this.primingMonitor) {
      this.primingAuditDetails--;
      this.primingMonitor.notifyAll();
    }
  }

  static File auditCacheDir(long projectId) {
    try {
      File projectDir = getProjectDirectory(projectId);
      File cacheDir = new File(projectDir, "audit-cache");
      if (!cacheDir.exists()) cacheDir.mkdir();
      return cacheDir;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  File auditCacheDir() {
    return auditCacheDir(this.projectId);
  }

  static File auditPrimeFile(long projectId) {
    try {
      File projectDir = getProjectDirectory(projectId);
      return new File(projectDir, "audit-priming.tmp");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  File auditPrimeFile() {
    return auditPrimeFile(this.projectId);
  }

  void createPrimingAuditFile() {
    File primeFile = this.auditPrimeFile();
    try {
      log("CREATING AUDIT PRIMING FILE: " + primeFile);
      primeFile.createNewFile();
      log("CREATED AUDIT PRIMING FILE: " + primeFile
          + " (" + primeFile.exists() + ")");

    } catch (IOException e) {
      throw new RuntimeException(
          "Failed to create audit prime file: " + primeFile, e);
    }
  }

  void deletePrimingAuditFile() {
    File primeFile = this.auditPrimeFile();
    if (primeFile.exists()) {
      log("DELETING AUDIT PRIMING FILE: " + primeFile);

      if (!primeFile.delete()) {
        throw new RuntimeException(
            "Failed to delete audit prime file: " + primeFile);
      }
      log("DELETED AUDIT PRIMING FILE: " + primeFile
              + " (" + primeFile.exists() + ")");
    }
  }

  boolean primingAuditFileExists() {
    return (this.auditPrimeFile().exists());
  }

  boolean isReinitializing() {
    synchronized (this.monitor) {
      return (this.reinitializeThread != null);
    }
  }

  public static ProjectState getClientProjectState(long projectId) {
    synchronized (project_state_map) {
      ProjectState state = project_state_map.get(projectId);
      return (state == null) ? ProjectState.CLOSED : ProjectState.OPEN;
    }
  }

  public static boolean setClientProjectState(long projectId, ProjectState state) {
    Project project = null;
    try {
      if (state == ProjectState.OPEN) {
        log("TOUCHING PROJECT: " + projectId);
        ProjectDataAccess pda = new ProjectDataAccess();
        pda.touchProject(projectId);
        project = pda.selectProject(projectId);
        if (project == null) {
          log("PROJECT NOT FOUND: " + projectId);
          throw new IllegalArgumentException("Project not found: " + projectId);
        }
      }
    } catch (SQLException ignore) {
      log(ignore);
    }

    synchronized (project_state_map) {
      if (state == project_state_map.get(projectId)) {
        return false;
      }
      project_state_map.put(projectId, state);
    }

    boolean running = checkEngineRunning(projectId);
    EnumSet<EngineProcessState> processState = checkProjectState(projectId);
    boolean resolving = processState.contains(RESOLVING);

    log("CHANGING STATE TO " + state + " PROJECT " + projectId
        + ": ENGINE IS CURRENTLY "
        + ((running) ? "" : "NOT ") + "RUNNING AND "
        + ((resolving) ? "" : "NOT ") + "RESOLVING.");

    EngineProcess engineProcess;
    if (state == ProjectState.OPEN) {
      try {
        DataSourceDataAccess dsrcDA = new DataSourceDataAccess(projectId);
        List<DataSource> dataSources = dsrcDA.selectDataSources();
        if (dataSources.size() == 0 && !project.isExternal()) {
          log("PROJECT HAS NO DATA SOURCES.  CANCELLING ENGINE STARTUP");
          return true;
        }
      } catch (SQLException e) {
        log(e);
        return true;
      }
      engineProcess = getInstance(projectId);
      synchronized (engineProcess.monitor) {
        try {
          engineProcess.startEngine();
        } catch (Throwable t) {
          ServerError se = new ServerError(true, "engine-start", t);
          recordProjectError(projectId, se);
        }
      }

    } else if (state == ProjectState.CLOSED) {
      if (running) {
        engineProcess = getInstance(projectId);
        engineProcess.attemptEngineShutdown(true, true);
      }
    }
    return true;
  }

  public static EnumSet<EngineProcessState> checkProjectState(long projectId) {
    EngineProcess engineProcess = getEngineProcess(projectId);
    if (engineProcess == null) {
      return EnumSet.noneOf(EngineProcessState.class);
    }
    boolean resolving = engineProcess.isResolving();
    boolean primingAudit = engineProcess.isPrimingAudit();
    boolean primingAuditSummary = engineProcess.isPrimingAuditSummary();
    boolean aborted = engineProcess.isAborted();
    if (resolving && primingAuditSummary) {
      // if primingAuditSummary, then primingAudit
      return EnumSet.of(ACTIVE, RESOLVING, PRIMING_AUDIT, PRIMING_AUDIT_SUMMARY);
    } else if (resolving && primingAudit) {
      return EnumSet.of(ACTIVE, RESOLVING, PRIMING_AUDIT);
    } else if (primingAuditSummary) {
      return EnumSet.of(ACTIVE, PRIMING_AUDIT, PRIMING_AUDIT_SUMMARY);
    } else if (resolving) {
      return EnumSet.of(ACTIVE, RESOLVING);
    } else if (primingAudit) {
      return EnumSet.of(ACTIVE, PRIMING_AUDIT);
    } else if (aborted) {
      return EnumSet.noneOf(EngineProcessState.class);
    } else {
      return EnumSet.of(ACTIVE);
    }
  }

  private static void shutdownAllInstances() {
    // copy the list of instances
    List<EngineProcess> list;
    synchronized (INSTANCE_MAP) {
      list = new ArrayList(INSTANCE_MAP.values());
    }
    for (EngineProcess ep: list) {
      ep.abort();
      try {
        ep.join();
      } catch (InterruptedException ignore) {
        log("JOIN THREAD INTERRUPTED FOR ENGINE PROCESS: "
                + ep.getProjectId());
        log(ignore);
      }
    }
  }


  private static EngineProcess getEngineProcess(long projectId) {
    synchronized (INSTANCE_MAP) {
      return INSTANCE_MAP.get(projectId);
    }
  }

  public boolean isResolving() {
    synchronized (fileQueue) {
      return this.currentFileId != -1L || fileQueue.size() > 0;
    }
  }

  public boolean isRestartRequired() {
    synchronized (this.monitor) {
      return this.restartRequired;
    }
  }

  public static void markAllRestartRequired() {
    List<EngineProcess> list;
    synchronized (INSTANCE_MAP) {
      list = new ArrayList<>(INSTANCE_MAP.values());
    }
    for (EngineProcess ep : list) {
      ep.markRestartRequired();
    }
  }

  public void markRestartRequired() {
    synchronized (this.monitor) {
      this.restartRequired = true;
    }
  }

  public void clearRestartRequired() {
    synchronized (this.monitor) {
      this.restartRequired = false;
    }
  }

  Set<Long> getPendingFileIds() {
    synchronized (fileQueue) {
      Set<Long> result = new LinkedHashSet<>();
      long currentFileId = getCurrentFileId();
      if (currentFileId >= 0L) {
        result.add(currentFileId);
      }
      for (ProjectFile file : fileQueue) {
        result.add(file.getId());
      }
      return result;
    }
  }

  private static boolean checkEngineRunning(long projectId) {
    EngineProcess engineProcess = getEngineProcess(projectId);
    return engineProcess != null && engineProcess.isEngineRunning();
  }

  public long getProjectId() {
    return this.projectId;
  }

  public long getCurrentFileId() {
    return this.currentFileId;
  }

  private boolean isEngineRunning() {
    synchronized (this.monitor) {
      return this.engineRunning;
    }
  }

  public static boolean checkFileResolving(long projectId, long fileId) {
    EngineProcess engineProcess = getEngineProcess(projectId);
    return engineProcess != null && engineProcess.isFileResolving(fileId);
  }

  private boolean isFileResolving(long fileId) {
    if (this.currentFileId == fileId) {
      return true;
    }
    synchronized (this.fileQueue) {
      return this.fileQueue.stream()
          .filter(f -> f != null)
          .anyMatch(f -> f.getId() == fileId);
    }
  }

  public void purgeProjectRepository() {
    if (this.isResolving()) {
      throw new IllegalStateException("Could not purge repository because engine is resolving: " + this.projectId);
    }
    if (this.isPrimingAudit()) {
      log("Attempting to purge while calculating statistics for audit review. " +
          "Canceling audit for projectId= " + this.projectId);
      this.waitIfPrimingAudit(true);
    }
    reinitializeEngine(true);

    AccessToken token = new AccessToken();

    ProjectDataAccess pda = new ProjectDataAccess();

    Project project;
    try {
      project = pda.selectProject(this.projectId, token);
      project.setValue("licenseRecordCount", 0, token);
      project = pda.updateProject(project, token);

      // delete any exports previously generated
      EngineExportUtilities.deleteExistingExportFiles(this.projectId);

    } catch (SQLException ignore) {
      log(ignore);
      return;
    }

    ProjectFileDataAccess fda = new ProjectFileDataAccess();
    project.getFiles().forEach(f -> resetFileState(token, fda, f));
  }

  private void resetFileState(AccessToken token, ProjectFileDataAccess fda, ProjectFile f) {
    f.setValue("resolvedRecordCount", 0, token);
    f.setValue("failedRecordCount", 0, token);
    f.setValue("suppressedRecordCount", 0, token);
    f.setValue("resolved", false, token);
    try {
      fda.updateFile(f, token);
    } catch (SQLException ignore) {
      log(ignore);
    }
  }

  public static boolean deleteProject(long projectId)
      throws SQLException, IOException {
    EngineProcess engineProcess = getEngineProcess(projectId);

    if (engineProcess != null && !engineProcess.attemptEngineShutdown(true, true)) {
      throw new IllegalStateException(
          "Could not shutdown engine because engine is resolving: " + projectId);
    }

    ProjectDataAccess pda = new ProjectDataAccess();

    boolean deleted = pda.deleteProject(projectId);
    if (deleted) {
      markProjectFileDeletion(projectId);
      boolean success = false;
      try {
        // attempt to delete project directory
        File projectDir = Workbench.getProjectDirectory(projectId);
        try {
          int count = IOUtilities.recursiveDeleteDirectory(projectDir);
          if (count == 0 && !projectDir.exists()) success = true;

        } catch (Exception e) {
          e.printStackTrace();
        }

      } finally {
        if (success) completeProjectFileDeletion(projectId);
      }
    }

    return deleted;
  }

  private boolean attemptEngineShutdown(boolean wait, boolean terminate) {
    if (isResolving()) {
      return false;
    }
    if (isPrimingAudit()) {
      return false;
    }
    if (terminate) {
      this.shutdown(wait);
      return true;
    } else {
      synchronized (monitor) {
        if (isResolving()) {
          return false;
        }
        if (isPrimingAudit()) {
          return false;
        }
        if (wait) {
          stopEngineAndWait(false);
        } else {
          stopEngine(false);
        }
      }
    }
    return true;
  }

  long getNextRequestId() {
    synchronized (this.requestIdMonitor) {
      return this.nextRequestId++;
    }
  }

  private void reinitializeEngine() {
    reinitializeEngine(false);
  }

  private void reinitializeEngine(boolean purge) {
    log("REINITIALIZING ENGINE.....");
    synchronized (this.monitor) {
      prepareEngineConfigFiles(getProjectId());
      this.reinitializeThread = Thread.currentThread();
      this.monitor.notifyAll();
    }

    // wait for any pending requests
    log("WAITING FOR COMPLETION OF PENDING REQUESTS....");
    synchronized (this.pendingRequests) {
      while (this.pendingRequests.size() > 0) {
        log("WAITING FOR PENDING REQUESTS: " + this.pendingRequests.size());
        try {
          pendingRequests.wait(2000L);
        } catch (InterruptedException ignore) {
          // ignore
        }
      }
    }
    log("PENDING REQUESTS COMPLETED.");

    // wait for any pending sync requests
    log("WAITING FOR COMPLETION OF SYNC REQUESTS....");
    synchronized (this.syncRequests) {
      while (this.syncRequests.size() > 0) {
        log("WAITING FOR PENDING REQUESTS: " + this.syncRequests.size());
        try {
          this.syncRequests.wait(2000L);
        } catch (InterruptedException ignore) {
          // ignore
        }
      }
    }
    log("SYNC REQUESTS COMPLETED.");

    // all pending requests have been received at this point
    log("CLEANING UP AUDIT SESSION....");
    cleanupAuditSession(false);
    log("CLEANED UP AUDIT SESSION....");
    this.engineLock.writeLock().lock();
    try {
      EngineOperation op = (purge) ? PURGE_AND_REINITIALIZE : REINITIALIZE;
      long rid = getNextRequestId();
      log("SENDING REINITIALIZE REQUEST: " + rid);
      EngineRequest request = new EngineRequest(op, rid, SYNC);

      EngineResponse response = this.sendSyncRequest(request);
      log("REINITIALIZE RESPONSE RECEIVED: " + rid);

      if (!response.isSuccessful()) {
        throw response.getException();
      }
    } finally {
      engineLock.writeLock().unlock();
      synchronized (monitor) {
        this.reinitializeThread = null;
        this.monitor.notifyAll();
      }
    }
  }

  protected Thread restartEngine() {
    synchronized (monitor) {
      if (restartingEngine) return null;
      restartingEngine = true;
    }

    Thread thread = new Thread(() -> {
      engineLock.writeLock().lock();
      try {
        stopEngineAndWait(false);
        startEngine();
      } finally {
        engineLock.writeLock().unlock();
        synchronized (monitor) {
          restartingEngine = false;
        }
      }
    }, "EngineRestartThread-" + this.projectId);
    thread.start();
    return thread;
  }

  public long getStatsLoadedRecordCount() {
    long rid = getNextRequestId();

    EngineRequest request = new EngineRequest(STATS, rid, SYNC);

    EngineResponse response = this.sendSyncRequest(request);

    if (!response.isSuccessful()) {
      throw response.getException();
    }

    String jsonText = (String) response.getResult();

    try {
      return EngineUtilities.parseLoadedRecordsFromStats(jsonText);

    } catch (RuntimeException e) {
      log("JSON TEXT: " + jsonText);
      log(e);
      throw e;
    }
  }

  public Long getRepositoryLastModifiedTime() {
    long rid = getNextRequestId();

    EngineRequest request
        = new EngineRequest(GET_REPOSITORY_LAST_MODIFIED, rid, SYNC);

    EngineResponse response = this.sendSyncRequest(request);

    if (!response.isSuccessful()) {
      EngineException e = response.getException();
      ServerError se = new ServerError(true, e);
      ServerErrorLog.recordProjectError(this.getProjectId(), se);
      throw e;
    }

    Long timestamp = (Long) response.getResult();

    return timestamp;
  }

  EngineResourceHandle getAuditSessionId() {
    return this.getAuditSessionId(true);
  }

  public EntityData getEntityByEntityId(long entityId)
  {
    AccessToken token = new AccessToken();

    EngineRequest request = new EngineRequest(FIND_NETWORK_BY_ENTITY_ID,
                                              getNextRequestId(),
                                              SYNC);

    request.setParameter(ENTITY_IDS, Collections.singletonList(entityId));
    request.setParameter(MAX_DEGREES, 1);
    request.setParameter(BUILD_OUT_DEGREES, 1);
    request.setParameter(MAX_ENTITY_COUNT, 1000);

    EngineResponse response = this.sendSyncRequest(request);

    if (!response.isSuccessful()) {
      throw response.getException();
    }

    String        jsonText    = (String) response.getResult();
    StringReader  sr          = new StringReader(jsonText);
    JsonReader    jsonReader  = Json.createReader(sr);
    JsonObject    jsonObject  = jsonReader.readObject();
    JsonArray     jsonArray   = jsonObject.getJsonArray("ENTITIES");

    List<EntityData> list = EntityData.parseEntityDataList(null,
                                                           jsonArray,
                                                           token);

    Map<Long,EntityData> dataMap = new LinkedHashMap<>();
    for (EntityData entityData : list) {
      ResolvedEntity resolvedEntity = entityData.getResolvedEntity();
      dataMap.put(resolvedEntity.getEntityId(), entityData);
    }
    EntityData          entityData = dataMap.get(entityId);
    List<RelatedEntity> discovered = entityData.getDiscoveredRelationships();
    List<RelatedEntity> disclosed  = entityData.getDisclosedRelationships();
    List<RelatedEntity> possibles  = entityData.getPossibleMatches();

    List<List<RelatedEntity>> relatedLists = new ArrayList<>(3);
    relatedLists.add(discovered);
    relatedLists.add(disclosed);
    relatedLists.add(possibles);

    for (List<RelatedEntity> relatedList : relatedLists) {
      for (RelatedEntity relatedEntity : relatedList) {
        EntityData relatedData = dataMap.get(relatedEntity.getEntityId());
        ResolvedEntity resolvedEntity = relatedData.getResolvedEntity();
        Map<String,List<EntityFeature>> features  = resolvedEntity.getFeatures();
        List<EntityRecord>              records   = resolvedEntity.getRecords();
        List<DataSourceRecordInfo>      breakdown = summarizeRecords(records,
                                                                     token);

        relatedEntity.initValue("features", features, token);
        relatedEntity.initValue("records", records, token);
        relatedEntity.initValue( "dataSourceBreakdown", breakdown, token);
      }
    }

    return entityData;
  }

  public List<EntitySearchResult> queryEntities(String queryText) {
    AccessToken token = new AccessToken();

    long rid = getNextRequestId();

    EngineRequest request = new EngineRequest(QUERY_ENTITIES,
        rid,
        queryText,
        SYNC);

    EngineResponse response = this.sendSyncRequest(request);

    if (!response.isSuccessful()) {
      throw response.getException();
    }

    String jsonText = (String) response.getResult();

    StringReader sr = new StringReader(jsonText);
    JsonReader jsonReader = Json.createReader(sr);
    JsonObject jsonObject = jsonReader.readObject();
    JsonValue jsonValue = jsonObject.getValue("/RESULT_ENTITIES");
    JsonArray jsonArray = jsonValue.asJsonArray();

    return EntitySearchResult.parseEntitySearchResultList(null, jsonArray, token);
  }

  public AttributeSearchResult searchEntities(String queryText) {
    AccessToken token = new AccessToken();
    long requestId = getNextRequestId();

    EngineRequest request = new EngineRequest(SEARCH_ENTITIES, requestId, queryText, SYNC);
    EngineResponse response = this.sendSyncRequest(request);

    if (!response.isSuccessful()) {
      throw response.getException();
    }

    String jsonText = (String) response.getResult();

    StringReader sr = new StringReader(jsonText);
    JsonReader jsonReader = Json.createReader(sr);
    JsonObject jsonObject = jsonReader.readObject();
    JsonArray resolvedEntityArray = jsonObject.getJsonObject("SEARCH_RESPONSE").getJsonArray("RESOLVED_ENTITIES");
    List<RelatedEntity> relatedEntities = resolvedEntityArray.stream()
        .map(jsonValue -> RelatedEntity.parseRelatedEntity(null, jsonValue.asJsonObject(), token))
        .collect(Collectors.toList());

    AttributeSearchResult attributeSearchResult = new AttributeSearchResult();
    for (RelatedEntity relatedEntity : relatedEntities) {
      attributeSearchResult.addRelatedEntity(relatedEntity);
    }
    return attributeSearchResult;
  }

  public Map<ObservedEntityId, String> getRecords(
      Collection<ObservedEntityId> ids) {
    Map<ObservedEntityId, EngineRequest> requestMap
        = new LinkedHashMap<>();
    for (ObservedEntityId id : ids) {
      if (requestMap.containsKey(id)) continue;
      long requestId = getNextRequestId();
      EngineRequest request = new EngineRequest(GET_RECORD, requestId, SYNC);
      request.setParameter(DATA_SOURCE, id.getDataSource());
      request.setParameter(RECORD_ID, id.getRecordId());
      requestMap.put(id, request);
    }

    Map<Long, EngineResponse> responses = this.sendSyncRequests(requestMap.values());

    Map<ObservedEntityId, String> result = new LinkedHashMap<>();

    for (ObservedEntityId id : ids) {
      EngineRequest request = requestMap.get(id);
      long requestId = request.getRequestId();
      EngineResponse response = responses.get(requestId);
      if (!response.isSuccessful()) {
        throw response.getException();
      }
      result.put(id, (String) response.getResult());
    }
    return result;
  }

  public String getEntityByRecordId(String dataSource, String recordId) {

    EngineRequest request = new EngineRequest(GET_ENTITY_BY_RECORD_ID,
        getNextRequestId(),
        SYNC);

    request.setParameter(DATA_SOURCE, dataSource);
    request.setParameter(RECORD_ID, recordId);

    EngineResponse response = this.sendSyncRequest(request);

    if (!response.isSuccessful()) {
      throw response.getException();
    }

    return (String) response.getResult();
  }

  EngineResourceHandle getAuditSessionId(boolean establishSession) {
    // check first if we already have an audit session ID
    this.auditSessionLock.readLock().lock();
    try {
      if (this.auditSessionId != null || !establishSession) {
        return this.auditSessionId;
      }
    } finally {
      this.auditSessionLock.readLock().unlock();
    }

    // if we get here then we don't have an audit session ID so now we need
    // a write lock instead of a read lock -- do a double-check though
    System.out.println("WATING FOR WRITE LOCK....");
    this.auditSessionLock.writeLock().lock();
    try {
      System.out.println("OBTAINED WRITE LOCK.");
      // double-check since we unlocked above (the read lock)
      if (this.auditSessionId != null) {
        return this.auditSessionId;
      }

      System.out.println("OPENING AUDIT SESSION....");
      // let's open an audit session (there should be only one)
      EngineResourceHandle sessionId = openAuditSession();
      System.out.println("OPENED AUDIT SESSION: " + sessionId);

      // make sure the same audit session was not previously closed as the
      // values can theoretically repeat themselves
      synchronized (this.reportReaders) {
        this.reportReaders.remove(this.auditSessionId);
      }

      // set the value
      this.auditSessionId = sessionId;

      // return the value
      return this.auditSessionId;

    } finally {
      System.out.println("RELEASING WRITE LOCK...");
      this.auditSessionLock.writeLock().unlock();
      System.out.println("RELEASED WRITE LOCK.");
    }
  }

  private void deleteAuditCacheFiles() {
    File auditCacheDir = this.auditCacheDir();

    File[] cacheFiles = auditCacheDir.listFiles();
    if (cacheFiles != null) {
      for (File cacheFile : cacheFiles) {
        cacheFile.delete();
      }
    }
  }

  private void invalidateAudits() {
    this.setCurrentAuditSummary(null);
    EngineResourceHandle sessionId;
    this.auditSessionLock.writeLock().lock();
    try {
      this.deleteAuditCacheFiles();

      if (this.auditSessionId == null) {
        log("*** NO AUDIT SESSION TO CLEANUP");
        return;
      }
      sessionId = this.auditSessionId;
      this.auditSessionId = null;
    } finally {
      auditSessionLock.writeLock().unlock();
    }
    final EngineResourceHandle targetSessionId = sessionId;
    log("INVALIDATING AUDITS FOR SESSION: " + targetSessionId);
    cleanupAuditSession(targetSessionId, true);
  }

  private void cleanupAuditSession(boolean invalidate) {
    cleanupAuditSession(null, invalidate);
  }

  private void cleanupAuditSession(EngineResourceHandle sessionId,
                                   boolean              invalidate)
  {
    auditSessionLock.writeLock().lock();
    try {
      if (sessionId == null && auditSessionId == null) {
        log("*** NO AUDIT SESSION TO CLEANUP");
        return;
      }
      if (sessionId == null) {
        sessionId = this.auditSessionId;
        this.auditSessionId = null;
      }
    } finally {
      auditSessionLock.writeLock().unlock();
    }

    log("*** CLOSING ALL OPEN AUDIT REPORTS FOR SESSION: " + sessionId);
    Map<String,ReportReader> map;
    synchronized (this.reportReaders) {
      map = this.reportReaders.get(sessionId);

      // mark this session as destroyed -- no further report readers
      reportReaders.put(sessionId, null);
    }

    if (map != null) {
      synchronized (map) {
        Iterator<Map.Entry<String, ReportReader>> iter = map.entrySet().iterator();

        while (iter.hasNext()) {
          Map.Entry<String, ReportReader> entry = iter.next();
          ReportReader reportReader = entry.getValue();
          if (reportReader == null) {
            iter.remove();
            continue;
          }
          if (reportReader.getAuditSessionId().equals(sessionId)) {
            if (invalidate) reportReader.invalidate();
            reportReader.closeReport();
            iter.remove();
          }
        }
      }
    }

    log("*** CLOSING AUDIT SESSION: " + sessionId);
    closeAuditSession(sessionId);
    log("*** CLOSED AUDIT SESSION: " + sessionId);
  }

  private void primeEngine() {
    log("*********** PRIMING ENGINE....");
    EngineRequest request = new EngineRequest(PRIME_ENGINE,
        getNextRequestId(),
        SYNC);

    EngineResponse response = this.sendSyncRequest(request);

    if (!response.isSuccessful()) {
      throw response.getException();
    }
    log("*********** PRIMED ENGINE.");
  }

  private EngineResourceHandle openAuditSession() {
    auditSessionLock.writeLock().lock();
    try {
      EngineRequest request = new EngineRequest(AUDIT_OPEN_SESSION,
          getNextRequestId(),
          SYNC);

      EngineResponse response = this.sendSyncRequest(request);

      if (!response.isSuccessful()) {
        throw response.getException();
      }
      long handleId = (Long) response.getResult();
      long engineAuthId = response.getEngineAuthenticationId();

      EngineResourceHandle handle = new EngineResourceHandle(handleId, engineAuthId);

      log("OPENED AUDIT SESSION: " + handle);

      return handle;

    } finally {
      auditSessionLock.writeLock().unlock();
    }
  }

  private void cancelAuditSession(EngineResourceHandle sessionId) {
    log("CANCEL AUDIT SESSION: " + sessionId);
    EngineRequest request = new EngineRequest(AUDIT_CANCEL_SESSION,
        getNextRequestId(),
        SYNC);

    request.setParameter(SESSION_ID, sessionId.getHandleId());
    request.setEngineAuthenticationId(sessionId.getEngineId());

    synchronized (this.primingMonitor) {
      this.cancelledAuditSessionId = sessionId;
    }

    auditSessionLock.readLock().lock();
    try {
      EngineResponse response = this.sendSyncRequest(request);

      if (!response.isSuccessful()) {
        throw response.getException();
      }
    } catch (EngineRequestCancelledException ignore) {
      // do nothing
    } finally {
      auditSessionLock.readLock().unlock();
    }
  }

  private void closeAuditSession(EngineResourceHandle sessionId) {
    log("CLOSING AUDIT SESSION: " + sessionId);
    EngineRequest request = new EngineRequest(AUDIT_CLOSE_SESSION,
        getNextRequestId(),
        SYNC);

    request.setParameter(SESSION_ID, sessionId.getHandleId());
    request.setEngineAuthenticationId(sessionId.getEngineId());

    try {
      EngineResponse response = this.sendSyncRequest(request);

      if (!response.isSuccessful()) {
        throw response.getException();
      }
    } catch (EngineRequestCancelledException ignore) {
      // do nothing
    }
  }

  private boolean firstAuditSummary = true;

  public ResultsSummary getAuditSummary() {
    return this.getAuditSummary(false);
  }

  public ResultsSummary getAuditSummary(boolean forceRefresh) {
    if (forceRefresh) {
      this.auditFailure = null;
      this.auditWasCached = false;
      log("****** AUDIT SUMMARY: forcing refresh");
      waitIfPrimingAudit(true);
      invalidateAudits();
    }

    log("****** AUDIT SUMMARY: requested");
    ResultsSummary result;
    boolean firstCall;
    synchronized (this.primingMonitor) {
      if (isPrimingAuditSummary()) {
        // allow for a cache to be deleted and recreated before failing hard
        if (this.auditFailure != null) {
          throw new RuntimeException(
              "Audit cannot be obtained due to previous failure.",
              this.auditFailure);
        }
        return null;
      }
      if (this.currentAuditSummary == null) {
        primeAuditSession();
      }
      firstCall = this.firstAuditSummary;
      this.firstAuditSummary = false;
      result = this.currentAuditSummary;
    }
    if (result == null && firstCall) {
      // wait up to half a second for the first audit summary call
      result = waitForAuditSummary(3000L);
      // allow for a cache to be deleted and recreated before failing hard
      synchronized (this.primingMonitor) {
        if (this.auditFailure != null) {
          throw new RuntimeException(
              "Audit cannot be obtained due to previous failure.",
              this.auditFailure);
        }
      }
      return result;
    } else {
      // allow for a cache to be deleted and recreated before failing hard
      synchronized (this.primingMonitor) {
        if (this.auditFailure != null) {
          throw new RuntimeException(
              "Audit cannot be obtained due to previous failure.",
              this.auditFailure);
        }
      }
      return result;
    }
  }

  protected ResultsSummary waitForAuditSummary() {
    return waitForAuditSummary(-1L);
  }

  private ResultsSummary waitForAuditSummary(long maxWait) {
    long start = System.currentTimeMillis();
    synchronized (primingMonitor) {
      ResultsSummary auditSummary = getAuditSummary();
      while ((auditSummary == null || isPrimingAuditSummary())
          && (maxWait < 0 || ((System.currentTimeMillis() - start) < maxWait))) {
        // determine how long we can wait on this iteration of the loop
        long maxDelay = 10000L;
        if (maxWait >= 0L) {
          maxDelay = maxWait - (System.currentTimeMillis() - start);
        }

        // check if we can wait no longer
        if (maxDelay <= 0L) {
          // just get the audit summary if it is there (one last try)
          auditSummary = getAuditSummary();
          continue; // this should end the loop
        }

        try {
          this.primingMonitor.wait(maxDelay);
        } catch (InterruptedException ignore) {
          // do nothing
        }
        auditSummary = getAuditSummary();
      }
      return auditSummary;
    }
  }

  private ResultsSummary primeAuditSummary(EngineResourceHandle sessionId) {
    ResultsSummary auditSummary = getCurrentAuditSummary();
    if (auditSummary != null) {
      return auditSummary;
    }

    AccessToken token = new AccessToken();
    synchronized (this.auditSummaryFile) {
      if (isResolving()) return null;

      log("****** AUDIT SUMMARY: priming");
      if (auditSummaryFile.exists()) {
        log("****** AUDIT SUMMARY: cache file exists");
        // get the last modified time
        long modified = auditSummaryFile.lastModified();

        // check the file last modified time
        long projectModified = getMostRecentTime(this.projectId, true);
        log("****** AUDIT SUMMARY: cache file modified at " + new Date(modified));
        log("****** AUDIT SUMMARY: project modified at " + new Date(projectModified));

        if (modified >= projectModified) {
          log("****** AUDIT SUMMARY: using cached file");
          try (FileInputStream fis = new FileInputStream(auditSummaryFile);
               InputStreamReader isr = new InputStreamReader(fis, "UTF-8");
               BufferedReader br = new BufferedReader(isr))
          {
            int length = ((int) auditSummaryFile.length()) + 10;
            StringBuilder sb = new StringBuilder(length);
            char[] buffer = new char[1000];
            int readCount;
            while ((readCount = br.read(buffer)) >= 0) {
              sb.append(buffer, 0, readCount);
            }

            String jsonText = sb.toString();

            StringReader sr = new StringReader(jsonText);
            JsonReader jsonReader = Json.createReader(sr);
            JsonObject jsonObject = jsonReader.readObject();

            ResultsSummary result
                = ResultsSummary.parseResultsSummary(null, jsonObject, token);

            result.initValue("projectId", this.getProjectId(), token);
            Date lastModified = new Date(auditSummaryFile.lastModified());
            result.initValue("lastModified", lastModified, token);

            this.auditWasCached = true;
            this.auditFailure = null;
            result = setCurrentAuditSummary(result);
            return result;

          } catch (IOException e) {
            log("****** AUDIT SUMMARY: error using cached file... deleting....");
            log(e);
            this.deleteAuditCacheFiles();
          }
        }
      }
      this.auditWasCached = false;
      if (this.isAborted()) return null;

      // delete the cached files for good measure
      log("****** AUDIT SUMMARY: deleting any cached files");
      this.deleteAuditCacheFiles();

      // get the audit summary
      log("****** AUDIT SUMMARY: generating new audit summary");

      if (sessionId == null) sessionId = getAuditSessionId();
      log("GETTING AUDIT SUMMARY WITH SESSION ID: " + sessionId);
      synchronized (primingMonitor) {
        if (isPrimingAuditSummary()) return null;
        beginPrimingAuditSummary();
      }
      log("OBTAINING AUDIT SESSION READ LOCK....");
      auditSessionLock.readLock().lock();
      log("OBTAINED AUDIT SESSION READ LOCK.");
      try {
        EngineRequest request = new EngineRequest(AUDIT_SUMMARY_DATA,
            getNextRequestId(),
            SYNC);

        request.setParameter(SESSION_ID, sessionId.getHandleId());
        request.setEngineAuthenticationId(sessionId.getEngineId());

        log("SENDING REQUEST FOR AUDIT SUMMARY....");
        long startEngineAuditSummary = System.currentTimeMillis();
        EngineResponse response = this.sendSyncRequest(request);
        if (this.isAborted()) return null;
        long endEngineAuditSummary = System.currentTimeMillis();
        log("RECEIVED RESPONSE FOR AUDIT SUMMARY.");
        log("** PERFORMANCE ** ENGINE AUDIT SUMMARY TIME: "
            + (endEngineAuditSummary - startEngineAuditSummary) + "ms");

        if (response == null) {
          throw new IllegalStateException(
              "Did not get response for audit summary data.  Possibly engine is "
                  + "shutting down or aborting.  this.projectId=[ " + this.projectId + " ]");
        }

        if (!response.isSuccessful()) {
          throw response.getException();
        }

        String jsonText = (String) response.getResult();
        StringReader sr = new StringReader(jsonText);
        JsonReader jsonReader = Json.createReader(sr);
        JsonObject jsonObject = jsonReader.readObject();

        ResultsSummary result = ResultsSummary.parseResultsSummary(null,
                                                                   jsonObject,
                                                                   token);

        result.initValue("projectId", this.getProjectId(), token);

        // cache the result
        log("****** AUDIT SUMMARY: creating cached file...");
        try (FileOutputStream fos = new FileOutputStream(auditSummaryFile);
             OutputStreamWriter osw = new OutputStreamWriter(fos, "UTF-8")) {
          osw.write(jsonText);
          osw.flush();
        } catch (IOException e) {
          log(e);
          // ignore the exception otherwise
        }
        log("****** AUDIT SUMMARY: created cached file.");

        Date lastModified = new Date(auditSummaryFile.lastModified());
        result.initValue("lastModified", lastModified, token);

        setCurrentAuditSummary(result);

        return result;

      } finally {
        auditSessionLock.readLock().unlock();
        endPrimingAuditSummary();
      }
    }
  }

  private String performAuditRequest(EngineOperation operation,
                                     String fromDataSource,
                                     String toDataSource,
                                     int matchLevel) {
    EngineResourceHandle sessionId = getAuditSessionId(true);
    auditSessionLock.readLock().lock();
    try {
      EngineRequest request = new EngineRequest(operation,
          getNextRequestId(),
          SYNC);

      request.setParameter(SESSION_ID, sessionId.getHandleId());
      request.setParameter(FROM_DATA_SOURCE, fromDataSource);
      request.setParameter(TO_DATA_SOURCE, toDataSource);
      request.setParameter(MATCH_LEVEL, matchLevel);
      request.setEngineAuthenticationId(sessionId.getEngineId());

      EngineResponse response = this.sendSyncRequest(request);

      if (!response.isSuccessful()) {
        throw response.getException();
      }

      return (String) response.getResult();
    } finally {
      auditSessionLock.readLock().unlock();
    }
  }

  public Map<String,Integer> getUsedMatchKeys(String              fromDataSource,
                                              String              toDataSource,
                                              int                 matchLevel)
  {
    return this.getAuditBaseIndexCounts(MATCH_KEY,
                                        fromDataSource,
                                        toDataSource,
                                        matchLevel);
  }

  public Map<String,Integer> getUsedPrinciples(String             fromDataSource,
                                               String             toDataSource,
                                               int                matchLevel)
  {
    return this.getAuditBaseIndexCounts(PRINCIPLE,
                                        fromDataSource,
                                        toDataSource,
                                        matchLevel);
  }

  private Map<String,Integer> getAuditBaseIndexCounts(
      String              key,
      String              fromDataSource,
      String              toDataSource,
      int                 matchLevel)
  {
    // check if reinitializing and just return null to inform client to check
    // back later
    if (this.isReinitializing()) {
      log("***** EngineProcess: AUDIT REPORT MATCH KEYS REQUESTED WHILE REINITIALIZING");
      return Collections.emptyMap();
    }

    log("***** EngineProcess: GETTING REPORT KEY....");
    String reportKey = formatReportKey(fromDataSource, toDataSource, matchLevel);

    log("***** EngineProcess: GOT REPORT KEY: " + reportKey);

    log("****** AUDIT REPORT: requested " + reportKey);

    ReportReader reportReader = getAuditReportReader(reportKey);

    log("***** EngineProcess: GOT REPORT READER: "
            + (reportReader == null ? "null" : "existing"));

    // check if there is no report reader
    if (reportReader == null) {
      // check if we are already priming the audit
      if (!isPrimingAudit()) {
        // if not priming the audit, prime it
        primeAuditSession();
      }

      // get the report reader and create it if needed
      reportReader = this.obtainAuditReportReader(fromDataSource,
                                                  toDataSource,
                                                  matchLevel);

      // prioritize this report reader since it was requested
      this.reportSender.prioritizeThread(reportReader);

      // return null for now
      if (this.isPrimingAuditSummary()) {
        return Collections.emptyMap();
      }
    }

    // check if already priming, but not ready
    if (!reportReader.isReady()) {
      this.reportSender.prioritizeThread(reportReader);
      return Collections.emptyMap();
    }

    log("***** EngineProcess: GETTING MATCH KEYS....");
    return reportReader.getBaseIndexCounts(key);
  }

  private Map<String, ReportReader> getAuditReportReaders(EngineResourceHandle sessionId) {
    sessionId = (sessionId != null) ? sessionId : this.getAuditSessionId(false);

    if (sessionId == null) {
      return null;
    }

    synchronized (this.reportReaders) {
      if (!this.reportReaders.containsKey(sessionId)) {
        this.reportReaders.put(sessionId, new HashMap<>());
      }
      return reportReaders.get(sessionId);
    }
  }

  ReportReader getAuditReportReader(String reportKey) {
    Map<String, ReportReader> readers = this.getAuditReportReaders(null);
    if (readers == null) {
      return null;
    }

    synchronized (readers) {
      ReportReader reader = readers.get(reportKey);
      if (reader != null && reader.isInvalid()) {
        readers.remove(reportKey);
        reader = null;
      }
      return reader;
    }
  }

  private ReportReader obtainAuditReportReader(String fromDataSource, String toDataSource, int matchLevel) {
    EngineResourceHandle sessionId = null;
    Map<String, ReportReader> reportReaders = null;
    while (reportReaders == null) {
      sessionId = this.getAuditSessionId(true);
      reportReaders = this.getAuditReportReaders(sessionId);
    }
    synchronized (reportReaders) {
      String reportKey = formatReportKey(fromDataSource, toDataSource, matchLevel);
      ReportReader reader = reportReaders.get(reportKey);
      if (reader != null && reader.isInvalid()) {
        reportReaders.remove(reportKey);
        reader = null;
      }

      if (reader == null) {
        reader = new ReportReader(this,
            this.reportSender,
            sessionId,
            fromDataSource,
            toDataSource,
            matchLevel);
        reportReaders.put(reportKey, reader);
      }

      return reader;
    }
  }

  static String formatBaseCacheFileName(String reportKey) {
    try {
      StringBuilder sb = new StringBuilder("audit-report-");
      MessageDigest md = MessageDigest.getInstance("SHA-256");
      md.update(reportKey.getBytes("UTF-8"));
      byte[] digest = md.digest();

      String digestString = BASE64_ENCODER.encodeToString(digest);
      digestString = digestString.replaceAll("^(.*[^-=])[-=]+$", "$1");

      sb.append(digestString);
      sb.append("-");

      return sb.toString();

    } catch (Exception e) {
      log("**** FAILED TO GENERATE CACHE FILE NAME");
      log(e);
      throw new RuntimeException(e);
    }
  }

  void closeAuditReport(EngineResourceHandle sessionId, long reportId) {
    auditSessionLock.readLock().lock();
    try {
      EngineRequest request = new EngineRequest(AUDIT_CLOSE_REPORT, getNextRequestId(), SYNC);

      request.setParameter(SESSION_ID, sessionId.getHandleId());
      request.setParameter(REPORT_ID, reportId);
      request.setEngineAuthenticationId(sessionId.getEngineId());

      EngineResponse response = this.sendSyncRequest(request);

      if (!response.isSuccessful()) {
        throw response.getException();
      }

    } catch (EngineRequestCancelledException ignore) {
      // do nothing

    } finally {
      auditSessionLock.readLock().unlock();
    }
  }

  static String formatReportKey(String fromDataSource,
                                String toDataSource,
                                int matchLevel) {
    return fromDataSource.replaceAll("([|\\\\])", "\\$1")
        + "|"
        + toDataSource.replaceAll("([|\\\\])", "\\$1")
        + "|"
        + matchLevel;
  }

  public String getAuditReportPage(String             fromDataSource,
                                   String             toDataSource,
                                   int                matchLevel,
                                   int                fromIndex,
                                   int                toIndex,
                                   Map<String,Filter> filterMap,
                                   boolean            forExport)
  {
    // check if reinitializing and just return null to inform client to check
    // back later
    if (this.isReinitializing()) {
      log("***** EngineProcess: AUDIT REPORT PAGE REQUESTED WHILE REINITIALIZING");
      return "{\"AUDIT_PAGE\": null}";
    }
    synchronized (this.primingMonitor) {
      if (this.currentAuditSummary == null) {
        // no current audit summary, but report is being requested
        throw new IllegalStateException("Audit summary is not available and "
          + "therefore the associated audit reports are not available.");
      }
    }

    log("***** EngineProcess: GETTING REPORT KEY....");
    String reportKey = formatReportKey(fromDataSource, toDataSource, matchLevel);

    log("***** EngineProcess: GOT REPORT KEY: " + reportKey);

    log("****** AUDIT REPORT: requested " + reportKey);

    ReportReader reportReader = getAuditReportReader(reportKey);

    log("***** EngineProcess: GOT REPORT READER: "
        + (reportReader == null ? "null" : "existing"));

    // truncate to the configured sample size if not for export
    if (!forExport) {
      int sampleSize = WorkbenchConfig.getAuditSampleSize();
      if (sampleSize > 0 && toIndex > sampleSize) {
        toIndex = sampleSize;
      }
    }

    // check if there is no report reader
    if (reportReader == null) {
      // check if we are already priming the audit
      if (!isPrimingAudit()) {
        // if not priming the audit, prime it
        primeAuditSession();
      }

      // get the report reader and create it if needed
      reportReader = this.obtainAuditReportReader(fromDataSource,
                                                  toDataSource,
                                                  matchLevel);

      // prioritize this report reader since it was requested
      this.reportSender.prioritizeThread(reportReader);

      // return null for now
      if (this.isPrimingAuditSummary()) {
        return "{\"AUDIT_PAGE\": null}";
      }
    }

    // check if already priming, but not ready
    if (!reportReader.isReady()) {
      this.reportSender.prioritizeThread(reportReader);
      return "{\"AUDIT_PAGE\": null}";
    }

    log("***** EngineProcess: GETTING AUDIT REPORT PAGE ( " + fromIndex
        + " / " + toIndex + " )....");
    String result = reportReader.getAuditReportPage(fromIndex,
                                                    toIndex,
                                                    filterMap);

    log("***** EngineProcess: GOT AUDIT REPORT PAGE");
    return result;
  }

  private String obtainAuditReportPage(String             fromDataSource,
                                       String             toDataSource,
                                       int                matchLevel,
                                       int                fromIndex,
                                       int                toIndex,
                                       Map<String,Filter> filterMap)
  {
    // check if reinitializing and just return null to inform client to check
    // back later
    if (this.isReinitializing()) {
      log("***** EngineProcess: AUDIT REPORT PAGE REQUESTED WHILE REINITIALIZING");
      return null;
    }

    log("***** EngineProcess: GETTING REPORT KEY....");
    String reportKey = formatReportKey(fromDataSource, toDataSource, matchLevel);

    log("***** EngineProcess: GOT REPORT KEY: " + reportKey);

    log("***** EngineProcess: GETTING REPORT READER: " + reportKey);
    ReportReader reportReader = obtainAuditReportReader(fromDataSource,
                                                        toDataSource,
                                                        matchLevel);

    log("***** EngineProcess: GOT REPORT READER: "
        + (reportReader == null ? "null" : "existing"));

    // check if not yet ready and let client know they need to check back
    if (reportReader == null || !reportReader.isReady()) {
      log("****** EngineProcess: AUDIT REPORT NOT YET READY: " + reportKey);
      return null;
    }

    log("***** EngineProcess: GETTING AUDIT REPORT PAGE ( " + fromIndex
        + " / " + toIndex + " )....");
    String result = reportReader.getAuditReportPage(fromIndex,
                                                    toIndex,
                                                    filterMap);

    log("***** EngineProcess: GOT AUDIT REPORT PAGE");
    return result;
  }

  public Integer getAuditGroupCount(String fromDataSource,
                                    String toDataSource,
                                    int    matchLevel)
  {
    // check if reinitializing and just return null to inform client to check
    // back later
    if (this.isReinitializing()) {
      log("***** EngineProcess: AUDIT REPORT COUNT REQUESTED WHILE REINITIALIZING");
      return null;
    }

    log("***** EngineProcess: GETTING REPORT KEY....");
    String reportKey = formatReportKey(fromDataSource, toDataSource, matchLevel);

    log("***** EngineProcess: GOT REPORT KEY: " + reportKey);

    log("***** EngineProcess: GETTING REPORT READER: " + reportKey);
    ReportReader reportReader = this.obtainAuditReportReader(fromDataSource,
                                                             toDataSource,
                                                             matchLevel);

    // check if not yet ready and let client know they need to check back
    if (!reportReader.isReady()) {
      log("***** EngineProcess: REPORT NOT YET READY: " + reportKey);
      return null;
    }

    // return the count
    return reportReader.getAuditGroupCount();
  }

  EngineResponse sendSyncRequest(EngineRequest request) {
    engineLock.readLock().lock();
    try {
      Long requestId = request.getRequestId();
      synchronized (syncRequests) {
        syncRequests.put(requestId, request);
      }
      connector.writeRequest(request);
      synchronized (syncRequests) {
        syncRequests.notifyAll();
      }

      synchronized (syncResponses) {
        while (!syncResponses.containsKey(requestId)) {
          try {
            syncResponses.wait(2000L);
            if (this.connector.isClosed() || this.isAborted()) {
              IllegalStateException e = new IllegalStateException(
                  "Communication with the engine process was interrupted.  "
                      + "Check the logs for details.  request=[ "
                      + request + " ]");
              ServerError se = new ServerError(true, e);
              ServerErrorLog.recordProjectError(this.getProjectId(), se);
              throw e;
            }
          } catch (InterruptedException ignore) {
            // ignore
          }
        }
        EngineResponse response = syncResponses.remove(requestId);
        if (response == null) {
          throw new IllegalStateException(
              "It appears the engine process was aborted due to an error.  "
                  + "Check the logs for details.  request=[ " + request + " ]");
        }
        return response;
      }

    } catch (EngineRequestCancelledException e) {
      log("ENGINE REQUEST CANCELLED DUE TO RESTART: " + request.getOperation());
      synchronized (syncRequests) {
        syncRequests.remove(request.getRequestId());
        syncRequests.notifyAll();
      }
      throw e;

    } finally {
      engineLock.readLock().unlock();
    }
  }

  private Map<Long, EngineResponse> sendSyncRequests(Collection<EngineRequest> requests) {
    Map<Long, EngineRequest> requestMap = new LinkedHashMap<>();
    for (EngineRequest request : requests) {
      requestMap.put(request.getRequestId(), request);
    }
    synchronized (syncRequests) {
      syncRequests.putAll(requestMap);
    }

    requestMap.values().forEach(request -> connector.writeRequest(request));

    synchronized (syncRequests) {
      syncRequests.notifyAll();
    }

    Set<Long> requestIds = requestMap.keySet();
    synchronized (syncResponses) {
      while (!syncResponses.keySet().containsAll(requestIds)) {
        try {
          syncResponses.wait(2000L);
        } catch (InterruptedException ignore) {
          // ignore
        }
      }
      Map<Long, EngineResponse> result = new LinkedHashMap<>();
      for (Long requestId : requestIds) {
        EngineResponse response = syncResponses.remove(requestId);
        if (response == null) {
          throw new IllegalStateException(
              "It appears the engine process was aborted due to an error.  "
                  + "Check the logs for details.  request=[ "
                  + requestMap.get(requestId) + " ]");
        }
        result.put(requestId, response);
      }
      return result;
    }
  }

  static long getMostRecentTime(long projectId, boolean resolved) {
    try {
      ProjectDataAccess dataAccess = new ProjectDataAccess();
      Project project = dataAccess.selectProject(projectId);
      List<ProjectFile> files = project.getFiles();
      long maxTime = 0L;
      for (ProjectFile file : files) {
        // skip files with no loaded reords if resolved flag
        if (resolved && file.getLoadedRecordCount() == 0) {
          continue;
        }
        long lastModified = file.getLastModified().getTime();
        if (lastModified > maxTime) maxTime = lastModified;
      }
      return maxTime;
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }


  public static void main(String[] args) {
    try {
      ProjectDataAccess projDA = new ProjectDataAccess();
      List<Project> projects = projDA.selectProjects();
      if (args.length == 0) {
        System.out.println();
        System.out.println("Must specify numeric project ID which should be one of: ");
        for (Project p : projects) {
          System.out.println(" - " + p.getId() + " (" + p.getName() + ")");
        }
        System.out.println();
        return;
      }
      long projectId = Long.parseLong(args[0]);
      Project project = projDA.selectProject(projectId);
      if (project == null) {
        System.err.println();
        System.err.println("Unrecognized project ID: " + projectId);
        System.err.println();
        System.err.println("Valid project IDs are:");
        for (Project p : projects) {
          System.err.println(" - " + p.getId() + " (" + p.getName() + ")");
        }
        System.err.println();
        System.exit(1);
      }
      List<ProjectFile> files = project.getFiles();
      if (files.size() == 0) {
        System.out.println();
        System.out.println("The specified project: " + project.getName()
            + "(" + project.getId() + ") has no files to resolve.");
        System.err.println();
        return;
      }

      System.out.println();
      System.out.println("Enqueueing files: ");
      long[] fileIds = new long[files.size()];
      for (int index = 0; index < fileIds.length; index++) {
        ProjectFile file = files.get(index);
        fileIds[index] = file.getId();
        System.out.println(" - " + file.getName());
      }
      System.out.println();

      EngineProcess process = EngineProcess.prepareInstance(projectId, fileIds);

      process.join();

    } catch (Throwable t) {
      t.printStackTrace();
    }
  }

  public static EngineProcess getInstance(long projectId) {
    return EngineProcess.prepareInstance(projectId);
  }

  public static EngineProcess prepareInstance(long projectId, long... fileIds) {
    try {
      EngineProcess engineProcess;
      EngineProcess staleProcess = null;
      synchronized (INSTANCE_MAP) {
        engineProcess = INSTANCE_MAP.get(projectId);
        if (engineProcess != null
            && (engineProcess.aborted || engineProcess.completed)) {
          staleProcess = engineProcess;
        }
      }
      if (staleProcess != null) {
        staleProcess.join();
      }
      synchronized (INSTANCE_MAP) {
        engineProcess = INSTANCE_MAP.get(projectId);
        if (engineProcess == staleProcess) {
          engineProcess = null;
          INSTANCE_MAP.remove(projectId);
        }
        if (engineProcess == null) {
          engineProcess = new EngineProcess(projectId);
          INSTANCE_MAP.put(projectId, engineProcess);
        }
      }
      for (long fileId : fileIds) {
        engineProcess.loadFile(fileId);
      }


      return engineProcess;

    } catch (LoggedServerErrorException e) {
      // already logged
      throw e;
    } catch (RuntimeException e) {
      log(e);
      throw e;
    } catch (Exception e) {
      log(e);
      throw new RuntimeException(e);
    }
  }

  private EngineProcess(long projectId) throws Exception {
    this.projectId = projectId;
    this.accessToken = new AccessToken();
    this.reportSender
        = new SendPrioritizer(this, 4, this.accessToken);
    this.dataSourceCodes = new HashSet<>();
    this.fileQueue = new LinkedList<>();
    this.pendingRequests = new HashMap<>();
    this.pendingRedos = new HashMap<>();
    this.syncRequests = new HashMap<>();
    this.syncResponses = new HashMap<>();
    this.monitor = new Object();
    this.requestIdMonitor = new Object();
    this.fileDataAccess = new ProjectFileDataAccess();
    this.redoDataAccess = new RedoDataAccess();
    this.projectDataAccess = new ProjectDataAccess();

    File projectDir = Workbench.getProjectDirectory(projectId);
    File auditCacheDir = auditCacheDir(projectId);
    if (!auditCacheDir.exists()) {
      auditCacheDir.mkdirs();
    }
    this.engineLogFile = new File(projectDir, "engine-api.log");
    this.auditSummaryFile = new File(auditCacheDir, "audit-summary-v2.json");
    this.connector = new ServerSocketConnector(this, "RESOLVER");
    this.switchboard = new Switchboard(this, this.connector);
    String elasticSearchIndex = "api-project-" + projectId + "-entities";

    //indexer = new Indexer(ELASTIC_SEARCH_CLUSTER_NAME,
    //    ELASTIC_SEARCH_HOST,
    //    ELASTIC_SEARCH_CLIENT_PORT,
    //    elasticSearchIndex,
    //    this);


    this.project = this.projectDataAccess.selectProject(projectId);


    setName("EngineProcess-" + projectId
        + "-" + Math.abs(System.identityHashCode(this)));

    log("STARTING ENGINE....");
    startEngine();
    log("STARTED ENGINE.");
    this.syncReceiver = new SyncReceiver(this,
        this.switchboard,
        this.syncRequests,
        this.syncResponses);
    log("STARTED SYNC RECEIVER");
    log("PURGE CHECK POINT");
    if (this.pendingPurge) {
      log("PURGING REPOSITORY FOR PROJECT " + this.projectId);
      purgeProjectRepository();
      this.pendingPurge = false;
      log("PURGING REPOSITORY COMPLETED FOR PROJECT " + this.projectId);
    }

    start();
    this.reportSender.start();
  }

  public boolean isExternal() {
    return project.isExternal();
  }

  private void waitForThreads(List<Thread> threads) {
    if (threads == null) return;
    for (Thread t: threads) {
      try {
        t.join();
      } catch (InterruptedException ignore) {
        log(ignore);
      }
    }
  }

  private void stopEngineAndWait(boolean terminate) {
    List<Thread> disposers = stopEngine(terminate);
    waitForThreads(disposers);
  }

  private List<Thread> stopEngine(boolean terminate) {
    List<Thread> disposers = new ArrayList<>(2);
    log("SYNCHRONIZING ON PROCESS MONITOR....");
    synchronized (monitor) {
      if (terminate) {
        //log("WAITING FOR INDEXER THREAD TO COMPLETE....");
        //this.indexer.complete();
        //try {
        //  this.indexer.join();
        //} catch (InterruptedException ignore) {
        //  ignore.printStackTrace();
        //}
        log("JOIN WITH INDEXER COMPLETED.");
      }

      log("SYNCHRONIZED ON PROCESS MONITOR.");
      log("CHECKING IF ENGINE IS RUNNING....");
      if (isEngineRunning()) {
        log("ENGINE IS RUNNING.  START THE DISPOSER...");
        cleanupAuditSession(false);
        connector.setClientProcess(null);
        disposers.add(new EngineProcessDisposer(this,
                                                "PRIMARY",
                                                terminate,
                                                this.connector.detach(),
                                                this.process,
                                                this.outLogger,
                                                this.errLogger,
                                                null,
                                                //QUERY_DESTROY,
                                                AUDIT_DESTROY,
                                                DESTROY));
        log("STARTED THE ENGINE DISPOSER.");
        this.process = null;
        this.outLogger = null;
        this.errLogger = null;
        this.engineRunning = false;
      }

    }
    if (disposers.size() == 0) return null;
    return disposers;
  }

  private void startEngine() {
    try {
      this.doStartEngines();

    } catch (LoggedServerErrorException e) {
      // already logged for reporting back to the user
      throw e;

    } catch (Exception e) {
      // log any other exception so the user is aware of the problem
      ServerError se = new ServerError(true, e);
      ServerErrorLog.recordProjectError(this.projectId, se);
      this.abort();
      this.shutdown(false);
      throw new LoggedServerErrorException(
          "Engine process died before connecting.  Check logs.", e);
    }
  }

  private File prepareEngineConfigFiles(long projectId) {
    synchronized (monitor) {
      try {
        Set<String> dsrcCodes = dataSourceCodes;
        ProjectDataAccess projectDA = new ProjectDataAccess();
        projectDA.selectProject(projectId);

        File projectDir = Workbench.getProjectDirectory(projectId);

        File iniFile = new File(projectDir, "g2.ini");
        File configFile = new File(projectDir, "g2-config.json");
        File logCfgFile = new File(projectDir, "g2-logging.cfg");

        ConfigurationFileUtilities.createIniFile(
            iniFile, projectDir, configFile, logCfgFile);

        ConfigurationFileUtilities.createLogConfigFile(logCfgFile, projectDir);

        EngineConfig engineConfig
          = EngineConfig.getProjectConfig(projectId, false, dsrcCodes);

        configTimestamp = engineConfig.getLastModified();

        engineConfig.close();

        return iniFile;

      } catch (RuntimeException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  private void doStartEngines() throws IOException {
    log("DOING doStartEngines()");
    long projectId = this.projectId;
    int port = connector.getPort();

    synchronized (monitor) {
      if (engineRunning) {
        log("ENGINE RUNNING.  ABORTING DOSTARTENGINES");
        return;
      }

      Runtime runtime = Runtime.getRuntime();

      File projectDir = Workbench.getProjectDirectory(projectId);

      File iniFile;
      if (this.isExternal()) {
        iniFile = new File(projectDir, "g2.ini");

      } else {
        if (!new File(projectDir, "G2C.db").exists()) {
          File templateDB = new File(TEMPLATE_DB_PATH);
          copyTemplate(templateDB, new File(projectDir, "G2C.db"));
          copyTemplate(templateDB, new File(projectDir, "G2_RES.db"));
          copyTemplate(templateDB, new File(projectDir, "G2_LIB_FEAT.db"));
        } else if (new ProductVersionUpdateDetector().hasOutdatedProductVersions(projectId)) {
          File templateDB = new File(TEMPLATE_DB_PATH);
          overwrite(templateDB, new File(projectDir, "G2C.db"));
          overwrite(templateDB, new File(projectDir, "G2_RES.db"));
          overwrite(templateDB, new File(projectDir, "G2_LIB_FEAT.db"));
          this.pendingPurge = true;
          log("PROJECT REPOSITORY TO BE PURGED ON PRODUCT VERSION CHANGE, PROJECT " + this.projectId);
        }

        if (EngineConfig.isBaseConfigStale(projectId)) {
          long engineVersion = ConfigReader.getEngineCompatibilityVersion();
          long projVersion = ConfigReader.getProjectCompatibilityVersion(projectId);

          log("ENGINE CONFIG VERSION " + engineVersion);
          log("PROJECT CONFIG VERSION " + projVersion);
          if (projVersion != engineVersion) {
            this.pendingPurge = true;
            log("PROJECT REPOSITORY TO BE PURGED ON CONFIG COMPATIBILITY VERSION CHANGE, PROJECT " + this.projectId);
          }
          EngineConfig.refreshBaseConfig(projectId, true);
        }

        iniFile = prepareEngineConfigFiles(projectId);

        if (pendingPurge) {
          project.setValue("upgraded", true, AccessToken.getThreadAccessToken());
          updateProject(project);
        }
      }

      long authId = PRNG.nextLong();

      int coreCount = runtime.availableProcessors();
      int threadCount = (2 * coreCount) - 1;
      if (threadCount > 8) threadCount = 8;

      String engineConcurrency = System.getProperty("G2_ENGINE_CONCURRENCY");
      if (engineConcurrency == null) {
        engineConcurrency = "" + threadCount;
      } else {
        try {
          threadCount = Integer.parseInt(engineConcurrency);
        } catch (Exception ignore) {
          // do nothing
        }
      }
      engineThreadCount = threadCount;

      String logOperations = System.getProperty("LOG_OPERATIONS");
      if (logOperations != null) {
        logOperations = logOperations.trim();
        if (logOperations.length() == 0) logOperations = null;
      }

      String logOption = "-DLOG_OPERATIONS=";
      if (logOperations != null) {
        logOption = "-DLOG_OPERATIONS=" + logOperations;
      }

      String[] cmdArray = new String[] {
        JRE_PATH,
        "-cp",
        JAR_PATH + PATH_SEP + JAVA_LIBRARY_PATH,
        "-Djava.library.path=" + JAVA_LIBRARY_PATH,
        "-DG2_ENGINE_CONCURRENCY=" + engineConcurrency,
        logOption,
        "-Xmx2000M",
        "-XX:+HeapDumpOnOutOfMemoryError",
        "-XX:HeapDumpPath=" + projectDir.toString(),
        EngineMain.class.getName(),
        "" + authId,
        "project_" + projectId,
        iniFile.toString(),
        (ENGINE_VERBOSE_LOGGING ? "true" : "false"),
        "" + port,
        USER_DATA_DIR.toString(),
        EngineType.RESOLVER.toString(),
        EngineType.AUDITOR.toString()//,
        //EngineType.QUERIST.toString(),
        //ELASTIC_SEARCH_CLUSTER_NAME,
        //ELASTIC_SEARCH_HOST,
        //"" + ELASTIC_SEARCH_CLIENT_PORT,
        //this.elasticSearchIndex
      };

      StringBuilder sb = new StringBuilder();
      for (String token : cmdArray) {
        sb.append(token).append(" ");
      }
      log(sb.toString());

      Map<String,String> origEnv = System.getenv();
      List<String> envList = new ArrayList<>(origEnv.size()+4);
      for (Map.Entry<String,String> entry : origEnv.entrySet()) {
        String envKey = entry.getKey();
        String envVal = entry.getValue();
        if (envKey.equalsIgnoreCase("TMP")) continue;
        if (envKey.equalsIgnoreCase("TEMP")) continue;
        if (envKey.equalsIgnoreCase("SENZING_ROOT")) continue;
        if (envKey.equalsIgnoreCase("PATH")) {
          envVal = BIN_PATH + PATH_SEP + LIB_PATH + PATH_SEP + envVal;
        }
        envList.add(envKey + "=" + envVal);
      }
      envList.add("SENZING_ROOT=" + SENZING_ROOT);
      envList.add("TMP=" + USER_TEMP_DIR);
      envList.add("TEMP=" + USER_TEMP_DIR);
      String[] env = envList.toArray(new String[envList.size()]);

      if (!engineRunning) {
        connector.setClientProcess(null);
        connector.setAuthenticationId(authId);
        log("STARTING G2 ENGINE SUB-PROCESS....");
        process =  runtime.exec(cmdArray, env, projectDir);
        connector.setClientProcess(process);
        outLogger = new StreamLogger(process.getInputStream());
        errLogger = new StreamLogger(process.getErrorStream());
        if (!connector.waitForEngine()) {
          LoggedServerErrorException e = new LoggedServerErrorException(
              "Engine process died before connecting.  Check logs.");
          ServerError se = new ServerError(true, e);
          ServerErrorLog.recordProjectError(this.projectId, se);
          this.abort();
          this.shutdown(false);
          throw e;
        }
        log("STARTED G2 ENGINE SUB-PROCESS....");
        engineRunning = true;
      }
    }
  }

  private void copyTemplate(File templateDB, File destination) throws IOException {
    Files.copy(templateDB.toPath(), destination.toPath(), COPY_ATTRIBUTES);
  }

  private void overwrite(File templateDB, File destination) throws IOException {
    Files.copy(templateDB.toPath(), destination.toPath(), REPLACE_EXISTING);
    new File(destination.getParentFile(), destination.getName() + "-shm").delete();
    new File(destination.getParentFile(), destination.getName() + "-wal").delete();
  }

  private void primeAuditReports(List<ProjectFile>  files,
                                 ResultsSummary     auditSummary)
  {
    // check if already priming
    if (isPrimingAuditDetails() || isResolving()) {
      return;
    }
    Set<String> dataSources = new HashSet<>();
    Set<String> reportKeys = new HashSet<>();

    // find the resolved data sources
    if (this.isExternal()) {
      dataSources.addAll(auditSummary.getDataSources());

    } else {
      for (ProjectFile file : files) {
        if (file.isResolved() && file.getResolvedRecordCount() > 0) {
          dataSources.add(file.getDataSource());
        }
      }
    }

    ThreadJoinerPool joinerPool = new ThreadJoinerPool(3);

    try {
      final int reportCount = dataSources.size() * dataSources.size() * 3;
      final int[] completedReportCount = { 0 };
      final long[] totalReportDuration = { 0L };
      final int[] totalAuditGroupCount = { 0 };
      final Exception[] failure = { null };
      // iterate over from data SOURCES
      for (String fromDsrc : dataSources) {
        if (this.isAborted()) break;

        // iterate over to data sources
        for (String toDsrc : dataSources) {
          if (this.isAborted()) break;

          // iterate over match levels
          for (int matchLevel = 1; matchLevel < 4; matchLevel++) {
            if (this.isAborted()) break;

            // format the report key
            String key = formatReportKey(fromDsrc, toDsrc, matchLevel);

            // check if the key has already been handled
            if (reportKeys.contains(key)) {
              continue;
            }
            reportKeys.add(key);

            // get the audit report page to trigger report generation
            ReportReader reportReader = getAuditReportReader(key);
            if (reportReader == null) {
              obtainAuditReportPage(fromDsrc, toDsrc, matchLevel, 0, 10, null);

            } else if (reportReader.isComplete()) {
              log("AUDIT DETAIL ALREADY PRIMED: "
                      + fromDsrc + " / " + toDsrc + " / " + matchLevel);
              continue;

            } else {
              log("** WARNING ** AUDIT DETAIL ALREADY PRIMING: "
                      + fromDsrc + " / " + toDsrc + " / " + matchLevel);
              continue;
            }

            // get the report reader
            reportReader = getAuditReportReader(key);

            // check if the report is cached
            if (reportReader.isCached()) {
              log("REPORT IS CACHED: " + fromDsrc + " / " + toDsrc
                      + " / " + matchLevel);
              // don't need to join with ReportReader since it is not talking
              // to the engine... move on to the next one
              continue;
            }

            // join with the reader (this blocks if four are outstanding)
            joinerPool.join(reportReader, (thread) -> {
              ReportReader reader = (ReportReader) thread;
              completedReportCount[0]++;
              totalReportDuration[0] += reader.getDuration();
              totalAuditGroupCount[0] += reader.getAuditGroupCount();

              long duration = (long) totalReportDuration[0];
              long completeCount = (long) completedReportCount[0];
              long groupCount = (long) totalAuditGroupCount[0];
              long avgR = (completeCount == 0) ? 0 : (duration / completeCount);
              long avgG = (groupCount == 0) ? 0 : (duration / groupCount);

              log(completedReportCount[0] + " OF " + reportCount
                      + " AUDIT REPORTS COMPLETED IN "
                      + totalReportDuration[0] + "ms ("
                      + avgR + "ms AVG PER REPORT / "
                      + avgG + "ms AVG PER AUDIT GROUP)");

              Exception exception = reader.getReportException();
              if (exception != null) {
                synchronized (failure) {
                  failure[0] = exception;
                }
              }
            });

            // check if we had a failure
            synchronized (failure) {
              if (failure[0] != null) {
                if (failure[0] instanceof RuntimeException) {
                  throw ((RuntimeException) failure[0]);
                } else {
                  throw new RuntimeException(failure[0]);
                }
              }
            }
          }
        }
      }

    } finally {
      joinerPool.joinAndDestroy();
    }
  }

  private void primeAuditSession() {
    try {
      if (isResolving() || isAborted()) {
        return;
      }

      // delete the audit cache files if they are incomplete
      if (this.primingAuditFileExists()) {
        this.deleteAuditCacheFiles();
      }

      final List<ProjectFile> files = fileDataAccess.selectFiles(this.projectId);
      if (!this.isExternal()) {
        if (files.size() == 0) {
          log("AUDIT SESSION PRIME IS NOT REQUIRED BECAUSE THERE ARE NO FILES");
          return;
        }
        int resolvedRecordCount = 0;
        for (ProjectFile file : files) {
          resolvedRecordCount += file.getResolvedRecordCount();
        }
        if (resolvedRecordCount == 0) {
          log("AUDIT SESSION PRIME IS NOT REQUIRED DUE TO NO RESOLVED RECORDS");
          return;
        }
      }
      synchronized (this.primingMonitor) {
        if (this.isPrimingAudit()) return;
        this.primingAuditCancelled = false;
        this.primerThread = new Thread(() -> {
          EngineResourceHandle sessionId = null;
          boolean success = true;
          try {
            this.createPrimingAuditFile();
            success = false;
            if (isResolving()) return;
            sessionId = getAuditSessionId(true);
            ResultsSummary auditSummary = getCurrentAuditSummary();

            if (auditSummary == null) {
              log("**** PRIMING WITH AUDIT SESSION ID: " + sessionId);
              long startSummary = System.currentTimeMillis();
              auditSummary = primeAuditSummary(sessionId);
              long endSummary = System.currentTimeMillis();
              log("** PERFORMANCE ** TOTAL PRIME SUMMARY TIME: "
                  + (endSummary - startSummary) + "ms");
            }

            if (auditSummary != null) {
              long startReports = System.currentTimeMillis();
              primeAuditReports(files, auditSummary);
              success = true;
              long endReports = System.currentTimeMillis();
              log("** PERFORMANCE ** TOTAL AUDIT REPORT TIME: "
                  + (endReports - startReports) + "ms");
            }

          } catch (EngineRequestCancelledException e) {
            log("**** CANCELLED AUDIT PRIMING DUE TO ENGINE RESTART");

          } catch (RuntimeException e) {
            if (this.cancelledAuditSessionId == null
                || (!this.cancelledAuditSessionId.equals(sessionId)))
            {
              if (!this.auditWasCached) {
                this.auditFailure = e;
                ServerError se = new ServerError(true, e);
                ServerErrorLog.recordProjectError(this.projectId, se);
              }
              throw e;
            }

          } finally {
            synchronized (primingMonitor) {
              this.primerThread = null;
              this.primingMonitor.notifyAll();
              // check if completed with success
              if (success && this.primingAuditDetails == 0) {
                this.deletePrimingAuditFile();
                log("******** AUDIT DETAILS PRIMED");
              }
              // if we failed then cleanup the cache files and don't try to use
              // them later -- this is for good measure
              if (!success) {
                this.invalidateAudits();
              }
            }
          }

        }, "AuditReportPrimer-" + this.projectId
           + "-" + Math.abs(System.identityHashCode(this)));
        this.primerThread.start();
      }

    } catch (SQLException ignore) {
      log(ignore);
    }
  }

  public void complete() {
    synchronized (fileQueue) {
      if (completed) return;
      fileQueue.clear();
      fileQueue.add(null);
      fileQueue.notifyAll();
      completed = true;
    }
  }

  private boolean loadFile(long fileId) {
    try {
      ProjectFileDataAccess fileDA = fileDataAccess;
      ProjectFile file = fileDA.selectFile(this.projectId, fileId);
      if (!file.isUploadComplete()) {
        throw new IllegalStateException("Streaming to G2 engine not yet supported");
      }
      synchronized (fileQueue) {
        if (completed) {
          throw new IllegalStateException("Cannot add more files after completion.");
        }
        // check if this file is alredy enqueued
        if (this.currentFileId == fileId
            || fileQueue.stream().anyMatch(f -> f.getId() == fileId))
        {
          log("FILE ALREADY ENQUEUED");
          return false;
        }
        // check if the file is already resolved
        if (file.getResolvedRecordCount() == file.getRecordCount()) {
          log("ALL RECORDS ALREADY RESOLVED -- RERESOLVING....");
          //return false;
        }
        fileQueue.add(file);
        log("FILE ENQUEUED");
        fileQueue.notifyAll();
      }
      return true;

    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public void run() {
    try {
      openThreadLocalLog(engineLogFile);

      if (isExternal()) {
        doExternalRun();
      } else {
        doRun();
      }

    } catch (Throwable e) {
      log("**** EXCEPTION IN ENGINE THREAD MAIN LOOP");
      log(e);
      boolean recorded = false;
      ServerError se = new ServerError(true, "engine-error", e);
      synchronized (fileQueue) {
        for (ProjectFile file : fileQueue) {
          if (file != null && (this.currentFileId < 0L || this.currentFileId == file.getId())) {
            recordFileError(file.getProjectId(), file.getId(), se);
            recorded = true;
          }
        }
      }
      if (!recorded) {
        recordProjectError(this.projectId, se);
      }

    } finally {
      log("ENGINE PROCESS THREAD MAIN LOOP ENDED.");
      log("SHUTTING DOWN ENGINE PROCESS THREAD....");
      this.shutdown(true);
      log("SHUT DOWN ENGINE PROCESS THREAD.");
    }
  }

  protected boolean isShutdown() {
    synchronized (this.abortMonitor) {
      return this.shutdown;
    }
  }

  private void shutdown(boolean waitForEngines) {
    // if we are priming the audit, cancel and invalidate the audits
    if (this.isPrimingAudit()) {
      this.waitIfPrimingAudit(true);
      this.invalidateAudits();
    }
    this.reportSender.complete(this.accessToken);

    log("ENGINE PROCESS SHUTDOWN SIGNAL RECEIVED "
        + ((waitForEngines) ? "WITH " : "WITHOUT ")
        + "WAIT SPECIFIED");
    Thread currentThread = Thread.currentThread();
    if (currentThread != this) {
      log("ATTEMPT TO SHUTDOWN OUTSIDE OF MAIN EngineProcess THREAD");
      log("MARKING ENGINE PROCESS THREAD COMPLETE...");
      this.complete();
      log("MARKED ENGINE PROCESS THREAD COMPLETE");
      if (waitForEngines) {
        log("JOINING WITH ENGINE PROCESS THREAD....");
        try {
          join();
        } catch (InterruptedException ignore) {
          log(ignore);
        }
        log("JOINED WITH ENGINE PROCESS THREAD....");
      } else {
        log("NOT WAITING FOR SHUTDOWN");
      }
      return;
    }

    log("MARKING ENGINE PROCESS THREAD COMPLETE...");
    complete();
    log("MARKED ENGINE PROCESS THREAD COMPLETE");
    log("CLEANING UP AUDIT SESSION....");
    this.cleanupAuditSession(false);
    log("CLEANED UP AUDIT SESSION.");

    log("STOPPING ENGINE....");
    try {
      if (waitForEngines) {
        this.stopEngineAndWait(true);
      } else {
        this.stopEngine(true);
      }
    } catch(Exception ignore) {
      log(ignore);
    }
    log("STOP ENGINE COMPLETED.");

    // if we get here, decommission this instance
    log("DECOMMISSIONING ENGINE PROCESS THREAD....");
    synchronized (INSTANCE_MAP) {
      log("OBTAINED LOCK ON INSTANCE MAP");
      EngineProcess p = INSTANCE_MAP.get(this.projectId);
      if (p == this) {
        // unload any exports previously generated
        EngineExportUtilities.unloadExistingExports(this.projectId);

        log("********* DECOMMISSIONING ENGINE PROCESS: " + this.projectId);
        INSTANCE_MAP.remove(this.projectId);
      } else{
        log("********* ALREADY DECOMMISSIONED ENGINE PROCESS: "
                + this.projectId);
      }
    }
    log("DECOMMISSIONED ENGINE PROCESS THREAD.");

    synchronized (this.abortMonitor) {
      this.shutdown = true;
      this.abortMonitor.notifyAll();
    }

    log("CLOSING CONNECTORS.....");
    if (this.connector != null) {
      try {
        log("CLOSING RESOLVER CONNECTOR...");
        this.connector.close();
        log("CLOSED RESOLVER CONNECTOR.");
      } catch (Exception ignore) {
        log(ignore);
      }
    }
    log("CONNECTORS CLOSED.");

    try {
      log("ABORTING REMAINING THREADS....");
      abort();
      log("ABORTED REMAINING THREADS.");
    } catch(Exception ignore) {
      log(ignore);
    }

    try {
      log("CLOSING THREAD LOCAL LOG....");
      closeThreadLocalLog();
      log("CLOSED THREAD LOCAL LOG.");
    } catch (Exception ignore) {
      log(ignore);
    }
  }

  public void abort() {
    synchronized (this.abortMonitor) {
      this.aborted = true;
    }
    synchronized (this.fileQueue) {
      this.complete();
    }
  }

  public boolean isAborted() {
    synchronized (this.abortMonitor) {
      return this.aborted;
    }
  }

  public boolean isLoadCancelled() {
    synchronized (this.abortMonitor) {
      return this.loadCancelled;
    }
  }

  public void setLoadCancelled(boolean cancelled) {
    synchronized (this.abortMonitor) {
      this.loadCancelled = cancelled;
    }
    synchronized (this.fileQueue) {
      this.fileQueue.notifyAll();
    }
  }

  public void doRun()
    throws IOException, SQLException, ClassNotFoundException
  {
    File projectDir = Workbench.getProjectDirectory(this.projectId);
    boolean primeAuditRequired = true;

    if (!this.isReinitializing() && !isPrimingAudit()) {
      primeAuditSession();
      primeAuditRequired = false;
    }
    primeEngine();
    while (!isAborted())
    {
      if (this.isLoadCancelled()) {
        synchronized (this.fileQueue) {
          this.fileQueue.clear();
        }
        this.setLoadCancelled(false);
        continue;
      }

      ProjectFile file = null;
      while (file == null) {
        if (this.isRestartRequired()) {
          // reinitialize the engine
          log("**** DATA SOURCES, CONFIG OR LICENSE HAS CHANGED");
          log("**** REINITIALIZING ENGINES...");
          this.clearRestartRequired();
          this.reinitializeEngine();
          log("**** REINITIALIZED ENGINES.");
        }

        synchronized (this.fileQueue) {
          if (this.fileQueue.size() > 0) {
            log("FOUND A FILE TO RESOLVE.");
            file = fileQueue.remove(0);
            if (file == null) break;
            this.currentFileId = file.getId();
            primeAuditRequired = true;
          } else if (file == null) {
            try {
              fileQueue.wait(1000L);
              if (!this.isReinitializing()
                  && primeAuditRequired
                  && !isPrimingAudit())
              {
                primeAuditSession();
                primeAuditRequired = false;
              }
            } catch (InterruptedException ignore) {
              // ignore
            }
          }
        }
      }

      // this is the terminating condition for handling Files
      if (file == null) {
        break;
      }
      waitIfPrimingAudit(true);

      // we are about to resolve a new file which makes existing audits obsolete
      invalidateAudits();

      if(project.isUpgraded()) {
        project.clearUpgradedFlag();
        updateProject(project);
        log("CLEARED UPGRADED FLAG ON PROJECT " + project.getId());
      }

      // get the data source code for the file
      String dsrcCode = file.getDataSource();
      String entityType = file.getEntityType();
      Integer threadAffinity = null;
      if (file.getRecordCount() < SINGLE_THREAD_THRESHOLD) {
        threadAffinity = file.getName().hashCode();
      }

      // make sure we are configured to handle this data source code
      boolean reinitializeRequired = false;
      synchronized (this.monitor) {
        if (!this.isEngineRunning()
            || EngineConfig.isConfigChangedSince(this.projectId, this.configTimestamp)
            || !this.dataSourceCodes.contains(dsrcCode)
            || this.isRestartRequired())
        {
          reinitializeRequired = true;
        }
      }
      if (reinitializeRequired) {
        // reinitialize the engine
        log("**** DATA SOURCES, CONFIG OR LICENSE HAS CHANGED");
        log("**** REINITIALIZING ENGINES...");
        reinitializeEngine();
        log("**** REINITIALIZED ENGINES.");
      }

      String  uploadName    = file.getUploadName();
      File    cachedFile    = new File(projectDir, uploadName);
      String  fileUrl       = file.getUrl();
      int     slashIndex    = fileUrl.lastIndexOf("/");
      String  fileName      = fileUrl.substring(slashIndex+1);
      int     urlMin        = (fileUrl.length()>200 ? fileUrl.length()-200 : 0);
      String  loadId        = fileUrl.substring(urlMin);
      Receiver receiver = new Receiver(this,
          indexer,
          file,
          switchboard,
          pendingRequests,
          pendingRedos,
          engineThreadCount);
      log("OPENING CACHED FILE: " + cachedFile);

      String encoding = detectEncoding(cachedFile);

      int requestCount = 0;
      boolean checkRedos = true; // start out by clearing out redos
      int recordCount = 0;
      int suppressedCount = 0;
      int intraRedoCount = 0;
      int redoThreshold = REDO_THRESHOLD_INCREMENT;
      int previousRedoCount = Integer.MAX_VALUE;
      int redoDivergenceCount = 0;

      // For debugging the JSON record
      // File jsonFile = new File(
      //    cachedFile.toString().replaceAll("(?i).csv.gz$", ".json"));

      try (InputStream is = new FileInputStream(cachedFile);
           GZIPInputStream gzipIS = new GZIPInputStream(is);
           InputStreamReader isr = new InputStreamReader(gzipIS, encoding);
           Reader bomSkippingReader = IOUtilities.bomSkippingReader(isr, encoding);
           BufferedReader reader = new BufferedReader(bomSkippingReader))
           // For debugging the JSON record
           // BufferedReader reader = new BufferedReader(bomSkippingReader);
           // FileOutputStream os = new FileOutputStream(jsonFile);
           // OutputStreamWriter osw = new OutputStreamWriter(os, "UTF-8"))
      {
        String format = file.getFormat();
        Character delimiter = CSVUploadPeer.getDelimiterForFormat(format);

        String[] headerStrings = CSVUploadPeer.parseHeaders(
            reader.readLine(), delimiter.toString());
        String[] uniqueHeaders = new StringDifferentiator().unique(headerStrings);

        CSVFormat csvFormat = CSVFormat.DEFAULT
                                       .withHeader(uniqueHeaders)
                                       .withDelimiter(delimiter)
                                       .withAllowMissingColumnNames(true);

        CSVParser parser = new CSVParser(reader, csvFormat);

        Map<String,Integer> headerMap = parser.getHeaderMap();

        List<ProjectFileField> fields = file.getFields();

        boolean finalRedoAttempt = false;

        // iterate through the records, blocking until more records are available
        Iterator<CSVRecord> recordIter = parser.iterator();

        List<Redo> redos = new ArrayList<>();
        while (!this.isAborted() && (recordIter.hasNext() || redos.size() > 0 || checkRedos)) {
          // check if the current load has been cancelled and break out
          if (this.isLoadCancelled()) break;

          String msg = null;
          Redo redo = null;

          // if no more records then ensure that redo is enabled -- we're gonna
          // keep trying until we can get around the SQLite datrabase lock
          if (!recordIter.hasNext()) {
            receiver.recordsCompleted();
          }

          if (checkRedos && redos.size() == 0 && (!receiver.isRedoDisabled())) {
            int pendingRedoCount;
            synchronized (pendingRedos) {
              pendingRedoCount = pendingRedos.size();

              // check if we are waiting for redo responses and we have no
              // more records to send otherwise
              while (pendingRedoCount > 0 && !recordIter.hasNext()
                     && !receiver.isCompleted()
                     && redoDivergenceCount < REDO_DIVERGENCE_THRESHOLD) {
                try {
                  pendingRedos.wait(2000L);
                  pendingRedoCount = pendingRedos.size();

                } catch (InterruptedException ignore) {
                  log(ignore);
                }
              }
              if (pendingRedoCount > 0 && receiver.isCompleted()) {
                throw new IllegalStateException(
                  "Receiver completed before all redos were responded to.  pendingRedoCount=[ "
                  + pendingRedoCount + "]");
              }
            }
            if (pendingRedoCount == 0 && !receiver.isRedoDisabled()) {
              log("NO PENDING REDOS.");

              int redoCount = retrieveRedos(redos);

              log("REDO COUNT VS. PREVIOUS REDO COUNT: " + redoCount + " vs. " + previousRedoCount);
              if (redoCount >= previousRedoCount) {
                redoDivergenceCount++;
                log("****************** INCREMENTED REDO DIVERGENCE COUNT: "
                    + redoDivergenceCount);
              }
              previousRedoCount = redoCount;
              log("SET PREVIOUS REDO COUNT: " + previousRedoCount);

              // if we diverge too much, leave the redos in the database
              if (redoDivergenceCount > REDO_DIVERGENCE_THRESHOLD) {
                redos.clear();
                redoCount = 0;
              }

              if (redoCount == 0 && redos.size() == 0) {
                checkRedos = false;
                log("*** NO MORE PENDING REDOS ***");
                continue;
              }

              if (intraRedoCount > 0) {
                log("SENT " + intraRedoCount
                    + " STANDARD REQUESTS BETWEEN REDOS");
              }
              intraRedoCount = 0;
              log("PROCESSING REDOS: " + redos.size()
                  + " CURRENT / " + redoCount
                  + " TOTAL / " + redoDivergenceCount + " DIVERGENCES");
            }
          }

          String recordId = null;

          if (redos.size() > 0 && !receiver.isRedoDisabled()) {
            redo = redos.remove(0);
            msg = redo.getMessage();

          } else if (recordIter.hasNext()) {
            if (checkRedos) {
              intraRedoCount++;
            }
            CSVRecord record = recordIter.next();

            // check if record has all blank fields and skip it of so
            boolean allBlank = true;
            for (String val : record) {
              val = val.trim();
              if (val.length() > 0) {
                allBlank = false;
                break;
              }
            }
            if (allBlank) continue;

            boolean blankTail = false;
            if (record.size() > headerMap.size()) {
              blankTail = true;
              for (int col = headerMap.size(); col < record.size(); col++) {
                String tailValue = record.get(col);
                if (tailValue != null && tailValue.trim().length() > 0) {
                  blankTail = false;
                  break;
                }
              }
            }

            // skip malformed records (those that have more values than header columns)
            if (record.size() > headerMap.size() && !blankTail) continue;

            // skip records for which all non-suppressed fields are blank
            boolean allIncludedBlank = true;
            for (ProjectFileField field : fields) {
              int fieldRank = field.getRank();

              // check if the field value is missing
              if (fieldRank >= record.size()) continue;

              String attrCode   = field.getAttributeCode();
              String recordVal  = record.get(fieldRank);

              // check if field is suppressed and skip if so
              if (attrCode == null || attrCode.length() == 0) continue;

              // check if the value is blank and skip if so
              if (recordVal == null || recordVal.length() == 0) continue;

              // check if the value is invalid
              if (isFieldValueInvalid(attrCode, recordVal)) continue;

              // if we get here then we have a non-blank included value
              allIncludedBlank = false;
              break;
            }

            if (allIncludedBlank) {
              suppressedCount++;
              updateFile(file, suppressedCount);
              continue;
            }

            recordCount++;
            JsonObjectBuilder builder = Json.createObjectBuilder();

            JsonUtils.add(builder, "DATA_SOURCE", dsrcCode);
            JsonUtils.add(builder, "ENTITY_TYPE", entityType);
            JsonUtils.add(builder, "SOURCE_ID", fileName);

            for (ProjectFileField field : fields) {
              String fieldName  = field.getName();
              int    fieldRank  = field.getRank();

              // check if the field value is missing
              if (fieldRank >= record.size()) continue;

              String attrCode   = field.getAttributeCode();
              String usageGroup = field.getGrouping();
              String jsonName   = field.getAttributeCode();
              String recordVal  = record.get(fieldRank);
              if (recordVal != null) recordVal = recordVal.trim();
              if (recordVal == null || recordVal.length() == 0) {
                // don't send json fields for blank values
                continue;
              }
              // check if the value is invalid
              if (isFieldValueInvalid(attrCode, recordVal)) {
                // don't send invalid values -- suppress them
                continue;
              }
              if (jsonName == null || jsonName.length() == 0) {
                // this field is unammped and suppressed -- skip it
                continue;
              } else if ("_".equals(jsonName)) {
                // this field is unmapped, but included
                jsonName = fieldName.toLowerCase();
              }
              String usagePrefix = "";
              if (usageGroup != null && usageGroup.length() > 0) {
                usagePrefix = usageGroup.replaceAll("[\\s-_]+", " ") + "_";
              }
              JsonUtils.add(builder, usagePrefix+jsonName, recordVal);
              if (jsonName.equals("RECORD_ID")) {
                recordId = recordVal;
              }
            }
            JsonObject jsonObj = builder.build();
            try (StringWriter sw = new StringWriter();
                 JsonWriter   jw = WRITER_FACTORY.createWriter(sw))
            {
              jw.writeObject(jsonObj);
              msg = sw.toString();
            }

            // for debugging the JSON code
            // osw.write(msg + "\r\n");
            // osw.flush();
          }

          synchronized (this.monitor) {
            // check if we should wait before writing another request
            if (BLOCK_THRESHOLD > 0 && redo == null) {
              synchronized (this.pendingRequests) {
                if (this.pendingRequests.size() >= BLOCK_THRESHOLD) {
                  boolean logFlag = false;
                  long start = 0L;

                  while (!receiver.isCompleted()
                      && !this.isAborted()
                      && this.pendingRequests.size() > UNBLOCK_THRESHOLD)
                  {
                    try {
                      if (!logFlag) {
                        logFlag = true;
                        start = System.currentTimeMillis();
                        log("****** WAITING FOR ENGINE PROCESS TO CATCH UP: "
                                + this.pendingRequests.size() + " PENDING");
                      }
                      this.pendingRequests.wait(5000L);
                    } catch (Exception ignore) {
                      // ignore the interruption
                    }
                  }
                  if (this.isAborted()) continue;
                  if (logFlag) {
                    log("****** WAITED " + (System.currentTimeMillis()-start)
                            + "ms FOR ENGINE PROCESS TO CATCH UP: "
                            + this.pendingRequests.size() + " PENDING");
                  }
                }
              }
            }
            if (msg != null) {
              long rid = getNextRequestId();
              EngineRequest request = createRequest(dsrcCode, loadId, msg, redo, recordId, rid);

              if (threadAffinity != null) {
                request.setThreadAffinity(threadAffinity);
              }
              long statsId = -1L;
              EngineRequest statsRequest = null;
              if (receiver.checkStatsNeeded(true)) {
                statsId = getNextRequestId();
                statsRequest = new EngineRequest(STATS, statsId, SYNC);
                if (threadAffinity != null) {
                  statsRequest.setThreadAffinity(threadAffinity);
                }
                log("****** SENDING STATS REQUEST: " + statsId);
              }

              // ensure the receiver is still around
              if (receiver.isCompleted()) {
                throw new IllegalStateException("Receiver terminated unexpectedly.");
              }

              // record the pending request
              synchronized (pendingRequests) {
                pendingRequests.put(rid, request);
                if (statsRequest != null) {
                  pendingRequests.put(statsId, statsRequest);
                }
                pendingRequests.notifyAll();
              }

              if (redo != null) {
                // record the pending redo
                synchronized (pendingRedos) {
                  pendingRedos.put(rid, redo);
                  pendingRedos.notifyAll();
                }
              }

              // write the request
              try {
                connector.writeRequest(request);
                requestCount++;
                if (statsRequest != null) {
                  connector.writeRequest(statsRequest);
                }
                if (requestCount % 2000 == 0) {
                  log("REQUESTS SENT TO ENGINE PROCESS (INCLUDING REDO) : " + requestCount);
                  log("REQUESTS SENT TO ENGINE PROCESS (EXCLUDING REDO)  : " + recordCount);
                }

              } catch (Exception e) {
                // if write fails then don't expect a response
                e.printStackTrace();
                synchronized (pendingRequests) {
                  pendingRequests.remove(rid);
                  pendingRequests.notifyAll();
                  if (redo != null) {
                    synchronized (pendingRedos) {
                      pendingRedos.remove(rid);
                      pendingRedos.notifyAll();
                    }
                  }
                }
              }

              if ((recordCount > redoThreshold) || (!recordIter.hasNext())) {
                checkRedos = true;
                // we want to reset the divergence count for this round of
                // redos, but we don't want to do this infinitely if this is
                // the final round of redos.
                if (recordCount > redoThreshold || !finalRedoAttempt) {
                  // reset the count
                  redoDivergenceCount = 0;
                  previousRedoCount = Integer.MAX_VALUE;
                  log("SET PREVIOUS REDO COUNT: " + previousRedoCount);
                  log("****************** RESET REDO DIVERGENCE COUNT: "
                      + redoDivergenceCount);

                  // check if we reset because this is the final round of redos
                  if (!recordIter.hasNext()) {
                    finalRedoAttempt = true;
                  }
                }
                if (recordCount > redoThreshold) {
                  redoThreshold += REDO_THRESHOLD_INCREMENT;
                }
              }
            }
          }
        }
      }
      // check if the load was cancelled
      if (this.isLoadCancelled()) {
        synchronized (this.fileQueue) {
          this.fileQueue.clear();
        }
        this.setLoadCancelled(false);
      }
      log("MARKING SEND COMPLETION....");
      receiver.sendCompleted();
      log("SEND COMPLETED FOR FILE: " + requestCount + " REQUESTS SENT");
      try {
        log("WAITING FOR RECEIVER THREAD TO COMPLETE....");
        receiver.join();
        log("JOIN WITH RECEIVER COMPLETED.");

      } catch (Exception ignore) {
        log(ignore);
      }
      this.currentFileId = -1L;
    }
  }

  private EngineRequest createRequest(String dsrcCode, String loadId, String msg, Redo redo, String recordId, long rid) {
    if (redo != null) {
      // construct a redo request
      return new EngineRequest(PROCESS, rid, msg, REDO);
    } else {
      // construct an add-record request
      EngineRequest request = new EngineRequest(ADD_RECORD, rid, msg, STANDARD);
      request.setParameter(DATA_SOURCE, dsrcCode);
      request.setParameter(RECORD_ID, recordId);
      request.setParameter(LOAD_ID, loadId);
      return request;
    }
  }

  private String detectEncoding(File cachedFile) throws IOException {
    String encoding;
    try (InputStream is = new FileInputStream(cachedFile);
         GZIPInputStream gzipIS = new GZIPInputStream(is))
    {
      encoding = IOUtilities.detectCharacterEncoding(gzipIS);
    }

    if (encoding == null) {
      encoding = Charset.defaultCharset().name();
    }
    return encoding;
  }

  private void doExternalRun()
    throws IOException
  {
    File projectDir = Workbench.getProjectDirectory(this.projectId);
    boolean primeAuditRequired = true;
    if (!this.isReinitializing() && !this.isPrimingAudit()) {
      this.primeAuditSession();
      primeAuditRequired = false;
    }
    this.primeEngine();
    while (!this.isAborted()) {
      ProjectFile file = null;
      while (file == null) {
        if (this.isRestartRequired()) {
          // reinitialize the engine
          this.clearRestartRequired();
          this.reinitializeEngine();
          log("**** REINITIALIZED ENGINES.");
        }

        synchronized (this.fileQueue) {
          if (this.fileQueue.size() > 0) {
            log("FOUND A FILE TO RESOLVE.");
            file = fileQueue.remove(0);
            if (file == null) break;
          } else if (file == null) {
            try {
              fileQueue.wait(1000L);
            } catch (InterruptedException ignore) {
              // ignore
            }
          }
        }
      }

      // this is the terminating condition for handling Files
      if (file == null) {
        break;
      }
    }
  }

  private int retrieveRedos(List<Redo> redos) {
    List<Redo> block;
    int redoCount;

    try {
      block = redoDataAccess.selectRedos(this.projectId, REDO_BLOCK_SIZE);
      redoCount = redoDataAccess.countRedos(this.projectId);
    } catch (SQLException e) {
      log(e);
      throw new RuntimeException(e);
    }

    redos.addAll(block);
    return redoCount;
  }

  private void updateFile(ProjectFile file, int suppressedCount)
  {
    try {
      AccessToken token = new AccessToken();
      file = fileDataAccess.selectFile(this.projectId, file.getId(), token);
      file.setValue("suppressedRecordCount", suppressedCount, token);
      fileDataAccess.updateFile(file);

    } catch (SQLException e) {
      log(e);
      throw new RuntimeException(e);
    }
  }

  private void updateProject(Project project) {
    try {
      projectDataAccess.updateProject(project, AccessToken.getThreadAccessToken());
    }
    catch (SQLException e) {
      log(e);
      throw new RuntimeException(e);
    }
  }

  private static void markProjectFileDeletion(long projectId) throws IOException {
    File file = new File(Workbench.USER_DATA_DIR, "project_" + projectId + ".del");
    if (!file.exists()) {
      file.createNewFile();
    }
  }

  private static void completeProjectFileDeletion(long projectId) {
    File file = new File(Workbench.USER_DATA_DIR, "project_" + projectId + ".del");
    if (file.exists()) {
      file.delete();
    }
  }

}
