package com.senzing.api.engine.process;

import com.senzing.util.*;
import com.senzing.api.*;
import com.senzing.api.engine.EngineRequest;
import com.senzing.api.engine.EngineResourceHandle;
import com.senzing.api.engine.EngineResponse;
import com.senzing.api.engine.EngineException;
import com.senzing.api.util.WorkbenchConfig;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.security.MessageDigest;
import java.util.*;
import java.io.StringReader;
import java.util.stream.Collectors;
import javax.json.*;

import static com.senzing.api.Workbench.*;
import static com.senzing.api.engine.EngineOperation.*;
import static com.senzing.api.engine.EnginePriority.*;
import static com.senzing.api.engine.process.EngineProcess.*;
import static com.senzing.util.FilterType.*;
import static com.senzing.api.KeyFilterCounts.*;

class ReportReader extends Thread {
  private static final int AUDIT_SEGMENT_SIZE = 1000;
  private static final int AUDIT_INITIAL_CAPACITY = 20000;

  private EngineProcess                 engineProcess;
  private SendPrioritizer               sender;
  private EngineResourceHandle          sessionId;
  private Long                          reportId;
  private boolean                       complete;
  private String                        fromDataSource;
  private String                        toDataSource;
  private int                           matchLevel;
  private String                        reportError;
  private Exception                     reportException;
  private FileBackedAppendList<String>  auditGroups;
  private final Object                  monitor;
  private int                           sampleCount;
  private int                           totalCount;
  private boolean                       invalid;
  private File                          auditCacheDir;
  private String                        baseFileName;
  private boolean                       ready;
  private boolean                       cached = false;
  private long                          reportDuration;

  private Map<Map<String,Filter>, ReportIndex> indexes;
  private Map<Map<String,Filter>, ReportIndex> baseIndexes;
  private Map<Map<String,Filter>, ReportIndex> dualValueIndexes;

  private static int instanceCount = 0;
  private static final Object MONITOR = new Object();

  ReportReader(EngineProcess        engineProcess,
               SendPrioritizer      sender,
               EngineResourceHandle sessionId,
               String               fromDataSource,
               String               toDataSource,
               int                  matchLevel)
  {
    this.ready              = false;
    this.engineProcess      = engineProcess;
    this.sender             = sender;
    this.sessionId          = sessionId;
    this.fromDataSource     = fromDataSource;
    this.toDataSource       = toDataSource;
    this.matchLevel         = matchLevel;
    this.complete           = false;
    this.reportError        = null;
    this.reportException    = null;
    this.sampleCount        = WorkbenchConfig.getAuditSampleSize();
    this.invalid            = false;
    this.auditGroups        = null;
    this.monitor            = new Object();
    this.indexes            = new HashMap<>();
    this.baseIndexes        = new HashMap<>();
    this.dualValueIndexes   = new HashMap<>();
    this.reportDuration     = -1L;

    this.setName("ReportReader-" + this.engineProcess.getProjectId() + "-"
                 + fromDataSource + "_VS_" + toDataSource + "_@_" + matchLevel);

    String key = EngineProcess.formatReportKey(fromDataSource,
                                               toDataSource,
                                               matchLevel);

    synchronized (MONITOR) {
      instanceCount++;
      log(instanceCount + " ReportReader instances constructed: " + key);
    }

    log(key + " AUDIT SAMPLE COUNT: " + this.sampleCount);

    this.baseFileName = EngineProcess.formatBaseCacheFileName(key);

    this.auditCacheDir = this.engineProcess.auditCacheDir();
    if (!auditCacheDir.exists()) auditCacheDir.mkdirs();

    this.start();
  }


  private static String encodeFilterKey(Map<String,Filter> filter) {
    StringBuffer sb = new StringBuffer();
    filter.entrySet().forEach(e -> {
      if (sb.length() > 0) sb.append("|");
      sb.append(e.getKey());
      sb.append("=");
      sb.append(e.getValue());
    });
    return sb.toString();
  }

  private String formatBaseIndexFileName(String filterKey)
  {
    try {
      StringBuilder sb = new StringBuilder("audit-index-");
      String uniqueId = this.baseFileName + filterKey;
      MessageDigest md = MessageDigest.getInstance("SHA-256");
      md.update(uniqueId.getBytes("UTF-8"));
      byte[] digest = md.digest();

      String digestString = BASE64_ENCODER.encodeToString(digest);
      digestString = digestString.replaceAll("^(.*[^-=])[-=]+$", "$1");

      sb.append(digestString);
      sb.append("-");

      return sb.toString();

    } catch (Exception e) {
      log("**** FAILED TO GENERATE INDEX FILE NAME");
      log(e);
      throw new RuntimeException(e);
    }
  }

  static String jsonQuote(String text) {
    return "\"" + jsonEscape(text) + "\"";
  }

  static String jsonEscape(String text) {
    text = text.replaceAll("([\\\\\\\"])","\\\\$1");
    text = text.replace("\n","\\t");
    text = text.replace("\r","\\t");
    text = text.replace("\t","\\t");
    text = text.replace("\b","\\b");
    text = text.replace("\f","\\f");
    return text;
  }

  public String getReportKey() {
    return EngineProcess.formatReportKey(this.fromDataSource,
                                         this.toDataSource,
                                         this.matchLevel);
  }

  public Exception getReportException() {
    return this.reportException;
  }

  public void closeReport() {
    this.complete();
    try {
      this.join();
    } catch (InterruptedException ignore) {
      log(ignore);
    }
    if (this.reportId != null) {
      log("*** CLOSING AUDIT REPORT: session=[ " + this.sessionId
          + " ], report=[ " + this.reportId + " ]");
      this.engineProcess.closeAuditReport(this.sessionId, this.reportId);
      log("*** CLOSED AUDIT REPORT: session=[ " + this.sessionId
          + " ], report=[ " + this.reportId + " ]");
    }
  }

  public EngineResourceHandle getAuditSessionId() {
    return this.sessionId;
  }

  public synchronized boolean isReady() {
    return this.ready || this.complete || this.invalid;
  }

  public synchronized long getDuration() {
    return this.reportDuration;
  }

  public synchronized void markReady() {
    this.ready = true;
    this.notifyAll();
  }

  public void invalidate() {
    // mark this instance as invalid
    synchronized (this) {
      this.invalid = true;
    }

    // get the existing indexes for invalidation
    List<ReportIndex> reportIndexes;
    synchronized (this.indexes) {
      // create the array
      reportIndexes = new ArrayList<>(this.indexes.size());

      // add each to the array
      this.indexes.values().forEach(reportIndex -> {
        reportIndexes.add(reportIndex);
      });
    }

    // invalidate the report indexes
    reportIndexes.forEach(reportIndex -> {
      reportIndex.invalidate();
    });
  }

  public synchronized boolean isInvalid() {
    return this.invalid;
  }

  public void complete() {
    synchronized (this) {
      if (this.complete) return;
      this.complete = true;
    }
    synchronized (this.monitor) {
      this.monitor.notifyAll();
    }
  }

  public synchronized boolean isCached() {
    return this.cached;
  }

  public synchronized void markCached() {
    this.cached = true;
  }

  public synchronized boolean isComplete() {
    return this.complete;
  }

  public int getAuditGroupCount() {
    synchronized (this.monitor) {
      if (this.auditGroups == null) return 0;
      return this.auditGroups.size();
    }
  }

  public Map<String,Integer> getBaseIndexCounts(String key) {
    List<Map<String,Filter>> list = new LinkedList<>();
    synchronized (this.indexes) {
      // find the single-valued filter maps
      this.indexes.keySet().forEach(map -> {
        // exclude composite filters with multiple keys
        if (map.size() != 1) return;

        // only include those with the specified key
        if (!map.keySet().contains(key)) return;

        // get the associated filter
        Filter filter = map.values().iterator().next();

        // exclude filters that are not single-valued
        if (filter.getFilterType() != SINGLE) return;

        // add the filter map to the list
        list.add(map);
      });

      // sort the filter maps
      list.sort((m1,m2) -> {
        // indexes with more hits come first
        ReportIndex rptIdx1 = this.indexes.get(m1);
        ReportIndex rptIdx2 = this.indexes.get(m2);
        int diff = rptIdx2.getHitCount() - rptIdx1.getHitCount();
        if (diff != 0) return diff;

        // if the same number of hits, compare by filter value name
        String val1 = m1.values().iterator().next().getFilterValues()
            .iterator().next().toString();
        String val2 = m2.values().iterator().next().getFilterValues()
            .iterator().next().toString();
        return val1.compareTo(val2);
      });

      // populate the result
      Map<String,Integer> result = new LinkedHashMap<>();
      list.forEach(map -> {
        String val = map.values().iterator().next().getFilterValues()
            .iterator().next().toString();

        ReportIndex reportIndex = this.indexes.get(map);

        result.put(val, reportIndex.getHitCount());
      });

      // return the result
      return result;
    }
  }

  public String getAuditReportPage(int from, int to) {
    return this.getAuditReportPage(from, to, null);
  }

  public String getAuditReportPage(int                from,
                                   int                to,
                                   Map<String,Filter> filterMap)
  {
    if (from < 0 || to < 0) {
      throw new IllegalArgumentException(
        "Both the from and to index must be greater than zero.  from=[ "
        + from + " ], to=[ " + to + " ]");
    }
    if (from > to) {
      throw new IllegalArgumentException(
        "The from must be less than the to index.  from=[ "
        + from + " ], to=[ " + to + " ]");
    }

    log();
    log("GETTING AUDIT REPORT PAGE: " + from + " - " + to + " WITH "
        + filterMap);

    ReportIndex reportIndex = this.getReportIndex(filterMap, false);

    log("GOT REPORT INDEX: " + reportIndex);
    long start = System.currentTimeMillis();
    if (reportIndex == null) {
      log("NO REPORT INDEX....");
      filterMap = null;
      // check if we need to wait for the audit report to catch up
      while (!this.isComplete() && this.getAuditGroupCount() < to) {
        synchronized (this.monitor) {
          int count = (this.auditGroups == null) ? 0 : this.auditGroups.size();
          if (count < to) {
            try {
              // wait 5 seconds for audit groups to catch up
              this.monitor.wait(5000L);
            } catch (Exception ignore) {
              log(ignore);
            }
          }
        }
      }
    } else {
      log("USING REPORT INDEX....");
      // check if we need to wait for the index to catch up
      while (!reportIndex.isComplete() && reportIndex.getHitCount() < from) {
        synchronized (reportIndex) {
          int count = reportIndex.getHitCount();
          if (count < from) {
            try {
              log("WAITING FOR REPORT INDEX TO COMPLETE....");
              log("COMPLETE: " + reportIndex.isComplete());
              log("HIT COUNT: " + reportIndex.getHitCount());
              // wait 5 seconds for index to catch up
              reportIndex.wait(5000L);
            } catch (Exception ignore) {
              log(ignore);
            }
          }
        }
      }
    }
    long now = System.currentTimeMillis();
    log("READY TO PULL RESULT: " + (now - start) + "ms");
    log();

    boolean reportComplete;
    boolean complete;
    int     count;
    if (reportIndex == null) {
      synchronized (this.monitor) {
        complete  = this.isComplete();
        count     = this.getAuditGroupCount();
      }
      reportComplete = complete;

    } else {
      synchronized (reportIndex) {
        complete  = reportIndex.isComplete();
        count     = reportIndex.getHitCount();
      }
      synchronized (this.monitor) {
        reportComplete = this.isComplete();
      }
    }

    log("AUDIT COUNT: " + count);
    if (count < from) {
      from = count;
    }
    if (count < to) {
      to = count;
    }

    StringBuilder sb = new StringBuilder(10000);
    sb.append("{\"AUDIT_PAGE\": {");
    sb.append("\"FROM_DATA_SOURCE\": ");
    sb.append(jsonQuote(this.fromDataSource));
    sb.append(",");
    sb.append("\"TO_DATA_SOURCE\": ");
    sb.append(jsonQuote(this.toDataSource));
    sb.append(",");
    sb.append("\"MATCH_LEVEL\": ");
    sb.append(this.matchLevel);
    sb.append(", ");
    sb.append("\"FIRST_RECORD_INDEX\": ");
    sb.append(from);
    sb.append(", ");
    sb.append("\"LAST_RECORD_INDEX\": ");
    sb.append(to < 1 ? 0 : to - 1);
    sb.append(", ");
    sb.append("\"COMPLETE\": ");
    sb.append(complete);
    sb.append(", ");
    sb.append("\"FILTERED\": ");
    sb.append((reportIndex != null));
    sb.append(", ");
    sb.append("\"FILTER\": ");
    if (reportIndex == null) {
      sb.append("null");
    } else {
      sb.append("{ ");
      StringBuilder prefix1 = new StringBuilder();
      filterMap.entrySet().forEach(entry -> {
        String        key         = entry.getKey();
        Filter        filter      = entry.getValue();
        FilterType    filterType  = filter.getFilterType();
        Set           values      = filter.getFilterValues();
        StringBuilder prefix2     = new StringBuilder();
        sb.append(prefix1);
        sb.append("\"").append(key).append("\": ");
        sb.append("{ ");
        sb.append("\"TYPE\": ");
        sb.append("\"").append(filterType).append("\",");
        sb.append("\"VALUES\": ");
        sb.append("[ ");
        prefix2.delete(0, prefix2.length());
        values.forEach(val -> {
          sb.append(prefix2);
          sb.append("\"").append(String.valueOf(val)).append("\"");
          if (prefix2.length() == 0) {
            prefix2.append(", ");
          }
        });
        sb.append(" ] }");
        if (prefix1.length() == 0) {
          prefix1.append(", ");
        }
      });
      sb.append(" }");
    }
    sb.append(",");
    sb.append("\"NEXT_FILTER_COUNTS\": ");

    start = System.currentTimeMillis();
    List<KeyFilterCounts> keyFilterCounts
        = this.getKeyFilterCounts(filterMap, this.totalCount, reportComplete);

    sb.append(KeyFilterCounts.toJsonArray(keyFilterCounts));

    sb.append(",");
    sb.append("\"BASE_FILTER_COUNTS\": ");

    start = System.currentTimeMillis();
    List<KeyFilterCounts> baseFilterCounts
        = this.getKeyFilterCounts(null, this.totalCount, reportComplete);

    sb.append(KeyFilterCounts.toJsonArray(baseFilterCounts));
    sb.append(",");

    now = System.currentTimeMillis();
    log("ADDED BASE FILTER COUNTS TO RESPONSE: " + (now-start) + "ms");

    sb.append("\"TOTAL_RECORD_COUNT\": ");
    sb.append(this.totalCount);
    sb.append(", ");
    sb.append("\"FILTERED_RECORD_COUNT\": ");
    sb.append(reportIndex == null ? this.totalCount : count);
    sb.append(", ");
    sb.append("\"SAMPLE_RECORD_COUNT\": ");
    int currentSampleCount = WorkbenchConfig.getAuditSampleSize();
    sb.append((currentSampleCount <= 0) ? this.totalCount : currentSampleCount);
    sb.append(",");
    synchronized (this.monitor) {
      if (this.reportError != null) {
        sb.append("\"REPORT_ERROR\": ");
        sb.append(jsonQuote(this.reportError));
        sb.append(",");
      }
    }
    sb.append("\"AUDIT_GROUPS\": [");

    start = System.currentTimeMillis();
    synchronized (this.monitor) {
      if (from < count && to <= count && from < to) {
        List<String> subList;
        if (reportIndex == null) {
          subList = this.auditGroups.subList(from,to);
        } else {
          List<Integer> indexList = reportIndex.getHits(from,to);
          subList = new IndexedList<>(this.auditGroups, indexList);
        }
        String prefix = "";
        int index = from;
        for (String auditGroup : subList) {
          sb.append(prefix);
          sb.append(auditGroup);
          prefix = ",";
        }
      }
    }
    sb.append("] } }");
    now = System.currentTimeMillis();
    log("ADDED AUDIT GROUPS TO RESPONSE: " + (now-start) + "ms");
    return sb.toString();
  }

  public void run()
  {
    long reportStart = System.currentTimeMillis();
    try {
      boolean cacheExists = false;
      boolean cacheStale  = true;
      try {
        cacheExists = FileBackedAppendList.filesExist(this.auditCacheDir,
                                                      this.baseFileName);
        if (cacheExists) {
          long projectId = this.engineProcess.getProjectId();
          long modified = FileBackedAppendList.lastModified(this.auditCacheDir,
                                                            this.baseFileName);

          long projectModified = getMostRecentTime(projectId, true);

          if (projectModified > modified) {
            FileBackedAppendList.deleteExistingFiles(this.auditCacheDir,
                                                     this.baseFileName);
            cacheStale = true;
          } else {
            cacheStale = false;
          }
        }
        log("CACHE EXISTS / STALE / FILE: " + cacheExists
                + " / " + cacheStale + " / " + this.baseFileName);

      } catch (Exception e) {
        log(e);
        // ignore
      }

      if (!cacheExists || cacheStale) {
        this.engineProcess.beginPrimingAuditDetails();

        try {
          this.doRun();
        } finally {
          this.engineProcess.endPrimingAuditDetails();
        }

      } else {
        this.markCached();
        this.markReady();
        this.doCachedRun();
      }

    } catch (Exception e) {
      log(e);
      synchronized (this.monitor) {
        if (this.reportError == null) {
          this.reportException = e;
          this.reportError = e.toString();
        }
      }
    } finally {
      long reportEnd = System.currentTimeMillis();
      this.complete();

      // if priortized previously then unregister
      this.sender.unregisterThread(this);

      long now = System.currentTimeMillis();
      log("AUDIT REPORT TOOK " + (reportEnd - reportStart)
              + "ms TO COMPLETE WITH " + (now-reportEnd) + "ms CLEANUP");

      synchronized (this) {
        this.reportDuration = (now - reportStart);
      }
    }
  }

  private void validateSessionId() {
    this.engineProcess.auditSessionLock.readLock().lock();
    try {
      EngineResourceHandle current = this.engineProcess.getAuditSessionId(false);
      if (current == null || !current.equals(this.sessionId)) {
        throw new IllegalStateException(
          "Audit Session IDs no longer match.  expected=[ "
          + this.sessionId + "], current=[ " + current + " ]");
      }
    } finally {
      this.engineProcess.auditSessionLock.readLock().unlock();
    }
  }

  private void openAuditReport() {
    this.engineProcess.auditSessionLock.readLock().lock();
    try {
      this.validateSessionId();

      log("WAITING FOR AUDIT SUMMARY....");
      ResultsSummary summary = this.engineProcess.waitForAuditSummary();
      log("GOT AUDIT SUMMARY.");
      this.validateSessionId();

      SourceSummary sourceSummary
          = summary.getSummariesBySource().get(this.fromDataSource);

      SourceResults sourceResults
          = sourceSummary.getVersusResultsBySource().get(this.toDataSource);

      int totalCount = -1;
      switch (this.matchLevel) {
        case 1:
          totalCount = sourceResults.getMatchedCount();
          break;
        case 2:
          totalCount = sourceResults.getPossibleMatchCount();
          break;
        case 3:
          totalCount = sourceResults.getDiscoveredRelationshipCount();
          break;
        case 4:
          totalCount = sourceResults.getDisclosedRelationshipCount();
          break;
        default:
          throw new IllegalStateException(
              "Unrecognized match level: " + this.matchLevel);
      }
      if (totalCount < 0) {
        throw new IllegalArgumentException(
          "The specified from and/or to datasource are not valid:  "
          + "fromDataSource=[ " + fromDataSource + " ], toDataSource=[ "
          + toDataSource + " ]");
      }
      this.totalCount = totalCount;

      log("OPENING AUDIT REPORT....");
      EngineRequest request = new EngineRequest(
        AUDIT_OPEN_REPORT, this.engineProcess.getNextRequestId(), SYNC);

      request.setParameter(SESSION_ID, sessionId.getHandleId());
      request.setParameter(FROM_DATA_SOURCE, fromDataSource);
      request.setParameter(TO_DATA_SOURCE, toDataSource);
      request.setParameter(MATCH_LEVEL, matchLevel);
      request.setEngineAuthenticationId(sessionId.getEngineId());

      EngineResponse response = (this.sender == null)
          ? this.engineProcess.sendSyncRequest(request)
          : this.sender.send(request); // this will prioritize if provided

      if (!response.isSuccessful()) {
        log("FAILED TO OPEN AUDIT REPORT");
        log(response.getException());
        throw response.getException();
      }
      log("OPENED AUDIT REPORT....");

      Long reportId = (Long) response.getResult();

      if (reportId == null || reportId <= 0L) {
        throw new EngineException(
          AUDIT_OPEN_REPORT, reportId.intValue(),
          "Failed to create audit report.");
      }

      this.reportId = reportId;

    } catch (Exception e) {
      log(e);
      synchronized (this.monitor) {
        this.reportException = e;
        this.reportError = e.toString();
      }
      this.invalidate();
      this.complete();
    } finally {
      this.engineProcess.auditSessionLock.readLock().unlock();
    }
  }

  public void doRun()
  {
    log("***** AUDIT REPORT: GENERATING FROM ENGINE...");
    this.openAuditReport();
    if (this.isInvalid() || this.isComplete()) return;
    this.markReady();

    Map<String,String> metaData
        = Collections.singletonMap("totalCount",
                                   String.valueOf(this.totalCount));

    AccessToken token = new AccessToken();

    this.auditGroups = new FileBackedAppendList<>(USER_TEMP_DIR,
                                                  this.baseFileName,
                                                  true,
                                                  AUDIT_SEGMENT_SIZE,
                                                  AUDIT_INITIAL_CAPACITY,
                                                  metaData);

    Set<String> allMatchKeys = new HashSet<>();
    Set<String> allPrinciples = new HashSet<>();

    boolean logProgress;
    int logProgressThreshold = 2000;
    long prevResponseTime = -1L;
    long totalFetchTime = 0L;
    long totalNonFetchTime = 0L;
    long totalFetchEngineTime = 0L;
    while (!this.isComplete() && !this.isInvalid())
    {
      // ensure we still have an active session
      EngineResourceHandle sessionId
        = this.engineProcess.getAuditSessionId(false);
      if (sessionId == null || !sessionId.equals(this.sessionId)) {
        log("****** WARNING ****** SESSION ID'S HAVE CHANGED SINCE REPORT READER STARTED.");
        this.invalidate();
        this.complete();
        continue;
      }

      // ensure this is still the active reader
      ReportReader reader
        = this.engineProcess.getAuditReportReader(this.getReportKey());
      if (reader != this) {
        log("****** WARNING ****** TERMINATING THIS READER SINCE A SECOND READER HAS BEEN CREATED.");
        this.complete();
        continue;
      }
      if (this.getAuditGroupCount() > logProgressThreshold) {
        logProgress = true;
        logProgressThreshold *= 3;

      } else {
        logProgress = false;
      }

      if (logProgress) {
        log("REPORT READER AUDIT GROUP COUNT: " + this.getAuditGroupCount());
      }
      long requestId = this.engineProcess.getNextRequestId();
      EngineRequest request = new EngineRequest(AUDIT_FETCH_NEXT,
                                                requestId,
                                                SYNC);

      int fetchCount = 500;
      request.setParameter(SESSION_ID, this.sessionId.getHandleId());
      request.setParameter(REPORT_ID, this.reportId);
      request.setParameter(FETCH_COUNT, fetchCount);
      request.setEngineAuthenticationId(this.sessionId.getEngineId());

      long start = System.currentTimeMillis();
      if (prevResponseTime > 0) {
        totalNonFetchTime += (start-prevResponseTime);
      }
      EngineResponse response = this.sender.send(request);
      long end = System.currentTimeMillis();
      prevResponseTime = end;
      totalFetchTime += (end-start);
      totalFetchEngineTime += response.getEngineDuration();
      if (logProgress) {
        log("FETCHED AUDIT REPORT RECORDS IN " + (end-start) + "ms");
        log("TOTAL FETCH TIME: " + totalFetchTime + "ms");
        log("TOTAL FETCH ENGINE TIME: " + totalFetchEngineTime + "ms");
        log("TOTAL NON-FETCH TIME: " + totalNonFetchTime + "ms");
      }

      if (!response.isSuccessful()) {
        synchronized (this.monitor) {
          this.reportException = response.getException();
          log(this.reportException);
          this.reportError = this.reportException.toString();
        }
        this.complete();
        continue;
      }

      List<String> result     = (List<String>) response.getResult();
      final String valuePath  = "/AUDIT_PAGE/AUDIT_GROUPS/0";

      if (result != null) {
        if (logProgress) {
          log("FETCHED / REQUESTED COUNT: " + result.size() + " / " + fetchCount);
          log("READ " + result.size() + " AUDIT GROUPS FOR AUDIT REPORT");
        }
        for (String jsonText : result) {
          StringReader  stringReader    = new StringReader(jsonText);
          JsonReader    jsonReader      = Json.createReader(stringReader);
          JsonObject    jsonObject      = jsonReader.readObject();
          JsonValue     jsonValue       = jsonObject.getValue(valuePath);
          JsonObject    jsonAuditGroup  = jsonValue.asJsonObject();

          AuditGroup auditGroup = AuditGroup.parseAuditGroup(null,
                                                             jsonAuditGroup,
                                                             token);

          Set<String> matchKeys   = auditGroup.getAllMatchKeys();
          Set<String> principles  = auditGroup.getAllPrinciples();

          int auditGroupIndex;
          synchronized (this.monitor) {
            auditGroupIndex = this.auditGroups.size();
            this.auditGroups.add(jsonAuditGroup.toString());
            this.monitor.notifyAll();
          }

          this.primeReportIndexes(auditGroupIndex,
                                  matchKeys,
                                  allMatchKeys,
                                  principles,
                                  allPrinciples);
        }
      }

      if (result == null || result.size() < fetchCount)
      {
        if (result == null) log("**** RESULT IS NULL ****");
        this.complete();
        this.auditGroups.complete();
        try {
          String principles = this.encodeIndexArray(allPrinciples);
          String matchKeys  = this.encodeIndexArray(allMatchKeys);
          Map<String,String> addlMetaData = new HashMap<>();
          addlMetaData.put("principles", principles);
          addlMetaData.put("matchKeys", matchKeys);

          this.auditGroups.saveCopyTo(this.auditCacheDir,
                                      this.baseFileName,
                                      addlMetaData);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }

        // copy the indexes map to avoid double synchronization
        Map<Map<String,Filter>,ReportIndex> indexesCopy = new HashMap<>();
        synchronized (this.indexes) {
          indexesCopy.putAll(this.indexes);
        }

        // iterate over the entries in the copied map
        indexesCopy.entrySet().forEach(entry -> {
          // get the next filter map key
          Map<String,Filter> filterMap = entry.getKey();

          // check if this is a single-value index -- multi-valued ones will
          // be handled when they background complete
          if (filterMap.size() > 1) return;

          // get the single filter value
          Filter filter = filterMap.values().iterator().next();

          // double-check we have a single-valued filter (base index)
          if (filter.getFilterType() == FilterType.SINGLE
              && filter.getFilterValues().size() == 1)
          {
            // get the index
            ReportIndex reportIndex = entry.getValue();

            // synchronize and mark the index the index complete
            reportIndex.markCompleted(this.getAuditGroupCount());
          }
        });

        synchronized (this.monitor) {
          this.monitor.notifyAll();
        }
        log("***** AUDIT REPORT: GENERATED FROM ENGINE.");
        continue;
      }
    }
  }

  public void doCachedRun()
  {
    log("***** AUDIT REPORT: READING FROM CACHE...");
    boolean retry = false;
    try {
      boolean filesExist = FileBackedAppendList.filesExist(this.auditCacheDir,
                                                           this.baseFileName);
      if (!filesExist) {
        throw new IllegalStateException(
            "The backing files for the audit cache do not exist: "
            + this.auditCacheDir + " / " + baseFileName);
      }

      this.auditGroups = FileBackedAppendList.load(this.auditCacheDir,
                                                   this.baseFileName);

      Map<String,String> metaData = this.auditGroups.getOpaqueMetaData();

      this.totalCount = Integer.parseInt(metaData.get("totalCount"));

      Set<String> principles = this.parseIndexArray(metaData.get("principles"));
      Set<String> matchKeys  = this.parseIndexArray(metaData.get("matchKeys"));

      for (String p: principles) {
        this.restoreIndex(PRINCIPLE, p);
      }

      for (String k: matchKeys) {
        this.restoreIndex(MATCH_KEY, k);
      }

      log("***** AUDIT REPORT: READ FROM CACHE.");

      synchronized (this.monitor) {
        this.monitor.notifyAll();
      }

    } catch (Exception e) {
      log("FAILED TO READ REPORT FROM FILE: "
              + this.auditCacheDir + "/ " + this.baseFileName);
      log(e);
      if (retry) {
        log("RETRYING REPORT WITHOUT CACHE FILE....");
        this.auditGroups = null;
        this.doRun();
        log("RETRIED REPORT WITHOUT CACHE FILE.");
      } else {
        synchronized (this.monitor) {
          this.reportException = e;
          this.reportError = e.toString();
        }
      }
    }
  }

  /**
   * Saves the specified completed report index and associates it with the
   * specified filter map.
   *
   * @param filterMap The filter map specifying the critieria for the index.
   *
   * @reportIndex The completed {@link ReportIndex} to save.
   */
  private void saveReportIndex(Map<String,Filter> filterMap,
                               ReportIndex        reportIndex)
  {
    // get the filter key and base index file name
    String filterKey = encodeFilterKey(filterMap);
    String baseIndexName = this.formatBaseIndexFileName(filterKey);

    try {
      reportIndex.save(this.auditCacheDir, baseIndexName);

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * <p>
   * Obtains the {@link ReportIndex} for the specified filter {@link Map}.
   * If the specified parameter is <tt>null</tt> or an empty {@link Map} then
   * <tt>null</tt> is returned.
   * </p>
   * <p>
   * Otherwise, the filter map is broken down to the base filters and the
   * {@link ReportIndex} is either constructed with the default constructor
   * or created by merging two or more base {@link ReportIndex} instances via
   * intersection or union.
   * </p>
   *
   * @param filterMap The {@link Map} describing the filter.
   *
   * @param indexing <tt>true</tt> if index is being requested as part of
   *                 the initial indexing process, and <tt>false</tt> if
   *                 requested as part of a report page request.
   *
   * @return The associated {@link ReportIndex}.
   */
  private ReportIndex getReportIndex(Map<String,Filter> filterMap,
                                     boolean            indexing) {
    // check if the filter map has no filters
    if (filterMap == null || filterMap.size() == 0) {
      return null;
    }

    // declare the result
    ReportIndex reportIndex;

    int valueCount = filterMap.values().stream()
                              .mapToInt(f->f.getFilterValues().size())
                              .sum();

    // synchronize on the indexes map
    synchronized (this.indexes) {
      // check the indexes map to see if we already have this index
      reportIndex = this.indexes.get(filterMap);

      // if found, return the report index -- we're done
      if (reportIndex != null) {
        // if the index is needed now, prioritize it's completion
        if (!indexing && valueCount > 1 && !reportIndex.isComplete()) {
          reportIndex.prioritize();
        }

        return reportIndex;
      }

      // check if the filter map is already cached on the file system
      String filterKey      = encodeFilterKey(filterMap);
      String baseIndexName  = this.formatBaseIndexFileName(filterKey);
      if (ReportIndex.checkExists(this.auditCacheDir, baseIndexName)) {
        // load the report index from disk
        try {
          reportIndex = ReportIndex.load(this.auditCacheDir, baseIndexName);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }

        // cache the report index in memory
        log("LOADED INDEX FROM DISK: " + encodeFilterKey(filterMap));
        this.indexes.put(filterMap, reportIndex);

        // return the report index
        return reportIndex;
      }

      // get the first entry info in case there is only one entry
      String      firstProperty     = filterMap.keySet().iterator().next();
      Filter      firstFilter       = filterMap.values().iterator().next();
      FilterType  firstFilterType   = firstFilter.getFilterType();
      Set         firstFilterValues = firstFilter.getFilterValues();

      // check if we have more than one entry in the filter map
      if (filterMap.size() > 1) {
        // handle multiple entries by breaking it down to individual entries
        List<ReportIndex> sources = new ArrayList<>(filterMap.size());

        // iterate over the entries
        filterMap.entrySet().forEach(entry -> {
          // get the property key and the filter
          String key    = entry.getKey();
          Filter filter = entry.getValue();

          // create the source filter
          Map<String,Filter> srcFilter = Collections.singletonMap(key, filter);

          // get the source index
          ReportIndex source = this.getReportIndex(srcFilter, indexing);

          // add the source to the list to be intersected
          sources.add(source);
        });

        // check if any of the sources are null (i.e.: bad base indexes)
        if (sources.stream().anyMatch(s -> s == null)) {
          return null;
        }

        // create the report index as an intersection
        reportIndex = ReportIndex.intersect(sources);

      } else if (firstFilterType == SINGLE) {
        if (!indexing) {
          return null;
        }
        reportIndex = new ReportIndex();

      } else if (firstFilterType == ANY) {
        int count = firstFilterValues.size();

        // create list to hold the source indexes for the union
        List<ReportIndex> unionSources = new ArrayList<>(count);

        // iterate over the values
        firstFilterValues.forEach(value -> {
          // create source filter
          Map<String,Filter> srcFilter
            = Collections.singletonMap(firstProperty, new Filter(value));

          // get the source report index
          ReportIndex source = this.getReportIndex(srcFilter, indexing);

          // add the source to the list to be unioned together
          unionSources.add(source);
        });

        // check if any of the sources are null (i.e.: bad base indexes)
        if (unionSources.stream().anyMatch(s -> s == null)) {
          return null;
        }

        // create the report index as a union
        reportIndex = ReportIndex.union(unionSources);

      } else if (firstFilterType == ALL) {
        // handle multiple entries by breaking it down to the
        int count = firstFilterValues.size();
        List<ReportIndex> sources = new ArrayList<>(count);

        // iterate over the values
        firstFilterValues.forEach(value -> {
          Map<String, Filter> srcFilter
              = Collections.singletonMap(firstProperty, new Filter(value));

          // get the source index
          ReportIndex source = this.getReportIndex(srcFilter, indexing);

          // add the source to the list for the intersection
          sources.add(source);
        });

        if (sources.stream().anyMatch(s -> s == null)) {
          return null;
        }

        // create the report index as an intersection
        reportIndex = ReportIndex.intersect(sources);

      } else {
        throw new IllegalStateException(
            "Unrecognized filter type: " + firstFilterType);
      }

      // ensure the index gets saved when complete
      reportIndex.onComplete(completedIndex -> {
        if (!this.isInvalid()) {
          this.saveReportIndex(filterMap, completedIndex);
        }
      });

      // create an unmodifiable key for looking up the index later
      Map<String,Filter> filterMapCopy;
      if (filterMap.size() == 1) {
        String key    = filterMap.keySet().iterator().next();
        Filter value  = filterMap.get(key);
        filterMapCopy = Collections.singletonMap(key,value);
      } else {
        filterMapCopy = new TreeMap<>();
        filterMapCopy.putAll(filterMap);
        filterMapCopy = Collections.unmodifiableMap(filterMapCopy);
      }

      // keep a reference to the index
      this.indexes.put(filterMapCopy, reportIndex);
      if (valueCount == 1) {
        this.baseIndexes.put(filterMapCopy, reportIndex);
      } else if (valueCount == 2) {
        this.dualValueIndexes.put(filterMapCopy, reportIndex);
      }
      if (valueCount > 1 && !indexing && !reportIndex.isComplete()) {
        // prioritize this index
        reportIndex.prioritize();
      }
    }

    // return the report index
    return reportIndex;
  }

  private AuditGroup parseAuditGroup(String jsonText) {
    AccessToken   token           = new AccessToken();
    final String  valuePath       = "/AUDIT_PAGE/AUDIT_GROUPS/0";
    StringReader  stringReader    = new StringReader(jsonText);
    JsonReader    jsonReader      = Json.createReader(stringReader);
    JsonObject    jsonObject      = jsonReader.readObject();
    JsonValue     jsonValue       = jsonObject.getValue(valuePath);
    JsonObject    jsonAuditGroup  = jsonValue.asJsonObject();

    return AuditGroup.parseAuditGroup(null, jsonAuditGroup, token);
  }

  private Set<String> parseIndexArray(String jsonText) {
    StringReader  sr    = new StringReader(jsonText);
    JsonReader    jr    = Json.createReader(sr);
    JsonArray     arr   = jr.readArray();
    return arr.getValuesAs(JsonString.class).stream()
              .map(JsonString::getString).collect(Collectors.toSet());
  }

  private String encodeIndexArray(Set<String> values) {
    StringWriter sw = new StringWriter();
    JsonWriter   jw = Json.createWriter(sw);

    JsonArrayBuilder builder = Json.createArrayBuilder();
    values.forEach(v -> builder.add(v));
    JsonArray arr = builder.build();
    jw.writeArray(arr);
    return sw.toString();
  }

  private void restoreIndex(String key, String val)
    throws IOException
  {
    Map<String,Filter> filterMap
        = Collections.singletonMap(key, new Filter(val));

    String filterKey = encodeFilterKey(filterMap);
    String baseIndexName = this.formatBaseIndexFileName(filterKey);

    ReportIndex reportIndex = ReportIndex.load(this.auditCacheDir,
                                               baseIndexName);
    synchronized (this.indexes) {
      int valueCount = filterMap.values().stream()
          .mapToInt(f->f.getFilterValues().size())
          .sum();

      this.indexes.put(filterMap, reportIndex);
      if (valueCount == 1) {
        this.baseIndexes.put(filterMap, reportIndex);
      } else if (valueCount == 2) {
        this.dualValueIndexes.put(filterMap, reportIndex);
      }
    }
  }

  private Map<String,Filter> crossFilterMap(String matchKey, String principle) {
    Map<String,Filter> treeMap = new TreeMap<>();
    treeMap.put(MATCH_KEY, new Filter(matchKey));
    treeMap.put(PRINCIPLE, new Filter(principle));
    return treeMap;
  }

  private void primeReportIndexes(int           auditGroupIndex,
                                  Set<String>   matchKeys,
                                  Set<String>   allMatchKeys,
                                  Set<String>   principles,
                                  Set<String>   allPrinciples)
  {
    this.primeBaseReportIndexes(auditGroupIndex, MATCH_KEY, matchKeys);
    this.primeBaseReportIndexes(auditGroupIndex, PRINCIPLE, principles);

    Set<String> newMatchKeys = new HashSet<>(matchKeys);
    newMatchKeys.removeAll(allMatchKeys);

    Set<String> newPrinciples = new HashSet<>(principles);
    newPrinciples.removeAll(allPrinciples);

    // prime cross-property dual-valued indexes
    this.primeCrossReportIndexes(
        newMatchKeys, allMatchKeys, newPrinciples, allPrinciples);

    // add the new values to the sets containing all values
    allMatchKeys.addAll(newMatchKeys);
    allPrinciples.addAll(newPrinciples);

    // prime intra-property dual-valued indexes
    this.primeIntraReportIndexes(MATCH_KEY, newMatchKeys, allMatchKeys);
    this.primeIntraReportIndexes(PRINCIPLE, newPrinciples, allPrinciples);
  }

  private void primeBaseReportIndexes(int         auditGroupIndex,
                                      String      property,
                                      Set<String> values)
  {
    values.forEach(v -> {
      // create the single-valued filter
      Filter filter = new Filter(v.trim().toUpperCase());

      // create a filter map
      final Map<String,Filter> filterMap = Collections.singletonMap(property,
                                                                    filter);

      // get the index
      ReportIndex reportIndex = this.getReportIndex(filterMap, true);

      // add it to the index
      reportIndex.recordHit(auditGroupIndex);
    });
  }

  private void primeCrossReportIndexes(Set<String>   newMatchKeys,
                                       Set<String>   allMatchKeys,
                                       Set<String>   newPrinciples,
                                       Set<String>   allPrinciples)
  {
    // create cross-property dual-valued indexes
    newMatchKeys.forEach(matchKey -> {
      allPrinciples.forEach(principle -> {
        this.getReportIndex(this.crossFilterMap(matchKey, principle), true);
      });
      newPrinciples.forEach(principle -> {
        this.getReportIndex(this.crossFilterMap(matchKey, principle), true);
      });
    });
    newPrinciples.forEach(principle -> {
      allMatchKeys.forEach(matchKey -> {
        this.getReportIndex(this.crossFilterMap(matchKey, principle), true);
      });
    });
  }

  private void primeIntraReportIndexes(String        property,
                                       Set<String>   newValues,
                                       Set<String>   allValues)
  {
    // create intra-property dual-valued indexes
    newValues.forEach(newValue -> {
      allValues.forEach(otherValue -> {
        if (newValue.equalsIgnoreCase(otherValue)) return;

        Map<String, Filter> filterMap = Collections.singletonMap(
            property, new Filter(ANY, newValue, otherValue));

        this.getReportIndex(filterMap, true);
      });
    });
  }

  private List<KeyFilterCounts> getKeyFilterCounts(
      Map<String,Filter>  filterMap,
      int                 reportCount,
      boolean             reportComplete)
  {
    long preSync = System.currentTimeMillis();
    // get the base index keys
    Map<String, List<ValueCount>> baseCounts = new HashMap<>();
    synchronized (this.indexes) {
      long now = System.currentTimeMillis();
      log("TIME TO SYNCHRONIZE ON INDEXES: " + (now-preSync) + "ms");

      log("BASE INDEX COUNT: " + this.baseIndexes.size());
      // find the single-valued filter maps
      this.baseIndexes.keySet().forEach(map -> {
        // get the key
        String key = map.keySet().iterator().next();

        // get the associated filter
        Filter filter = map.get(key);

        // get the value
        Object value = filter.getFilterValues().iterator().next();

        // create the augmented filter map
        Map<String, Filter> filterMap2;
        if (filterMap == null || filterMap.size() == 0) {
          // just initialize with the new (next) filter itself
          filterMap2 = map;

        } else {
          // initialize the filter that is passed as a parameter
          filterMap2 = new TreeMap();
          filterMap2.putAll(filterMap);

          // check if the base key is present in the specified filter
          Filter filter2 = filterMap2.get(key);
          if (filter2 == null) {
            // base key is not present so create a new filter to add to the map
            filter2 = new Filter(value);

          } else if (filter2.getFilterValues().contains(value)) {
            // base key is present AND the base value is also present
            // nothing to do since the specified filter is already contains base
            Set set = new HashSet(filter2.getFilterValues());
            set.remove(value);
            if (set.size() == 0) {
              filter2 = null;
            } else if (set.size() == 1) {
              filter2 = new Filter(set.iterator().next());
            } else {
              filter2 = new Filter(ANY, set);
            }

          } else {
            // base key is present, but the base value is not so add the base
            // value to the filter to expend it (use ANY instead of ALL match)
            Set set = new HashSet(filter2.getFilterValues());
            set.add(value);
            filter2 = new Filter(ANY, set);
          }

          // check if we are removing this filter from the map
          if (filter2 == null) {
            filterMap2.remove(key);
            if (filterMap2.size() == 0) {
              filterMap2 = null;
            }
          } else {
            // augment filterMap2 with the new filter
            filterMap2.put(key, filter2);
          }
        }

        // get the report index
        ReportIndex reportIndex = null;

        // check if we have an augmented filter map
        if (filterMap2 != null) {
          // get the index for the specified filter
          reportIndex = this.getReportIndex(filterMap2, false);

          if (reportIndex != null) {
            final Map<String,Filter> otherFilterMap = filterMap2;

            Thread indexPrimerThread = new Thread(() -> {
              // ensure the next round of indexes is primed
              this.baseIndexes.keySet().forEach(baseFilterMap -> {
                String      baseKey     = baseFilterMap.keySet().iterator().next();
                Filter      baseFilter  = baseFilterMap.get(baseKey);
                Set<String> baseValues  = baseFilter.getFilterValues();
                Filter      otherFilter = otherFilterMap.get(baseKey);
                Set<String> otherValues = (otherFilter != null
                    ? otherFilter.getFilterValues()
                    : Collections.emptySet());

                // check if this base value is already in the map
                if (otherValues.containsAll(baseValues)) return;

                // create a new filter map
                Set primeValues = new HashSet(otherValues);
                primeValues.addAll(baseValues);
                Map<String, Filter> primeMap = new TreeMap<>(otherFilterMap);
                FilterType filterType = primeValues.size() == 1 ? SINGLE : ANY;
                Filter primeFilter = new Filter(filterType, primeValues);
                primeMap.put(baseKey, primeFilter);

                // get the report index for the prime map to ensure it is primed
                this.getReportIndex(primeMap, false);
              });
            });
            indexPrimerThread.start();
          }
        }

        boolean complete;
        Integer hitCount;
        if (reportIndex != null) {
          synchronized (reportIndex) {
            complete = reportIndex.isComplete();
            hitCount = (filterMap2 != null) ? reportIndex.getHitCount() : null;
          }
        } else {
          complete = reportComplete;
          hitCount = reportCount;
        }

        // create the value count
        ValueCount vc = new ValueCount(value, hitCount, complete);
        List<ValueCount> list = baseCounts.get(key);
        if (list == null) {
          list = new LinkedList<>();
          baseCounts.put(key, list);
        }
        list.add(vc);
      });

      baseCounts.entrySet().forEach(entry -> {
        String  field       = entry.getKey();
        Filter  fieldFilter = (filterMap == null) ? null : filterMap.get(field);
        Set     fieldValues = ((fieldFilter == null)
                               ? null : fieldFilter.getFilterValues());

        List<ValueCount> list = entry.getValue();

        list.sort((vc1, vc2) -> {
          // Do NOT sort by counts because each iteration reorders the count
          //Integer count1 = vc1.getCount();
          //Integer count2 = vc2.getCount();
          //if (count1 == null && count2 != null) {
          //  return -1;
          //}
          //if (count1 != null && count2 == null) {
          //  return 1;
          //}
          //int diff = 0;
          //if (count1 != null && count2 != null) {
          //  diff = count2.intValue() - count1.intValue();
          //}
          //if (diff != 0) return diff;
          Object v1 = vc1.getValue();
          Object v2 = vc2.getValue();
          if (filterMap != null && fieldValues != null) {
            if (fieldValues.contains(v1) && !fieldValues.contains(v2)) {
              return -1;
            }
            if (fieldValues.contains(v2) && !fieldValues.contains(v1)) {
              return 1;
            }
          }
          if (v1 instanceof Comparable && v2 instanceof Comparable) {
            Comparable c1 = (Comparable) v1;
            Comparable c2 = (Comparable) v2;
            return c1.compareTo(c2);
          } else {
            return v1.toString().compareTo(v2.toString());
          }
        });
      });

      List<KeyFilterCounts> result = new LinkedList<>();
      baseCounts.entrySet().forEach(entry -> {
        String key = entry.getKey();
        List<ValueCount> vals = entry.getValue();
        KeyFilterCounts kfc = new KeyFilterCounts(key);
        vals.forEach(v -> {
          kfc.append(v.getValue(), v.getCount(), v.isComplete());
        });
        result.add(kfc);
      });

      return result;
    }
  }

}
