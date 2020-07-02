package com.senzing.api.services;

import com.senzing.api.model.*;
import com.senzing.g2.engine.G2Engine;
import com.senzing.io.IOUtilities;
import com.senzing.io.RecordReader;
import com.senzing.io.TemporaryDataCache;
import com.senzing.util.AsyncWorkerPool;
import com.senzing.util.JsonUtils;
import com.senzing.util.Timers;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;

import javax.json.*;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;
import javax.ws.rs.sse.OutboundSseEvent;
import javax.ws.rs.sse.Sse;
import javax.ws.rs.sse.SseEventSink;
import java.io.*;
import java.security.MessageDigest;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static com.senzing.api.model.SzHttpMethod.POST;
import static com.senzing.api.services.ServicesUtil.*;
import static com.senzing.text.TextUtilities.*;
import static com.senzing.util.AsyncWorkerPool.*;
import static com.senzing.api.model.SzBulkDataStatus.*;
import static javax.ws.rs.core.MediaType.*;
import static com.senzing.util.LoggingUtilities.*;

/**
 * Bulk data REST services.
 */
@Path("/bulk-data")
@Produces("application/json; charset=UTF-8; qs=1.0")
public class BulkDataServices {
  /**
   * The file date pattern.
   */
  private static final String FILE_DATE_PATTERN = "yyyyMMdd_HHmmssX";

  /**
   * The time zone used for the time component of the build number.
   */
  private static final ZoneId UTC_ZONE = ZoneId.of("UTC");

  /**
   * The formatter for the file date.
   */
  private static final DateTimeFormatter FILE_DATE_FORMATTER
      = DateTimeFormatter.ofPattern(FILE_DATE_PATTERN);

  /**
   * The reconnect delay to use for events when providing SSE events.
   */
  public static final long RECONNECT_DELAY = 5000L;

  /**
   * SSE event type string for progress events.
   */
  public static final String PROGRESS_EVENT = "progress";

  /**
   * SSE event type string for failure events.
   */
  public static final String FAILED_EVENT = "failed";

  /**
   * SSE event type string for completion events.
   */
  public static final String COMPLETED_EVENT = "completed";

  /**
   * Analyzes the bulk data records.
   */
  @POST
  @Path("/analyze")
  public SzBulkDataAnalysisResponse analyzeBulkRecordsViaForm(
      @HeaderParam("Content-Type") MediaType mediaType,
      @FormDataParam("data") InputStream dataInputStream,
      @Context UriInfo uriInfo)
  {
    try {
      return this.analyzeBulkRecords(mediaType,
                                     dataInputStream,
                                     uriInfo,
                                     null,
                                     null,
                                     null);
    } catch (RuntimeException e) {
      throw logOnceAndThrow(e);

    } catch (Exception e) {
      throw logOnceAndThrow(new RuntimeException(e));
    }
  }

  /**
   * Analyzes the bulk data records.
   */
  @POST
  @Path("/analyze")
  @Consumes({ MediaType.APPLICATION_JSON,
      MediaType.TEXT_PLAIN,
      "text/csv",
      "application/x-jsonlines"})
  public SzBulkDataAnalysisResponse analyzeBulkRecordsDirect(
      @HeaderParam("Content-Type") MediaType mediaType,
      InputStream dataInputStream,
      @Context UriInfo uriInfo)
  {
    try {
      return this.analyzeBulkRecords(mediaType,
                                     dataInputStream,
                                     uriInfo,
                                     null,
                                     null,
                                     null);
    } catch (RuntimeException e) {
      throw logOnceAndThrow(e);

    } catch (Exception e) {
      throw logOnceAndThrow(new RuntimeException(e));
    }
  }

  /**
   * Analyzes the bulk data records.
   */
  @POST
  @Path("/analyze")
  @Consumes({ MediaType.APPLICATION_JSON,
              MediaType.TEXT_PLAIN,
              "text/csv",
              "application/x-jsonlines"})
  @Produces("text/event-stream;qs=0.9")
  public void analyzeBulkRecordsDirect(
      @HeaderParam("Content-Type") MediaType mediaType,
      InputStream dataInputStream,
      @Context UriInfo uriInfo,
      @QueryParam("progressPeriod") @DefaultValue("3000") long progressPeriod,
      @Context SseEventSink sseEventSink,
      @Context Sse sse)
  {
    try {
      this.analyzeBulkRecords(mediaType,
                              dataInputStream,
                              uriInfo,
                              progressPeriod,
                              sseEventSink,
                              sse);
    } catch (RuntimeException e) {
      throw logOnceAndThrow(e);

    } catch (Exception e) {
      throw logOnceAndThrow(new RuntimeException(e));

    }
  }

  /**
   * Analyzes the bulk data records.
   */
  @POST
  @Path("/analyze")
  @Produces("text/event-stream;qs=0.9")
  public void analyzeBulkRecordsViaForm(
      @HeaderParam("Content-Type") MediaType mediaType,
      @FormDataParam("data") InputStream dataInputStream,
      @Context UriInfo uriInfo,
      @QueryParam("progressPeriod") @DefaultValue("3000") long progressPeriod,
      @Context SseEventSink sseEventSink,
      @Context Sse sse)
  {
    try {
      this.analyzeBulkRecords(mediaType,
                              dataInputStream,
                              uriInfo,
                              progressPeriod,
                              sseEventSink,
                              sse);

    } catch (RuntimeException e) {
      throw logOnceAndThrow(e);

    } catch (Exception e) {
      throw logOnceAndThrow(new RuntimeException(e));
    }
  }

  /**
   * Analyzes the bulk data records.
   */
  @POST
  @Path("/load")
  public SzBulkLoadResponse loadBulkRecordsViaForm(
      @QueryParam("dataSource") String dataSource,
      @QueryParam("mapDataSources") String mapDataSources,
      @QueryParam("mapDataSource") List<String> mapDataSourceList,
      @QueryParam("entityType") String entityType,
      @QueryParam("mapEntityTypes") String mapEntityTypes,
      @QueryParam("mapEntityType") List<String> mapEntityTypeList,
      @QueryParam("loadId") String loadId,
      @DefaultValue("0") @QueryParam("maxFailures") int maxFailures,
      @HeaderParam("Content-Type") MediaType mediaType,
      @FormDataParam("data") InputStream dataInputStream,
      @FormDataParam("data") FormDataContentDisposition fileMetaData,
      @Context UriInfo uriInfo)
  {
    try {
      return this.loadBulkRecords(dataSource,
                                  mapDataSources,
                                  mapDataSourceList,
                                  entityType,
                                  mapEntityTypes,
                                  mapEntityTypeList,
                                  loadId,
                                  maxFailures,
                                  mediaType,
                                  dataInputStream,
                                  fileMetaData,
                                  uriInfo,
                                  null,
                                  null,
                                  null);

    } catch (ForbiddenException e) {
      throw e;

    } catch (RuntimeException e) {
      throw logOnceAndThrow(e);

    } catch (Exception e) {
      throw logOnceAndThrow(new RuntimeException(e));

    }
  }

  /**
   * Analyzes the bulk data records.
   */
  @POST
  @Path("/load")
  @Consumes({ MediaType.APPLICATION_JSON,
      MediaType.TEXT_PLAIN,
      "text/csv",
      "application/x-jsonlines"})
  public SzBulkLoadResponse loadBulkRecordsDirect(
      @QueryParam("dataSource") String dataSource,
      @QueryParam("mapDataSources") String mapDataSources,
      @QueryParam("mapDataSource") List<String> mapDataSourceList,
      @QueryParam("entityType") String entityType,
      @QueryParam("mapEntityTypes") String mapEntityTypes,
      @QueryParam("mapEntityType") List<String> mapEntityTypeList,
      @QueryParam("loadId") String loadId,
      @DefaultValue("0") @QueryParam("maxFailures") int maxFailures,
      @HeaderParam("Content-Type") MediaType mediaType,
      InputStream dataInputStream,
      @Context UriInfo uriInfo)
  {
    try {
      return this.loadBulkRecords(dataSource,
                                  mapDataSources,
                                  mapDataSourceList,
                                  entityType,
                                  mapEntityTypes,
                                  mapEntityTypeList,
                                  loadId,
                                  maxFailures,
                                  mediaType,
                                  dataInputStream,
                                  null,
                                  uriInfo,
                                  null,
                                  null,
                                  null);

    } catch (ForbiddenException e) {
      throw e;

    } catch (RuntimeException e) {
      throw logOnceAndThrow(e);

    } catch (Exception e) {
      throw logOnceAndThrow(new RuntimeException(e));

    }
  }

  /**
   * Analyzes the bulk data records.
   */
  @POST
  @Path("/load")
  @Produces("text/event-stream;qs=0.9")
  public void loadBulkRecordsViaForm(
      @QueryParam("dataSource") String dataSource,
      @QueryParam("mapDataSources") String mapDataSources,
      @QueryParam("mapDataSource") List<String> mapDataSourceList,
      @QueryParam("entityType") String entityType,
      @QueryParam("mapEntityTypes") String mapEntityTypes,
      @QueryParam("mapEntityType") List<String> mapEntityTypeList,
      @QueryParam("loadId") String loadId,
      @DefaultValue("0") @QueryParam("maxFailures") int maxFailures,
      @HeaderParam("Content-Type") MediaType mediaType,
      @FormDataParam("data") InputStream dataInputStream,
      @FormDataParam("data") FormDataContentDisposition fileMetaData,
      @Context UriInfo uriInfo,
      @QueryParam("progressPeriod") @DefaultValue("3000") long progressPeriod,
      @Context SseEventSink sseEventSink,
      @Context Sse sse)

  {
    try {
      this.loadBulkRecords(dataSource,
                           mapDataSources,
                           mapDataSourceList,
                           entityType,
                           mapEntityTypes,
                           mapEntityTypeList,
                           loadId,
                           maxFailures,
                           mediaType,
                           dataInputStream,
                           fileMetaData,
                           uriInfo,
                           progressPeriod,
                           sseEventSink,
                           sse);

    } catch (ForbiddenException e) {
      throw e;

    } catch (RuntimeException e) {
      throw logOnceAndThrow(e);

    } catch (Exception e) {
      throw logOnceAndThrow(new RuntimeException(e));

    }
  }

  /**
   * Analyzes the bulk data records.
   */
  @POST
  @Path("/load")
  @Consumes({ MediaType.APPLICATION_JSON,
      MediaType.TEXT_PLAIN,
      "text/csv",
      "application/x-jsonlines"})
  @Produces("text/event-stream;qs=0.9")
  public void loadBulkRecordsDirect(
      @QueryParam("dataSource") String dataSource,
      @QueryParam("mapDataSources") String mapDataSources,
      @QueryParam("mapDataSource") List<String> mapDataSourceList,
      @QueryParam("entityType") String entityType,
      @QueryParam("mapEntityTypes") String mapEntityTypes,
      @QueryParam("mapEntityType") List<String> mapEntityTypeList,
      @QueryParam("loadId") String loadId,
      @DefaultValue("0") @QueryParam("maxFailures") int maxFailures,
      @HeaderParam("Content-Type") MediaType mediaType,
      InputStream dataInputStream,
      @Context UriInfo uriInfo,
      @QueryParam("progressPeriod") @DefaultValue("3000") long progressPeriod,
      @Context SseEventSink sseEventSink,
      @Context Sse sse)
  {
    try {
      this.loadBulkRecords(dataSource,
                           mapDataSources,
                           mapDataSourceList,
                           entityType,
                           mapEntityTypes,
                           mapEntityTypeList,
                           loadId,
                           maxFailures,
                           mediaType,
                           dataInputStream,
                           null,
                           uriInfo,
                           progressPeriod,
                           sseEventSink,
                           sse);

    } catch (ForbiddenException e) {
      throw e;

    } catch (RuntimeException e) {
      throw logOnceAndThrow(e);

    } catch (Exception e) {
      throw logOnceAndThrow(new RuntimeException(e));

    }
  }

  /**
   * Analyzes the bulk data and returns information about it.
   */
  private SzBulkDataAnalysisResponse analyzeBulkRecords(
      MediaType                   mediaType,
      InputStream                 dataInputStream,
      UriInfo                     uriInfo,
      Long                        sseProgressPeriod,
      SseEventSink                sseEventSink,
      Sse                         sse)
  {
    OutboundSseEvent.Builder eventBuilder
        = (sseEventSink != null && sse != null) ? sse.newEventBuilder() : null;
    int eventId = 0;

    SzBulkDataAnalysis dataAnalysis = new SzBulkDataAnalysis();
    Timers timers = newTimers();
    try {
      BulkDataSet bulkDataSet = new BulkDataSet(mediaType, dataInputStream);
      TemporaryDataCache dataCache = bulkDataSet.dataCache;

      // if charset is unknown then try to detect
      String charset = bulkDataSet.characterEncoding;
      dataAnalysis.setCharacterEncoding(charset);

      long start = System.currentTimeMillis();
      // check if we need to auto-detect the media type
      try (InputStream        is  = dataCache.getInputStream(true);
           InputStreamReader  isr = new InputStreamReader(is, charset);
           BufferedReader     br  = new BufferedReader(isr))
      {
        // if format is null then RecordReader will auto-detect
        RecordReader recordReader = new RecordReader(bulkDataSet.format, br);
        bulkDataSet.format = recordReader.getFormat();
        dataAnalysis.setMediaType(bulkDataSet.format.getMediaType());

        for (JsonObject record = recordReader.readRecord();
             (record != null);
             record = recordReader.readRecord())
        {
          String dataSrc    = JsonUtils.getString(record, "DATA_SOURCE");
          String entityType = JsonUtils.getString(record, "ENTITY_TYPE");
          String recordId   = JsonUtils.getString(record, "RECORD_ID");
          dataAnalysis.trackRecord(dataSrc, entityType, recordId);

          long now = System.currentTimeMillis();
          if (eventBuilder != null && (now - start > sseProgressPeriod)) {
            start = now;
            OutboundSseEvent event =
                eventBuilder.name(PROGRESS_EVENT)
                    .id(String.valueOf(eventId++))
                    .mediaType(APPLICATION_JSON_TYPE)
                    .data(new SzBulkDataAnalysisResponse(
                        POST, 200, uriInfo, timers, dataAnalysis))
                    .reconnectDelay(RECONNECT_DELAY)
                    .build();
            sseEventSink.send(event);
          }
        }
      }

    } catch (IOException e) {
      e.printStackTrace();
      dataAnalysis.setStatus(ABORTED);

      SzBulkDataAnalysisResponse response = new SzBulkDataAnalysisResponse(
          POST,200, uriInfo, timers, dataAnalysis);

      abortOperation(e,
                     response,
                     uriInfo,
                     timers,
                     eventId,
                     eventBuilder,
                     sseEventSink);
    }

    dataAnalysis.setStatus(COMPLETED);

    SzBulkDataAnalysisResponse response = new SzBulkDataAnalysisResponse(
        POST,200, uriInfo, timers, dataAnalysis);

    return completeOperation(
        eventBuilder, sseEventSink, eventId, response);
  }

  /**
   * Analyzes the bulk data and returns information about it.
   */
  private SzBulkLoadResponse loadBulkRecords(
      String                      dataSource,
      String                      mapDataSources,
      List<String>                mapDataSourceList,
      String                      entityType,
      String                      mapEntityTypes,
      List<String>                mapEntityTypeList,
      String                      explicitLoadId,
      int                         maxFailures,
      MediaType                   mediaType,
      InputStream                 dataInputStream,
      FormDataContentDisposition  fileMetaData,
      UriInfo                     uriInfo,
      Long                        sseProgressPeriod,
      SseEventSink                sseEventSink,
      Sse                         sse)
  {
    OutboundSseEvent.Builder eventBuilder
        = (sseEventSink != null && sse != null) ? sse.newEventBuilder() : null;
    int eventId = 0;

    SzBulkLoadResult bulkLoadResult = new SzBulkLoadResult();

    Timers timers = newTimers();
    SzApiProvider provider = SzApiProvider.Factory.getProvider();
    ensureLoadingIsAllowed(provider, POST, uriInfo, timers);

    // normalize and validate the data source
    if (dataSource != null) {
      dataSource = dataSource.trim().toUpperCase();

      if (!provider.getDataSources(dataSource).contains(dataSource)) {
        throw newBadRequestException(
            POST, uriInfo, timers,
            "The value for the specified \"dataSource\" parameter is not a "
            + "configured data source: " + dataSource);
      }
    }

    // normalize and validate the entity type
    if (entityType != null) {
      entityType = entityType.trim().toUpperCase();

      if (!provider.getEntityTypes(entityType).contains(entityType)) {
        throw newBadRequestException(
            POST, uriInfo, timers,
            "The value for the specified \"entityType\" parameter is not a "
            + "configured entity type: " + entityType);
      }
    }

    // populate the entity type and data source maps
    Map<String, String> dataSourceMap = new HashMap<>();
    Map<String, String> entityTypeMap = new HashMap<>();

    // by default we override missing entity types to GENERIC (though this may
    // get overridden after processing the entity types)
    entityTypeMap.put("", "GENERIC");

    processDataSources(
        dataSourceMap, mapDataSources, provider, timers, uriInfo);
    processDataSources(
        dataSourceMap, mapDataSourceList, provider, timers, uriInfo);
    processEntityTypes(
        entityTypeMap, mapEntityTypes, provider, timers, uriInfo);
    processEntityTypes(
        entityTypeMap, mapEntityTypeList, provider, timers, uriInfo);

    // put the default overrides in the map with the null key
    if (dataSource != null) dataSourceMap.put(null, dataSource);
    if (entityType != null) entityTypeMap.put(null, entityType);

    try {
      BulkDataSet bulkDataSet = new BulkDataSet(mediaType, dataInputStream);

      TemporaryDataCache dataCache = bulkDataSet.dataCache;

      String charset = bulkDataSet.characterEncoding;

      String loadId = (explicitLoadId == null)
          ? formatLoadId(dataCache, fileMetaData) : explicitLoadId;

      int concurrency = provider.getConcurrency();
      AsyncWorkerPool<EngineResult> asyncPool
          = new AsyncWorkerPool<>(loadId, concurrency);

      List<Timers> timerPool = new ArrayList<>(concurrency);
      for (int index = 0; index < concurrency; index++) {
        timerPool.add(new Timers());
      }

      long start = System.currentTimeMillis();

      // check if we need to auto-detect the media type
      try (InputStream        is  = dataCache.getInputStream(true);
           InputStreamReader  isr = new InputStreamReader(is, charset);
           BufferedReader     br  = new BufferedReader(isr))
      {
        // if format is null then RecordReader will auto-detect
        RecordReader recordReader = new RecordReader(bulkDataSet.format,
                                                     br,
                                                     dataSourceMap,
                                                     entityTypeMap,
                                                     loadId);
        bulkDataSet.format = recordReader.getFormat();
        bulkLoadResult.setCharacterEncoding(charset);
        bulkLoadResult.setMediaType(bulkDataSet.format.getMediaType());

        boolean           concurrent       = false;
        boolean           done             = false;
        List<JsonObject>  first1000Records = new LinkedList<>();

        // loop through the records and handle each record
        while (!done) {
          JsonObject record = null;
          if (concurrent && first1000Records.size() > 0) {
            // get the first record from the buffer of up to 1000 records
            record = first1000Records.remove(0);
          } else {
            record = recordReader.readRecord();
          }

          // check if the record is null
          if (record == null) {
            done = true;
            continue;
          }

          // peel off the first 1000 records to see if we have less than 1000
          if (!concurrent && first1000Records.size() <= 1000) {
            // add the record to the first-1000 cache
            first1000Records.add(record);

            // check if we have more than 1000 records
            if (first1000Records.size() > 1000) concurrent = true;

            // continue for now
            continue;
          }

          // check if we have a data source and entity type
          String resolvedDS = JsonUtils.getString(record, "DATA_SOURCE");
          String resolvedET = JsonUtils.getString(record, "ENTITY_TYPE");
          if (resolvedDS == null || resolvedDS.trim().length() == 0
              || resolvedET == null || resolvedET.trim().length() == 0)
          {
            bulkLoadResult.trackIncompleteRecord(resolvedDS, resolvedET);

          } else {
            Timers subTimers  = timerPool.remove(0);
            AsyncResult<EngineResult> asyncResult = null;
            try {
              asyncResult = this.asyncProcessRecord(asyncPool,
                                                    provider,
                                                    subTimers,
                                                    record,
                                                    loadId);
            } finally {
              this.trackLoadResult(asyncResult, bulkLoadResult);
              timerPool.add(subTimers);
            }
          }

          // count the number of failures
          int failedCount = bulkLoadResult.getFailedRecordCount()
              + bulkLoadResult.getIncompleteRecordCount();

          if (maxFailures > 0 && failedCount >= maxFailures) {
            bulkLoadResult.setStatus(ABORTED);
            break;
          }

          long now = System.currentTimeMillis();

          if (eventBuilder != null && (now - start > sseProgressPeriod)) {
            start = now;
            OutboundSseEvent event =
                eventBuilder.name(PROGRESS_EVENT)
                    .id(String.valueOf(eventId++))
                    .mediaType(APPLICATION_JSON_TYPE)
                    .data(new SzBulkLoadResponse(
                        POST, 200, uriInfo, timers, bulkLoadResult))
                    .reconnectDelay(RECONNECT_DELAY)
                    .build();
            sseEventSink.send(event);
          }
        }

        // check if we have less than 1000 records
        if (first1000Records.size()>0 && bulkLoadResult.getStatus()!=ABORTED)
        {
          this.processRecords(provider,
                              timers,
                              first1000Records,
                              loadId,
                              bulkLoadResult,
                              maxFailures);
        }

        // close out any in-flight loads from the asynchronous pool
        List<AsyncResult<EngineResult>> results = asyncPool.close();
        for (AsyncResult<EngineResult> asyncResult : results) {
          this.trackLoadResult(asyncResult, bulkLoadResult);
        }

        // merge the timers
        for (Timers subTimer: timerPool) {
          timers.mergeWith(subTimer);
        }

        if (bulkLoadResult.getStatus() != ABORTED) {
          bulkLoadResult.setStatus(COMPLETED);
        }

      } finally {
        dataCache.delete();
      }

    } catch (IOException e) {
      bulkLoadResult.setStatus(ABORTED);
      SzBulkLoadResponse response
          = new SzBulkLoadResponse(POST,
                                   200,
                                   uriInfo,
                                   timers,
                                   bulkLoadResult);
      abortOperation(e,
                     response,
                     uriInfo,
                     timers,
                     eventId,
                     eventBuilder,
                     sseEventSink);
    }

    SzBulkLoadResponse response
        = new SzBulkLoadResponse(POST,
                                 200,
                                 uriInfo,
                                 timers,
                                 bulkLoadResult);

    return completeOperation(
        eventBuilder, sseEventSink, eventId, response);
  }

  /**
   * Asynchronously process a record using the specified {@link SzApiProvider}
   * and {@link AsyncWorkerPool}.  The returned {@link AsyncResult} is from
   * a previously executed task on the same thread or <tt>null</tt> if the
   * worker thread employed has not previously executed a task.
   */
  private AsyncResult<EngineResult> asyncProcessRecord(
      AsyncWorkerPool<EngineResult> asyncPool,
      SzApiProvider                 provider,
      Timers                        timers,
      JsonObject                    record,
      String                        loadId)
  {
    String dataSource = JsonUtils.getString(record, "DATA_SOURCE");
    String entityType = JsonUtils.getString(record, "ENTITY_TYPE");
    String recordId   = JsonUtils.getString(record, "RECORD_ID");
    String recordJSON = JsonUtils.toJsonText(record);

    G2Engine engineApi = provider.getEngineApi();
    return asyncPool.execute(() -> {
      try {
        // otherwise try to load the record
        enteringQueue(timers);
        return provider.executeInThread(() -> {
          exitingQueue(timers);
          int returnCode = this.addRecord(engineApi,
                                          dataSource,
                                          recordId,
                                          recordJSON,
                                          loadId,
                                          timers);

          return new EngineResult(
              dataSource, entityType, timers, returnCode, engineApi);
        });

      } catch (Exception e) {
        JsonObjectBuilder job = Json.createObjectBuilder();
        job.add("dataSource", dataSource);
        job.add("entityType", entityType);
        String details = JsonUtils.toJsonText(job);
        throw new Exception(details, e);
      }
    });
  }

  /**
   * Asynchronously process a record using the specified {@link SzApiProvider}
   * and {@link AsyncWorkerPool}.  The returned {@link AsyncResult} is from
   * a previously executed task on the same thread or <tt>null</tt> if the
   * worker thread employed has not previously executed a task.
   */
  private void processRecords(
      SzApiProvider     provider,
      Timers            timers,
      List<JsonObject>  records,
      String            loadId,
      SzBulkLoadResult  bulkLoadResult,
      int               maxFailures)
  {
    G2Engine engineApi = provider.getEngineApi();
    // otherwise try to load the record
    enteringQueue(timers);
    provider.executeInThread(() -> {
      exitingQueue(timers);
      for (JsonObject record : records) {

        String dataSource = JsonUtils.getString(record, "DATA_SOURCE");
        String entityType = JsonUtils.getString(record, "ENTITY_TYPE");
        String recordId   = JsonUtils.getString(record, "RECORD_ID");
        String recordJSON = JsonUtils.toJsonText(record);

        // check if we have a data source and entity type
        if (dataSource == null || dataSource.trim().length() == 0
            || entityType == null || entityType.trim().length() == 0) {
          bulkLoadResult.trackIncompleteRecord(dataSource, entityType);

        } else {
          int returnCode = this.addRecord(engineApi,
                                          dataSource,
                                          recordId,
                                          recordJSON,
                                          loadId,
                                          timers);

          EngineResult engineResult = new EngineResult(
              dataSource, entityType, timers, returnCode, engineApi);

          this.trackLoadResult(engineResult, bulkLoadResult);
        }

        // count the number of failures
        int failedCount = bulkLoadResult.getFailedRecordCount()
            + bulkLoadResult.getIncompleteRecordCount();

        if (maxFailures > 0 && failedCount >= maxFailures) {
          bulkLoadResult.setStatus(ABORTED);
          break;
        }
      }

      // return null
      return null;
    });
  }

  /**
   * Adds the record either with or without a record ID and tracks the timing.
   */
  private int addRecord(G2Engine    engineApi,
                        String      dataSource,
                        String      recordId,
                        String      recordJSON,
                        String      loadId,
                        Timers      timers)
  {
    int returnCode;
    if (recordId != null) {
      callingNativeAPI(timers, "engine", "addRecord");
      returnCode = engineApi.addRecord(dataSource,
                                       recordId,
                                       recordJSON,
                                       loadId);
      calledNativeAPI(timers, "engine", "addRecord");

    } else {
      callingNativeAPI(timers, "engine",
                       "addRecordWithReturnedRecordID");
      StringBuffer sb = new StringBuffer();
      returnCode = engineApi.addRecordWithReturnedRecordID(
          dataSource, sb, recordJSON, loadId);
      calledNativeAPI(timers, "engine",
                      "addRecordWithReturnedRecordID");
    }
    return returnCode;
  }

  /**
   * Tracks the asynchronous record load result in the {@link SzBulkLoadResult}.
   */
  private void trackLoadResult(AsyncResult<EngineResult> asyncResult,
                               SzBulkLoadResult          bulkLoadResult)
  {
    // check the result
    if (asyncResult != null) {
      EngineResult engineResult = null;
      try {
        // get the value from the async result (may throw an exception)
        engineResult = asyncResult.getValue();

      } catch (Exception e) {
        // an exception was thrown in trying to get the result
        String      jsonText  = e.getMessage();
        JsonObject  jsonObj   = JsonUtils.parseJsonObject(jsonText);

        String failDataSource = JsonUtils.getString(jsonObj, "dataSource");
        String failEntityType = JsonUtils.getString(jsonObj, "entityType");
        Throwable cause = e.getCause();
        bulkLoadResult.trackFailedRecord(
            failDataSource, failEntityType, new SzError(cause.getMessage()));
      }

      // track the result
      if (engineResult != null) {
        this.trackLoadResult(engineResult, bulkLoadResult);
      }
    }
  }

  /**
   * Tracks the asynchronous record load result in the {@link SzBulkLoadResult}.
   */
  private void trackLoadResult(EngineResult       engineResult,
                               SzBulkLoadResult   bulkLoadResult)
  {
    // check if the add failed or succeeded
    if (engineResult.isFailed()) {
      // adding the record failed, record the failure
      bulkLoadResult.trackFailedRecord(
          engineResult.dataSource,
          engineResult.entityType,
          engineResult.errorCode,
          engineResult.errorMsg);
    } else {
      // adding the record succeeded, record the loaded record
      bulkLoadResult.trackLoadedRecord(engineResult.dataSource,
                                       engineResult.entityType);
    }
  }

  /**
   * Formats load ID using the specified data cache
   */
  private static String formatLoadId(TemporaryDataCache         dataCache,
                                     FormDataContentDisposition fileMetaData)
  {
    String fileKey = (fileMetaData != null) ? fileMetaData.getName() : null;
    if (fileKey == null) {
      try (InputStream is = dataCache.getInputStream();)
      {
        byte[]        bytes     = new byte[1024];
        MessageDigest md5       = MessageDigest.getInstance("MD5");
        int           readCount = is.read(bytes);
        md5.update(bytes, 0, readCount);
        byte[] hash = md5.digest();
        fileKey = Base64.getEncoder().encodeToString(hash);

      } catch (Exception e) {
        fileKey = randomPrintableText(30);
      }
    }
    Date fileDate = (fileMetaData != null)
        ? fileMetaData.getModificationDate()
        : null;

    if (fileDate == null && fileMetaData != null) {
      fileDate = fileMetaData.getCreationDate();
    }

    ZonedDateTime fileDateTime = (fileDate == null)
        ? null : ZonedDateTime.ofInstant(fileDate.toInstant(), UTC_ZONE);
    ZonedDateTime now = ZonedDateTime.now(UTC_ZONE);

    String fileDateText = (fileDate == null)
        ? "?" : FILE_DATE_FORMATTER.format(fileDate.toInstant());
    String nowText = (now == null) ? "?" : FILE_DATE_FORMATTER.format(now);

    return fileKey + "_" + fileDateText + "_" + nowText;
  }

  /**
   * Encapsulates a bulk data set.
   */
  private static class BulkDataSet {
    private String characterEncoding;
    private String mediaType = null;
    private RecordReader.Format format;
    private TemporaryDataCache dataCache;

    public BulkDataSet(MediaType mediaType, InputStream inputStream)
        throws IOException
    {
      this.characterEncoding = mediaType.getParameters().get("charset");
      String baseMediaType = mediaType.getType() + "/" + mediaType.getSubtype();
      if (baseMediaType != null) baseMediaType = baseMediaType.toLowerCase();
      switch (baseMediaType) {
        case "multipart/form-data":
          this.characterEncoding = null;
        default:
          this.format = RecordReader.Format.fromMediaType(baseMediaType);
      }

      if (this.format != null) {
        this.mediaType = this.format.getMediaType();
      }

      try {
        this.dataCache = new TemporaryDataCache(inputStream);

        // if charset is unknown then try to detect
        if (this.characterEncoding == null) {
          try (InputStream is = this.dataCache.getInputStream()) {
            this.characterEncoding = IOUtilities.detectCharacterEncoding(is);
          }
          if (this.characterEncoding == null) this.characterEncoding = "UTF-8";
        }

      } catch (IOException e) {
        if (!isLastLoggedException(e)) {
          e.printStackTrace();
        }
        setLastLoggedAndThrow(e);
      }
    }
  }

  /**
   * The result from the engine.
   */
  public static class EngineResult {
    private int     returnCode  = 0;
    private String  dataSource  = null;
    private String  entityType  = null;
    private String  errorCode   = null;
    private String  errorMsg    = null;
    private Timers  timers      = null;
    private EngineResult(String   dataSource,
                         String   entityType,
                         Timers   timers,
                         int      returnCode,
                         G2Engine engine)
    {
      this.dataSource = dataSource;
      this.entityType = entityType;
      this.returnCode = returnCode;
      this.timers     = timers;
      if (this.returnCode != 0) {
        this.errorCode  = "" + engine.getLastExceptionCode();
        this.errorMsg   = engine.getLastException();
      }
    }
    private boolean isFailed() {
      return (this.returnCode != 0);
    }
    public String toString() {
      return "{ returnCode=[ " + this.returnCode
              + " ], dataSource=[ " + this.dataSource
              + " ], entityType=[ " + this.entityType
              + " ], errorCode=[ " + this.errorCode
              + " ], errorMsg=[ " + this.errorMsg
              + " ] }";
    }
  }

  private static <T extends SzBasicResponse> T completeOperation(
      OutboundSseEvent.Builder  eventBuilder,
      SseEventSink              sseEventSink,
      int                       eventId,
      T                         response)
  {
    if (eventBuilder != null) {
      OutboundSseEvent event
          = eventBuilder.name(COMPLETED_EVENT)
          .id(String.valueOf(eventId++))
          .mediaType(APPLICATION_JSON_TYPE)
          .data(response)
          .reconnectDelay(RECONNECT_DELAY)
          .build();
      sseEventSink.send(event);
      sseEventSink.close();
      return null;
    }

    return response;
  }

  private static <T extends SzBasicResponse> T abortOperation(
      Exception                 failure,
      T                         response,
      UriInfo                   uriInfo,
      Timers                    timers,
      int                       eventId,
      OutboundSseEvent.Builder  eventBuilder,
      SseEventSink              sseEventSink)
      throws WebApplicationException
  {
    if (!isLastLoggedException(failure)) {
      failure.printStackTrace();
    }
    setLastLoggedException(failure);
    if (eventBuilder == null) {
      throw newInternalServerErrorException(POST, uriInfo, timers, failure);

    } else {
      OutboundSseEvent abortEvent
          = eventBuilder.name(PROGRESS_EVENT)
          .id(String.valueOf(eventId++))
          .mediaType(APPLICATION_JSON_TYPE)
          .data(response)
          .reconnectDelay(RECONNECT_DELAY)
          .build();
      sseEventSink.send(abortEvent);

      SzErrorResponse errorResponse
          = new SzErrorResponse(POST, 500, uriInfo, timers, failure);

      OutboundSseEvent failEvent
          = eventBuilder.name(FAILED_EVENT)
          .id(String.valueOf(eventId++))
          .mediaType(APPLICATION_JSON_TYPE)
          .data(errorResponse)
          .reconnectDelay(RECONNECT_DELAY)
          .build();
      sseEventSink.send(failEvent);
      sseEventSink.close();

      return null;
    }
  }

  private static void processDataSources(Map<String, String> dataSourceMap,
                                         String              mapDataSources,
                                         SzApiProvider       provider,
                                         Timers              timers,
                                         UriInfo             uriInfo)
  {
    // check if the mapDataSources parameter is provided
    if (mapDataSources != null && mapDataSources.trim().length() > 0) {
      try {
        JsonObject jsonObject = JsonUtils.parseJsonObject(mapDataSources);
        jsonObject.entrySet().forEach(entry -> {
          String key = entry.getKey();
          JsonValue value = entry.getValue();
          if (value.getValueType() != JsonValue.ValueType.STRING) {
            throw newBadRequestException(
                POST, uriInfo, timers,
                "At least one JSON property (\"" + key + "\") in the "
                    + "\"mapDataSources\" parameter does NOT have a "
                    + "String JSON value (" + JsonUtils.toJsonText(value)
                    + "): " + mapDataSources);
          }
          String source = ((JsonString) value).getString().trim().toUpperCase();
          if (!provider.getDataSources(source).contains(source)) {
            throw newBadRequestException(
                POST, uriInfo, timers,
                "The data source mapping for \"" + key + "\" in the "
                    + "\"mapDataSources\" parameter has a value (\"" + source
                    + "\") that is not a configured data source: "
                    + mapDataSources);
          }
          dataSourceMap.put(key, source);
        });
      } catch (Exception e) {
        throw newBadRequestException(
            POST, uriInfo, timers,
            "The \"mapDataSources\" parameter is not a valid URL-encoded JSON "
                + "of String property names and String data source code values: "
                + mapDataSources);
      }
    }
  }


  private static void processDataSources(Map<String, String> dataSourceMap,
                                         List<String>        mapDataSourceList,
                                         SzApiProvider       provider,
                                         Timers              timers,
                                         UriInfo             uriInfo)
  {
    // check if the mapDataSources parameter is provided
    if (mapDataSourceList != null && mapDataSourceList.size() > 0) {
      for (String mapping : mapDataSourceList) {
        char sep = mapping.charAt(0);
        int index = mapping.indexOf(sep, 1);
        if (index < 0 || index == mapping.length() - 1) {
          throw newBadRequestException(
              POST, uriInfo, timers,
              "The specified data source mapping is not a valid "
                  + "delimited string: " + mapping);
        }
        String source1 = mapping.substring(1, index).trim();
        String source2 = mapping.substring(index + 1).trim().toUpperCase();

        if (!provider.getDataSources(source2).contains(source2)) {
          throw newBadRequestException(
              POST, uriInfo, timers,
              "The data source mapping for \"" + source1 + "\" in the "
                  + "\"mapDataSource\" parameter has a value (\"" + source2
                  + "\") that is not a configured data source: "
                  + mapping);
        }
        dataSourceMap.put(source1, source2);
      }
    }
  }

  private static void processEntityTypes(Map<String, String> entityTypeMap,
                                         String              mapEntityTypes,
                                         SzApiProvider       provider,
                                         Timers              timers,
                                         UriInfo             uriInfo)
  {
    // check if the mapDataSources parameter is provided
    if (mapEntityTypes != null && mapEntityTypes.trim().length() > 0) {
      try {
        JsonObject jsonObject = JsonUtils.parseJsonObject(mapEntityTypes);
        jsonObject.entrySet().forEach(entry -> {
          String key = entry.getKey();
          JsonValue value = entry.getValue();
          if (value.getValueType() != JsonValue.ValueType.STRING) {
            throw newBadRequestException(
                POST, uriInfo, timers,
                "At least one JSON property (\"" + key + "\") in the "
                    + "\"mapEntityTypes\" parameter does NOT have a "
                    + "String JSON value (" + JsonUtils.toJsonText(value)
                    + "): " + mapEntityTypes);
          }
          String etype = ((JsonString) value).getString().trim().toUpperCase();
          if (!provider.getEntityTypes(etype).contains(etype)) {
            throw newBadRequestException(
                POST, uriInfo, timers,
                "The entity type mapping for \"" + key + "\" in the "
                    + "\"mapEntityTypes\" parameter has a value (\"" + etype
                    + "\") that is not a configured entity type: "
                    + mapEntityTypes);
          }
          entityTypeMap.put(key, etype);
        });
      } catch (Exception e) {
        throw newBadRequestException(
            POST, uriInfo, timers,
            "The \"mapEntityTypes\" parameter is not a valid URL-encoded JSON "
                + "of String property names and String entity type code values: "
                + mapEntityTypes);
      }
    }
  }


  private static void processEntityTypes(Map<String, String> entityTypeMap,
                                         List<String>        mapEntityTypeList,
                                         SzApiProvider       provider,
                                         Timers              timers,
                                         UriInfo             uriInfo)
  {
    // check if the mapDataSources parameter is provided
    if (mapEntityTypeList != null && mapEntityTypeList.size() > 0) {
      for (String mapping : mapEntityTypeList) {
        char sep = mapping.charAt(0);
        int index = mapping.indexOf(sep, 1);
        if (index < 0 || index == mapping.length() - 1) {
          throw newBadRequestException(
              POST, uriInfo, timers,
              "The specified entity type mapping is not a valid "
                  + "delimited string: " + mapping);
        }
        String etype1 = mapping.substring(1, index).trim();
        String etype2 = mapping.substring(index + 1).trim().toUpperCase();

        if (!provider.getEntityTypes(etype2).contains(etype2)) {
          throw newBadRequestException(
              POST, uriInfo, timers,
              "The entity type mapping for \"" + etype1 + "\" in the "
                  + "\"mapEntityType\" parameter has a value (\"" + etype2
                  + "\") that is not a configured entity type: "
                  + mapping);
        }
        entityTypeMap.put(etype1, etype2);
      }
    }
  }
}
