package com.senzing.api.services;

import com.senzing.api.model.*;
import com.senzing.api.websocket.JsonEncoder;
import com.senzing.api.websocket.OnUpgrade;
import com.senzing.api.websocket.StringDecoder;
import com.senzing.g2.engine.G2Engine;
import com.senzing.io.IOUtilities;
import com.senzing.io.RecordReader;
import com.senzing.io.TemporaryDataCache;
import com.senzing.util.AccessToken;
import com.senzing.util.AsyncWorkerPool;
import com.senzing.util.JsonUtils;
import com.senzing.util.Timers;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;

import javax.json.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.websocket.*;
import javax.websocket.server.ServerEndpoint;
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
import static com.senzing.api.model.SzHttpMethod.GET;
import static com.senzing.text.TextUtilities.*;
import static com.senzing.util.AsyncWorkerPool.*;
import static com.senzing.api.model.SzBulkDataStatus.*;
import static javax.ws.rs.core.MediaType.*;
import static com.senzing.util.LoggingUtilities.*;
import static com.senzing.io.IOUtilities.*;

/**
 * Bulk data REST services.
 */
@Path("/bulk-data")
@Produces(APPLICATION_JSON)
public class BulkDataServices extends ServicesSupport {
  /**
   * The size of the piped input stream buffer size (10MB).
   */
  public static final int PIPE_SIZE = 1024 * 1024 * 10;

  /**
   * The {@link MediaType} with text/plain and charset=utf8
   */
  private static final MediaType TEXT_PLAIN_UTF8_TYPE = new MediaType(
      TEXT_PLAIN_TYPE.getType(), TEXT_PLAIN_TYPE.getSubtype(), UTF_8);

  /**
   * The <tt>"text/csv"</tt> media type string.
   */
  private static final String TEXT_CSV = "text/csv";

  /**
   * The <tt>"application/x-jsonlines"</tt> media type string.
   */
  private static final String APPLICATION_JSONLINES = "application/x-jsonlines";

  /**
   * The <tt>"text/event-stream"</tt> media type string.
   */
  private static final String TEXT_EVENT_STREAM = "text/event-stream";

  /**
   * The file date pattern.
   */
  private static final String FILE_DATE_PATTERN = "yyyyMMdd_HHmmssX";

  /**
   * The default maximum number of seconds to wait between receiving
   * Web Socket messages from the client before triggering EOF on the
   * incoming stream.
   */
  public static final Long DEFAULT_EOF_SEND_TIMEOUT = 3L;

  /**
   * The default progress period as the number of milliseconds between sending
   * progress responses to the client for SSE and Web Sockets.
   */
  public static final Long DEFAULT_PROGRESS_PERIOD = 3000L;

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
   * A private instance of the {@link BulkDataServices} class.
   */
  private static final BulkDataServices INSTANCE = new BulkDataServices();

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
    SzApiProvider provider    = this.getApiProvider();
    Timers        timers      = this.newTimers();
    AccessToken   accessToken = null;
    accessToken = provider.authorizeProlongedOperation();
    if (accessToken == null) {
      throw this.newServiceUnavailableErrorException(
          POST, uriInfo, timers,
          "Too many prolonged operations running.  Try again later.");
    }
    try {
      return this.analyzeBulkRecords(provider,
                                     timers,
                                     mediaType,
                                     dataInputStream,
                                     uriInfo,
                                     null,
                                     null,
                                     null,
                                     null);

    } catch (RuntimeException e) {
      throw logOnceAndThrow(e);

    } catch (Exception e) {
      throw logOnceAndThrow(new RuntimeException(e));
    } finally {
      provider.concludeProlongedOperation(accessToken);
    }
  }

  /**
   * Analyzes the bulk data records.
   */
  @POST
  @Path("/analyze")
  @Consumes({ APPLICATION_JSON,
              TEXT_PLAIN,
              TEXT_CSV,
              APPLICATION_JSONLINES })
  public SzBulkDataAnalysisResponse analyzeBulkRecordsDirect(
      @HeaderParam("Content-Type") MediaType mediaType,
      InputStream dataInputStream,
      @Context UriInfo uriInfo)
  {
    SzApiProvider provider    = this.getApiProvider();
    Timers        timers      = this.newTimers();
    AccessToken   accessToken = provider.authorizeProlongedOperation();
    if (accessToken == null) {
      throw this.newServiceUnavailableErrorException(
          POST, uriInfo, timers,
          "Too many prolonged operations running.  Try again later.");
    }
    try {
      return this.analyzeBulkRecords(provider,
                                     timers,
                                     mediaType,
                                     dataInputStream,
                                     uriInfo,
                                     null,
                                     null,
                                     null,
                                     null);

    } catch (RuntimeException e) {
      throw logOnceAndThrow(e);

    } catch (Exception e) {
      throw logOnceAndThrow(new RuntimeException(e));

    } finally {
      provider.concludeProlongedOperation(accessToken);
    }
  }

  /**
   * Validates the progress period parameter for SSE or Web Socket requests.
   *
   * @param progressPeriod The specified progress period.
   * @param timers The {@link Timers} for the request.
   * @param uriInfo The {@link UriInfo} for the request.
   * @param sseEventSink The {@link SseEventSink} or <tt>null</tt> if not an
   *                     SSE request.
   * @param sse The {@link Sse} or <tt>null</tt> if not an SSE request.
   * @param webSocketSession The Web Socket {@link Session} for the request.
   *
   * @throws BadRequestException If the progress period is negative, but we are
   *                             handling an SSE request.
   */
  private void validateProgressPeriod(Long         progressPeriod,
                                      Timers       timers,
                                      UriInfo      uriInfo,
                                      SseEventSink sseEventSink,
                                      Sse          sse,
                                      Session      webSocketSession)
    throws BadRequestException
  {
    // check if the progress period parameter is being ignored
    if (sseEventSink == null && sse == null && webSocketSession == null) {
      return;
    }

    // check the parameters
    if (progressPeriod != null && progressPeriod < 0L) {
      throw this.newBadRequestException(
          POST, uriInfo, timers,
          "The progressPeriod parameter cannot be negative: " + progressPeriod);
    }
  }

  /**
   * Analyzes the bulk data records via direct upload using SSE.
   *
   * @param mediaType The media type for the content.
   * @param dataInputStream The input stream to read the uploaded data.
   * @param uriInfo The {@link UriInfo} for the request.
   * @param progressPeriod The suggested maximum time between SSE `progress`
   *                       events specified in milliseconds.  If not specified
   *                       then the default of `3000` milliseconds (i.e.: 3
   *                       seconds) is used.
   * @param sseEventSink The {@link SseEventSink} for the SSE protocol.
   * @param sse The {@link Sse} instance for the SSE protocol.
   */
  @POST
  @Path("/analyze")
  @Consumes({ APPLICATION_JSON,
              TEXT_PLAIN,
              TEXT_CSV,
              APPLICATION_JSONLINES })
  @Produces(TEXT_EVENT_STREAM)
  public void analyzeBulkRecordsDirect(
      @HeaderParam("Content-Type") MediaType mediaType,
      InputStream dataInputStream,
      @Context UriInfo uriInfo,
      @QueryParam("progressPeriod") @DefaultValue("3000") long progressPeriod,
      @Context SseEventSink sseEventSink,
      @Context Sse sse)
  {
    try {
      SzApiProvider provider = this.getApiProvider();
      Timers timers = this.newTimers();
      AccessToken accessToken = provider.authorizeProlongedOperation();
      if (accessToken == null) {
        throw this.newServiceUnavailableErrorException(
            POST, uriInfo, timers,
            "Too many prolonged operations running.  Try again later.");
      }
      try {
        this.analyzeBulkRecords(provider,
                                timers,
                                mediaType,
                                dataInputStream,
                                uriInfo,
                                progressPeriod,
                                sseEventSink,
                                sse,
                                null);

      } catch (RuntimeException e) {
        throw logOnceAndThrow(e);

      } catch (Exception e) {
        throw logOnceAndThrow(new RuntimeException(e));

      } finally {
        provider.concludeProlongedOperation(accessToken);
      }

    } catch (WebApplicationException e) {
      OutboundSseEvent.Builder eventBuilder = sse.newEventBuilder();
      OutboundSseEvent event =
          eventBuilder.name(FAILED_EVENT)
              .id(String.valueOf(0))
              .mediaType(APPLICATION_JSON_TYPE)
              .data(e.getResponse().getEntity())
              .reconnectDelay(RECONNECT_DELAY)
              .build();
      sseEventSink.send(event);
    }
  }

  /**
   * Analyzes the bulk data records via form data using SSE.
   *
   * @param mediaType The media type for the content.
   * @param dataInputStream The input stream to read the uploaded data.
   * @param uriInfo The {@link UriInfo} for the request.
   * @param progressPeriod The suggested maximum time between SSE `progress`
   *                       events specified in milliseconds.  If not specified
   *                       then the default of `3000` milliseconds (i.e.: 3
   *                       seconds) is used.
   * @param sseEventSink The {@link SseEventSink} for the SSE protocol.
   * @param sse The {@link Sse} instance for the SSE protocol.
   */
  @POST
  @Path("/analyze")
  @Produces(TEXT_EVENT_STREAM)
  public void analyzeBulkRecordsViaForm(
      @HeaderParam("Content-Type") MediaType mediaType,
      @FormDataParam("data") InputStream dataInputStream,
      @Context UriInfo uriInfo,
      @QueryParam("progressPeriod") @DefaultValue("3000") long progressPeriod,
      @Context SseEventSink sseEventSink,
      @Context Sse sse)
  {
    try {
      Timers        timers      = this.newTimers();
      SzApiProvider provider    = this.getApiProvider();
      AccessToken   accessToken = provider.authorizeProlongedOperation();
      if (accessToken == null) {
        throw this.newServiceUnavailableErrorException(
            POST, uriInfo, timers,
            "Too many prolonged operations running.  Try again later.");
      }
      try {
        this.analyzeBulkRecords(provider,
                                timers,
                                mediaType,
                                dataInputStream,
                                uriInfo,
                                progressPeriod,
                                sseEventSink,
                                sse,
                                null);

      } catch (RuntimeException e) {
        throw logOnceAndThrow(e);

      } catch (Exception e) {
        throw logOnceAndThrow(new RuntimeException(e));
      } finally {
        provider.concludeProlongedOperation(accessToken);
      }
    } catch (WebApplicationException e) {
      OutboundSseEvent.Builder eventBuilder = sse.newEventBuilder();
      OutboundSseEvent event =
          eventBuilder.name(FAILED_EVENT)
              .id(String.valueOf(0))
              .mediaType(APPLICATION_JSON_TYPE)
              .data(e.getResponse().getEntity())
              .reconnectDelay(RECONNECT_DELAY)
              .build();
      sseEventSink.send(event);
    }
  }

  /**
   * Loads the bulk data records via form.
   *
   * @param dataSource The data source to assign to the loaded records unless
   *                   another data source mapping supercedes this default.
   * @param mapDataSources The JSON string mapping specific data sources to
   *                       alternate data source names.  A mapping from
   *                       empty-string is used for mapping records with no
   *                       data source specified.
   * @param mapDataSourceList The {@link List} of delimited strings that begin
   *                          the delimiter, followed by the "from" data source
   *                          then the delimiter then the target data source.
   * @param entityType The entity type to assign to the loaded records unless
   *                   another entity type mapping supercedes this default.
   * @param mapEntityTypes The JSON string mapping specific entity types to
   *                       alternate entity type names.  A mapping from
   *                       empty-string is used for mapping records with no
   *                       entity type specified.
   * @param mapEntityTypeList The {@link List} of delimited strings that begin
   *                          the delimiter, followed by the "from" entity type
   *                          then the delimiter then the target entity type.
   * @param loadId The optional load ID to use for loading the records.
   * @param maxFailures The maximum number of failures or a negative number if
   *                    no maximum.
   * @param mediaType The media type for the content.
   * @param dataInputStream The input stream to read the uploaded data.
   * @param fileMetaData The form meta data for the uploaded file.
   * @param uriInfo The {@link UriInfo} for the request.
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
    SzApiProvider provider    = this.getApiProvider();
    Timers        timers      = this.newTimers();
    AccessToken   accessToken = provider.authorizeProlongedOperation();
    if (accessToken == null) {
      throw this.newServiceUnavailableErrorException(
          POST, uriInfo, timers,
          "Too many prolonged operations running.  Try again later.");
    }
    try {
      return this.loadBulkRecords(provider,
                                  timers,
                                  dataSource,
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
                                  null,
                                  null);

    } catch (ForbiddenException e) {
      throw e;

    } catch (RuntimeException e) {
      throw logOnceAndThrow(e);

    } catch (Exception e) {
      throw logOnceAndThrow(new RuntimeException(e));

    } finally {
      provider.concludeProlongedOperation(accessToken);
    }
  }

  /**
   * Loads the bulk data records via direct upload.
   *
   * @param dataSource The data source to assign to the loaded records unless
   *                   another data source mapping supercedes this default.
   * @param mapDataSources The JSON string mapping specific data sources to
   *                       alternate data source names.  A mapping from
   *                       empty-string is used for mapping records with no
   *                       data source specified.
   * @param mapDataSourceList The {@link List} of delimited strings that begin
   *                          the delimiter, followed by the "from" data source
   *                          then the delimiter then the target data source.
   * @param entityType The entity type to assign to the loaded records unless
   *                   another entity type mapping supercedes this default.
   * @param mapEntityTypes The JSON string mapping specific entity types to
   *                       alternate entity type names.  A mapping from
   *                       empty-string is used for mapping records with no
   *                       entity type specified.
   * @param mapEntityTypeList The {@link List} of delimited strings that begin
   *                          the delimiter, followed by the "from" entity type
   *                          then the delimiter then the target entity type.
   * @param loadId The optional load ID to use for loading the records.
   * @param maxFailures The maximum number of failures or a negative number if
   *                    no maximum.
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
    SzApiProvider provider    = this.getApiProvider();
    Timers        timers      = this.newTimers();
    AccessToken   accessToken = provider.authorizeProlongedOperation();
    if (accessToken == null) {
      throw this.newServiceUnavailableErrorException(
          POST, uriInfo, timers,
          "Too many prolonged operations running.  Try again later.");
    }
    try {
      return this.loadBulkRecords(provider,
                                  timers,
                                  dataSource,
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
                                  null,
                                  null);

    } catch (ForbiddenException e) {
      throw e;

    } catch (RuntimeException e) {
      throw logOnceAndThrow(e);

    } catch (Exception e) {
      throw logOnceAndThrow(new RuntimeException(e));

    } finally {
      provider.concludeProlongedOperation(accessToken);
    }
  }

  /**
   * Loads bulk data records via form using SSE.
   *
   * @param dataSource The data source to assign to the loaded records unless
   *                   another data source mapping supercedes this default.
   * @param mapDataSources The JSON string mapping specific data sources to
   *                       alternate data source names.  A mapping from
   *                       empty-string is used for mapping records with no
   *                       data source specified.
   * @param mapDataSourceList The {@link List} of delimited strings that begin
   *                          the delimiter, followed by the "from" data source
   *                          then the delimiter then the target data source.
   * @param entityType The entity type to assign to the loaded records unless
   *                   another entity type mapping supercedes this default.
   * @param mapEntityTypes The JSON string mapping specific entity types to
   *                       alternate entity type names.  A mapping from
   *                       empty-string is used for mapping records with no
   *                       entity type specified.
   * @param mapEntityTypeList The {@link List} of delimited strings that begin
   *                          the delimiter, followed by the "from" entity type
   *                          then the delimiter then the target entity type.
   * @param loadId The optional load ID to use for loading the records.
   * @param maxFailures The maximum number of failures or a negative number if
   *                    no maximum.
   * @param progressPeriod The suggested maximum time between SSE `progress`
   *                       events specified in milliseconds.  If not specified
   *                       then the default of `3000` milliseconds (i.e.: 3
   *                       seconds) is used.
   * @param mediaType The media type for the content.
   * @param dataInputStream The input stream to read the uploaded data.
   * @param uriInfo The {@link UriInfo} for the request.
   * @param sseEventSink The {@link SseEventSink} for the SSE protocol.
   * @param sse The {@link Sse} instance for the SSE protocol.
   */
  @POST
  @Path("/load")
  @Produces(TEXT_EVENT_STREAM)
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
    SzApiProvider provider    = this.getApiProvider();
    Timers        timers      = this.newTimers();
    AccessToken   accessToken = provider.authorizeProlongedOperation();
    if (accessToken == null) {
      throw this.newServiceUnavailableErrorException(
          POST, uriInfo, timers,
          "Too many prolonged operations running.  Try again later.");
    }
    try {
      this.loadBulkRecords(provider,
                           timers,
                           dataSource,
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
                           sse,
                           null);

    } catch (ForbiddenException e) {
      throw e;

    } catch (RuntimeException e) {
      throw logOnceAndThrow(e);

    } catch (Exception e) {
      throw logOnceAndThrow(new RuntimeException(e));

    } finally {
      provider.concludeProlongedOperation(accessToken);
    }
  }

  /**
   * Loads the bulk data records via direct upload using SSE.
   *
   * @param dataSource The data source to assign to the loaded records unless
   *                   another data source mapping supercedes this default.
   * @param mapDataSources The JSON string mapping specific data sources to
   *                       alternate data source names.  A mapping from
   *                       empty-string is used for mapping records with no
   *                       data source specified.
   * @param mapDataSourceList The {@link List} of delimited strings that begin
   *                          the delimiter, followed by the "from" data source
   *                          then the delimiter then the target data source.
   * @param entityType The entity type to assign to the loaded records unless
   *                   another entity type mapping supercedes this default.
   * @param mapEntityTypes The JSON string mapping specific entity types to
   *                       alternate entity type names.  A mapping from
   *                       empty-string is used for mapping records with no
   *                       entity type specified.
   * @param mapEntityTypeList The {@link List} of delimited strings that begin
   *                          the delimiter, followed by the "from" entity type
   *                          then the delimiter then the target entity type.
   * @param loadId The optional load ID to use for loading the records.
   * @param maxFailures The maximum number of failures or a negative number if
   *                    no maximum.
   * @param progressPeriod The suggested maximum time between SSE `progress`
   *                       events specified in milliseconds.  If not specified
   *                       then the default of `3000` milliseconds (i.e.: 3
   *                       seconds) is used.
   * @param mediaType The media type for the content.
   * @param dataInputStream The input stream to read the uploaded data.
   * @param uriInfo The {@link UriInfo} for the request.
   * @param sseEventSink The {@link SseEventSink} for the SSE protocol.
   * @param sse The {@link Sse} instance for the SSE protocol.
   */
  @POST
  @Path("/load")
  @Consumes({ APPLICATION_JSON,
              TEXT_PLAIN,
              TEXT_CSV,
              APPLICATION_JSONLINES })
  @Produces(TEXT_EVENT_STREAM)
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
    SzApiProvider provider    = this.getApiProvider();
    Timers        timers      = this.newTimers();
    AccessToken   accessToken = provider.authorizeProlongedOperation();
    if (accessToken == null) {
      throw this.newServiceUnavailableErrorException(
          POST, uriInfo, timers,
          "Too many prolonged operations running.  Try again later.");
    }
    try {
      this.loadBulkRecords(provider,
                           timers,
                           dataSource,
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
                           sse,
                           null);

    } catch (ForbiddenException e) {
      throw e;

    } catch (RuntimeException e) {
      throw logOnceAndThrow(e);

    } catch (Exception e) {
      throw logOnceAndThrow(new RuntimeException(e));

    } finally {
      provider.concludeProlongedOperation(accessToken);
    }
  }

  /**
   * The EOF detector thread.
   */
  private static class EOFDetector extends Thread {
    /**
     * The {@link WebSocketThread} being monitored.
     */
    private WebSocketThread webSocketThread = null;

    /**
     * Checks if this instance is completed.
     */
    private boolean completed = false;

    /**
     * Constructs with the {@link WebSocketThread} to monitor.
     */
    private EOFDetector(WebSocketThread thread) {
      this.webSocketThread = thread;
    }

    /**
     * Implemented to close the stream when we have not received in the
     * specified timeout for the web socket thread.
     */
    public void run() {
      long nanoTimeout = this.webSocketThread.eofSendTimeout * 1000000000L;

      synchronized (this.webSocketThread) {
        long waitTime = this.webSocketThread.eofSendTimeout * 1000L;
        while (!this.completed
                && this.webSocketThread.pipedOutputStream != null)
        {
          // wait for a period
          try {
            this.webSocketThread.wait(waitTime);

          } catch (InterruptedException ignore) {
            // ignore
          }

          // check the time
          long now = System.nanoTime();
          long duration = (now - this.webSocketThread.lastMessageTime);
          boolean timedOut = (duration > nanoTimeout);

          if (this.completed || timedOut) {
            // signal EOF
            IOUtilities.close(this.webSocketThread.pipedOutputStream);
            this.webSocketThread.pipedOutputStream = null;
            this.webSocketThread.notifyAll();
            break;
          }

          // now check how long to wait next time
          waitTime = (this.webSocketThread.eofSendTimeout * 1000L);
          waitTime -= (duration/1000000L);
        }
      }
    }

    /**
     * Method to mark this instance as completed.
     */
    private void complete() {
      synchronized (this.webSocketThread) {
        this.completed = true;
        this.webSocketThread.notifyAll();
      }
    }
  }

  /**
   * An abstract base class for the Web Socket endpoints.
   */
  public static abstract class WebSocketThread extends Thread {
    /**
     * The Web Socket {@link Session} for this instance.
     */
    protected Session session = null;

    /**
     * The {@link PipedInputStream} to attach to the {@link PipedOutputStream}.
     */
    protected PipedInputStream pipedInputStream = null;

    /**
     * The {@link PipedOutputStream} to attach to the {@link PipedInputStream}.
     */
    protected PipedOutputStream pipedOutputStream = null;

    /**
     * Define the progress period for reporting progress on the web socket.
     */
    protected Long progressPeriod = DEFAULT_PROGRESS_PERIOD;

    /**
     * The EOF send timeout.
     */
    protected Long eofSendTimeout = DEFAULT_EOF_SEND_TIMEOUT;

    /**
     * The time in milliseconds that the last message was received.
     */
    protected long lastMessageTime = -1L;

    /**
     * The {@link UriInfo} for the request.
     */
    protected UriInfo uriInfo = null;

    /**
     * The {@link EOFDetector} thread for monitoring for EOF.
     */
    protected EOFDetector eofDetector = null;

    /**
     * Flag indicating if we have started processing.
     */
    protected boolean started = false;

    /**
     * Flag indicating if we have begun shutting down.
     */
    protected boolean closing = false;

    /**
     * The {@link MediaType} to assume -- if text is sent rather than binary
     * then {@link BulkDataServices#TEXT_PLAIN_UTF8_TYPE} will be used.
     */
    protected MediaType mediaType = TEXT_PLAIN_TYPE;

    /**
     * Gets the {@link BulkDataServices} instance that will be used by
     * this thread.  The default implementation returns the result from
     * {@link #getDefaultInstance()}.
     *
     * @return The {@link BulkDataServices} instance that will be used by this
     *         thread.
     */
    protected BulkDataServices getBulkDataServices() {
      return BulkDataServices.getDefaultInstance();
    }

    @OnOpen
    public synchronized void onOpen(Session session)
        throws IOException, IllegalArgumentException
    {
      BulkDataServices services = this.getBulkDataServices();

      this.session            = session;
      this.pipedInputStream   = new PipedInputStream(PIPE_SIZE);
      this.pipedOutputStream  = new PipedOutputStream(this.pipedInputStream);
      this.uriInfo            = services.newProxyUriInfo(this.session);
      this.started            = false;
      this.lastMessageTime    = System.nanoTime();

      Map<String, List<String>> params = this.session.getRequestParameterMap();

      // get the progress period
      List<String> paramValues = params.get("progressPeriod");
      if (paramValues != null && paramValues.size() > 0) {
        try {
          this.progressPeriod = Long.parseLong(paramValues.get(0));
          if (this.progressPeriod < 0L) throw new IllegalArgumentException();

        } catch (IllegalArgumentException e) {
          throw new BadRequestException(
              "The specified progress period (progressPeriod) must be a "
              + "non-negative long integer: " + paramValues.get(0));
        }
      }

      // get the EOF send timeout
      paramValues = params.get("eofSendTimeout");
      if (paramValues != null && paramValues.size() > 0) {
        try {
          this.eofSendTimeout = Long.parseLong(paramValues.get(0));
          if (this.eofSendTimeout < 0L) throw new IllegalArgumentException();

        } catch (IllegalArgumentException e) {
          throw new BadRequestException(
              "The specified EOF send timeout (eofSendTimeout) must be a "
                  + "non-negative long integer: " + paramValues.get(0));
        }
      }

      // create the EOF thread
      this.eofDetector = new EOFDetector(this);
      this.eofDetector.start();
    }

    @OnMessage
    public synchronized void onMessage(byte[] bytes) throws IOException
    {
      long now = System.nanoTime();
      if (this.pipedOutputStream == null) {
        // if session closed, ignore the message
        if (!this.session.isOpen() || this.closing) return;

        // if session is not closed then throw an exception
        throw new IllegalStateException(
            "Output stream is already closed: "
                + ((now-this.lastMessageTime)/1000000L)
                + "ms since last message");
      }


      // check if started, and if not then start the thread
      if (!this.started) {
        this.started = true;
        this.start();
      }

      if (this.pipedOutputStream != null) {
        this.pipedOutputStream.write(bytes);
        this.pipedOutputStream.flush();
      }
      this.lastMessageTime = System.nanoTime();
      this.notifyAll();
    }

    @OnMessage
    public synchronized void onMessage(String text)  throws IOException
    {
      long now = System.nanoTime();
      if (this.pipedOutputStream == null) {
        if (!this.session.isOpen() || this.closing) return;
        throw new IllegalStateException(
            "Output stream is already closed: "
                + ((now-this.lastMessageTime)/1000000L)
                + "ms since last message");
      }

      // check if started, and if not then start the thread
      if (!this.started) {
        // text is being sent so set the media type to use UTF-8 charset
        this.mediaType = TEXT_PLAIN_UTF8_TYPE;
        this.started = true;
        this.start();
      }

      if (this.pipedOutputStream != null) {
        this.pipedOutputStream.write(text.getBytes(UTF_8));
        this.pipedOutputStream.flush();
      }
      this.lastMessageTime = System.nanoTime();
      this.notifyAll();
    }

    @OnClose
    public synchronized void onClose(Session session) throws IOException {
      IOUtilities.close(this.pipedOutputStream);
      this.pipedOutputStream = null;
      this.notifyAll();
    }

    @OnError
    public synchronized void onError(Session session, Throwable throwable)
        throws IOException
    {
      throwable.printStackTrace();
      IOUtilities.close(this.pipedOutputStream);
      this.pipedOutputStream = null;

      CloseReason.CloseCode closeCode
          = (throwable instanceof BadRequestException)
          ? CloseReason.CloseCodes.PROTOCOL_ERROR
          : CloseReason.CloseCodes.UNEXPECTED_CONDITION;

      this.closing = true;
      this.session.close(new CloseReason(closeCode, throwable.getMessage()));
    }

    /**
     * Implemented to start the EOF detector and then defer to the {@link
     * #doRun()} method.
     */
    public final void run() {
      // defer the run
      try {
        this.doRun();

      } finally {
        // notify completion
        this.eofDetector.complete();
        try {
          this.eofDetector.join();

        } catch (InterruptedException ignore) {
          // ignore
        }
      }
    }

    /**
     * Override this to handle running the web socket functionality.
     */
    protected abstract void doRun();
  }

  /**
   * Provides a nested class to handle analyzing bulk data records via
   * web sockets.
   */
  @ServerEndpoint(value="/bulk-data/analyze",
                  decoders = StringDecoder.class,
                  encoders = JsonEncoder.class)
  public static class AnalyzeWebSocket extends WebSocketThread {
    /**
     * Implemented to load the records once the thread is started.
     */
    protected void doRun() {
      BulkDataServices services = this.getBulkDataServices();

      SzApiProvider provider  = services.getApiProvider();
      Timers        timers    = services.newTimers();
      services.analyzeBulkRecords(provider,
                                  timers,
                                  this.mediaType,
                                  this.pipedInputStream,
                                  this.uriInfo,
                                  this.progressPeriod,
                                  null,
                                  null,
                                  this.session);

      // close the web sockets session
      try {
        synchronized (this) {
          this.closing = true;
          this.session.close();
        }
      } catch (IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }

      synchronized (this) {
        IOUtilities.close(this.pipedInputStream);
        this.pipedInputStream = null;
        IOUtilities.close(this.pipedOutputStream);
        this.pipedOutputStream = null;
        this.notifyAll();
      }
    }
  }

  /**
   * Provides a nested class to handle loading bulk data records via
   * web sockets.
   */
  @ServerEndpoint(value="/bulk-data/load",
                  decoders = StringDecoder.class,
                  encoders = JsonEncoder.class)
  public static class LoadWebSocket extends WebSocketThread {
    /**
     * The data source to assign to the records loaded unless there is another
     * mapping that supercedes this one.
     */
    private String dataSource;

    /**
     * The JSON string mapping specific data sources to alternate data source
     * names.  A mapping from empty-string is used for mapping records with no
     * data source specified.
     */
    private String mapDataSources;

    /**
     * The {@link List} of delimited strings that begin the delimiter, followed
     * by the "from" data source then the delimiter then the target data source.
     */
    private List<String> mapDataSourceList;

    /**
     * The entity type to assign to the loaded records unless another entity
     * type mapping supercedes this default.
     */
    private String entityType;

    /**
     * The JSON string mapping specific entity types to alternate entity type
     * names.  A mapping from empty-string is used for mapping records with no
     * entity type specified.
     */
    private String mapEntityTypes;

    /**
     * The {@link List} of delimited strings that begin the delimiter, followed
     * by the "from" entity type then the delimiter then the target entity type.
     */
    private List<String> mapEntityTypeList;

    /**
     * The optional load ID to use for loading the records.
     */
    private String loadId;

    /**
     * The maximum number of failures or a negative number if no maximum.
     */
    private int maxFailures;

    /**
     * Provides a pre-flight check to make sure the server is not in read-only
     * mode before opening the web socket.
     *
     * @param request The {@link HttpServletRequest}.
     * @param response The {@link HttpServletResponse}.
     *
     * @return <tt>true</tt> if not read-only and <tt>false</tt> if so.
     */
    @OnUpgrade
    public static boolean onUpgrade(HttpServletRequest  request,
                                    HttpServletResponse response)
      throws IOException
    {
      BulkDataServices services = BulkDataServices.getDefaultInstance();

      // get the provider
      SzApiProvider provider = services.getApiProvider();

      // if not read only then simply return true
      if (!provider.isReadOnly()) return true;

      // create the timers and construct an error response
      Timers timers = services.newTimers();
      SzErrorResponse errorResponse = services.newErrorResponse(
          services.newMeta(GET, 403, timers),
          services.newLinks(request.getRequestURI()),
          "Loading data is not allowed if Senzing API Server started "
              + "in read-only mode");
      errorResponse.concludeTimers();

      response.setStatus(HttpServletResponse.SC_FORBIDDEN);
      response.setContentType("application/json; charset=utf-8");

      String  jsonText  = services.toJsonString(errorResponse);
      byte[]  jsonBytes = jsonText.getBytes(UTF_8);
      int     length    = jsonBytes.length;

      response.setContentLength(length);

      OutputStream os = response.getOutputStream();
      os.write(jsonBytes);
      os.flush();

      // return false
      return false;
    }

    @Override
    public void onOpen(Session session)
        throws IOException, IllegalArgumentException
    {
      super.onOpen(session);

      // get the other query parameters
      Map<String, List<String>> params = this.session.getRequestParameterMap();
      List<String> paramList = params.get("dataSource");

      this.dataSource = (paramList == null || paramList.size() == 0) ? null
          : paramList.get(0);

      paramList = params.get("mapDataSources");
      this.mapDataSources = (paramList == null || paramList.size() == 0) ? null
          : paramList.get(0);

      this.mapDataSourceList = params.get("mapDataSource");

      paramList = params.get("entityType");
      this.entityType = (paramList == null || paramList.size() == 0) ? null
          : paramList.get(0);

      paramList = params.get("mapEntityTypes");
      this.mapEntityTypes = (paramList == null || paramList.size() == 0) ? null
          : paramList.get(0);

      this.mapEntityTypeList = params.get("mapEntityType");

      paramList = params.get("loadId");
      this.loadId = (paramList == null || paramList.size() == 0) ? null
          : paramList.get(0);

      paramList = params.get("maxFailures");
      if (paramList != null && paramList.size() > 0) {
        try {
          this.maxFailures = Integer.parseInt(paramList.get(0));

        } catch (IllegalArgumentException e) {
          throw new BadRequestException(
              "The specified maximum number of failures (maxFailures) must be "
              + "an integer: " + paramList.get(0));
        }
      }
    }

    /**
     * Implemented to load the records once the thread is started.
     */
    protected void doRun() {
      BulkDataServices  services  = this.getBulkDataServices();
      SzApiProvider     provider  = SzApiProvider.Factory.getProvider();
      Timers            timers    = services.newTimers();

      services.loadBulkRecords(provider,
                               timers,
                               this.dataSource,
                               this.mapDataSources,
                               this.mapDataSourceList,
                               this.entityType,
                               this.mapEntityTypes,
                               this.mapEntityTypeList,
                               this.loadId,
                               this.maxFailures,
                               this.mediaType,
                               this.pipedInputStream,
                               null,
                               this.uriInfo,
                               this.progressPeriod,
                               null,
                               null,
                               this.session);

      // close the web sockets session
      try {
        synchronized (this) {
          this.closing = true;
          this.session.close();
        }
      } catch (IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
      synchronized (this) {
        IOUtilities.close(this.pipedInputStream);
        IOUtilities.close(this.pipedOutputStream);
        this.pipedInputStream = null;
        this.pipedOutputStream = null;
        this.notifyAll();
      }
    }
  }

  /**
   * Creates a new instance of {@link SzBulkDataAnalysis} and returns it.
   *
   * @return The new instance of {@link SzBulkDataAnalysis}.
   */
  protected SzBulkDataAnalysis newBulkDataAnalysis() {
    return new SzBulkDataAnalysis();
  }

  /**
   * Analyzes the bulk data and returns information about it.
   */
  private SzBulkDataAnalysisResponse analyzeBulkRecords(
      SzApiProvider               provider,
      Timers                      timers,
      MediaType                   mediaType,
      InputStream                 dataInputStream,
      UriInfo                     uriInfo,
      Long                        progressPeriod,
      SseEventSink                sseEventSink,
      Sse                         sse,
      Session                     webSocketSession)
  {
    // convert progress period to nanoseconds
    Long progressNanos = (progressPeriod == null)
        ? null : progressPeriod * 1000000L;

    OutboundSseEvent.Builder eventBuilder
        = (sseEventSink != null && sse != null) ? sse.newEventBuilder() : null;
    int eventId = 0;

    SzBulkDataAnalysis dataAnalysis = this.newBulkDataAnalysis();

    // check the progress period
    this.validateProgressPeriod(progressPeriod,
                                timers,
                                uriInfo,
                                sseEventSink,
                                sse,
                                webSocketSession);

    try {
      BulkDataSet bulkDataSet = new BulkDataSet(mediaType, dataInputStream);
      TemporaryDataCache dataCache = bulkDataSet.getDataCache();

      // if charset is unknown then try to detect
      String charset = bulkDataSet.getCharacterEncoding();
      dataAnalysis.setCharacterEncoding(charset);

      long start = System.nanoTime();
      // check if we need to auto-detect the media type
      try (InputStream        is  = dataCache.getInputStream(true);
           InputStreamReader  isr = new InputStreamReader(is, charset);
           BufferedReader     br  = new BufferedReader(isr))
      {
        // if format is null then RecordReader will auto-detect
        RecordReader recordReader
            = new RecordReader(bulkDataSet.getFormat(), br);
        bulkDataSet.setFormat(recordReader.getFormat());
        if (bulkDataSet.getFormat() != null) {
          dataAnalysis.setMediaType(bulkDataSet.getFormat().getMediaType());
        } else {
          dataAnalysis.setMediaType(null);
        }

        for (JsonObject record = recordReader.readRecord();
             (record != null);
             record = recordReader.readRecord())
        {
          String dataSrc    = JsonUtils.getString(record, "DATA_SOURCE");
          String entityType = JsonUtils.getString(record, "ENTITY_TYPE");
          String recordId   = JsonUtils.getString(record, "RECORD_ID");
          dataAnalysis.trackRecord(dataSrc, entityType, recordId);

          long now = System.nanoTime();
          long duration = now - start;
          // check if the progress period has expired
          if ((progressNanos != null) && (duration > progressNanos)) {
            // reset the start time
            start = now;

            // check if we are sending a response message
            SzBulkDataAnalysisResponse response = null;
            if (eventBuilder != null || webSocketSession != null) {
              // build the response message
              response = this.newBulkDataAnalysisResponse(
                  POST, 200, uriInfo, timers, dataAnalysis);
            }

            // check if sending SSE events
            if (eventBuilder != null) {
              // send an SSE event message
              OutboundSseEvent event =
                  eventBuilder.name(PROGRESS_EVENT)
                      .id(String.valueOf(eventId++))
                      .mediaType(APPLICATION_JSON_TYPE)
                      .data(response)
                      .reconnectDelay(RECONNECT_DELAY)
                      .build();
              sseEventSink.send(event);
            }

            // check if sending WebSocket messages
            if (webSocketSession != null) {
              // send a message no the web socket
              webSocketSession.getBasicRemote().sendObject(response);
            }
          }
        }
      }

    } catch (EncodeException|IOException e) {
      e.printStackTrace();
      dataAnalysis.setStatus(ABORTED);

      SzBulkDataAnalysisResponse response = this.newBulkDataAnalysisResponse(
          POST,200, uriInfo, timers, dataAnalysis);

      this.abortOperation(e,
                          response,
                          uriInfo,
                          timers,
                          eventId,
                          eventBuilder,
                          sseEventSink,
                          webSocketSession);

      return response;
    }

    dataAnalysis.setStatus(COMPLETED);

    SzBulkDataAnalysisResponse response = this.newBulkDataAnalysisResponse(
        POST, 200, uriInfo, timers, dataAnalysis);

    return this.completeOperation(
        eventBuilder, sseEventSink, eventId, webSocketSession, response);
  }

  /**
   * Creates a new instance of {@link SzBulkDataAnalysisResponse} with the
   * following parameters.
   *
   * @param httpMethod The {@link SzHttpMethod} for the response.
   * @param httpStatusCode The status code for the response.
   * @param uriInfo The {@link UriInfo} for the operation.
   * @param timers The {@link Timers} tracking the timing for the operation.
   * @param dataAnalysis The {@link SzBulkDataAnalysis} describing the analysis.
   */
  protected SzBulkDataAnalysisResponse newBulkDataAnalysisResponse(
      SzHttpMethod        httpMethod,
      int                 httpStatusCode,
      UriInfo             uriInfo,
      Timers              timers,
      SzBulkDataAnalysis  dataAnalysis)
  {
    return new SzBulkDataAnalysisResponse(
        this.newMeta(httpMethod, httpStatusCode, timers),
        this.newLinks(uriInfo), dataAnalysis);
  }

  /**
   * Creates a new instance of {@link SzBulkLoadResult} and returns it.
   *
   * @return The new instance of {@link SzBulkLoadResult}.
   */
  protected SzBulkLoadResult newBulkLoadResult() {
    return new SzBulkLoadResult();
  }

  /**
   * Analyzes the bulk data and returns information about it.
   */
  protected SzBulkLoadResponse loadBulkRecords(
      SzApiProvider               provider,
      Timers                      timers,
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
      Long                        progressPeriod,
      SseEventSink                sseEventSink,
      Sse                         sse,
      Session                     webSocketSession)
  {
    // convert the progress period to nanoseconds
    Long progressNanos = (progressPeriod == null)
        ? null : progressPeriod * 1000000L;

    OutboundSseEvent.Builder eventBuilder
        = (sseEventSink != null && sse != null) ? sse.newEventBuilder() : null;
    int eventId = 0;

    SzBulkLoadResult bulkLoadResult = this.newBulkLoadResult();

    this.ensureLoadingIsAllowed(provider, POST, uriInfo, timers);

    // normalize and validate the data source
    if (dataSource != null) {
      dataSource = dataSource.trim().toUpperCase();

      if (!provider.getDataSources(dataSource).contains(dataSource)) {
        throw this.newBadRequestException(
            POST, uriInfo, timers,
            "The value for the specified \"dataSource\" parameter is not a "
            + "configured data source: " + dataSource);
      }
    }

    // normalize and validate the entity type
    if (entityType != null) {
      entityType = entityType.trim().toUpperCase();

      if (!provider.getEntityTypes(entityType).contains(entityType)) {
        throw this.newBadRequestException(
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

    this.processDataSources(
        dataSourceMap, mapDataSources, provider, timers, uriInfo);
    this.processDataSources(
        dataSourceMap, mapDataSourceList, provider, timers, uriInfo);
    this.processEntityTypes(
        entityTypeMap, mapEntityTypes, provider, timers, uriInfo);
    this.processEntityTypes(
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
      AsyncWorkerPool<AddRecordResult> asyncPool
          = new AsyncWorkerPool<>(loadId, concurrency);

      List<Timers> timerPool = new ArrayList<>(concurrency);
      for (int index = 0; index < concurrency; index++) {
        timerPool.add(new Timers());
      }

      long start = System.nanoTime();

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
        bulkDataSet.setFormat(recordReader.getFormat());
        bulkLoadResult.setCharacterEncoding(charset);
        bulkLoadResult.setMediaType(bulkDataSet.getFormat().getMediaType());

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
            AsyncResult<AddRecordResult> asyncResult = null;
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

          long now = System.nanoTime();
          long duration = now - start;

          // check if the timing has gone beyond the specified progress period
          if ((progressNanos != null) && (duration > progressNanos)) {
            // create the update response if there is a client expecting it
            SzBulkLoadResponse update = null;
            if (eventBuilder != null || webSocketSession != null) {
              start = now;
              update = this.newBulkLoadResponse(
                  POST, 200, uriInfo, timers, bulkLoadResult);
            }

            // check if sending an SSE response
            if (eventBuilder != null) {
              OutboundSseEvent event =
                  eventBuilder.name(PROGRESS_EVENT)
                      .id(String.valueOf(eventId++))
                      .mediaType(APPLICATION_JSON_TYPE)
                      .data(update)
                      .reconnectDelay(RECONNECT_DELAY)
                      .build();
              sseEventSink.send(event);
            }

            // check if sending a web socket response
            if (webSocketSession != null) {
              try {
                // send the web socket message and handle exceptions
                webSocketSession.getBasicRemote().sendObject(update);

              } catch (RuntimeException e) {
                e.printStackTrace();
                throw e;

              } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
              }
            }
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
        List<AsyncResult<AddRecordResult>> results = asyncPool.close();
        for (AsyncResult<AddRecordResult> asyncResult : results) {
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
      SzBulkLoadResponse response = this.newBulkLoadResponse(POST,
                                                             200,
                                                             uriInfo,
                                                             timers,
                                                             bulkLoadResult);
      this.abortOperation(e,
                          response,
                          uriInfo,
                          timers,
                          eventId,
                          eventBuilder,
                          sseEventSink,
                          webSocketSession);
    }

    SzBulkLoadResponse response = this.newBulkLoadResponse(POST,
                                                           200,
                                                           uriInfo,
                                                           timers,
                                                           bulkLoadResult);

    return this.completeOperation(
        eventBuilder, sseEventSink, eventId, webSocketSession, response);
  }

  /**
   * Creates a new instance of {@link SzBulkDataAnalysisResponse} with the
   * following parameters.
   *
   * @param httpMethod The {@link SzHttpMethod} for the response.
   * @param httpStatusCode The status code for the response.
   * @param uriInfo The {@link UriInfo} for the operation.
   * @param timers The {@link Timers} tracking the timing for the operation.
   * @param bulkLoadResult The {@link SzBulkLoadResult} describing the analysis.
   */
  protected SzBulkLoadResponse newBulkLoadResponse(
      SzHttpMethod        httpMethod,
      int                 httpStatusCode,
      UriInfo             uriInfo,
      Timers              timers,
      SzBulkLoadResult    bulkLoadResult)
  {
    return new SzBulkLoadResponse(
        this.newMeta(httpMethod, httpStatusCode, timers),
        this.newLinks(uriInfo), bulkLoadResult);
  }

  /**
   * Asynchronously process a record using the specified {@link SzApiProvider}
   * and {@link AsyncWorkerPool}.  The returned {@link AsyncResult} is from
   * a previously executed task on the same thread or <tt>null</tt> if the
   * worker thread employed has not previously executed a task.
   */
  protected AsyncResult<AddRecordResult> asyncProcessRecord(
      AsyncWorkerPool<AddRecordResult> asyncPool,
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
        this.enteringQueue(timers);
        return provider.executeInThread(() -> {
          this.exitingQueue(timers);
          int returnCode = this.addRecord(engineApi,
                                          provider,
                                          dataSource,
                                          recordId,
                                          recordJSON,
                                          loadId,
                                          timers);

          return this.newAddRecordResult(
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
  protected void processRecords(
      SzApiProvider     provider,
      Timers            timers,
      List<JsonObject>  records,
      String            loadId,
      SzBulkLoadResult  bulkLoadResult,
      int               maxFailures)
  {
    G2Engine engineApi = provider.getEngineApi();
    // otherwise try to load the record
    this.enteringQueue(timers);
    provider.executeInThread(() -> {
      this.exitingQueue(timers);
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
                                          provider,
                                          dataSource,
                                          recordId,
                                          recordJSON,
                                          loadId,
                                          timers);

          AddRecordResult addRecordResult = this.newAddRecordResult(
              dataSource, entityType, timers, returnCode, engineApi);

          this.trackLoadResult(addRecordResult, bulkLoadResult);
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
   * Constructs with the specified parameters.
   *
   * @param dataSource The data source for the record.
   * @param entityType The entity type for the record.
   * @param timers The {@link Timers} for the operation.
   * @param returnCode The return code from native add-record function.
   * @param engine The {@link G2Engine} instance that was used.
   *
   * @return The newly created instance of {@link AddRecordResult}.
   */
  protected AddRecordResult newAddRecordResult(String    dataSource,
                                               String    entityType,
                                               Timers    timers,
                                               int       returnCode,
                                               G2Engine  engine)
  {
    return new AddRecordResult(dataSource,
                               entityType,
                               timers,
                               returnCode,
                               engine);
  }

  /**
   * Adds the record either with or without a record ID and tracks the timing.
   */
  protected int addRecord(G2Engine      engineApi,
                          SzApiProvider provider,
                          String        dataSource,
                          String        recordId,
                          String        recordJSON,
                          String        loadId,
                          Timers        timers)
  {
    int returnCode;
    boolean asyncInfo = provider.hasInfoSink();
    if (asyncInfo) {
      StringBuffer sb = new StringBuffer();
      this.callingNativeAPI(timers, "engine", "addRecordWithInfo");
      returnCode = engineApi.addRecordWithInfo(
          dataSource,
          (recordId == null) ? "" : recordId, // empty record ID
          recordJSON,
          loadId,
          0,
          sb);
      this.calledNativeAPI(timers, "engine", "addRecordWithInfo");

      // check the return code before trying to send out the info
      if (returnCode == 0) {
        String rawInfo = sb.toString();

        // check if we have raw info to send
        if (rawInfo != null && rawInfo.trim().length() > 0) {
          SzMessageSink infoSink = provider.acquireInfoSink();
          SzMessage message = new SzMessage(rawInfo);
          try {
            infoSink.send(message, this::logFailedAsyncInfo);

          } catch (Exception e) {
            this.logFailedAsyncInfo(e, message);

          } finally {
            provider.releaseInfoSink(infoSink);
          }
        }
      }

    } else if (recordId != null) {
      this.callingNativeAPI(timers, "engine", "addRecord");
      returnCode = engineApi.addRecord(dataSource,
                                       recordId,
                                       recordJSON,
                                       loadId);
      this.calledNativeAPI(timers, "engine", "addRecord");

    } else {
      this.callingNativeAPI(timers, "engine",
                       "addRecordWithReturnedRecordID");
      StringBuffer sb = new StringBuffer();
      returnCode = engineApi.addRecordWithReturnedRecordID(
          dataSource, sb, recordJSON, loadId);
      this.calledNativeAPI(timers, "engine",
                      "addRecordWithReturnedRecordID");
    }
    return returnCode;
  }

  /**
   * Tracks the asynchronous record load result in the {@link SzBulkLoadResult}.
   */
  protected void trackLoadResult(AsyncResult<AddRecordResult>  asyncResult,
                                 SzBulkLoadResult           bulkLoadResult)
  {
    // check the result
    if (asyncResult != null) {
      AddRecordResult addRecordResult = null;
      try {
        // get the value from the async result (may throw an exception)
        addRecordResult = asyncResult.getValue();

      } catch (Exception e) {
        // an exception was thrown in trying to get the result
        String      jsonText  = e.getMessage();
        JsonObject  jsonObj   = JsonUtils.parseJsonObject(jsonText);

        String failDataSource = JsonUtils.getString(jsonObj, "dataSource");
        String failEntityType = JsonUtils.getString(jsonObj, "entityType");
        Throwable cause = e.getCause();
        bulkLoadResult.trackFailedRecord(
            failDataSource, failEntityType, this.newError(cause.getMessage()));
      }

      // track the result
      if (addRecordResult != null) {
        this.trackLoadResult(addRecordResult, bulkLoadResult);
      }
    }
  }

  /**
   * Tracks the asynchronous record load result in the {@link SzBulkLoadResult}.
   */
  protected void trackLoadResult(AddRecordResult addRecordResult,
                                 SzBulkLoadResult bulkLoadResult)
  {
    // check if the add failed or succeeded
    if (addRecordResult.isFailed()) {
      // adding the record failed, record the failure
      bulkLoadResult.trackFailedRecord(
          addRecordResult.getDataSource(),
          addRecordResult.getEntityType(),
          addRecordResult.getErrorCode(),
          addRecordResult.getErrorMessage());
    } else {
      // adding the record succeeded, record the loaded record
      bulkLoadResult.trackLoadedRecord(addRecordResult.getDataSource(),
                                       addRecordResult.getEntityType());
    }
  }

  /**
   * Formats load ID using the specified data cache
   */
  protected String formatLoadId(TemporaryDataCache          dataCache,
                                FormDataContentDisposition  fileMetaData)
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
  protected static class BulkDataSet {
    protected String characterEncoding;
    protected String mediaType = null;
    protected RecordReader.Format format;
    protected TemporaryDataCache dataCache;

    /**
     * Constructs with the specified media type (if known) and the specified
     * {@link InputStream}.  If the media type is not known it will be
     * automatically detected.
     *
     * @param mediaType The media type for the data that will be read, or
     *                  <tt>null</tt> if not known and it should be
     *                  automatically detected.
     * @param inputStream The {@link InputStream} to read the data.
     * @throws IOException If an I/O failure occurs.
     */
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

    /**
     * Gets the character encoding for this instance.
     *
     * @return The character encoding for this instance.
     */
    public String getCharacterEncoding() {
      return this.characterEncoding;
    }

    /**
     * Gets the media type for this instance.
     *
     * @return The media type for this instance.
     */
    public String getMediaType() {
      return this.mediaType;
    }

    /**
     * Gets the format for this instance.
     *
     * @return The format for this instance.
     */
    public RecordReader.Format getFormat() {
      return this.format;
    }

    /**
     * Sets the format for this instance.
     *
     * @param format The format to set.
     */
    public void setFormat(RecordReader.Format format) {
      this.format = format;
    }

    /**
     * Gets the {@link TemporaryDataCache} for this instance to read the data.
     *
     * @return The {@link TemporaryDataCache} for this instance to read the
     *         data.
     */
    public TemporaryDataCache getDataCache() {
      return dataCache;
    }
  }

  /**
   * Describes the result from the engine to track the result of attempting to
   * add a record.  This is used
   */
  public static class AddRecordResult {
    protected int     returnCode  = 0;
    protected String  dataSource  = null;
    protected String  entityType  = null;
    protected String  errorCode   = null;
    protected String  errorMsg    = null;
    protected Timers  timers      = null;

    /**
     * Constructs with the specified parameters.
     *
     * @param dataSource The data source for the record.
     * @param entityType The entity type for the record.
     * @param timers The {@link Timers} for the operation.
     * @param returnCode The return code from native add-record function.
     * @param engine The {@link G2Engine} instance that was used.
     */
    public AddRecordResult(String    dataSource,
                           String    entityType,
                           Timers    timers,
                           int       returnCode,
                           G2Engine  engine)
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

    /**
     * Gets the return code from add-record operation.
     *
     * @return The return code form the add-record operation.
     */
    public int getReturnCode() {
      return this.returnCode;
    }

    /**
     * Gets the data source for the record that was being added.
     *
     * @return The data source for the record that was being added.
     */
    public String getDataSource() {
      return this.dataSource;
    }

    /**
     * Gets the entity type for the record that was being added.
     *
     * @return The entity type for the record that was being added.
     */
    public String getEntityType() {
      return this.entityType;
    }

    /**
     * Gets the error code (if any) from the {@link G2Engine} with which this
     * instance was constructed.
     *
     * @return The error code (if any) from the {@link G2Engine} with which this
     *         instance was constructed.
     */
    public String getErrorCode() {
      return this.errorCode;
    }

    /**
     * Gets the error message (if any) from the {@link G2Engine} with which this
     * instance was constructed.
     *
     * @return The error message (if any) from the {@link G2Engine} with which
     *         this instance was constructed.
     */
    public String getErrorMessage() {
      return this.errorMsg;
    }

    /**
     * Gets the {@link Timers} instance for the add-record operation.
     *
     * @return The {@link Timers} instance for the add-record operation.
     */
    public Timers getTimers() {
      return this.timers;
    }

    /**
     * Checks if this instance describes a failure when performing the
     * add-record operation.  If this returns <tt>true</tt> then details can
     * be obtained from {@link #getErrorCode()} and {@link #getErrorMessage()}.
     *
     * @return <tt>true</tt> if the add-record operation failed, and
     *         <tt>false</tt> if it succeeded.
     */
    public boolean isFailed() {
      return (this.returnCode != 0);
    }

    /**
     * Produces a diagnostic {@link String} describing this instance.
     *
     * @return A diagnostic {@link String} describing this instance.
     */
    public String toString() {
      return "{ returnCode=[ " + this.returnCode
              + " ], dataSource=[ " + this.dataSource
              + " ], entityType=[ " + this.entityType
              + " ], errorCode=[ " + this.errorCode
              + " ], errorMsg=[ " + this.errorMsg
              + " ] }";
    }
  }

  /**
   * Completes the operation using the specified parameters to determine what
   * sort of request was made.
   *
   * @param eventBuilder The event builder if an SSE request.
   * @param sseEventSink The event sink if an SSE request.
   * @param eventId The event ID if an SSE request.
   * @param webSocketSession The web socket session if a web socket request.
   * @param response The response object to be sent.
   * @param <T> The type of the response object.
   *
   * @return The specified response object.
   */
  protected <T extends SzBasicResponse> T completeOperation(
      OutboundSseEvent.Builder  eventBuilder,
      SseEventSink              sseEventSink,
      int                       eventId,
      Session                   webSocketSession,
      T                         response)
  {
    if (eventBuilder != null || webSocketSession != null) {
      // check if sending an SSE event
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
      }

      // check if sending a message on the web socket session
      if (webSocketSession != null) {
        try {
          webSocketSession.getBasicRemote().sendObject(response);
          webSocketSession.close();

        } catch (Exception e) {
          e.printStackTrace();
        }
      }

      // return null here if we sent the response
      return null;
    }

    // return the response
    return response;
  }

  /**
   * Aborts the operation using the specified parameters to determine what
   * sort of request was made.
   *
   * @param failure The failure that triggered the abort.
   * @param response The response object to send.
   * @param uriInfo The {@link UriInfo} for the request.
   * @param timers The {@link Timers} associated with the operation.
   * @param eventId The event ID if an SSE request.
   * @param eventBuilder The event builder if an SSE request.
   * @param sseEventSink The event sink if an SSE request.
   * @param webSocketSession The web socket session if a web sockets request.
   * @param <T> The type of the response object.
   * @return The specified response object.
   * @throws WebApplicationException If a web application failure occurs.
   */
  protected <T extends SzBasicResponse> T abortOperation(
      Exception                 failure,
      T                         response,
      UriInfo                   uriInfo,
      Timers                    timers,
      int                       eventId,
      OutboundSseEvent.Builder  eventBuilder,
      SseEventSink              sseEventSink,
      Session                   webSocketSession)
      throws WebApplicationException
  {
    if (!isLastLoggedException(failure)) {
      failure.printStackTrace();
    }
    setLastLoggedException(failure);

    // determine if we need to construct an error response
    SzErrorResponse errorResponse = null;
    if (eventBuilder != null || webSocketSession != null) {
      errorResponse = this.newErrorResponse(
          this.newMeta(POST, 500, timers),
          this.newLinks(uriInfo), failure);
    }

    // check if this is a standard HTTP request
    if (eventBuilder == null && webSocketSession == null) {
      throw this.newInternalServerErrorException(
          POST, uriInfo, timers, failure);
    }

    if (eventBuilder != null) {
      // handle SSE response
      OutboundSseEvent abortEvent
          = eventBuilder.name(PROGRESS_EVENT)
          .id(String.valueOf(eventId++))
          .mediaType(APPLICATION_JSON_TYPE)
          .data(response)
          .reconnectDelay(RECONNECT_DELAY)
          .build();
      sseEventSink.send(abortEvent);

      OutboundSseEvent failEvent
          = eventBuilder.name(FAILED_EVENT)
          .id(String.valueOf(eventId++))
          .mediaType(APPLICATION_JSON_TYPE)
          .data(errorResponse)
          .reconnectDelay(RECONNECT_DELAY)
          .build();
      sseEventSink.send(failEvent);
      sseEventSink.close();
    }

    // check if we have a web socket session
    if (webSocketSession != null) {
      try {
        webSocketSession.getBasicRemote().sendObject(response);
        webSocketSession.getBasicRemote().sendObject(errorResponse);

      } catch (RuntimeException e) {
        e.printStackTrace();
        throw e;

      } catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }

    // return null
    return null;
  }

  /**
   * Populates the specified {@link Map} with the data source mappings found
   * in the specified JSON text.
   *
   * @param dataSourceMap The {@link Map} to be populated.
   * @param mapDataSources The JSON text describing the data source mappings.
   * @param provider The {@link SzApiProvider} to use.
   * @param timers The {@link Timers} for the operation.
   * @param uriInfo The {@link UriInfo} for the operation.
   */
  protected void processDataSources(Map<String, String> dataSourceMap,
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
            throw this.newBadRequestException(
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

  /**
   * Populates the specified {@link Map} with the data source mappings found
   * in the specified {@link List} of encoded strings.
   *
   * @param dataSourceMap The {@link Map} to be populated.
   * @param mapDataSourceList The {@link List} of encoded strings describing the
   *                          data source mappings.
   * @param provider The {@link SzApiProvider} to use.
   * @param timers The {@link Timers} for the operation.
   * @param uriInfo The {@link UriInfo} for the operation.
   */
  protected void processDataSources(Map<String, String> dataSourceMap,
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

  /**
   * Populates the specified {@link Map} with the entity type mappings found
   * in the specified JSON text.
   *
   * @param entityTypeMap The {@link Map} to be populated.
   * @param mapEntityTypes The JSON text describing the entity type mappings.
   * @param provider The {@link SzApiProvider} to use.
   * @param timers The {@link Timers} for the operation.
   * @param uriInfo The {@link UriInfo} for the operation.
   */
  protected void processEntityTypes(Map<String, String> entityTypeMap,
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


  /**
   * Populates the specified {@link Map} with the entity type mappings found
   * in the specified {@link List} of encoded strings.
   *
   * @param entityTypeMap The {@link Map} to be populated.
   * @param mapEntityTypeList The {@link List} of encoded strings describing the
   *                          entity type mappings.
   * @param provider The {@link SzApiProvider} to use.
   * @param timers The {@link Timers} for the operation.
   * @param uriInfo The {@link UriInfo} for the operation.
   */
  protected void processEntityTypes(Map<String, String> entityTypeMap,
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

  /**
   * The static instance of this class.
   */
  private static BulkDataServices DEFAULT_INSTANCE = null;

  /**
   * Gets the default instance of this class to be used by static methods.
   *
   * @return The default instance of this class to be used by static methods.
   */
  protected static synchronized BulkDataServices getDefaultInstance() {
    if (DEFAULT_INSTANCE == null) {
      DEFAULT_INSTANCE = new BulkDataServices();
    }
    return DEFAULT_INSTANCE;
  }

  /**
   * Sets the default instance of this class to be used by static methods.
   * If the specified parameter is <tt>null</tt> then a new instance of
   * {@link BulkDataServices} is created.
   *
   * @param instance The default instance of this class to use.
   */
  protected static synchronized void setDefaultInstance(
      BulkDataServices instance)
  {
    DEFAULT_INSTANCE = instance;
  }
}
