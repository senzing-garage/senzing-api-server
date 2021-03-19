package com.senzing.api.services;

import com.senzing.api.model.*;
import com.senzing.api.websocket.JsonEncoder;
import com.senzing.api.websocket.StringDecoder;
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
import java.nio.ByteBuffer;
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
import static com.senzing.io.IOUtilities.*;
import static javax.ws.rs.core.MediaType.TEXT_PLAIN_TYPE;

/**
 * Bulk data REST services.
 */
@Path("/bulk-data")
@Produces("application/json; charset=UTF-8; qs=1.0")
public class BulkDataServices {
  /**
   * The size of the piped input stream buffer size (10MB).
   */
  public static final int PIPE_SIZE = 1024 * 1024 * 10;

  /**
   * The {@link MediaType} with text/plain and charset=utf8
   */
  private static final MediaType TEXT_PLAIN_UTF8 = new MediaType(
      TEXT_PLAIN_TYPE.getType(), TEXT_PLAIN_TYPE.getSubtype(), UTF_8);

  /**
   * The file date pattern.
   */
  private static final String FILE_DATE_PATTERN = "yyyyMMdd_HHmmssX";

  /**
   * The default maximum amount of time timeout to wait between receiving
   * Web Socket messages from the client before triggering EOF on the
   * incoming stream.
   */
  public static final Long DEFAULT_EOF_SEND_TIMEOUT = 30000L;

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
    try {
      return analyzeBulkRecords(mediaType,
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
      return analyzeBulkRecords(mediaType,
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
  private static void validateProgressPeriod(Long         progressPeriod,
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
      throw newBadRequestException(
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
      analyzeBulkRecords(mediaType,
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
      analyzeBulkRecords(mediaType,
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
    try {
      return loadBulkRecords(dataSource,
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
    try {
      return loadBulkRecords(dataSource,
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
      loadBulkRecords(dataSource,
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
      loadBulkRecords(dataSource,
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
      synchronized (this.webSocketThread) {
        long waitTime = this.webSocketThread.eofSendTimeout;
        while (!this.completed) {
          // wait for a period
          try {
            this.webSocketThread.wait(waitTime);

          } catch (InterruptedException ignore) {
            // ignore
          }

          // check the time
          long now = System.currentTimeMillis();
          long duration = (now - this.webSocketThread.lastMessageTime);
          boolean timedOut = (duration > this.webSocketThread.eofSendTimeout);

          if (this.completed || timedOut) {
            // signal EOF
            IOUtilities.close(this.webSocketThread.pipedOutputStream);
            this.webSocketThread.pipedOutputStream = null;
            this.webSocketThread.notifyAll();
            break;
          }

          // now check how long to wait next time
          waitTime = this.webSocketThread.eofSendTimeout - duration;
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

    @OnOpen
    public void onOpen(Session session)
        throws IOException, IllegalArgumentException
    {
      System.err.println("OPENING WEB SOCKET");
      this.session            = session;
      this.pipedInputStream   = new PipedInputStream(PIPE_SIZE);
      this.pipedOutputStream  = new PipedOutputStream(this.pipedInputStream);
      this.uriInfo            = newProxyUriInfo(this.session);
      this.started            = false;
      this.lastMessageTime    = System.currentTimeMillis();

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

      // start the thread
      this.start();
    }

    @OnMessage
    public void onMessage(byte[] bytes) throws IOException
    {
      if (this.pipedOutputStream == null) {
        throw new IllegalStateException(
            "Output stream is already closed.");
      }

      System.err.println();
      System.err.println("BINARY MESSAGE RECEIVED: " + bytes.length);
      System.err.println();
      synchronized (this) {
        if (this.pipedOutputStream != null) {
          this.pipedOutputStream.write(bytes);
          this.pipedOutputStream.flush();
          this.lastMessageTime = System.currentTimeMillis();
          this.notifyAll();
        }
      }
    }

    @OnMessage
    public void onMessage(String text)  throws IOException
    {
      if (this.pipedOutputStream == null) {
        throw new IllegalStateException(
            "Output stream is already closed.");
      }

      System.err.println();
      System.err.println("TEXT MESSAGE RECEIVED: " + text.length());
      System.err.println();
      synchronized (this) {
        if (this.pipedInputStream != null) {
          this.pipedOutputStream.write(text.getBytes(UTF_8));
          this.pipedOutputStream.flush();
          this.lastMessageTime = System.currentTimeMillis();
          this.notifyAll();
        }
      }
    }

    @OnClose
    public void onClose(Session session) throws IOException {
      System.err.println("CLOSING WEB SOCKET");
      synchronized (this) {
        IOUtilities.close(this.pipedOutputStream);
        this.pipedOutputStream = null;
        this.notifyAll();
      }
    }

    @OnError
    public void onError(Session session, Throwable throwable)
        throws IOException
    {
      System.err.println("ERROR ON WEB SOCKET");
      throwable.printStackTrace();
      synchronized (this) {
        IOUtilities.close(this.pipedOutputStream);
        this.pipedOutputStream = null;

        CloseReason.CloseCode closeCode
            = (throwable instanceof BadRequestException)
            ? CloseReason.CloseCodes.PROTOCOL_ERROR
            : CloseReason.CloseCodes.UNEXPECTED_CONDITION;

        this.session.close(new CloseReason(closeCode, throwable.getMessage()));
      }
    }

    /**
     * Implemented to start the EOF detector and then defer to the {@link
     * #doRun()} method.
     */
    public final void run() {
      // create the EOF thread
      this.eofDetector = new EOFDetector(this);
      this.eofDetector.start();

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
      System.out.println("ANALYZING BULK RECORDS....");
      analyzeBulkRecords(TEXT_PLAIN_UTF8,
                         this.pipedInputStream,
                         this.uriInfo,
                         this.progressPeriod,
                         null,
                         null,
                         this.session);

      synchronized (this) {
        IOUtilities.close(this.pipedInputStream);
        this.pipedInputStream = null;
        IOUtilities.close(this.pipedOutputStream);
        this.pipedOutputStream = null;
        this.notifyAll();
      }

      try {
        this.session.close();
      } catch (IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
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
          this.progressPeriod = Long.parseLong(paramList.get(0));

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
      loadBulkRecords(this.dataSource,
                      this.mapDataSources,
                      this.mapDataSourceList,
                      this.entityType,
                      this.mapEntityTypes,
                      this.mapEntityTypeList,
                      this.loadId,
                      this.maxFailures,
                      TEXT_PLAIN_UTF8,
                      this.pipedInputStream,
                      null,
                      this.uriInfo,
                      this.progressPeriod,
                      null,
                      null,
                      this.session);

      synchronized (this) {
        IOUtilities.close(this.pipedInputStream);
        IOUtilities.close(this.pipedOutputStream);
        this.pipedInputStream = null;
        this.pipedOutputStream = null;
        this.notifyAll();
      }

      try {
        this.session.close();
      } catch (IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }

  }

  /**
   * Analyzes the bulk data and returns information about it.
   */
  private static SzBulkDataAnalysisResponse analyzeBulkRecords(
      MediaType                   mediaType,
      InputStream                 dataInputStream,
      UriInfo                     uriInfo,
      Long                        progressPeriod,
      SseEventSink                sseEventSink,
      Sse                         sse,
      Session                     webSocketSession)
  {
    System.out.println("ANALYZING BULK RECORDS: " + progressPeriod);

    OutboundSseEvent.Builder eventBuilder
        = (sseEventSink != null && sse != null) ? sse.newEventBuilder() : null;
    int eventId = 0;

    SzBulkDataAnalysis dataAnalysis = new SzBulkDataAnalysis();
    Timers timers = newTimers();

    // check the progress period
    validateProgressPeriod(progressPeriod,
                           timers,
                           uriInfo,
                           sseEventSink,
                           sse,
                           webSocketSession);

    try {
      System.out.println("CREATING BULK DATA SET....");
      BulkDataSet bulkDataSet = new BulkDataSet(mediaType, dataInputStream);
      System.out.println("CREATED BULK DATA SET.");
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
          long duration = now - start;
          // check if the progress period has expired
          if ((progressPeriod != null) && (now - start > progressPeriod)) {
            // reset the start time
            start = now;

            // check if we are sending a response message
            SzBulkDataAnalysisResponse response = null;
            if (eventBuilder != null || webSocketSession != null) {
              // build the response message
              response = new SzBulkDataAnalysisResponse(
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

      SzBulkDataAnalysisResponse response = new SzBulkDataAnalysisResponse(
          POST,200, uriInfo, timers, dataAnalysis);

      abortOperation(e,
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

    SzBulkDataAnalysisResponse response = new SzBulkDataAnalysisResponse(
        POST,200, uriInfo, timers, dataAnalysis);

    return completeOperation(
        eventBuilder, sseEventSink, eventId, webSocketSession, response);
  }

  /**
   * Analyzes the bulk data and returns information about it.
   */
  private static SzBulkLoadResponse loadBulkRecords(
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
              asyncResult = asyncProcessRecord(asyncPool,
                                               provider,
                                               subTimers,
                                               record,
                                               loadId);

            } finally {
              trackLoadResult(asyncResult, bulkLoadResult);
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

          // check if the timing has gone beyond the specified progress period
          if ((progressPeriod != null) && (now - start > progressPeriod)) {
            // create the update response if there is a client expecting it
            SzBulkLoadResponse update = null;
            if (eventBuilder != null || webSocketSession != null) {
              start = now;
              update = new SzBulkLoadResponse(
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
          processRecords(provider,
                         timers,
                         first1000Records,
                         loadId,
                         bulkLoadResult,
                         maxFailures);
        }

        // close out any in-flight loads from the asynchronous pool
        List<AsyncResult<EngineResult>> results = asyncPool.close();
        for (AsyncResult<EngineResult> asyncResult : results) {
          trackLoadResult(asyncResult, bulkLoadResult);
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
                     sseEventSink,
                     webSocketSession);
    }

    SzBulkLoadResponse response
        = new SzBulkLoadResponse(POST,
                                 200,
                                 uriInfo,
                                 timers,
                                 bulkLoadResult);

    return completeOperation(
        eventBuilder, sseEventSink, eventId, webSocketSession, response);
  }

  /**
   * Asynchronously process a record using the specified {@link SzApiProvider}
   * and {@link AsyncWorkerPool}.  The returned {@link AsyncResult} is from
   * a previously executed task on the same thread or <tt>null</tt> if the
   * worker thread employed has not previously executed a task.
   */
  private static AsyncResult<EngineResult> asyncProcessRecord(
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
          int returnCode = addRecord(engineApi,
                                     provider,
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
  private static void processRecords(
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
          int returnCode = addRecord(engineApi,
                                     provider,
                                     dataSource,
                                     recordId,
                                     recordJSON,
                                     loadId,
                                     timers);

          EngineResult engineResult = new EngineResult(
              dataSource, entityType, timers, returnCode, engineApi);

          trackLoadResult(engineResult, bulkLoadResult);
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
  private static int addRecord(G2Engine      engineApi,
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
      callingNativeAPI(timers, "engine", "addRecordWithInfo");
      returnCode = engineApi.addRecordWithInfo(
          dataSource,
          (recordId == null) ? "" : recordId, // empty record ID
          recordJSON,
          loadId,
          0,
          sb);
      calledNativeAPI(timers, "engine", "addRecordWithInfo");

      // check the return code before trying to send out the info
      if (returnCode == 0) {
        String rawInfo = sb.toString();

        // check if we have raw info to send
        if (rawInfo != null && rawInfo.trim().length() > 0) {
          SzMessageSink infoSink = provider.acquireInfoSink();
          SzMessage message = new SzMessage(rawInfo);
          try {
            infoSink.send(message, ServicesUtil::logFailedAsyncInfo);

          } catch (Exception e) {
            logFailedAsyncInfo(e, message);

          } finally {
            provider.releaseInfoSink(infoSink);
          }
        }
      }

    } else if (recordId != null) {
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
  private static void trackLoadResult(AsyncResult<EngineResult> asyncResult,
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
        trackLoadResult(engineResult, bulkLoadResult);
      }
    }
  }

  /**
   * Tracks the asynchronous record load result in the {@link SzBulkLoadResult}.
   */
  private static void trackLoadResult(EngineResult       engineResult,
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

  private static <T extends SzBasicResponse> T abortOperation(
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
      errorResponse
          = new SzErrorResponse(POST, 500, uriInfo, timers, failure);
    }

    // check if this is a standard HTTP request
    if (eventBuilder == null && webSocketSession == null) {
      throw newInternalServerErrorException(POST, uriInfo, timers, failure);
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
