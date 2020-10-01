package com.senzing.api.server;

import com.senzing.util.JsonUtils;

import javax.json.JsonObject;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static com.senzing.api.server.SzApiServer.*;
import static com.senzing.api.server.SzApiServerOption.*;

/**
 * Describes the options to be set when constructing an instance of
 * {@link SzApiServer}.
 *
 */
public class SzApiServerOptions {
  private int         httpPort          = DEFAULT_PORT;
  private InetAddress bindAddress       = null;
  private int         concurrency       = DEFAULT_CONCURRENCY;
  private String      moduleName        = DEFAULT_MODULE_NAME;
  private boolean     verbose           = false;
  private boolean     quiet             = false;
  private boolean     readOnly          = false;
  private boolean     adminEnabled      = false;
  private boolean     skipStartupPerf   = false;
  private long        statsInterval     = DEFAULT_STATS_INTERVAL;
  private String      allowedOrigins    = null;
  private Long        configId          = null;
  private Long        autoRefreshPeriod = null;
  private JsonObject  jsonInit          = null;

  /**
   * Constructs with the JSON initialization parameters as a {@link
   * JsonObject}.
   *
   * @param jsonInit The JSON initialization parameters.
   */
  public SzApiServerOptions(JsonObject jsonInit) {
    Objects.requireNonNull(jsonInit,
                           "JSON init parameters cannot be null");
    this.jsonInit = jsonInit;
  }

  /**
   * Constructs with the JSON initialization parameters as JSON text.
   *
   * @param jsonInitText The JSON initialization parameters as JSON text.
   */
  public SzApiServerOptions(String jsonInitText) {
    this(JsonUtils.parseJsonObject(jsonInitText));
  }

  /**
   * Returns the {@link JsonObject} describing the initialization parameters
   * for the Senzing engine.
   *
   * @return The {@link JsonObject} describing the initialization parameters
   *         for the Senzing engine.
   */
  public JsonObject getJsonInitParameters() {
    return this.jsonInit;
  }

  /**
   * Returns the HTTP port to bind to.  Zero (0) is returned if binding to
   * a random available port.  This is initialized to the {@linkplain
   * SzApiServer#DEFAULT_PORT default port number} if not explicitly set.
   *
   * @return The HTTP port to bind to or zero (0) if the server will bind
   *         to a random available port.
   */
  public int getHttpPort() {
    return this.httpPort;
  }

  /**
   * Sets the HTTP port to bind to.  Use zero to bind to a random port and
   * <tt>null</tt> to bind to the {@linkplain SzApiServer#DEFAULT_PORT default
   * port}.
   *
   * @param port The HTTP port to bind to, zero (0) if the server should bind
   *             to a random port and <tt>null</tt> if server should bind to
   *             the default port.
   *
   * @return A reference to this instance.
   */
  public SzApiServerOptions setHttpPort(Integer port) {
    this.httpPort = (port != null) ? port : DEFAULT_PORT;
    return this;
  }

  /**
   * Gets the {@link InetAddress} for the address that the server will bind
   * to.  This returns <tt>null</tt> then the loopback address is to be used.
   *
   * @return The {@link InetAddress} for the address that the server will
   *         bind, or <tt>null</tt> then the loopback address is to be used.
   */
  public InetAddress getBindAddress() {
    return this.bindAddress;
  }

  /**
   * Sets the {@link InetAddress} for the address that the server will bind
   * to.  Set to <tt>null</tt> to bind to the loopback address.
   *
   * @param addr The {@link InetAddress} for the address that the server will
   *             bind, or <tt>null</tt> if the loopback address is to be used.
   *
   * @return A reference to this instance.
   */
  public SzApiServerOptions setBindAddress(InetAddress addr) {
    this.bindAddress = addr;
    return this;
  }

  /**
   * Gets the number of threads that the server will create for the engine.
   * If the value has not {@linkplain #setConcurrency(Integer) explicitly set}
   * then {@link SzApiServer#DEFAULT_CONCURRENCY} is returned.
   *
   * @return The number of threads that the server will create for the engine.
   */
  public int getConcurrency() {
    return this.concurrency;
  }

  /**
   * Sets the number of threads that the server will create for the engine.
   * Set to <tt>null</tt> to use the {@linkplain SzApiServer#DEFAULT_CONCURRENCY
   * default number of threads}.
   *
   * @param concurrency The number of threads to create for the engine, or
   *                    <tt>null</tt> for the default number of threads.
   *
   * @return A reference to this instance.
   */
  public SzApiServerOptions setConcurrency(Integer concurrency) {
    this.concurrency = (concurrency != null)
        ? concurrency : DEFAULT_CONCURRENCY;
    return this;
  }

  /**
   * Gets the module name to initialize with.  If <tt>null</tt> is returned
   * then {@link SzApiServer#DEFAULT_MODULE_NAME} is used.
   *
   * @return The module name to initialize with, or <tt>null</tt> is returned
   *         then {@link SzApiServer#DEFAULT_MODULE_NAME} is used.
   */
  public String getModuleName() {
    return this.moduleName;
  }

  /**
   * Sets the module name to initialize with.  Set to <tt>null</tt> if the
   * default value of {@link SzApiServer#DEFAULT_MODULE_NAME} is to be used.
   *
   * @param moduleName The module name to bind to, or <tt>null</tt> then
   *                   the {@link SzApiServer#DEFAULT_MODULE_NAME} is used.
   *
   * @return A reference to this instance.
   */
  public SzApiServerOptions setModuleName(String moduleName) {
    this.moduleName = moduleName;
    return this;
  }

  /**
   * Checks whether or not to initialize the Senzing API's in verbose mode.
   * If the verbosity has not been {@linkplain #setVerbose(boolean)
   * explicitly set} then <tt>false</tt> is returned.
   *
   * @return <tt>true</tt> if the native Senzing API's should be initialized
   *         in verbose mode, otherwise <tt>false</tt>.
   */
  public boolean isVerbose() {
    return this.verbose;
  }

  /**
   * Sets whether or not to initialize the Senzing API's in verbose mode.
   *
   * @param verbose <tt>true</tt> if the native Senzing API's should be
   *                initialized in verbose mode, otherwise <tt>false</tt>.
   */
  public SzApiServerOptions setVerbose(boolean verbose) {
    this.verbose = verbose;
    return this;
  }

  /**
   * Checks whether or not the API server should forbid access to operations
   * that would modify the entity repository and allow only read operations.
   * If the read-only restriction has not been {@linkplain
   * #setReadOnly(boolean) explicitly set} then <tt>false</tt> is returned.
   *
   * @return <tt>true</tt> if the API server should only allow read
   *         operations, and <tt>false</tt> if all operations are allowed.
   */
  public boolean isReadOnly() {
    return this.readOnly;
  }

  /**
   * Sets whether or not the API server should forbid access to operations
   * that would modify the entity repository and allow only read operations.
   *
   * @param readOnly <tt>true</tt> if the API server should forbid write
   *                 operations, and <tt>false</tt> if all operations are
   *                 allowed.
   */
  public SzApiServerOptions setReadOnly(boolean readOnly) {
    this.readOnly = readOnly;
    return this;
  }

  /**
   * Checks whether or not the API server should allow access to admin
   * operations.  If the admin features have not been {@linkplain
   * #setAdminEnabled(boolean) explicitly enabled} then <tt>false</tt> is
   * returned.
   *
   * @return <tt>true</tt> if the API server should only allow admin
   *         operations, and <tt>false</tt> if admin operations are forbidden.
   */
  public boolean isAdminEnabled() {
    return this.adminEnabled;
  }

  /**
   * Sets whether or not the API server should allow access to admin operations.
   *
   * @param adminEnabled <tt>true</tt> if the API server should allow admin
   *                     operations, and <tt>false</tt> if admin operations
   *                     are forbidden.
   */
  public SzApiServerOptions setAdminEnabled(boolean adminEnabled) {
    this.adminEnabled = adminEnabled;
    return this;
  }

  /**
   * Checks whether or not the API server should reduce the number of messages
   * sent to standard output.  This applies to messages specific to the API
   * server and NOT messages generated by the underlying native API (especially
   * if the API is initialized in verbose mode).
   *
   * @return <tt>true</tt> if the API server should reduce the number of
   *         messages sent to standard output, otherwise <tt>false</tt>
   */
  public boolean isQuiet() {
    return this.quiet;
  }

  /**
   * Sets whether or not the API server should reduce the number of messages
   * sent to standard output.  This applies to messages specific to the API
   * server and NOT messages generated by the underlying native API (especially
   * if the API is initialized in verbose mode).
   *
   * @param quiet <tt>true</tt> if the API server should reduce the number of
   *              messages sent to standard output, otherwise <tt>false</tt>
   */
  public SzApiServerOptions setQuiet(boolean quiet) {
    this.quiet = quiet;
    return this;
  }

  /**
   * Returns the CORS Access-Control-Allow-Origin header to use for responses
   * from all HTTP REST API endpoints.  This returns <tt>null</tt> if the
   * CORS Access-Control-Allow-Origin header is to be omitted from the HTTP
   * responses.
   *
   * @return The CORS Access-Control-Allow-Origin header to use for responses
   *         from all HTTP REST API endpoints, or <tt>null</tt> if the header
   *         is to be omitted.
   */
  public String getAllowedOrigins() {
    return this.allowedOrigins;
  }

  /**
   * Sets the CORS Access-Control-Allow-Origin header to use for responses
   * from all HTTP REST API endpoints.  Set this to <tt>null</tt> if the
   * CORS Access-Control-Allow-Origin header is to be omitted from the HTTP
   * responses.
   *
   * @param allowOriginHeader The CORS Access-Control-Allow-Origin header to
   *                          use for responses from all HTTP REST API
   *                          endpoints, or <tt>null</tt> if the header
   *                          is to be omitted.
   */
  public SzApiServerOptions setAllowedOrigins(String allowOriginHeader) {
    this.allowedOrigins = allowOriginHeader;
    return this;
  }

  /**
   * Gets the explicit configuration ID with which to initialize the Senzing
   * native engine API.  This method returns <tt>null</tt> if the API server
   * should use the current default configuration ID from the entity
   * repository.  This method returns <tt>null</tt> if the value has not been
   * {@linkplain #setConfigurationId(Long) explicitly set}.
   *
   * @return The explicit configuration ID with which to initialize the
   *         Senzing native engine API, or <tt>null</tt> if the API server
   *         should use the current default configuration ID from the entity
   *         repository.
   */
  public Long getConfigurationId() {
    return this.configId;
  }

  /**
   * Sets the explicit configuration ID with which to initialize the Senzing
   * native engine API.  Set the value to <tt>null</tt> if the API server
   * should use the current default configuration ID from the entity
   * repository.
   *
   * @param configId The explicit configuration ID with which to initialize
   *                 the Senzing native engine API, or <tt>null</tt> if the
   *                 API server should use the current default configuration
   *                 ID from the entity repository.
   */
  public SzApiServerOptions setConfigurationId(Long configId) {
    this.configId = configId;
    return this;
  }

  /**
   * Returns the auto refresh period which is positive to indicate a number of
   * seconds to delay, zero if auto-refresh is disabled, and negative to
   * indicate that the auto refresh thread should run but refreshes will be
   * requested manually (used for testing).
   *
   * @return The auto refresh period.
   */
  public Long getAutoRefreshPeriod() {
    return this.autoRefreshPeriod;
  }

  /**
   * Sets the configuration auto refresh period.  Set the value to <tt>null</tt>
   * if the API server should use {@link
   * SzApiServer#DEFAULT_CONFIG_REFRESH_PERIOD}.
   *
   * @param autoRefreshPeriod The number of seconds to automatically
   */
  public SzApiServerOptions setAutoRefreshPeriod(Long autoRefreshPeriod) {
    this.autoRefreshPeriod = autoRefreshPeriod;
    return this;
  }

  /**
   * Gets the minimum time interval for logging stats.  This is the minimum
   * period between logging of stats assuming the API Server is performing
   * operations that will affect stats (i.e.: activities pertaining to entity
   * scoring).  If the API Server is idle or active, but not performing entity
   * scoring activities then stats logging will be delayed until activities are
   * performed that will affect stats.  If the returned interval is zero (0)
   * then stats logging will be suppressed.
   *
   * @return The interval for logging stats, or zero (0) if stats logging is
   *         suppressed.
   */
  public long getStatsInterval() {
    return this.statsInterval;
  }

  /**
   * Sets the minimum interval for logging stats.  This is the minimum
   * period between logging of stats assuming the API Server is performing
   * operations that will affect stats (i.e.: activities pertaining to entity
   * scoring).  If the API Server is idle or active, but not performing entity
   * scoring activities then stats logging will be delayed until activities are
   * performed that will affect stats.  If the specified value is zero (0)
   * then stats logging will be suppressed.  If the specified value is less-than
   * zero (0) then the value will be set to zero (0).
   *
   * @param statsInterval The stats interval, or a non-positive number (e.g.:
   *                      zero) to suppress logging stats.
   */
  public SzApiServerOptions setStatsInterval(long statsInterval) {
    this.statsInterval = (statsInterval < 0L) ? 0L : statsInterval;
    return this;
  }

  /**
   * Checks whether or not the API server should skip the performance check that
   * is performed at startup.
   *
   * @return <tt>true</tt> if the API server should skip the performance
   *         check performed at startup, and <tt>false</tt> if not.
   */
  public boolean isSkippingStartupPerformance() {
    return this.skipStartupPerf;
  }

  /**
   * Sets whether or not the API server should skip the performance check that
   * is performed at startup.
   *
   * @param skipping <tt>true</tt> if the API server should skip the performance
   *                 check performed at startup, and <tt>false</tt> if not.
   */
  public SzApiServerOptions setSkippingStartupPerformance(boolean skipping) {
    this.skipStartupPerf = skipping;
    return this;
  }

  /**
   * Creates a {@link Map} of {@link SzApiServerOption} keys to {@link Object} values
   * for initializing an {@link SzApiServer} instance.
   *
   * @return The {@link Map} of {@link SzApiServerOption} keys to {@link Object} values
   *         for initializing an {@link SzApiServer} instanc
   */
  Map<SzApiServerOption, ?> buildOptionsMap() {
    Map<SzApiServerOption, Object> map = new HashMap<>();
    map.put(HTTP_PORT,            this.getHttpPort());
    map.put(BIND_ADDRESS,         this.getBindAddress());
    map.put(CONCURRENCY,          this.getConcurrency());
    map.put(MODULE_NAME,          this.getModuleName());
    map.put(VERBOSE,              this.isVerbose());
    map.put(QUIET,                this.isQuiet());
    map.put(READ_ONLY,            this.isReadOnly());
    map.put(ENABLE_ADMIN,         this.isAdminEnabled());
    map.put(ALLOWED_ORIGINS,      this.getAllowedOrigins());
    map.put(CONFIG_ID,            this.getConfigurationId());
    map.put(INIT_JSON,            this.getJsonInitParameters());
    map.put(AUTO_REFRESH_PERIOD,  this.getAutoRefreshPeriod());
    map.put(STATS_INTERVAL,       this.getStatsInterval());
    map.put(SKIP_STARTUP_PERF,    this.isSkippingStartupPerformance());
    return map;
  }
}
