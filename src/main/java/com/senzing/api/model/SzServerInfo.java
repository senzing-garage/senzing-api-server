package com.senzing.api.model;

/**
 * Describes the server features and state.
 */
public class SzServerInfo {
  /**
   * The server concurrency.
   */
  private int concurrency;

  /**
   * The active config ID being used by the server or <tt>null</tt>
   */
  private long activeConfigId;

  /**
   * Whether or not the server will automatically pickup the latest
   * default configuration if it changes.
   */
  private boolean dynamicConfig;

  /**
   * Whether or not the server was started in read-only mode.  If in
   * read-only mode then operations that modify the repository (e.g.:
   * loading records or configuring new data sources) are not allowed.
   */
  private boolean readOnly;

  /**
   * Whether or not admin features are enabled.  If admin features are
   * not enabled then the configuration cannot be modified.
   */
  private boolean adminEnabled;

  /**
   * Default constructor.
   */
  public SzServerInfo() {
    this.concurrency      = 0;
    this.activeConfigId   = 0;
    this.dynamicConfig    = false;
    this.readOnly         = false;
    this.adminEnabled     = false;
  }

  /**
   * Gets the number of Senzing worker threads pooled for handling requests.
   *
   * @return The number of Senzing worker threads pooled for handling requests.
   */
  public int getConcurrency() {
    return concurrency;
  }

  /**
   * Sets the number of Senzing worker threads pooled for handling requests.
   *
   * @param concurrency The number of Senzing worker threads pooled for
   *                    handling requests.
   */
  public void setConcurrency(int concurrency) {
    this.concurrency = concurrency;
  }

  /**
   * The active configuration ID being used by the API server.  This
   * is still available if the server was started with a static file
   * configuration via the `G2CONFIGFILE` initialization property.
   *
   * @return The active configuration ID being used b the API server.
   */
  public long getActiveConfigId() {
    return activeConfigId;
  }

  /**
   * The active configuration ID being used by the API server.  This
   * is still available if the server was started with a static file
   * configuration via the `G2CONFIGFILE` initialization property.
   *
   * @param activeConfigId The active configuration ID being used by the
   *                       API server.
   */
  public void setActiveConfigId(Long activeConfigId) {
    this.activeConfigId = activeConfigId;
  }

  /**
   * Checks whether or not the server will automatically pickup the latest
   * default configuration if it changes.
   *
   * @return <tt>true</tt> if the server will automatically pickup the latest
   *         default configuration if it changes, and <tt>false</tt> if the
   *         configuration is static and the server will not recognize changes.
   */
  public boolean isDynamicConfig() {
    return dynamicConfig;
  }

  /**
   * Sets whether or not the server will automatically pickup the latest
   * default configuration if it changes.
   *
   * @param dynamicConfig <tt>true</tt> if the server will automatically pickup
   *                      the latest default configuration if it changes, and
   *                      <tt>false</tt> if the configuration is static and the
   *                      server will not recognize changes.
   */
  public void setDynamicConfig(boolean dynamicConfig) {
    this.dynamicConfig = dynamicConfig;
  }

  /**
   * Checks whether or not the server was started in read-only mode.  If in
   * read-only mode then operations that modify the repository (e.g.: loading
   * records or configuring new data sources) are not allowed.
   *
   * @return <tt>true</tt> if the server was started in read-only mode,
   *         and <tt>false</tt> if write operations are allowed.
   */
  public boolean isReadOnly() {
    return readOnly;
  }

  /**
   * Sets whether or not the server was started in read-only mode.  If in
   * read-only mode then operations that modify the repository (e.g.: loading
   * records or configuring new data sources) are not allowed.
   *
   * @param readOnly <tt>true</tt> if the server was started in read-only mode,
   *                 and <tt>false</tt> if write operations are allowed.
   */
  public void setReadOnly(boolean readOnly) {
    this.readOnly = readOnly;
  }

  /**
   * Checks whether or not admin features are enabled.  If admin features are
   * not enabled then the configuration cannot be modified.
   *
   * @return <tt>true</tt> if admin features are enabled, otherwise
   *         <tt>false</tt>.
   */
  public boolean isAdminEnabled() {
    return adminEnabled;
  }

  /**
   * Sets whether or not admin features are enabled.  If admin features are
   * not enabled then the configuration cannot be modified.
   *
   * @param adminEnabled <tt>true</tt> if admin features are enabled, otherwise
   *                     <tt>false</tt>.
   */
  public void setAdminEnabled(boolean adminEnabled) {
    this.adminEnabled = adminEnabled;
  }
}
