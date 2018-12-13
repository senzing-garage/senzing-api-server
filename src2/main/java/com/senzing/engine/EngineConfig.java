package com.senzing.engine;

import java.io.*;
import java.nio.file.Files;
import java.sql.SQLException;
import java.util.*;

import com.senzing.g2.engine.G2Config;
import com.senzing.api.DataSource;
import com.senzing.api.engine.process.ConfigReader;
import com.senzing.api.util.WorkbenchLogging;

import static java.nio.file.StandardCopyOption.*;
import static com.senzing.api.Workbench.*;

public class EngineConfig {
  /**
   *
   */
  private static final G2Config G2_CONFIG = EngineAdmin.getConfigAPI();

  /**
   * The configuration handle.
   */
  private long configHandle;

  /**
   *
   */
  private boolean closed;

  /**
   *
   */
  private long lastModified;

  /**
   *
   */
  public EngineConfig() {
    this.configHandle = G2_CONFIG.create();
    this.closed = false;
    this.lastModified = -1L;
  }

  /**
   *
   */
  public EngineConfig(File configFile) throws IOException {
    if (!configFile.exists()) {
      throw new IllegalArgumentException(
        "The specified config file does not exist: " + configFile);
    }
    StringBuilder sb = new StringBuilder();
    char[] buffer = new char[8192];
    try (FileInputStream fis = new FileInputStream(configFile);
         InputStreamReader isr = new InputStreamReader(fis, "UTF-8"))
    {
      int readCount;
      while ((readCount = isr.read(buffer)) >= 0) {
        sb.append(buffer, 0, readCount);
      }
    }
    this.configHandle = G2_CONFIG.load(sb.toString());
    this.closed       = false;
    this.lastModified = configFile.lastModified();
  }

  /**
   * Returns the last-modified time of the configuration.
   */
  public long getLastModified() {
    return this.lastModified;
  }

  public static EngineConfig getProjectConfig(long projectId)
    throws IOException, SQLException
  {
    return getProjectConfig(projectId, false);
  }

  public static EngineConfig getProjectConfig(long     projectId,
                                              boolean  baseConfig)
    throws IOException, SQLException
  {
    return getProjectConfig(projectId, baseConfig, null);
  }

  public static boolean refreshBaseConfig(long projectId)
    throws IOException
  {
    return refreshBaseConfig(projectId, false);
  }

  public static boolean refreshBaseConfig(long projectId, boolean force)
    throws IOException
  {
    if (!force && !isBaseConfigStale(projectId)) return false;
    File projectDir = getProjectDirectory(projectId);
    File baseProjectConfig = new File(projectDir, "base-g2-config.json");
    File dataDir = new File(DATA_PATH);
    File templateConfig = new File(dataDir, "g2config.json");
    Files.copy(templateConfig.toPath(),
               baseProjectConfig.toPath(),
               COPY_ATTRIBUTES,
               REPLACE_EXISTING);

    // delete any previously existing project config since it is now stale
    File projectConfig = new File(projectDir, "g2-config.json");
    if (projectConfig.exists()) {
      projectConfig.delete();
    }
    
    return true;
  }

  public static boolean isConfigChangedSince(long projectId, long timestamp)
    throws IOException
  {
    File projectDir = getProjectDirectory(projectId);
    File baseFile = new File(projectDir, "base-g2-config.json");
    File confFile = new File(projectDir, "g2-config.json");
    File dsrcFile = new File(projectDir, "data-sources.properties");

    if (!baseFile.exists()) return true;
    if (!confFile.exists()) return true;
    if (!dsrcFile.exists()) return true;

    if (baseFile.lastModified() > timestamp) return true;
    if (confFile.lastModified() > timestamp) return true;
    if (dsrcFile.lastModified() > timestamp) return true;

    return false;
  }

  public static boolean isBaseConfigStale(long projectId)
    throws IOException
  {
    File projectDir         = getProjectDirectory(projectId);
    File baseProjectConfig  = new File(projectDir, "base-g2-config.json");
    File projectConfig      = new File(projectDir, "g2-config.json");
    File dataDir = new File(DATA_PATH);
    File templateConfig = new File(dataDir, "g2config.json");
    if (!baseProjectConfig.exists() && !projectConfig.exists()) return false;

    // check if we never got the base config for this project
    if (!baseProjectConfig.exists()
        && (projectConfig.lastModified() > templateConfig.lastModified()))
    {
      Files.copy(templateConfig.toPath(),
                 baseProjectConfig.toPath(),
                 COPY_ATTRIBUTES,
                 REPLACE_EXISTING);
    }

    long baseCompatVersion
        = ConfigReader.getConfigFileCompatibilityVersion(baseProjectConfig);
    long templateCompatVersion
        = ConfigReader.getConfigFileCompatibilityVersion(templateConfig);

    if (baseCompatVersion < templateCompatVersion) {
      return true;
    }

    // if the base config file is as new as the template then it is current
    if (baseProjectConfig.lastModified() >= templateConfig.lastModified()) {
      return false;
    }

    // compare the files byte for byte
    try (InputStream is1 = new FileInputStream(templateConfig);
         BufferedInputStream bis1 = new BufferedInputStream(is1);
         InputStream is2 = new FileInputStream(baseProjectConfig);
         BufferedInputStream bis2 = new BufferedInputStream(is2))
    {
      int byte1 = bis1.read();
      int byte2 = bis2.read();
      do {
        if (byte1 != byte2) {
          return true;
        }
        byte1 = bis1.read();
        byte2 = bis2.read();
      } while (byte1 >= 0 && byte2 >= 0);
    }

    // update the last modified time if the files are identical
    baseProjectConfig.setLastModified(System.currentTimeMillis());

    // if we get here then return true
    return false;
  }

  public static EngineConfig getProjectConfig(
      long                projectId,
      boolean             baseConfig,
      Collection<String>  dataSourceCodes)
    throws IOException, SQLException
  {
    File projectDir         = getProjectDirectory(projectId);
    File baseProjectConfig  = new File(projectDir, "base-g2-config.json");
    File projectConfig      = new File(projectDir, "g2-config.json");

    // make sure the base config exists
    if (!baseProjectConfig.exists()) {
      File dataDir = new File(DATA_PATH);
      File templateConfig = new File(dataDir, "g2config.json");
      Files.copy(templateConfig.toPath(),
                 baseProjectConfig.toPath(),
                 COPY_ATTRIBUTES,
                 REPLACE_EXISTING);
    }

    File dataSourcePropsFile = new File(projectDir, "data-sources.properties");
    Properties prevDataSourceProps = null;
    if (dataSourcePropsFile.exists()) {
      prevDataSourceProps = new Properties();
      try (FileInputStream fis = new FileInputStream(dataSourcePropsFile);
           InputStreamReader isr = new InputStreamReader(fis, "UTF-8")) {

        prevDataSourceProps.load(isr);
      }
    }

    Properties dataSourceProps = new Properties();
    DataSourceDataAccess dsrcDA = new DataSourceDataAccess(projectId);
    List<DataSource> dataSources = dsrcDA.selectDataSources();
    for (DataSource ds : dataSources) {
      dataSourceProps.setProperty(ds.getCode(), String.valueOf(ds.getId()));
      if (dataSourceCodes != null) dataSourceCodes.add(ds.getCode());
    }

    boolean dataSourcesChanged = (!dataSourceProps.equals(prevDataSourceProps));

    boolean configStale
      = (baseProjectConfig.lastModified() > projectConfig.lastModified())
        || (projectConfig.lastModified() < dataSourcePropsFile.lastModified());

    // update the current config - make sure the current config exists
    if (!projectConfig.exists() || dataSourcesChanged || configStale)
    {
      try (FileOutputStream fos = new FileOutputStream(dataSourcePropsFile);
           OutputStreamWriter osw = new OutputStreamWriter(fos, "UTF-8")) {
        dataSourceProps.store(osw, null);
      }

      EngineConfig engineConfig = new EngineConfig(baseProjectConfig);

      for (DataSource ds : dataSources) {
        engineConfig.addDataSource(ds.getCode(), ds.getId());
      }

      engineConfig.writeToFile(projectConfig);
      engineConfig.lastModified = projectConfig.lastModified();

      if (!baseConfig) return engineConfig;
    }

    File configFile = (baseConfig) ? baseProjectConfig : projectConfig;

    return new EngineConfig(configFile);
  }

  /**
   * Ensures this instance is not closed.
   */
  private void assertNotClosed() throws IllegalStateException {
    if (this.closed) {
      throw new IllegalStateException("The engine config is already closed.");
    }
  }

  /**
   * Converts to a JSON string.
   *
   * @return A JSON string for the config.
   */
  public String toString() {
    this.assertNotClosed();
    StringBuffer sb = new StringBuffer();
    G2_CONFIG.save(this.configHandle, sb);
    return sb.toString();
  }

  /**
   * Closes this instance and does nothing if already closed.  The instance
   * will be unusable after closing.
   */
  public void close() {
    if (this.closed) return;
    this.closed = true;
    G2_CONFIG.close(this.configHandle);
  }

  /**
   * Finalizes this instance by closing the config.
   */
  protected void finalize() throws Throwable {
    this.close();
    super.finalize();
  }

  /**
   * Adds a data source to the config with the specified data source code
   * and data source ID.
   *
   * @param dataSourceCode The data source code to add.
   *
   * @param dataSourceId The data source ID for the data source.
   *
   */
  public void addDataSource(String dataSourceCode, int dataSourceId) {
    this.assertNotClosed();
    log("****** ADDING DATA SOURCE: " + dataSourceCode + " / " + dataSourceId);
    int result = G2_CONFIG.addDataSourceWithID(
      this.configHandle, dataSourceCode, dataSourceId);
    if (result != 0) {
      int code = G2_CONFIG.getLastExceptionCode();
      if (code == 0) code = result;
      String lastException = G2_CONFIG.getLastException();
      lastException = WorkbenchLogging.detailedLogsWrap(code, lastException);
      G2_CONFIG.clearLastException();
      throw new EngineException(
        "G2Config failed with error code " + result + ": " + lastException);
    }
  }

  /**
   * Writes this config to the specified {@link File}.
   *
   * @param file The {@link File} to write the configuration to.
   *
   * @throws IOException If an I/O failure occurs.
   */
  public void writeToFile(File file) throws IOException {
    try (FileOutputStream fos = new FileOutputStream(file);
         OutputStreamWriter osw = new OutputStreamWriter(fos, "UTF-8")) {
      osw.write(this.toString());
      osw.flush();
    }
  }
}
