package com.senzing.engine;

import com.senzing.g2.engine.G2Config;
import com.senzing.g2.engine.G2Product;
import com.senzing.util.JsonUtils;
import com.senzing.api.model.SzLicenseInfo;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import java.io.*;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class EngineAdmin {
  /**
   * The G2Product API instance.
   */
  private G2Product g2Product;

  /**
   * The G2Config API instance.
   */
  private G2Config g2Config;

  /**
   * The config init result.
   */
  private int configInitResult;

  /**
   * The product init result.
   */
  private int productInitResult;

  /**
   * Monitor to use to manage read/write locking.
   */
  private final ReentrantReadWriteLock monitor
      = new ReentrantReadWriteLock();

  /**
   * Constructs with module name, ini file and verbose logging flagging.
   *
   * @param moduleName The module name to initialize with.
   * @param iniFile The INI file to initialize with.
   * @param verbose Whether or not to initialize with verbose logging.
   */
  public EngineAdmin(String moduleName, File iniFile, boolean verbose) {
    if (!iniFile.exists()) {
      throw new IllegalArgumentException(
          "INI File does not exist: " + iniFile);
    }
    try {
      final String configImplClassName = "com.senzing.g2.engine.G2ConfigJNI";
      Class configImplClass = Class.forName(configImplClassName);
      g2Config = (G2Config) configImplClass.newInstance();

      final String productImplClassName = "com.senzing.g2.engine.G2ProductJNI";
      Class productImplClass = Class.forName(productImplClassName);
      g2Product = (G2Product) productImplClass.newInstance();


      monitor.writeLock().lock();
      try {
        String iniFilePath = iniFile.toString();

        configInitResult = g2Config.init(moduleName, iniFilePath, verbose);
        productInitResult = g2Product.init(moduleName, iniFilePath, verbose);

        // NOTE: configInitResult may be less-than zero to indicate an expired license
        // this is not checked here because we don't want to fail startup -- we
        // want to report the expired license to the user

        if (productInitResult < 0) {
          throw new RuntimeException(
              "Failed to initialize product API: " + productInitResult);
        }
      } finally {
        monitor.writeLock().unlock();
      }

    } catch (Throwable e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  /**
   * Package access to the underlying {@link G2Product} API.
   *
   * @return Return the underlying {@link G2Product} API.
   */
  G2Product getProductAPI() {
    return this.g2Product;
  }

  /**
   * Package access to the underlying {@link G2Config} API.
   *
   * @return Return the underlying {@link G2Config} API.
   */
  G2Config getConfigAPI() {
    return this.g2Config;
  }

  /**
   * Checks if the license was expired when we initialized.
   *
   * @return <tt>true</tt> if the license is expired, otherwise <tt>false</tt>
   */
  public boolean checkLicenseExpired() {
    this.monitor.readLock().lock();
    try {
      final int expiredLicense = -1;
      return (configInitResult == expiredLicense);
    } finally {
      this.monitor.readLock().unlock();
    }
  }

  /**
   * Returns the version string from the engine API.
   *
   * @return The version string from the engine API.
   */
  public String getEngineVersion() {
    Map<String,String> map = this.getVersionInfo();
    return map.get("VERSION");
  }

  /**
   * Returns the build number string from the engine API.
   *
   * @return The build number string from the engine API.
   */
  public String getEngineBuildNumber() {
    Map<String,String> map = this.getVersionInfo();
    return map.get("BUILD_NUMBER");
  }

  private Map<String,String> getVersionInfo()
  {
    this.monitor.readLock().lock();
    try {
      String versionInfo = this.g2Product.version();
      StringReader reader = new StringReader(versionInfo);

      JsonReader jsonReader = Json.createReader(reader);
      JsonObject jsonObject = jsonReader.readObject();

      String version = JsonUtils.getString(jsonObject, "VERSION");
      String buildNumber = JsonUtils.getString(jsonObject, "BUILD_NUMBER");

      Map<String, String> result = new HashMap<>();
      result.put("VERSION", version);
      result.put("BUILD_NUMBER", buildNumber);

      return result;
    } finally {
      this.monitor.readLock().unlock();
    }
  }

  public SzLicenseInfo getLicenseInfo() {
    this.monitor.readLock().lock();
    try {
      //if (EngineAdmin.checkLicenseExpired()) {
      //  return null;
      //  LicenseInfo.EXPIRED_LICENSE;
      //}

      String jsonText = this.g2Product.license();

      StringReader sr = new StringReader(jsonText);
      JsonReader jsonReader = Json.createReader(sr);
      JsonObject jsonObject = jsonReader.readObject();
      return SzLicenseInfo.parseLicenseInfo(null, jsonObject);

    } finally {
      this.monitor.readLock().unlock();
    }
  }
}
