package com.senzing.api;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class BuildInfo {
  public static final String MAVEN_VERSION;

  public static final String REST_API_VERSION = "2.3.0";

  static {
    String resource = "/com/senzing/api/build-info.properties";
    String version = "UNKNOWN";
    try (InputStream is = BuildInfo.class.getResourceAsStream(resource))
    {
      Properties buildProps = new Properties();
      buildProps.load(is);
      version = buildProps.getProperty("Maven-Version");

    } catch (IOException e) {
      System.err.println("FAILED TO READ " + resource + " FILE");
      e.printStackTrace();

    } catch (Exception e) {
      e.printStackTrace();
      throw new ExceptionInInitializerError(e);

    } finally {
      MAVEN_VERSION = version;
    }
  }

  /**
   * Private default constructor.
   */
  private BuildInfo() {
    // do nothing
  }
}
