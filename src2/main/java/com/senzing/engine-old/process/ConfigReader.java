package com.senzing.api.engine.process;

import com.senzing.g2.engine.G2Product;
import com.senzing.api.Workbench;
import com.senzing.api.engine.EngineClassLoader;

import javax.json.*;
import java.io.*;
import java.util.Optional;

import static com.senzing.api.Workbench.log;

public class ConfigReader {

  public static long getProjectCompatibilityVersion(long projectId) {
    try {
      File g2ConfigFile = new File(Workbench.getProjectDirectory(projectId), "g2-config.json");
      return getConfigFileCompatibilityVersion(g2ConfigFile);

    } catch (IOException e) {
      throw new RuntimeException("Unable to retrieve project compatibility version: " + projectId);
    }
  }

  public static long getConfigFileCompatibilityVersion(File g2ConfigFile) {
    try {
      JsonObject configObject;
      try (FileInputStream fis = new FileInputStream(g2ConfigFile);
           InputStreamReader isr = new InputStreamReader(fis, "UTF-8");
           JsonReader jsonReader = Json.createReader(fis))
      {
        configObject = jsonReader.readObject();
      }

      return Optional.of(configObject)
          .map(jo -> jo.getJsonObject("G2_CONFIG"))
          .map(jo -> jo.getJsonObject("COMPATIBILITY_VERSION"))
          .map(jo -> jo.getJsonNumber("CONFIG_VERSION"))
          .map(JsonNumber::longValue)
          .orElse(-1L);
    } catch (IOException e) {
      throw new RuntimeException("Unable to retrieve project compatibility version: " + g2ConfigFile);
    }
  }

  public static long getEngineCompatibilityVersion() {
    try {
      ClassLoader parent = Thread.currentThread().getContextClassLoader();
      if (parent == null) {
        parent = ClassLoader.getSystemClassLoader();
      }
      ClassLoader engineLoader = new EngineClassLoader(parent);

      final String implClassName = "com.senzing.g2.engine.G2ProductJNI";
      Class implClass = engineLoader.loadClass(implClassName);
      final G2Product impl = (G2Product) implClass.newInstance();

      String configString = impl.version();
      log(configString);
      JsonReader reader = Json.createReader(new StringReader(configString));

      return Optional.of(reader.readObject())
        .map(jo -> jo.getJsonObject("COMPATIBILITY_VERSION"))
        .map(jo -> jo.getJsonNumber("CONFIG_VERSION"))
        .map(JsonNumber::longValue)
        .orElse(-1L);
    }
    catch(Exception e) {
      throw new RuntimeException("Unable to retrieve EngineCompatibilityVersion.", e);
    }
  }
}
