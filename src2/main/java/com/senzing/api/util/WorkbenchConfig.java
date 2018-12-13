package com.senzing.api.util;


import com.senzing.util.JsonUtils;

import javax.json.*;
import java.io.*;
import java.util.Collections;

import static com.senzing.api.Workbench.*;

public class WorkbenchConfig {
  private static final String AUDIT_SAMPLE_SIZE_KEY = "auditSampleSize";

  private static final int DEFAULT_AUDIT_SAMPLE_SIZE = 150;

  private static final File CONFIG_FILE
      = new File(USER_DATA_DIR, "config-api.json");

  private static final JsonWriterFactory WRITER_FACTORY
      = Json.createWriterFactory(Collections.emptyMap());

  private WorkbenchConfig() {
    // do nothing
  }

  public static synchronized int getAuditSampleSize() {
    return Integer.parseInt(
        getValue(AUDIT_SAMPLE_SIZE_KEY,
                 String.valueOf(DEFAULT_AUDIT_SAMPLE_SIZE)));
  }

  public static synchronized int setAuditSampleSize(int size) {
    setValue(AUDIT_SAMPLE_SIZE_KEY, String.valueOf(size));
    return size;
  }

  private static synchronized void ensureConfigFile() {
    if (!CONFIG_FILE.exists()) {
      try (FileOutputStream fos = new FileOutputStream(CONFIG_FILE);
           OutputStreamWriter osw = new OutputStreamWriter(fos);
           PrintWriter pw = new PrintWriter(osw))
      {
        pw.println("{}");
      } catch (IOException e) {
        log("FAILED TO CREATE DEFAULT WORKBENCH CONFIGURATION: " + CONFIG_FILE);
        log(e);
        throw new RuntimeException(e);
      }
    }
  }

  public static synchronized JsonObject readConfigFile() {
    ensureConfigFile();
    try (FileInputStream fis = new FileInputStream(CONFIG_FILE);
         InputStreamReader isr = new InputStreamReader(fis, "UTF-8"))
    {
      JsonReader jsonReader = Json.createReader(isr);
      return jsonReader.readObject();

    } catch (Exception e) {
      log("FAILED TO READ WORKBENCH CONFIGURATION: " + CONFIG_FILE);
      log(e);

      try (FileInputStream fis = new FileInputStream(CONFIG_FILE);
           InputStreamReader isr = new InputStreamReader(fis, "UTF-8");
           BufferedReader br = new BufferedReader(isr))
      {
        StringBuilder sb = new StringBuilder();
        for (String line = br.readLine(); line != null; line = br.readLine()) {
          sb.append(line);
        }
        log("CONFIG CONTENTS: " + sb.toString());
      } catch (Exception ignore) {
        // do nothing
      }

      if (e instanceof RuntimeException) {
        throw (RuntimeException) e;
      } else {
        throw new RuntimeException(e);
      }
    }
  }

  public static synchronized String getValue(String key, String defaultValue)
  {
    ensureConfigFile();
    JsonObject jsonObject = readConfigFile();
    return JsonUtils.getString(jsonObject, key, defaultValue);
  }

  public static synchronized void setValue(String key, String value)
  {
    ensureConfigFile();

    // read the current config and modify it -- trying to guard against
    // the file being changed externally to this function
    long lastModified = CONFIG_FILE.lastModified();
    JsonObject jsonObject;
    do {
      jsonObject = readConfigFile();
      JsonObjectBuilder builder = Json.createObjectBuilder(jsonObject);
      builder.remove(key);
      builder.add(key, value);
      jsonObject = builder.build();

    } while (lastModified < CONFIG_FILE.lastModified());

    log("SETTING CONFIG VALUE: " + key + " = " + value);
    try (FileOutputStream fos = new FileOutputStream(CONFIG_FILE);
         OutputStreamWriter osw = new OutputStreamWriter(fos, "UTF-8"))
    {
      JsonWriter jsonWriter = WRITER_FACTORY.createWriter(osw);

      jsonWriter.writeObject(jsonObject);
      log("WROTE TO FILE: " + CONFIG_FILE);

    } catch (IOException e) {
      log("FAILED TO WRITE WORKBENCH CONFIGURATION: " + CONFIG_FILE);
      log(e);
      throw new RuntimeException(e);
    }
  }
}
