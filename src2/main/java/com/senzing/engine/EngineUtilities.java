package com.senzing.engine;

import javax.json.*;
import java.io.StringReader;

public class EngineUtilities {
  /**
   * Private default constructor.
   */
  private EngineUtilities() {
    // do nothing
  }

  /**
   * Parses the stats from the engine to extract the loaded records.
   *
   * @param statsResult The JSON stats result.
   *
   * @return The loaded record count.
   */
  public static long parseLoadedRecordsFromStats(String jsonText) {
    try {
      StringReader sr         = new StringReader(jsonText);
      JsonReader jsonReader = Json.createReader(sr);
      JsonObject jsonObject = jsonReader.readObject();
      JsonValue jsonValue  = jsonObject.getValue("/workload/loadedRecords");
      JsonNumber jsonNumber = (JsonNumber) jsonValue;

      return jsonNumber.longValue();

    } catch (RuntimeException e) {
      throw e;
    }
  }
}
