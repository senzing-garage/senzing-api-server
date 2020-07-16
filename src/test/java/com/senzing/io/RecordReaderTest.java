package com.senzing.io;

import com.senzing.util.JsonUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.json.*;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static com.senzing.io.RecordReader.Format.*;
import static com.senzing.util.LoggingUtilities.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/**
 * Tests for {@link RecordReader}.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class RecordReaderTest {

  private List<JsonObject> records;

  private List<JsonObject> recordsSansDS;

  private String csvRecords;

  private String csvRecordsSansDS;

  private String jsonRecords;

  private String jsonRecordsSansDS;

  private String jsonLinesRecords;

  private String jsonLinesRecordsSansDS;

  @BeforeAll
  public void setup() {
    this.records = new ArrayList<>();
    JsonObjectBuilder job = Json.createObjectBuilder();
    job.add("DATA_SOURCE", "EMPLOYEES");
    job.add("ENTITY_TYPE", "EMPL");
    job.add("NAME_FIRST", "JOE");
    job.add("NAME_LAST", "SCHMOE");
    job.add("PHONE_NUMBER", "702-555-1212");
    this.records.add(job.build());

    job = Json.createObjectBuilder();
    job.add("NAME_FIRST", "JOHN");
    job.add("NAME_LAST", "DOE");
    job.add("PHONE_NUMBER", "818-555-1212");
    this.records.add(job.build());

    job = Json.createObjectBuilder();
    job.add("DATA_SOURCE", "CUSTOMERS");
    job.add("ENTITY_TYPE", "CUST");
    job.add("NAME_FIRST", "JANE");
    job.add("NAME_LAST", "SMITH");
    job.add("PHONE_NUMBER", "702-444-1313");
    this.records.add(job.build());

    this.recordsSansDS = new ArrayList<>();
    job = Json.createObjectBuilder();
    job.add("NAME_FIRST", "JOE");
    job.add("NAME_LAST", "SCHMOE");
    job.add("PHONE_NUMBER", "702-555-1212");
    this.recordsSansDS.add(job.build());

    job = Json.createObjectBuilder();
    job.add("NAME_FIRST", "JOHN");
    job.add("NAME_LAST", "DOE");
    job.add("PHONE_NUMBER", "818-555-1212");
    this.recordsSansDS.add(job.build());

    job = Json.createObjectBuilder();
    job.add("NAME_FIRST", "JANE");
    job.add("NAME_LAST", "SMITH");
    job.add("PHONE_NUMBER", "702-444-1313");
    this.recordsSansDS.add(job.build());

    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);

    pw.println("DATA_SOURCE,ENTITY_TYPE,NAME_FIRST,NAME_LAST,PHONE_NUMBER");
    for (JsonObject obj: this.records) {
      pw.print(JsonUtils.getString(obj,"DATA_SOURCE", ""));
      pw.print(",");
      pw.print(obj.getString("ENTITY_TYPE", ""));
      pw.print(",");
      pw.print(obj.getString("NAME_FIRST"));
      pw.print(",");
      pw.print(obj.getString("NAME_LAST"));
      pw.print(",");
      pw.print(obj.getString("PHONE_NUMBER"));
      pw.println();
    }
    pw.flush();

    this.csvRecords = sw.toString();

    sw = new StringWriter();
    pw = new PrintWriter(sw);

    pw.println("NAME_FIRST,NAME_LAST,PHONE_NUMBER");
    for (JsonObject obj: this.recordsSansDS) {
      pw.print(obj.getString("NAME_FIRST"));
      pw.print(",");
      pw.print(obj.getString("NAME_LAST"));
      pw.print(",");
      pw.print(obj.getString("PHONE_NUMBER"));
      pw.println();
    }
    pw.flush();

    this.csvRecordsSansDS = sw.toString();

    JsonArrayBuilder jab = Json.createArrayBuilder();
    for (JsonObject obj : this.records) {
      jab.add(obj);
    }
    JsonArray recordArray = jab.build();

    jab = Json.createArrayBuilder();
    for (JsonObject obj : this.recordsSansDS) {
      jab.add(obj);
    }
    JsonArray recordArraySansDS = jab.build();

    this.jsonRecords = JsonUtils.toJsonText(recordArray, true);
    this.jsonRecordsSansDS = JsonUtils.toJsonText(recordArraySansDS, true);

    sw = new StringWriter();
    pw = new PrintWriter(sw);

    for (JsonObject obj: this.records) {
      pw.println(JsonUtils.toJsonText(obj));
    }
    pw.flush();
    this.jsonLinesRecords = sw.toString();

    sw = new StringWriter();
    pw = new PrintWriter(sw);

    for (JsonObject obj: recordsSansDS) {
      pw.println(JsonUtils.toJsonText(obj));
    }
    pw.flush();

    this.jsonLinesRecordsSansDS = sw.toString();
  }

  @Test
  public void detectCSVFormatTest() {
    StringReader sr = new StringReader(this.csvRecords);
    try {
      RecordReader rr = new RecordReader(sr);
      assertEquals(CSV, rr.getFormat(),
                   "Record format is not as expected for records: "
                       + this.csvRecords);

    } catch (IOException e) {
      e.printStackTrace();
      fail("Failed with I/O exception", e);
    }
  }

  @Test
  public void detectJsonFormatTest() {
    StringReader sr = new StringReader(this.jsonRecords);
    try {
      RecordReader rr = new RecordReader(sr);
      assertEquals(JSON, rr.getFormat(),
                   "Record format is not as expected for records: "
                       + this.jsonRecords);

    } catch (IOException e) {
      e.printStackTrace();
      fail("Failed with I/O exception", e);
    }
  }

  @Test
  public void detectJsonLinesFormatTest() {
    StringReader sr = new StringReader(this.jsonLinesRecords);
    try {
      RecordReader rr = new RecordReader(sr);
      assertEquals(JSON_LINES, rr.getFormat(),
                   "Record format is not as expected for records: "
                       + this.jsonLinesRecords);

    } catch (IOException e) {
      e.printStackTrace();
      fail("Failed with I/O exception", e);
    }
  }

  @Test
  public void readCsvRecordsTest() {
    StringReader sr = new StringReader(this.csvRecords);
    try {
      RecordReader rr = new RecordReader(sr);
      for (JsonObject expected : this.records) {
        expected = augmentRecord(
            expected, null, null, null);
        JsonObject actual = rr.readRecord();
        assertEquals(expected, actual,
                     multilineFormat(
                         "Record not as expected:",
                         "EXPECTED: ",
                         JsonUtils.toJsonText(expected, true),
                         "ACTUAL: ",
                         JsonUtils.toJsonText(actual, true)));
      }
    } catch (IOException e) {
      e.printStackTrace();
      fail("Failed with I/O exception", e);
    }
  }

  private List<Arguments> getTestParameters() {
    Map<String, List<JsonObject>> recordsMap = new LinkedHashMap<>();
    recordsMap.put(this.csvRecords, this.records);
    recordsMap.put(this.csvRecordsSansDS, this.recordsSansDS);
    recordsMap.put(this.jsonRecords, this.records);
    recordsMap.put(this.jsonRecordsSansDS, this.recordsSansDS);
    recordsMap.put(this.jsonLinesRecords, this.records);
    recordsMap.put(this.jsonLinesRecordsSansDS, this.recordsSansDS);

    Map<String,String> specificMap1 = new HashMap<>();
    specificMap1.put("EMPLOYEES", "EMPL");
    specificMap1.put(null, "CUST");
    specificMap1 = Collections.unmodifiableMap(specificMap1);

    Map<String,String> specificMap2 = new HashMap<>();
    specificMap2.put("EMPLOYEES", "EMPL");
    specificMap2.put("CUSTOMERS", "CUST");
    specificMap2.put(null, "PEOPLE");
    specificMap2 = Collections.unmodifiableMap(specificMap2);

    Map<String, String> specificMap3 = new HashMap<>();
    specificMap3.put("EMPL", "WORKER");
    specificMap3.put(null, "CONSUMER");

    Map<String, String> specificMap4 = new HashMap<>();
    specificMap4.put("EMPL", "WORKER");
    specificMap4.put("CUST", "CONSUMER");
    specificMap4.put(null, "GENERIC");

    List<Map<String,String>> dataSourceMaps = new LinkedList<>();
    dataSourceMaps.add(null);
    dataSourceMaps.add(Collections.emptyMap());
    dataSourceMaps.add(Collections.singletonMap(null, "PEOPLE"));
    dataSourceMaps.add(specificMap1);
    dataSourceMaps.add(specificMap2);

    List<Map<String,String>> entityTypeMaps = new LinkedList<>();
    entityTypeMaps.add(null);
    entityTypeMaps.add(Collections.emptyMap());
    entityTypeMaps.add(Collections.singletonMap(null, "GENERIC"));
    entityTypeMaps.add(specificMap3);
    entityTypeMaps.add(specificMap4);

    String[] sourceIds = { null, "", "SomeFile" };

    List<Arguments> result = new LinkedList<>();
    recordsMap.entrySet().forEach(entry -> {
      String recordsText = entry.getKey();
      List<JsonObject> expected = entry.getValue();

      for (Map<String,String> dataSourceMap : dataSourceMaps) {
        for (Map<String, String> entityTypeMap: entityTypeMaps) {
          for (String sourceId : sourceIds) {
            result.add(arguments(recordsText,
                                 expected,
                                 dataSourceMap,
                                 entityTypeMap,
                                 sourceId));
          }
        }
      }
    });
    return result;
  }

  @ParameterizedTest
  @MethodSource("getTestParameters")
  public void readRecordsTest(String              recordsText,
                              List<JsonObject>    expectedRecords,
                              Map<String,String>  dataSourceMap,
                              Map<String,String>  entityTypeMap,
                              String              sourceId)
  {
    StringReader sr = new StringReader(recordsText);
    Map<String,String> dsMap = dataSourceMap;
    Map<String,String> etMap = entityTypeMap;
    try {
      RecordReader rr = new RecordReader(
          sr, dataSourceMap, entityTypeMap, sourceId);
      for (JsonObject expected : expectedRecords) {
        expected = augmentRecord(
            expected, dataSourceMap, entityTypeMap, sourceId);
        JsonObject actual = rr.readRecord();
        assertEquals(expected, actual,
                     multilineFormat(
                         rr.getFormat() + " record not as expected:",
                         "RECORDS TEXT: ",
                         recordsText,
                         " --> dataSourceMap: "
                             + ((dsMap != null) ? dsMap.toString() : null),
                         " --> entityTypeMap: "
                             + ((etMap != null) ? etMap.toString() : null),
                         "EXPECTED: ",
                         JsonUtils.toJsonText(expected, true),
                         "ACTUAL: ",
                         JsonUtils.toJsonText(actual, true)));
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail("Failed with exception", e);
    }
  }

  private static JsonObject augmentRecord(JsonObject          record,
                                          Map<String,String>  dataSourceMap,
                                          Map<String,String>  entityTypeMap,
                                          String              sourceId)
  {
    JsonObjectBuilder job = Json.createObjectBuilder(record);
    String dataSource = JsonUtils.getString(
        record, "DATA_SOURCE", "");
    String entityType = JsonUtils.getString(
        record, "ENTITY_TYPE", "");

    dataSource = dataSource.trim().toUpperCase();
    entityType = entityType.trim().toUpperCase();

    // map the data source
    if (dataSourceMap != null) {
      String origDS = dataSource;
      dataSource = dataSourceMap.get(dataSource);
      if (dataSource == null) {
        dataSource = dataSourceMap.get(null);
      }
      if (dataSource == null) {
        dataSource = origDS;
      }
    }
    if (dataSource != null && dataSource.trim().length() > 0)
    {
      if (record.containsKey("DATA_SOURCE")) {
        job.remove("DATA_SOURCE");
      }
      job.add("DATA_SOURCE", dataSource);
    } else {
      dataSource = "";
    }

    // map the entity type
    if (entityTypeMap != null) {
      String origET = entityType;
      entityType = entityTypeMap.get(entityType);
      if (entityType == null) {
        entityType = entityTypeMap.get(null);
      }
      if (entityType == null) {
        entityType = origET;
      }
    }
    if (entityType != null && entityType.trim().length() > 0)
    {
      if (record.containsKey("ENTITY_TYPE")) {
        job.remove("ENTITY_TYPE");
      }
      job.add("ENTITY_TYPE", entityType);
    } else {
      entityType = "";
    }

    // add the source ID
    if (sourceId != null && sourceId.trim().length() > 0) {
      if (record.containsKey("SOURCE_ID")) {
        job.remove("SOURCE_ID");
      }
      job.add("SOURCE_ID", sourceId);
    }
    return job.build();
  }
}
