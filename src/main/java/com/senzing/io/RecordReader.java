package com.senzing.io;

import com.senzing.util.JsonUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.stream.JsonParser;
import javax.json.stream.JsonParserFactory;
import javax.json.stream.JsonParsingException;
import java.io.*;
import java.util.*;

/**
 * Provides a reader over records that are formatted as JSON, JSON-Lines
 * or CSV.
 */
public class RecordReader {
  /**
   * Represents the supported format for the records.
   */
  public enum Format {
    JSON("application/json", "JSON"),
    JSON_LINES("application/x-jsonlines", "JSON Lines"),
    CSV("text/csv", "CSV");

    /**
     *  The associated media type.
     */
    private String mediaType;

    /**
     * The simple name associated with the media type.
     */
    private String simpleName;

    /**
     * The lookup map to lookup format by media type.
     */
    private static Map<String,Format> MEDIA_TYPE_LOOKUP;

    /**
     * Constructs with the specified media type.
     * @param mediaType The media type.
     * @param simpleName The simple name for the format.
     */
    Format(String mediaType, String simpleName) {
      this.mediaType = mediaType;
      this.simpleName = simpleName;
    }

    /**
     * Returns the associated media type.
     */
    public String getMediaType() {
      return this.mediaType;
    }

    /**
     * Returns the simple name for the format.
     */
    public String getSimpleName() { return this.simpleName; }

    /**
     * Initializes the lookup.
     */
    static {
      try {
        Map<String, Format> map = new LinkedHashMap<>();
        for (Format format: Format.values()) {
          map.put(format.getMediaType(), format);
        }
        MEDIA_TYPE_LOOKUP = Collections.unmodifiableMap(map);

      } catch (Exception e) {
        e.printStackTrace();
        throw new ExceptionInInitializerError(e);
      }
    }

    /**
     * Returns the {@link Format} for the specified media type or <tt>null</tt>
     * if no format is associated with the media type.  This method returns
     * <tt>null</tt> if <tt>null</tt> is specified as the media type.
     *
     * @param mediaType The media type for which the {@link Format} is being
     *                  requested.
     *
     * @return The associated {@link Format} for the media type, or
     *         <tt>null</tt> if there is none or if the specified parameter is
     *         <tt>null</tt>
     */
    public static Format fromMediaType(String mediaType) {
      if (mediaType == null) return null;
      return MEDIA_TYPE_LOOKUP.get(mediaType.trim().toLowerCase());
    }
  }

  /**
   * The format for the records.
   */
  private Format format = null;

  /**
   * The backing character reader.
   */
  private Reader reader;

  /**
   * The mapping for the data sources.
   */
  private Map<String, String> dataSourceMap;

  /**
   * The mapping for the entity types.
   */
  private Map<String, String> entityTypeMap;

  /**
   * The source ID to assign to the records.
   */
  private String sourceId;

  /**
   * The backing {@link RecordProvider}.
   */
  private RecordProvider recordProvider;

  /**
   * Constructs a {@link RecordReader} with the specified {@link Reader}.
   * The format of the reader is inferred using the first character read.
   *
   * @param reader The {@link Reader} from which to read the text for the
   *               records.
   *
   * @throws IOException If an I/O failure occurs.
   */
  public RecordReader(Reader reader) throws IOException {
    this(null,
         reader,
         Collections.emptyMap(),
         Collections.emptyMap(),
         null);
  }

  /**
   * Constructs a {@link RecordReader} with the specified {@link Format} and
   * {@link Reader}.
   *
   * @param format The expected format of the records.
   *
   * @param reader The {@link Reader} from which to read the text for the
   *               records.
   *
   * @throws IOException If an I/O failure occurs.
   */
  public RecordReader(Format format, Reader reader) throws IOException {
    this(format,
         reader,
         Collections.emptyMap(),
         Collections.emptyMap(),
         null);
  }

  /**
   * Constructs a {@link RecordReader} with the specified {@link Reader},
   * data source code and entity type code.  The format of the reader is
   * inferred from the first character read.
   *
   * @param reader The {@link Reader} from which to read the text for the
   *               records.
   *
   * @param dataSource The data source to assign to each record.
   *
   * @param entityType The entity type to assign to each record.
   *
   * @throws IOException If an I/O failure occurs.
   */
  public RecordReader(Reader reader, String dataSource, String entityType)
      throws IOException
  {
    this(null,
         reader,
         Collections.singletonMap("", dataSource),
         Collections.singletonMap("", entityType),
         null);
  }

  /**
   * Constructs a {@link RecordReader} with the specified {@link Format},
   * {@link Reader}, data source code and entity type code.
   *
   * @param format The expected format of the records.
   *
   * @param reader The {@link Reader} from which to read the text for the
   *               records.
   *
   * @param dataSource The data source to assign to each record.
   *
   * @param entityType The entity type to assign to each record.
   *
   * @throws IOException If an I/O failure occurs.
   */
  public RecordReader(Format format,
                      Reader reader,
                      String dataSource,
                      String entityType)
      throws IOException
  {
    this(format,
         reader,
         Collections.singletonMap("", dataSource),
         Collections.singletonMap("", entityType),
         null);
  }

  /**
   * Constructs a {@link RecordReader} with the specified {@link Reader},
   * data source code, entity type code and source ID.  The format of the reader
   * is inferred from the first character.
   *
   * @param reader The {@link Reader} from which to read the text for the
   *               records.
   *
   * @param dataSource The data source to assign to each record.
   *
   * @param entityType The entity type to assign to each record.
   *
   * @param sourceId the source ID to assign to each record.
   *
   * @throws IOException If an I/O failure occurs.
   */
  public RecordReader(Reader reader,
                      String dataSource,
                      String entityType,
                      String sourceId)
      throws IOException
  {
    this(null,
         reader,
         Collections.singletonMap("", dataSource),
         Collections.singletonMap("", entityType),
         sourceId);
  }

  /**
   * Constructs a {@link RecordReader} with the specified {@link Format},
   * {@link Reader}, data source, entity type and source ID.
   *
   * @param format The expected format of the records.
   *
   * @param reader The {@link Reader} from which to read the text for the
   *               records.
   *
   * @param dataSource The data source to assign to each record.
   *
   * @param entityType The entity type to assign to each record.
   *
   * @param sourceId the source ID to assign to each record.
   *
   * @throws IOException If an I/O failure occurs.
   */
  public RecordReader(Format  format,
                      Reader  reader,
                      String  dataSource,
                      String  entityType,
                      String  sourceId)
      throws IOException
  {
    this(format,
         reader,
         Collections.singletonMap("", dataSource),
         Collections.singletonMap("", entityType),
         sourceId);
  }

  /**
   * Constructs a {@link RecordReader} with the specified {@link Reader},
   * data source code map and entity type code map.  The format of the reader
   * is inferred from the first character.
   *
   * @param reader The {@link Reader} from which to read the text for the
   *               records.
   *
   * @param dataSourceMap The map of original data source codes to replacement
   *                      data source codes.  The mapping from empty-string will
   *                      be used for any record that has no data source.  The
   *                      mapping from <tt>null</tt> will be for any data
   *                      source (including no data source) that has no key in
   *                      the map.
   *
   * @param entityTypeMap The map of original entity type codes to replacement
   *                      entity type codes.  The mapping from empty-string will
   *                      be used for any record that has no entity type.  The
   *                      mapping from <tt>null</tt> will be for any data
   *                      source (including no data source) that has no key in
   *                      the map.
   *
   * @throws IOException If an I/O failure occurs.
   */
  public RecordReader(Reader reader,
                      Map<String, String> dataSourceMap,
                      Map<String, String> entityTypeMap)
      throws IOException
  {
    this(null, reader, dataSourceMap, entityTypeMap, null);
  }

  /**
   * Constructs a {@link RecordReader} with the specified {@link Format},
   * {@link Reader}, data source code map, and entity type code map.
   *
   * @param format The expected format of the records.
   *
   * @param reader The {@link Reader} from which to read the text for the
   *               records.
   *
   * @param dataSourceMap The map of original data source names to replacement
   *                      data source name.  The mapping from empty-string will
   *                      be used for any record that has no data source or
   *                      whose data source is not in the map.
   *
   * @param entityTypeMap The map of original entity type codes to replacement
   *                      entity type codes.  The mapping from empty-string will
   *                      be used for any record that has no entity type or
   *                      whose entity type is not in the map.
   *
   * @throws IOException If an I/O failure occurs.
   */
  public RecordReader(Format              format,
                      Reader              reader,
                      Map<String, String> dataSourceMap,
                      Map<String, String> entityTypeMap)
      throws IOException
  {
    this(format, reader, dataSourceMap, entityTypeMap, null);
  }

  /**
   * Constructs a {@link RecordReader} with the specified {@link Reader},
   * data source code map, entity type code map and source ID.  The format of
   * the reader is inferred using the first character read.
   *
   * @param reader The {@link Reader} from which to read the text for the
   *               records.
   *
   * @param dataSourceMap The map of original data source names to replacement
   *                      data source name.  The mapping from empty-string will
   *                      be used for any record that has no data source or
   *                      whose data source is not in the map.
   *
   * @param entityTypeMap The map of original entity type codes to replacement
   *                      entity type codes.  The mapping from empty-string will
   *                      be used for any record that has no entity type or
   *                      whose entity type is not in the map.
   *
   * @param sourceId the source ID to assign to each record.
   *
   * @throws IOException If an I/O failure occurs.
   */
  public RecordReader(Reader              reader,
                      Map<String, String> dataSourceMap,
                      Map<String, String> entityTypeMap,
                      String              sourceId)
      throws IOException
  {
    this(null, reader, dataSourceMap, entityTypeMap, sourceId);
  }

  /**
   * Constructs a {@link RecordReader} with the specified {@link Format},
   * {@link Reader}, data source map, entity type map and source ID.
   * The format is explicitly specified by the first parameter.
   *
   * @param format The expected format of the records.
   *
   * @param reader The {@link Reader} from which to read the text for the
   *               records.
   *
   * @param dataSourceMap The map of original data source names to replacement
   *                      data source name.  The mapping from empty-string will
   *                      be used for any record that has no data source or
   *                      whose data source is not in the map.
   *
   * @param entityTypeMap The map of original entity type codes to replacement
   *                      entity type codes.  The mapping from empty-string will
   *                      be used for any record that has no entity type or
   *                      whose entity type is not in the map.
   *
   * @param sourceId the source ID to assign to each record.
   *
   * @throws IOException If an I/O failure occurs.
   */
  public RecordReader(Format              format,
                      Reader              reader,
                      Map<String, String> dataSourceMap,
                      Map<String, String> entityTypeMap,
                      String              sourceId)
      throws IOException
  {
    // set the format
    this.format = format;

    // if the format is unknown then try to infer it
    if (this.format == null) {
      // use a push-back reader so we can read the first non-whitespace
      // character and then push it back to read again later
      PushbackReader pushbackReader = new PushbackReader(reader);
      this.reader = pushbackReader;

      // read characters until the format is set or we hit EOF
      while (this.format == null) {
        // read the next character
        int nextChar = pushbackReader.read();

        // check for EOF
        if (nextChar < 0) break;

        // if whitespace then skip it
        if (Character.isWhitespace((char) nextChar)) continue;

        // if not whitespace then unread the character
        pushbackReader.unread(nextChar);

        // switch on the character to determine the format
        switch ((char) nextChar) {
          case '[':
            this.format = Format.JSON;
            break;
          case '{':
            this.format = Format.JSON_LINES;
            break;
          default:
            this.format = Format.CSV;
        }
      }

    } else {
      // just set the reader
      this.reader = reader;
    }

    switch (this.format) {
      case JSON:
        this.recordProvider = new JsonArrayRecordProvider(this.reader);
        break;
      case JSON_LINES:
        this.recordProvider = new JsonLinesRecordProvider(this.reader);
        break;
      case CSV:
        this.recordProvider = new CsvRecordProvider(this.reader);
        break;
      default:
        throw new IllegalStateException(
            "Unrecognized RecordReader.Format; " + this.format);
    }

    // initialize the data source map with upper-case keys
    this.dataSourceMap = (dataSourceMap == null) ? Collections.emptyMap()
        : new LinkedHashMap<>();
    try {
      if (dataSourceMap != null) {
        dataSourceMap.entrySet().forEach(entry -> {
          String key = entry.getKey();
          if (key != null) key = key.trim().toUpperCase();
          String value = entry.getValue().trim().toUpperCase();
          this.dataSourceMap.put(key, value);
        });
        this.dataSourceMap = Collections.unmodifiableMap(this.dataSourceMap);
      }

      // initialize the data source map with upper-case keys
      this.entityTypeMap = (entityTypeMap == null) ? Collections.emptyMap()
          : new LinkedHashMap<>();
      if (entityTypeMap != null) {
        entityTypeMap.entrySet().forEach(entry -> {
          String key = entry.getKey();
          if (key != null) key = key.trim().toUpperCase();
          String value = entry.getValue().trim().toUpperCase();
          this.entityTypeMap.put(key, value);
        });
        this.entityTypeMap = Collections.unmodifiableMap(this.entityTypeMap);
      }
    } catch (NullPointerException e) {
      System.err.println("DATA SOURCE MAP: " + dataSourceMap);
      System.err.println("ENTITY TYPE MAP: " + entityTypeMap);
      throw e;
    }
    this.sourceId = sourceId;
    if (this.sourceId != null) {
      this.sourceId = this.sourceId.trim();
      if (this.sourceId.length() == 0) {
        this.sourceId = null;
      }
    }
  }

  /**
   * Returns the {@link Format} of the records.
   *
   * @return The {@link Format} of the records.
   */
  public Format getFormat() {
    return this.format;
  }

  /**
   * Reads the next record and returns <tt>null</tt> if there are no more
   * records.
   *
   * @return The next record and returns <tt>null</tt> if there are no more
   *         records.
   */
  public JsonObject readRecord() {
    return this.recordProvider.getNextRecord();
  }

  /**
   * Gets the line number of an error after calling {@link #readRecord()}.
   * This returns <tt>null</tt> if there was no error after calling {@link
   * #readRecord()} and will return <tt>null</tt> if {@link #readRecord()}
   * has never been called.
   *
   * @return The line number associated with the error on the last attempt to
   *         get a record, or <tt>null</tt> if there was no error.
   */
  public Long getErrorLineNumber() {
    return this.recordProvider.getErrorLineNumber();
  }

  /**
   * A interface for providing records.
   */
  private interface RecordProvider {
    /**
     * Gets the next record as a {@link JsonObject}.
     * @return The next {@link JsonObject} record.
     */
    JsonObject getNextRecord();

    /**
     * Gets the line number of an error after calling {@link #getNextRecord()}.
     * This returns <tt>null</tt> if there was no error after calling {@link
     * #getNextRecord()} and will return <tt>null</tt> if {@link
     * #getNextRecord()} has never been called.
     *
     * @return The line number associated with the error on the last attempt to
     *         get a record, or <tt>null</tt> if there was no error.
     */
    Long getErrorLineNumber();
  }

  /**
   * Augments the specified record with <tt>"DATA_SOURCE"</tt>,
   * <tt>"ENTITY_TYPE"</tt> and <tt>"SOURCE_ID"</tt> as appropriate.
   *
   * @param record The {@link JsonObject} record to be updated.
   * @return The updated {@link JsonObject} record.
   */
  private JsonObject augmentRecord(JsonObject record)
  {
    if (record == null) return null;
    JsonObjectBuilder job = Json.createObjectBuilder(record);
    String dsrc = JsonUtils.getString(record, "DATA_SOURCE", "");
    dsrc = dsrc.trim().toUpperCase();
    String etype = JsonUtils.getString(record, "ENTITY_TYPE", "");
    etype = etype.trim().toUpperCase();

    // get the mapped data source
    String dataSource = this.dataSourceMap.get(dsrc);
    if (dataSource == null) dataSource = this.dataSourceMap.get(null);
    if (dataSource != null && dataSource.trim().length() == 0) {
      dataSource = null;
    }

    // get the mapped entity type
    String entityType = this.entityTypeMap.get(etype);
    if (entityType == null) entityType = this.entityTypeMap.get(null);
    if (entityType != null && entityType.trim().length() == 0) {
      entityType = null;
    }

    // remap the data source
    if (dataSource != null) {
      job.remove("DATA_SOURCE");
      job.add("DATA_SOURCE", dataSource);
      dsrc = dataSource;
    }

    // remap the entity type
    if (entityType != null) {
      job.remove("ENTITY_TYPE");
      job.add("ENTITY_TYPE", entityType);
      etype = entityType;
    }

    // set the source ID
    if (this.sourceId != null) {
      job.remove("SOURCE_ID");
      job.add("SOURCE_ID", this.sourceId);
    }

    // build the object
    return job.build();
  }

  /**
   * A {@link RecordProvider} implementation for records when reading
   * a JSON array.
   */
  private class JsonArrayRecordProvider implements RecordProvider
  {
    /**
     * Iterator over {@link JsonObject} records.
     */
    private Iterator<JsonObject> recordIter;

    /**
     * Indicates whether or not the JSON properly parses to avoid
     */
    private boolean errant = false;

    /**
     * The line number for the last error.
     */
    private Long errorLineNumber = null;

    /**
     * Constructor.
     */
    public JsonArrayRecordProvider(Reader reader) {
      JsonParserFactory jpf = Json.createParserFactory(Collections.emptyMap());
      JsonParser jp = jpf.createParser(reader);
      jp.next();
      this.recordIter = jp.getArrayStream()
          .map(jv -> (JsonObject) jv).iterator();
    }

    /**
     * Gets the next record from the JSON array.
     * @return The next {@link JsonObject} from the array.
     */
    public JsonObject getNextRecord() {
      RecordReader owner = RecordReader.this;
      JsonObject result = null;
      while (result == null) {
        try {
          if (!recordIter.hasNext()) break;
          result = owner.augmentRecord(this.recordIter.next());
          this.errant = false; // clear the errant flag

        } catch (Exception e) {
          if (this.errant) continue;
          if (e instanceof JsonParsingException) {
            JsonParsingException jpe = (JsonParsingException) e;
            System.out.println("LOCATION: " + jpe.getLocation());
            System.out.println("LINE NUMBER: " + jpe.getLocation().getLineNumber());
            this.errorLineNumber = jpe.getLocation().getLineNumber();
          }
          this.errant = true;
          throw e;
        }
      }
      return result;
    }

    @Override
    public Long getErrorLineNumber() {
      return this.errorLineNumber;
    }
  }

  /**
   * A {@link RecordProvider} implementation for records when reading
   * a files in a "JSON lines" format.
   */
  private class JsonLinesRecordProvider implements RecordProvider {
    /**
     * The backing {@link BufferedReader} for reading the lines from the file.
     */
    private BufferedReader reader;

    /**
     * The current line number.
     */
    private long lineNumber = 0;

    /**
     * The error line number if an error is found.
     */
    private Long errorLineNumber = null;

    /**
     * Default constructor.
     */
    public JsonLinesRecordProvider(Reader reader) {
      this.reader = new BufferedReader(reader);
    }

    /**
     * Implemented to get the next line from the file and parse it as
     * a {@link JsonObject} record.
     *
     * @return The next {@link JsonObject} record.
     */
    public JsonObject getNextRecord() {
      try {
        RecordReader  owner   = RecordReader.this;
        JsonObject    record  = null;

        while (this.reader != null && record == null) {
          // read the next line and check for EOF
          String line = this.reader.readLine();
          if (line == null) {
            this.reader.close();
            this.reader = null;
            continue;
          }
          this.lineNumber++;
          this.errorLineNumber = null; // clear the error line number

          // trim the line of extra whitespace
          line = line.trim();

          // check for blank lines and skip them
          if (line.length() == 0) continue;

          // check if the line begins with a "#" for a comment lines
          if (line.startsWith("#")) continue;

          // check if the line does NOT start with "{"
          if (!line.startsWith("{")) {
            throw new IllegalStateException(
                "Line does not appear to be JSON record: " + line);
          }

          // parse the line
          try {
            record = JsonUtils.parseJsonObject(line);

          } catch (JsonParsingException e) {
            this.errorLineNumber = this.lineNumber;
            throw e;
          }
        }

        return owner.augmentRecord(record);

      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public Long getErrorLineNumber() {
      return this.errorLineNumber;
    }
  }

  /**
   * Implements {@link RecordProvider} for a CSV file.
   *
   */
  private class CsvRecordProvider implements RecordProvider {
    /**
     * The CSV parser.
     */
    private CSVParser parser;

    /**
     * The record iterator.
     */
    private Iterator<CSVRecord> recordIter;

    /**
     * The line number for the last error.
     */
    private Long errorLineNumber = null;

    public CsvRecordProvider(Reader reader) {
      CSVFormat csvFormat = CSVFormat.DEFAULT
          .withFirstRecordAsHeader().withIgnoreEmptyLines(true).withTrim(true);

      try {
        this.parser = new CSVParser(reader, csvFormat);
        Map<String, Integer> headerMap = this.parser.getHeaderMap();
        Set<String> headers = new HashSet<>();
        headerMap.keySet().forEach(h -> {
          headers.add(h.toUpperCase());
        });
        this.recordIter = parser.iterator();

      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    public JsonObject getNextRecord() {
      RecordReader owner = RecordReader.this;
      this.errorLineNumber = null;
      try {
        if (!this.recordIter.hasNext()) return null;
        CSVRecord record = this.recordIter.next();
        Map<String,String> recordMap = record.toMap();
        Iterator<Map.Entry<String,String>> entryIter
            = recordMap.entrySet().iterator();
        while (entryIter.hasNext()) {
          Map.Entry<String,String> entry = entryIter.next();
          String value = entry.getValue();
          if (value == null || value.trim().length() == 0) {
            entryIter.remove();
          }
        }
        Map<String,Object> map = (Map) recordMap;

        JsonObject jsonObj = Json.createObjectBuilder(map).build();

        JsonObject result = owner.augmentRecord(jsonObj);

        return result;

      } catch (RuntimeException e) {
        this.errorLineNumber = this.parser.getCurrentLineNumber();
        throw e;

      } catch (Exception e) {
        this.errorLineNumber = this.parser.getCurrentLineNumber();
        throw new RuntimeException(e);
      }
    }

    @Override
    public Long getErrorLineNumber() {
      return this.errorLineNumber;
    }
  }

}
