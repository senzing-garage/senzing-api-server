package com.senzing.api.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.senzing.util.JsonUtils;

import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonString;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.function.Consumer;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_EMPTY;
import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;

/**
 * Describes an entity record.
 */
public class SzEntityRecord {
  /**
   * The pattern for parsing the date values returned from the native API.
   */
  private static final String NATIVE_DATE_PATTERN = "yyyy-MM-dd HH:mm:ss.SSS";

  /**
   * The time zone used for the time component of the build number.
   */
  private static final ZoneId UTC_ZONE = ZoneId.of("UTC");

  /**
   * The {@link DateTimeFormatter} for interpreting the timestamps from the
   * native API.
   */
  private static final DateTimeFormatter NATIVE_DATE_FORMATTER
      = DateTimeFormatter.ofPattern(NATIVE_DATE_PATTERN);

  /**
   * The data source code for the record.
   */
  private String dataSource;

  /**
   * The record ID for the record.
   */
  private String recordId;

  /**
   * The list of address data strings.
   */
  private List<String> addressData;

  /**
   * The list of characteristic data strings.
   */
  private List<String> characteristicData;

  /**
   * The list of identifier data strings.
   */
  private List<String> identifierData;

  /**
   * The list of name data strings.
   */
  private List<String> nameData;

  /**
   * The list of phone data strings.
   */
  private List<String> phoneData;

  /**
   * The list of relationship data strings.
   */
  private List<String> relationshipData;

  /**
   * The list of other data strings.
   */
  private List<String> otherData;

  /**
   * The object representing the original source data.
   */
  private Object originalSourceData;

  /**
   * The last seen timestamp.
   */
  @JsonFormat(shape   = JsonFormat.Shape.STRING,
      pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'",
      locale  = "en_GB")
  private Date lastSeenTimestamp;

  /**
   * Default constructor.
   */
  public SzEntityRecord() {
    this.dataSource         = null;
    this.recordId           = null;
    this.lastSeenTimestamp  = null;
    this.addressData        = new LinkedList<>();
    this.characteristicData = new LinkedList<>();
    this.identifierData     = new LinkedList<>();
    this.nameData           = new LinkedList<>();
    this.phoneData          = new LinkedList<>();
    this.relationshipData   = new LinkedList<>();
    this.otherData          = new LinkedList<>();
    this.originalSourceData = null;
  }

  /**
   * Gets the data source code for the record.
   *
   * @return The data source code for the record.
   */
  public String getDataSource()
  {
    return dataSource;
  }

  /**
   * Sets the data source code for the record.
   *
   * @param dataSource The data source code for the record.
   */
  public void setDataSource(String dataSource)
  {
    this.dataSource = dataSource;
  }

  /**
   * Gets the record ID for the record.
   *
   * @return The record ID for the record.
   */
  public String getRecordId()
  {
    return recordId;
  }

  /**
   * Sets the record ID for the record.
   *
   * @param recordId The record ID for the record.
   */
  public void setRecordId(String recordId)
  {
    this.recordId = recordId;
  }

  /**
   * Gets the last-seen timestamp for the entity.
   *
   * @return The last-seen timestamp for the entity.
   */
  @JsonInclude(NON_NULL)
  public Date getLastSeenTimestamp() {
    return this.lastSeenTimestamp;
  }

  /**
   * Sets the last-seen timestamp for the entity.
   *
   * @param timestamp The last-seen timestamp for the entity.
   */
  public void setLastSeenTimestamp(Date timestamp) {
    this.lastSeenTimestamp = timestamp;
  }

  /**
   * Returns the list of address data strings for the record.
   *
   * @return The list of address data strings for the record.
   */
  @JsonInclude(NON_EMPTY)
  public List<String> getAddressData() {
    return Collections.unmodifiableList(this.addressData);
  }

  /**
   * Sets the address data list for the record.
   *
   * @param addressData The list of address data strings.
   */
  public void setAddressData(List<String> addressData)
  {
    this.addressData.clear();
    if (addressData != null) {
      this.addressData.addAll(addressData);
    }
  }

  /**
   * Adds to the address data list for the record.
   *
   * @param addressData The address data string to add to the address data list.
   */
  public void addAddressData(String addressData)
  {
    this.addressData.add(addressData);
  }

  /**
   * Returns the list of characteristic data strings for the record.
   *
   * @return The list of characteristic data strings for the record.
   */
  @JsonInclude(NON_EMPTY)
  public List<String> getCharacteristicData() {
    return Collections.unmodifiableList(this.characteristicData);
  }

  /**
   * Sets the characteristic data list for the record.
   *
   * @param characteristicData The list of characteristic data strings.
   */
  public void setCharacteristicData(List<String> characteristicData)
  {
    this.characteristicData.clear();
    if (characteristicData != null) {
      this.characteristicData.addAll(characteristicData);
    }
  }

  /**
   * Adds to the characteristic data list for the record.
   *
   * @param characteristicData The characteristic data string to add to the address
   *                      data list.
   */
  public void addCharacteristicData(String characteristicData)
  {
    this.characteristicData.add(characteristicData);
  }

  /**
   * Returns the list of identifier data strings for the record.
   *
   * @return The list of identifier data strings for the record.
   */
  @JsonInclude(NON_EMPTY)
  public List<String> getIdentifierData() {
    return Collections.unmodifiableList(this.identifierData);
  }

  /**
   * Sets the identifier data list for the record.
   *
   * @param identifierData The list of identifier data strings.
   */
  public void setIdentifierData(List<String> identifierData)
  {
    this.identifierData.clear();
    if (identifierData != null) {
      this.identifierData.addAll(identifierData);
    }
  }

  /**
   * Adds to the identifier data list for the record.
   *
   * @param identifierData The identifier data string to add to the identifier
   *                       data list.
   */
  public void addIdentifierData(String identifierData)
  {
    this.identifierData.add(identifierData);
  }

  /**
   * Returns the list of name data strings for the record.
   *
   * @return The list of name data strings for the record.
   */
  @JsonInclude(NON_EMPTY)
  public List<String> getNameData() {
    return Collections.unmodifiableList(this.nameData);
  }

  /**
   * Sets the name data list for the record.
   *
   * @param nameData The list of name data strings.
   */
  public void setNameData(List<String> nameData) {
    this.nameData.clear();
    if (nameData != null) {
      this.nameData.addAll(nameData);
    }
  }

  /**
   * Adds to the name data list for the record.
   *
   * @param nameData The name data string to add to the name data list.
   */
  public void addNameData(String nameData)
  {
    this.nameData.add(nameData);
  }

  /**
   * Returns the list of phone data strings for the record.
   *
   * @return The list of phone data strings for the record.
   */
  @JsonInclude(NON_EMPTY)
  public List<String> getPhoneData() {
    return Collections.unmodifiableList(this.phoneData);
  }

  /**
   * Sets the phone data list for the record.
   *
   * @param phoneData The list of name data strings.
   */
  public void setPhoneData(List<String> phoneData) {
    this.phoneData.clear();
    if (phoneData != null) {
      this.phoneData.addAll(phoneData);
    }
  }

  /**
   * Adds to the phone data list for the record.
   *
   * @param phoneData The phone data string to add to the phone data list.
   */
  public void addPhoneData(String phoneData)
  {
    this.phoneData.add(phoneData);
  }

  /**
   * Returns the list of relationship data strings for the record.
   *
   * @return The list of relationship data strings for the record.
   */
  @JsonInclude(NON_EMPTY)
  public List<String> getRelationshipData() {
    return Collections.unmodifiableList(this.relationshipData);
  }

  /**
   * Sets the relationship data list for the record.
   *
   * @param relationshipData The list of relationship data strings.
   */
   public void setRelationshipData(List<String> relationshipData) {
     this.relationshipData.clear();
     if (relationshipData != null) {
       this.relationshipData.addAll(relationshipData);
     }
   }

   /**
   * Adds to the relationship data list for the record.
   *
   * @param relationshipData The relationship data string to add to the
   *                         relationship data list.
   */
  public void addRelationshipData(String relationshipData)
  {
    this.relationshipData.add(relationshipData);
  }

  /**
   * Returns the list of other data strings for the record.
   *
   * @return The list of other data strings for the record.
   */
  @JsonInclude(NON_EMPTY)
  public List<String> getOtherData() {
    return Collections.unmodifiableList(this.otherData);
  }

  /**
   * Sets the other data list for the record.
   *
   * @param otherData The list of other data strings.
   */
  public void setOtherData(List<String> otherData) {
    this.otherData.clear();
    if (otherData != null) {
      this.otherData.addAll(otherData);
    }
  }

  /**
   * Adds to the other data list for the record.
   *
   * @param otherData The other data string to add to the other data list.
   */
  public void addOtherData(String otherData)
  {
    this.otherData.add(otherData);
  }

  /**
   * Returns the original source data that was used to load the record.
   *
   * @return The original source data that was used to load the record.
   */
  @JsonInclude(NON_NULL)
  public Object getOriginalSourceData() {
    return originalSourceData;
  }

  /**
   * Sets the original source data using the specified object.
   *
   * @param jsonObject The object representation of the JSON for the
   *                   original source data.
   */
  public void setOriginalSourceData(Object jsonObject)
  {
    if (jsonObject != null && jsonObject instanceof String) {
      this.setOriginalSourceDataFromText((String) jsonObject);
    } else {
      this.originalSourceData = jsonObject;
    }
  }

  /**
   * Sets the original source data using the specified JSON text.
   *
   * @param jsonText The JSON text for the original source data.
   */
  public void setOriginalSourceDataFromText(String jsonText)
  {
    this.originalSourceData = JsonUtils.normalizeJsonText(jsonText);
  }


  /**
   * Parses the native JSON to construct/populate a {@link List}
   * of {@link SzEntityRecord} instances.
   *
   * @param list The {@link List} to populate or <tt>null</tt> if a new {@link
   *             List} should be created.
   * @param jsonArray The {@link JsonArray} describing the native JSON array.
   *
   * @return The specified (or constructed) {@link List} of {@link
   *         SzEntityRecord} instances.
   */
  public static List<SzEntityRecord> parseEntityRecordList(
      List<SzEntityRecord>  list,
      JsonArray             jsonArray)
  {
    for (JsonObject jsonObject : jsonArray.getValuesAs(JsonObject.class)) {
      if (list == null) {
        list = new ArrayList<SzEntityRecord>(jsonArray.size());
      }
      list.add(parseEntityRecord(null, jsonObject));
    }
    if (list != null) {
      list = Collections.unmodifiableList(list);
    } else {
      list = Collections.emptyList();
    }
    return list;
  }

  /**
   * Private method to get the various *Data fields for the record.
   *
   * @param jsonObject The {@link JsonObject} to pull from.
   * @param key The key for the attribute of the JSON object.
   * @return The {@link List} of strings.
   */
  private static void getValueList(JsonObject       jsonObject,
                                   String           key,
                                   Consumer<String> consumer)
  {
    JsonArray jsonArray = jsonObject.getJsonArray(key);
    if (jsonArray == null) return;
    for (JsonString value : jsonArray.getValuesAs(JsonString.class)) {
      consumer.accept(value.getString());
    }
  }

  /**
   * Parses the native API JSON to build an populate or create an instance of
   * {@link SzEntityRecord}.
   *
   * @param record The {@link SzEntityRecord} to populate or <tt>null</tt> if
   *               a new instance should be created.
   *
   * @param jsonObject The {@link JsonObject} describing the record using the
   *                   native API JSON format.
   *
   * @return The populated (or created) instance of {@link SzEntityRecord}
   */
  public static SzEntityRecord parseEntityRecord(SzEntityRecord record,
                                                 JsonObject     jsonObject)
  {
    if (record == null) record = new SzEntityRecord();
    final SzEntityRecord rec = record;

    // get the data source and record ID
    String dataSource = jsonObject.getString("DATA_SOURCE");
    String recordId   = jsonObject.getString("RECORD_ID");

    record.setDataSource(dataSource);
    record.setRecordId(recordId);


    // get the last seen date
    String lastSeen = JsonUtils.getString(jsonObject, "LAST_SEEN_DT");
    Date lastSeenDate = null;
    if (lastSeen != null && lastSeen.trim().length() > 0) {
      LocalDateTime localDateTime
          = LocalDateTime.parse(lastSeen, NATIVE_DATE_FORMATTER);
      ZonedDateTime zonedDateTime = ZonedDateTime.of(localDateTime, UTC_ZONE);
      lastSeenDate = Date.from(zonedDateTime.toInstant());
    }

    // get the raw data map
    JsonObject  jsonData    = jsonObject.getJsonObject("JSON_DATA");
    String      sourceData  = JsonUtils.toJsonText(jsonData);

    record.setLastSeenTimestamp(lastSeenDate);
    record.setOriginalSourceDataFromText(sourceData);

    getValueList(jsonObject, "ADDRESS_DATA", (addr) -> {
      rec.addAddressData(addr);
    });

    getValueList(jsonObject, "ATTRIBUTE_DATA", (attr) -> {
      rec.addCharacteristicData(attr);
    });

    getValueList(jsonObject, "IDENTIFIER_DATA", (ident) -> {
      rec.addIdentifierData(ident);
    });

    getValueList(jsonObject, "NAME_DATA", (name) -> {
      rec.addNameData(name);
    });

    getValueList(jsonObject, "PHONE_DATA", (phone) -> {
      rec.addPhoneData(phone);
    });

    getValueList(jsonObject, "RELATIONSHIP_DATA", (rel) -> {
      rec.addRelationshipData(rel);
    });

    getValueList(jsonObject, "OTHER_DATA", (other) -> {
      rec.addOtherData(other);
    });

    return record;
  }

  @Override
  public String toString() {
    return "SzEntityRecord{" + this.fieldsToString() + "}";
  }

  protected String fieldsToString() {
    return "dataSource='" + dataSource + '\'' +
        ", recordId='" + recordId + '\'' +
        ", lastSeenTimestamp=" + lastSeenTimestamp +
        ", addressData=" + addressData +
        ", characteristicData=" + characteristicData +
        ", identifierData=" + identifierData +
        ", nameData=" + nameData +
        ", phoneData=" + phoneData +
        ", relationshipData=" + relationshipData +
        ", otherData=" + otherData +
        ", originalSourceData=" + originalSourceData;
  }
}
