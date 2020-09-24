package com.senzing.api.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.senzing.util.JsonUtils;

import javax.json.JsonArray;
import javax.json.JsonObject;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.function.Function;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_EMPTY;
import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;

public class SzResolvedEntity {
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
   * The number of records to consider to be the top records.
   */
  private static final int TOP_COUNT = 10;

  /**
   * The entity ID.
   */
  private Long entityId;

  /**
   * The assigned name to the entity.
   */
  private String entityName;

  /**
   * The best name for the entity.
   */
  private String bestName;

  /**
   * The {@link List} of {@link SzDataSourceRecordSummary} instances.
   */
  private List<SzDataSourceRecordSummary> recordSummaries;

  /**
   * The list of address data strings.
   */
  private List<String> addressData;

  /**
   * The list of attribute data strings.
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
   * The {@link Map} of features.
   */
  private Map<String, List<SzEntityFeature>> features;

  /**
   * The {@link Map} of unmodifiable features.
   */
  private Map<String, List<SzEntityFeature>> unmodifiableFeatures;

  /**
   * The {@link List} of {@link SzMatchedRecord} instances for the
   * records in the entity.
   */
  private List<SzMatchedRecord> records;

  /**
   * Whether or not this entity is partially populated.
   */
  private boolean partial;

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
  public SzResolvedEntity() {
    this.entityId             = null;
    this.entityName           = null;
    this.bestName             = null;
    this.recordSummaries      = new LinkedList<>();
    this.addressData          = new LinkedList<>();
    this.characteristicData   = new LinkedList<>();
    this.identifierData       = new LinkedList<>();
    this.nameData             = new LinkedList<>();
    this.phoneData            = new LinkedList<>();
    this.relationshipData     = new LinkedList<>();
    this.otherData            = new LinkedList<>();
    this.features             = new LinkedHashMap<>();
    this.unmodifiableFeatures = new LinkedHashMap<>();
    this.records              = new LinkedList<>();
    this.lastSeenTimestamp    = null;
    this.partial              = true;
  }

  /**
   * Gets the entity ID for the entity.
   *
   * @return The entity ID for the entity.
   */
  public Long getEntityId() {
    return entityId;
  }

  /**
   * Sets the entity ID for the entity.
   *
   * @param entityId The entity ID for the entity.
   */
  public void setEntityId(Long entityId) {
    this.entityId = entityId;
  }

  /**
   * Gets the entity name.  This is usually the same as the {@link
   * #getBestName() best name} except in the case of search results
   * where it may differ.
   *
   * @return The highest fidelity name for the entity.
   */
  @JsonInclude(NON_NULL)
  public String getEntityName() {
    return this.entityName;
  }

  /**
   * Sets the entity name.  This is usually the same as the {@link
   * #getBestName() best name} except in the case of search results
   * where it may differ.
   *
   * @param entityName The highest fidelity name for the entity.
   */
  public void setEntityName(String entityName) {
    this.entityName = entityName;
  }

  /**
   * Gets the best name.  This is usually the same as the {@link
   * #getEntityName() entity name} except in the case of search results
   * where it may differ.
   *
   * @return The best name for the entity.
   */
  @JsonInclude(NON_NULL)
  public String getBestName() {
    return this.bestName;
  }

  /**
   * Sets the best name.  This is usually the same as the {@link
   * #getEntityName() entity name} except in the case of search results
   * where it may differ.
   *
   * @param bestName The best name for the entity.
   */
  public void setBestName(String bestName) {
    this.bestName = bestName;
  }

  /**
   * Returns the list of {@link SzMatchedRecord} instances describing the records
   * for the entity.
   *
   * @return The list of {@link SzMatchedRecord} instances describing the records
   *         for the entity.
   */
  @JsonInclude(NON_EMPTY)
  public List<SzMatchedRecord> getRecords() {
    return Collections.unmodifiableList(this.records);
  }

  /**
   * Sets the list {@linkplain SzMatchedRecord records} for the entity.
   *
   * @param records The list {@linkplain SzMatchedRecord records} for the entity.
   */
  public void setRecords(List<SzMatchedRecord> records) {
    this.records.clear();
    if (records != null) {
      this.records.addAll(records);

      // recalculate the "other data"
      this.otherData.clear();
      Set<String> set = new LinkedHashSet<>();
      for (SzMatchedRecord record : records) {
        List<String> recordOtherData = record.getOtherData();
        if (recordOtherData != null) {
          for (String data : recordOtherData) {
            set.add(data);
          }
        }
      }
      this.otherData.addAll(set);
    }
  }

  /**
   * Adds the specified {@link SzMatchedRecord} to the list of {@linkplain
   * SzMatchedRecord records}.
   *
   * @param record The {@link SzMatchedRecord} to add to the record list.
   */
  public void addRecord(SzMatchedRecord record)
  {
    this.records.add(record);
    List<String> recordOtherData = record.getOtherData();
    if (recordOtherData != null) {
      for (String data: recordOtherData) {
        if (! this.otherData.contains(data)) {
          this.otherData.add(data);
        }
      }
    }
  }


  /**
   * Returns the list of {@link SzDataSourceRecordSummary} instances for the entity.
   *
   * @return The list of {@link SzDataSourceRecordSummary} instances for the entity.
   */
  @JsonInclude(NON_EMPTY)
  public List<SzDataSourceRecordSummary> getRecordSummaries() {
    return Collections.unmodifiableList(this.recordSummaries);
  }

  /**
   * Sets the list {@link SzDataSourceRecordSummary record summaries} for the entity.
   *
   * @param summaries The list {@link SzDataSourceRecordSummary record summaries}
   *                  for the entity.
   */
  public void setRecordSummaries(List<SzDataSourceRecordSummary> summaries) {
    this.recordSummaries.clear();
    if (summaries != null) {
      this.recordSummaries.addAll(summaries);
    }
  }

  /**
   * Adds the specified {@link SzDataSourceRecordSummary} to the list of associated
   * {@linkplain SzDataSourceRecordSummary record summaries}.
   *
   * @param summary The {@link SzDataSourceRecordSummary} to add to the record summaries.
   */
  public void addRecordSummary(SzDataSourceRecordSummary summary)
  {
    this.recordSummaries.add(summary);
  }

  /**
   * Returns the list of address data strings for the entity.
   *
   * @return The list of address data strings for the entity.
   */
  @JsonInclude(NON_EMPTY)
  public List<String> getAddressData() {
    return Collections.unmodifiableList(this.addressData);
  }

  /**
   * Sets the address data list for the entity.
   *
   * @param addressData The list of address data strings.
   */
  public void setAddressData(List<String> addressData) {
    this.addressData.clear();
    if (addressData != null) {
      this.addressData.addAll(addressData);
    }
  }

  /**
   * Adds to the address data list for the entity.
   *
   * @param addressData The address data string to add to the address data list.
   */
  public void addAddressData(String addressData) {
    this.addressData.add(addressData);
  }

  /**
   * Returns the list of attribute data strings for the entity.
   *
   * @return The list of attribute data strings for the entity.
   */
  @JsonInclude(NON_EMPTY)
  public List<String> getCharacteristicData() {
    return Collections.unmodifiableList(this.characteristicData);
  }

  /**
   * Sets the attribute data list for the entity.
   *
   * @param characteristicData The list of attribute data strings.
   */
  public void setCharacteristicData(List<String> characteristicData) {
    this.characteristicData.clear();
    if (characteristicData != null) {
      this.characteristicData.addAll(characteristicData);
    }
  }

  /**
   * Adds to the attribute data list for the entity.
   *
   * @param attributeData The attribute data string to add to the address
   *                      data list.
   */
  public void addCharacteristicData(String attributeData) {
    this.characteristicData.add(attributeData);
  }

  /**
   * Returns the list of identifier data strings for the entity.
   *
   * @return The list of identifier data strings for the entity.
   */
  @JsonInclude(NON_EMPTY)
  public List<String> getIdentifierData() {
    return Collections.unmodifiableList(this.identifierData);
  }

  /**
   * Sets the identifier data list for the entity.
   *
   * @param identifierData The list of identifier data strings.
   */
  public void setIdentifierData(List<String> identifierData) {
    this.identifierData.clear();
    if (identifierData != null) {
      this.identifierData.addAll(identifierData);
    }
  }

  /**
   * Adds to the identifier data list for the entity.
   *
   * @param identifierData The identifier data string to add to the identifier
   *                       data list.
   */
  public void addIdentifierData(String identifierData) {
    this.identifierData.add(identifierData);
  }

  /**
   * Returns the list of name data strings for the entity.
   *
   * @return The list of name data strings for the entity.
   */
  @JsonInclude(NON_EMPTY)
  public List<String> getNameData() {
    return Collections.unmodifiableList(this.nameData);
  }

  /**
   * Sets the name data list for the entity.
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
   * Adds to the name data list for the entity.
   *
   * @param nameData The name data string to add to the name data list.
   */
  public void addNameData(String nameData)
  {
    this.nameData.add(nameData);
  }

  /**
   * Returns the list of phone data strings for the entity.
   *
   * @return The list of phone data strings for the entity.
   */
  @JsonInclude(NON_EMPTY)
  public List<String> getPhoneData() {
    return Collections.unmodifiableList(this.phoneData);
  }

  /**
   * Sets the phone data list for the entity.
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
   * Adds to the phone data list for the entity.
   *
   * @param phoneData The phone data string to add to the phone data list.
   */
  public void addPhoneData(String phoneData)
  {
    this.phoneData.add(phoneData);
  }

  /**
   * Returns the list of relationship data strings for the entity.
   *
   * @return The list of relationship data strings for the entity.
   */
  @JsonInclude(NON_EMPTY)
  public List<String> getRelationshipData() {
    return Collections.unmodifiableList(this.relationshipData);
  }

  /**
   * Sets the relationship data list for the entity.
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
   * Adds to the relationship data list for the entity.
   *
   * @param relationshipData The relationship data string to add to the
   *                         relationship data list.
   */
  public void addRelationshipData(String relationshipData)
  {
    this.relationshipData.add(relationshipData);
  }

  /**
   * Returns the list of other data strings for the entity.
   *
   * @return The list of other data strings for the entity.
   */
  @JsonInclude(NON_EMPTY)
  public List<String> getOtherData() {
    return Collections.unmodifiableList(this.otherData);
  }

  /**
   * Sets the other data list for the entity.
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
   * Returns an <b>unmodifiable</b> {@link Map} of {@link String} feature
   * names to {@link SzEntityFeature} instances describing those features.
   *
   * @return As <b>unmodifiable</b> {@link Map} of {@link String} feature
   *         names to {@link SzEntityFeature} instances describing those
   *         features.
   */
  @JsonInclude(NON_EMPTY)
  public Map<String, List<SzEntityFeature>> getFeatures() {
    return Collections.unmodifiableMap(this.unmodifiableFeatures);
  }

  /**
   * Private setter to help with JSON serialization and deserialization.
   *
   * @param featureMap The {@link Map} of features.
   */
  private void setFeatures(Map<String, List<SzEntityFeature>> featureMap) {
    this.features.clear();
    this.unmodifiableFeatures.clear();

    if (featureMap != null) {
      featureMap.entrySet().forEach(entry -> {
        String                featureName = entry.getKey();
        List<SzEntityFeature> list        = entry.getValue();
        List<SzEntityFeature> copiedList  = new ArrayList<>(list);

        List<SzEntityFeature> unmodifiableList
            = Collections.unmodifiableList(copiedList);

        this.features.put(featureName, copiedList);
        this.unmodifiableFeatures.put(featureName, unmodifiableList);
      });
    }
  }

  /**
   * Sets the features using the specified {@link Map}.
   *
   * @param featureMap The {@link Map} of features.
   */
  public void setFeatures(
      Map<String, List<SzEntityFeature>>  featureMap,
      Function<String,String>             featureToAttrClassMapper)
  {
    this.setFeatures(featureMap);

    // clear out the data lists
    this.addressData.clear();
    this.characteristicData.clear();
    this.identifierData.clear();
    this.nameData.clear();
    this.phoneData.clear();

    if (featureMap == null) return;

    Function<String,String> mapper = featureToAttrClassMapper;
    getDataFields("NAME", featureMap, mapper).forEach((name) -> {
      this.addNameData(name);
    });

    getDataFields("ATTRIBUTE", featureMap, mapper).forEach((attr) -> {
      this.addCharacteristicData(attr);
    });

    getDataFields("ADDRESS", featureMap, mapper).forEach((addr) -> {
      this.addAddressData(addr);
    });

    getDataFields("PHONE", featureMap, mapper).forEach((phone) -> {
      this.addPhoneData(phone);
    });

    getDataFields("IDENTIFIER", featureMap, mapper).forEach((ident) -> {
      this.addIdentifierData(ident);
    });

    getDataFields("RELATIONSHIP", featureMap, mapper).forEach((rel) -> {
      this.addRelationshipData(rel);
    });
  }

  /**
   * Sets the specified feature with the specified feature name to the
   * {@link List} of {@link SzEntityFeature} instances.
   *
   * @param featureName The name of the feature.
   *
   * @param values The {@link List} of {@link SzEntityFeature} instances
   *               describing the feature values.
   */
  public void setFeature(String featureName, List<SzEntityFeature> values)
  {
    List<SzEntityFeature> featureValues = this.features.get(featureName);

    if (featureValues != null && (values == null || values.size() == 0)) {
      // feature exists, so remove the feature since no specified values
      this.features.remove(featureName);

    } else if (featureValues != null) {
      // feature exists, but are being replaced
      featureValues.clear();
      featureValues.addAll(values);

    } else if (values != null && values.size() > 0) {
      // the feature does not exist but new values are being added
      featureValues = new LinkedList<>();
      featureValues.addAll(values);
      this.features.put(featureName, featureValues);

      List<SzEntityFeature> unmodifiableFeatureValues
          = Collections.unmodifiableList(featureValues);

      this.unmodifiableFeatures.put(featureName, unmodifiableFeatureValues);
    }
  }

  /**
   * Adds a {@link SzEntityFeature} value to the feature with the specified
   * feature name.
   *
   * @param featureName The name of the feature.
   *
   * @param value The {@link SzEntityFeature} describing the feature value.
   */
  public void addFeature(String featureName, SzEntityFeature value)
  {
    if (value == null) return;
    List<SzEntityFeature> featureValues = this.features.get(featureName);
    if (featureValues == null) {
      featureValues = new LinkedList<>();

      List<SzEntityFeature> unmodifiableFeatureValues
          = Collections.unmodifiableList(featureValues);

      this.features.put(featureName, featureValues);
      this.unmodifiableFeatures.put(featureName, unmodifiableFeatureValues);
    }
    featureValues.add(value);
  }

  /**
   * Checks whether or not the entity data is only partially populated.
   * If partially populated then it will not have complete features or records
   * and the record summaries may be missing the top record IDs.
   *
   * @return <tt>true</tt> if the entity data is only partially
   *         populated, otherwise <tt>false</tt>.
   */
  public boolean isPartial() {
    return this.partial;
  }

  /**
   * Sets whether or not the entity data is only partially populated.
   * If partially populated then it will not have complete features or records
   * and the record summaries may be missing the top record IDs.
   *
   * @param partial <tt>true</tt> if the entity data is only partially
   *                populated, otherwise <tt>false</tt>.
   */
  public void setPartial(boolean partial) {
    this.partial = partial;
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
   * Parses a list of resolved entities from a {@link JsonArray} describing a
   * JSON array in the Senzing native API format for entity features and
   * populates the specified {@link List} or creates a new {@link List}.
   *
   * @param list The {@link List} of {@link SzResolvedEntity} instances to
   *             populate, or <tt>null</tt> if a new {@link List}
   *             should be created.
   *
   * @param jsonArray The {@link JsonArray} describing the JSON in the
   *                  Senzing native API format.
   *
   * @param featureToAttrClassMapper Mapping function to map feature names to
   *                                 attribute classes.
   *
   * @return The populated (or created) {@link List} of {@link
   *         SzResolvedEntity} instances.
   */
  public static List<SzResolvedEntity> parseResolvedEntityList(
      List<SzResolvedEntity>  list,
      JsonArray               jsonArray,
      Function<String,String> featureToAttrClassMapper)
  {
    for (JsonObject jsonObject : jsonArray.getValuesAs(JsonObject.class)) {

      if (list == null) {
        list = new ArrayList<>(jsonArray.size());
      }

      list.add(parseResolvedEntity(null,
                                   jsonObject,
                                   featureToAttrClassMapper));
    }
    if (list != null) {
      list = Collections.unmodifiableList(list);
    } else {
      list = Collections.emptyList();
    }
    return list;
  }

  /**
   * Parses the resolved entity from a {@link JsonObject} describing JSON
   * for the Senzing native API format for a resolved entity and populates
   * the specified {@link SzResolvedEntity} or creates a new instance.
   *
   * @param entity The {@link SzResolvedEntity} instance to populate, or
   *               <tt>null</tt> if a new instance should be created.
   *
   * @param jsonObject The {@link JsonObject} describing the JSON in the
   *                   Senzing native API format.
   *
   * @param featureToAttrClassMapper Mapping function to map feature names to
   *                                 attribute classes.
   *
   * @return The populated (or created) {@link SzResolvedEntity}.
   */
  public static SzResolvedEntity parseResolvedEntity(
      SzResolvedEntity        entity,
      JsonObject              jsonObject,
      Function<String,String> featureToAttrClassMapper)
  {
    if (entity == null) entity = new SzResolvedEntity();

    long entityId     = jsonObject.getJsonNumber("ENTITY_ID").longValue();
    String entityName = JsonUtils.getString(jsonObject, "ENTITY_NAME");

    Map<String,List<SzEntityFeature>> featureMap = null;

    boolean partial = (!jsonObject.containsKey("FEATURES")
                      || !jsonObject.containsKey("RECORDS"));

    if (jsonObject.containsKey("FEATURES")) {
      JsonObject features = jsonObject.getJsonObject("FEATURES");
      for (String key : features.keySet()) {
        JsonArray jsonArray = features.getJsonArray(key);
        List<SzEntityFeature> featureValues
            = SzEntityFeature.parseEntityFeatureList(null, jsonArray);
        if (featureMap == null) {
          featureMap = new LinkedHashMap<>();
        }
        featureMap.put(key, featureValues);
      }

      if (featureMap != null) {
        featureMap = Collections.unmodifiableMap(featureMap);
      }
    }

    // get the records
    List<SzMatchedRecord> recordList = null;
    List<SzDataSourceRecordSummary> summaries = null;

    if (jsonObject.containsKey("RECORDS")) {
      JsonArray records = jsonObject.getJsonArray("RECORDS");
      recordList = SzMatchedRecord.parseMatchedRecordList(null, records);
      summaries = summarizeRecords(recordList);

    } else if (jsonObject.containsKey("RECORD_SUMMARY")) {
      JsonArray jsonArray = jsonObject.getJsonArray("RECORD_SUMMARY");
      summaries = SzDataSourceRecordSummary.parseRecordSummaryList(null, jsonArray);

    }

    // get the last seen date
    String lastSeen = JsonUtils.getString(jsonObject, "LAST_SEEN_DT");
    Date lastSeenDate = null;
    if (lastSeen != null && lastSeen.trim().length() > 0) {
      LocalDateTime localDateTime
          = LocalDateTime.parse(lastSeen, NATIVE_DATE_FORMATTER);
      ZonedDateTime zonedDateTime = ZonedDateTime.of(localDateTime, UTC_ZONE);
      lastSeenDate = Date.from(zonedDateTime.toInstant());
    }

    entity.setLastSeenTimestamp(lastSeenDate);
    entity.setEntityName(entityName);
    entity.setEntityId(entityId);
    entity.setFeatures(featureMap, featureToAttrClassMapper);
    entity.setRecords(recordList);
    entity.setRecordSummaries(summaries);
    entity.setPartial(partial);

    // iterate over the feature map
    return entity;
  }

  /**
   * Summarizes the specified {@link List} of {@linkplain SzMatchedRecord
   * records} and produces a {@link List} of {@link SzDataSourceRecordSummary} instances.
   *
   * @param records The records to be summarized.
   * @return The {@link List} of {@link SzDataSourceRecordSummary} instances describing
   *         the summaries.
   */
  public static List<SzDataSourceRecordSummary> summarizeRecords(
      List<SzMatchedRecord>  records)
  {
    // check if we have no records
    if (records.size() == 0) return Collections.emptyList();

    // calculate the result by accumulating records by data source
    Map<String, List<String>> map = new LinkedHashMap<>();

    // for each record....
    records.stream().forEach(record -> {
      // get the data source and record ID
      String dataSource = record.getDataSource();
      String recordId = record.getRecordId();

      // check if we already have a list of record IDs for this data source
      List<String> list = map.get(dataSource);
      if (list == null) {
        // if not, then create the list and keep it
        list = new LinkedList<>();
        map.put(dataSource, list);
      }
      // add to the list
      list.add(recordId);
    });

    // construct the result list
    final List<SzDataSourceRecordSummary> tempList = new ArrayList<>(map.size());

    // for each entry in the map....
    map.entrySet().stream().forEach(entry -> {
      // get the data source and record ID's
      String        dataSource  = entry.getKey();
      List<String>  recordIds   = entry.getValue();
      int           recordCount = recordIds.size();

      // sort the record ID's to ensure consistent responses
      Collections.sort(recordIds);

      // check if we have lots of record ID's (more than TOP_COUNT)
      if (recordIds.size() > TOP_COUNT) {
        // if so, truncate the list of "top record ID's"
        recordIds = new ArrayList<>(recordIds.subList(0, TOP_COUNT));
      }

      recordIds = Collections.unmodifiableList(recordIds);

      // create a new record summary
      SzDataSourceRecordSummary summary = new SzDataSourceRecordSummary();
      summary.setDataSource(dataSource);
      summary.setRecordCount(recordCount);
      summary.setTopRecordIds(recordIds);

      tempList.add(summary);
    });

    Collections.sort(
        tempList, Comparator.comparing(SzDataSourceRecordSummary::getDataSource));

    List<SzDataSourceRecordSummary> result = Collections.unmodifiableList(tempList);

    return result;
  }

  /**
   * Utility method to get the "data values" from the features.
   *
   * @param attrClass The attribute class for which to pull the values.
   *
   * @param featureMap The {@link Map} of features.
   *
   * @param featureToAttrClassMapper Mapping function to map feature names to
   *                                 attribute classes.
   *
   * @return The {@link List} of {@link String} values.
   */
  private static List<String> getDataFields(
      String                              attrClass,
      Map<String, List<SzEntityFeature>>  featureMap,
      Function<String,String>             featureToAttrClassMapper)
  {
    List<String> dataList = new LinkedList<>();
    List<String> result   = Collections.unmodifiableList(dataList);

    featureMap.entrySet().forEach(entry -> {
      String ftypeCode = entry.getKey();
      List<SzEntityFeature> values = entry.getValue();

      String ac = featureToAttrClassMapper.apply(ftypeCode);

      if (ac == null) return;
      if (!ac.equalsIgnoreCase(attrClass)) return;

      String prefix = (attrClass.equalsIgnoreCase(ftypeCode)
          ? "" : ftypeCode + ": ");

      boolean relLink = ftypeCode.equalsIgnoreCase("REL_LINK");
      values.forEach(val -> {
        String usageType = val.getUsageType();
        if (usageType != null && usageType.length() > 0) {
          usageType = usageType.trim()
              + (!relLink && prefix.endsWith(": ") ? " " : ": ");
        } else {
          usageType = "";
        }

        String dataValue = (relLink)
            ? prefix + usageType + val.getPrimaryValue()
            : usageType + prefix + val.getPrimaryValue();

        // add the value to the list
        dataList.add(dataValue);
      });
    });

    Collections.sort(dataList, (v1, v2) -> {
      if (v1.startsWith("PRIMARY") && !v2.startsWith("PRIMARY")) {
        return -1;
      } else if (v2.startsWith("PRIMARY") && !v1.startsWith("PRIMARY")) {
        return 1;
      }
      int comp = v2.length() - v1.length();
      if (comp != 0) return comp;
      return v1.compareTo(v2);
    });

    return result;
  }

  @Override
  public String toString() {
    return "SzResolvedEntity{" +
        "entityId=" + entityId +
        ", partial=" + partial +
        ", entityName='" + entityName + '\'' +
        ", bestName='" + bestName + '\'' +
        ", recordSummaries=" + recordSummaries +
        ", lastSeenTimestamp=" + lastSeenTimestamp +
        ", addressData=" + addressData +
        ", attributeData=" + characteristicData +
        ", identifierData=" + identifierData +
        ", nameData=" + nameData +
        ", phoneData=" + phoneData +
        ", relationshipData=" + relationshipData +
        ", otherData=" + otherData +
        ", features=" + features +
        ", records=" + records +
        '}';
  }
}
