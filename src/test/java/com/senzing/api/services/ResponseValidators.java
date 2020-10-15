package com.senzing.api.services;

import com.senzing.api.BuildInfo;
import com.senzing.api.model.*;
import com.senzing.nativeapi.NativeApiFactory;
import com.senzing.g2.engine.G2Product;
import com.senzing.util.JsonUtils;

import javax.json.JsonObject;
import java.util.*;

import static com.senzing.api.BuildInfo.MAVEN_VERSION;
import static com.senzing.api.BuildInfo.REST_API_VERSION;
import static com.senzing.api.model.SzFeatureMode.NONE;
import static com.senzing.api.model.SzFeatureMode.REPRESENTATIVE;
import static com.senzing.api.model.SzRelationshipMode.*;
import static com.senzing.api.model.SzHttpMethod.GET;
import static org.junit.jupiter.api.Assertions.*;

public class ResponseValidators {
  /**
   * Validates the basic response fields for the specified {@link
   * SzBasicResponse} using the specified self link, timestamp from before
   * calling the service function and timestamp from after calling the
   * service function.
   *
   * @param response        The {@link SzBasicResponse} to validate.
   * @param selfLink        The self link to be expected.
   * @param beforeTimestamp The timestamp from just before calling the service.
   * @param afterTimestamp  The timestamp from just after calling the service.
   */
  public static void validateBasics(SzBasicResponse response,
                                    String          selfLink,
                                    long            beforeTimestamp,
                                    long            afterTimestamp)
  {
    validateBasics(
        null, response, GET, selfLink, beforeTimestamp, afterTimestamp);
  }

  /**
   * Validates the basic response fields for the specified {@link
   * SzBasicResponse} using the specified self link, timestamp from before
   * calling the service function and timestamp from after calling the
   * service function.
   *
   * @param testInfo        Additional test information to be logged with failures.
   * @param response        The {@link SzBasicResponse} to validate.
   * @param selfLink        The self link to be expected.
   * @param beforeTimestamp The timestamp from just before calling the service.
   * @param afterTimestamp  The timestamp from just after calling the service.
   */
  public static void validateBasics(String          testInfo,
                                    SzBasicResponse response,
                                    String          selfLink,
                                    long            beforeTimestamp,
                                    long            afterTimestamp)
  {
    validateBasics(
        testInfo, response, GET, selfLink, beforeTimestamp, afterTimestamp);
  }

  /**
   * Validates the basic response fields for the specified {@link
   * SzBasicResponse} using the specified self link, timestamp from before
   * calling the service function and timestamp from after calling the
   * service function.
   *
   * @param response           The {@link SzBasicResponse} to validate.
   * @param expectedHttpMethod The {@link SzHttpMethod} that was used.
   * @param selfLink           The self link to be expected.
   * @param beforeTimestamp    The timestamp from just before calling the service.
   * @param afterTimestamp     The timestamp from just after calling the service.
   */
  public static void validateBasics(SzBasicResponse response,
                                    SzHttpMethod    expectedHttpMethod,
                                    String          selfLink,
                                    long            beforeTimestamp,
                                    long            afterTimestamp)
  {
    validateBasics(null,
                        response,
                        expectedHttpMethod,
                        selfLink,
                        beforeTimestamp,
                        afterTimestamp);
  }

  /**
   * Validates the basic response fields for the specified {@link
   * SzBasicResponse} using the specified self link, timestamp from before
   * calling the service function and timestamp from after calling the
   * service function.
   *
   * @param response The {@link SzBasicResponse} to validate.
   * @param expectedResponseCode The expected HTTP response code.
   * @param expectedHttpMethod The {@link SzHttpMethod} that was used.
   * @param selfLink The self link to be expected.
   * @param beforeTimestamp The timestamp from just before calling the service.
   * @param afterTimestamp The timestamp from just after calling the service.
   */
  public static void validateBasics(SzBasicResponse response,
                                    int             expectedResponseCode,
                                    SzHttpMethod    expectedHttpMethod,
                                    String          selfLink,
                                    long            beforeTimestamp,
                                    long            afterTimestamp)
  {
    validateBasics(null,
                   response,
                   expectedResponseCode,
                   expectedHttpMethod,
                   selfLink,
                   beforeTimestamp,
                   afterTimestamp);
  }

  /**
   * Validates the basic response fields for the specified {@link
   * SzBasicResponse} using the specified self link, timestamp from before
   * calling the service function and timestamp from after calling the
   * service function.
   *
   * @param testInfo           Additional test information to be logged with failures.
   * @param response           The {@link SzBasicResponse} to validate.
   * @param expectedHttpMethod The {@link SzHttpMethod} that was used.
   * @param selfLink           The self link to be expected.
   * @param beforeTimestamp    The timestamp from just before calling the service.
   * @param afterTimestamp     The timestamp from just after calling the service.
   */
  public static void validateBasics(String          testInfo,
                                    SzBasicResponse response,
                                    SzHttpMethod    expectedHttpMethod,
                                    String          selfLink,
                                    long            beforeTimestamp,
                                    long            afterTimestamp)
  {
    validateBasics(testInfo,
                   response,
                   200,
                   expectedHttpMethod,
                   selfLink,
                   beforeTimestamp,
                   afterTimestamp);
  }

  /**
   * Validates the basic response fields for the specified {@link
   * SzBasicResponse} using the specified self link, timestamp from before
   * calling the service function and timestamp from after calling the
   * service function.
   *
   * @param testInfo Additional test information to be logged with failures.
   * @param response The {@link SzBasicResponse} to validate.
   * @param expectedResponseCode The expected HTTP responsec code.
   * @param expectedHttpMethod The {@link SzHttpMethod} that was used.
   * @param selfLink The self link to be expected.
   * @param beforeTimestamp The timestamp from just before calling the service.
   * @param afterTimestamp The timestamp from just after calling the service.
   */
  public static void validateBasics(String          testInfo,
                                    SzBasicResponse response,
                                    int             expectedResponseCode,
                                    SzHttpMethod    expectedHttpMethod,
                                    String          selfLink,
                                    long            beforeTimestamp,
                                    long            afterTimestamp)
  {
    validateBasics(testInfo,
                   response,
                   expectedResponseCode,
                   expectedHttpMethod,
                   selfLink,
                   beforeTimestamp,
                   afterTimestamp,
                  1);
  }

  /**
   * Validates the basic response fields for the specified {@link
   * SzBasicResponse} using the specified self link, timestamp from before
   * calling the service function and timestamp from after calling the
   * service function.
   *
   * @param testInfo Additional test information to be logged with failures.
   * @param response The {@link SzBasicResponse} to validate.
   * @param expectedResponseCode The expected HTTP responsec code.
   * @param expectedHttpMethod The {@link SzHttpMethod} that was used.
   * @param selfLink The self link to be expected.
   * @param beforeTimestamp The timestamp from just before calling the service.
   * @param afterTimestamp The timestamp from just after calling the service.
   * @param serverConcurrency The concurrency for the server.
   */
  public static void validateBasics(String          testInfo,
                                    SzBasicResponse response,
                                    int             expectedResponseCode,
                                    SzHttpMethod    expectedHttpMethod,
                                    String          selfLink,
                                    long            beforeTimestamp,
                                    long            afterTimestamp,
                                    int             serverConcurrency)
  {
    String suffix = (testInfo != null && testInfo.trim().length() > 0)
        ? " ( " + testInfo + " )" : "";

    SzLinks links = response.getLinks();
    SzMeta meta = response.getMeta();

    String expectedLink = selfLink.replaceAll("%20", "+");
    String actualLink   = links.getSelf().replaceAll("%20", "+");
    assertEquals(expectedLink, actualLink, "Unexpected self link" + suffix);
    assertEquals(expectedHttpMethod, meta.getHttpMethod(),
                 "Unexpected HTTP method" + suffix);
    assertEquals(expectedResponseCode, meta.getHttpStatusCode(), "Unexpected HTTP status code" + suffix);
    assertEquals(MAVEN_VERSION, meta.getVersion(), "Unexpected server version" + suffix);
    assertEquals(REST_API_VERSION, meta.getRestApiVersion(), "Unexpected REST API version" + suffix);
    assertNotNull(meta.getTimestamp(), "Timestamp unexpectedly null" + suffix);
    long now = meta.getTimestamp().getTime();

    // check the timestamp
    if (now < beforeTimestamp || now > afterTimestamp) {
      fail("Timestamp (" + new Date(now) + ") should be between "
               + new Date(beforeTimestamp) + " and "
               + new Date(afterTimestamp) + suffix);
    }
    Map<String, Long> timings = meta.getTimings();

    // determine max duration
    long maxDuration = (afterTimestamp - beforeTimestamp) * serverConcurrency;

    timings.entrySet().forEach(entry -> {
      long duration = entry.getValue();
      if (duration > maxDuration) {
        fail("Timing value too large: " + entry.getKey() + " = "
                 + duration + "ms VS " + maxDuration + "ms" + suffix);
      }
    });
  }

  /**
   * Validates the basic response fields for the specified {@link
   * SzResponseWithRawData} using the specified self link, timestamp from before
   * calling the service function, timestamp from after calling the
   * service function, and flag indicating if raw data should be expected.
   *
   * @param response        The {@link SzBasicResponse} to validate.
   * @param selfLink        The self link to be expected.
   * @param beforeTimestamp The timestamp from just before calling the service.
   * @param afterTimestamp  The timestamp from just after calling the service.
   * @param expectRawData   <tt>true</tt> if raw data should be expected,
   *                        otherwise <tt>false</tt>
   */
  public static void validateBasics(SzResponseWithRawData response,
                                    String                selfLink,
                                    long                  beforeTimestamp,
                                    long                  afterTimestamp,
                                    boolean               expectRawData)
  {
    validateBasics(null,
                   response,
                   selfLink,
                   beforeTimestamp,
                   afterTimestamp,
                   expectRawData);
  }

  /**
   * Validates the basic response fields for the specified {@link
   * SzResponseWithRawData} using the specified self link, timestamp from before
   * calling the service function, timestamp from after calling the
   * service function, and flag indicating if raw data should be expected.
   *
   * @param response        The {@link SzBasicResponse} to validate.
   * @param selfLink        The self link to be expected.
   * @param beforeTimestamp The timestamp from just before calling the service.
   * @param afterTimestamp  The timestamp from just after calling the service.
   * @param expectRawData   <tt>true</tt> if raw data should be expected,
   *                        otherwise <tt>false</tt>
   */
  public static void validateBasics(SzResponseWithRawData response,
                                    SzHttpMethod          expectedHttpMethod,
                                    String                selfLink,
                                    long                  beforeTimestamp,
                                    long                  afterTimestamp,
                                    boolean               expectRawData)
  {
    validateBasics(null,
                   response,
                   expectedHttpMethod,
                   selfLink,
                   beforeTimestamp,
                   afterTimestamp,
                   expectRawData);
  }

  /**
   * Validates the basic response fields for the specified {@link
   * SzResponseWithRawData} using the specified self link, timestamp from before
   * calling the service function, timestamp from after calling the
   * service function, and flag indicating if raw data should be expected.
   *
   * @param testInfo        Additional test information to be logged with failures.
   * @param response        The {@link SzBasicResponse} to validate.
   * @param selfLink        The self link to be expected.
   * @param beforeTimestamp The timestamp from just before calling the service.
   * @param afterTimestamp  The timestamp from just after calling the service.
   * @param expectRawData   <tt>true</tt> if raw data should be expected,
   *                        otherwise <tt>false</tt>
   */
  public static void validateBasics(String                testInfo,
                                    SzResponseWithRawData response,
                                    String                selfLink,
                                    long                  beforeTimestamp,
                                    long                  afterTimestamp,
                                    boolean               expectRawData)
  {
    validateBasics(testInfo,
                   response,
                   GET,
                   selfLink,
                   beforeTimestamp,
                   afterTimestamp,
                   expectRawData);
  }

  /**
   * Validates the basic response fields for the specified {@link
   * SzResponseWithRawData} using the specified self link, timestamp from before
   * calling the service function, timestamp from after calling the
   * service function, and flag indicating if raw data should be expected.
   *
   * @param testInfo        Additional test information to be logged with failures.
   * @param response        The {@link SzBasicResponse} to validate.
   * @param expectedHttpMethod The expected HTTP method.
   * @param selfLink        The self link to be expected.
   * @param beforeTimestamp The timestamp from just before calling the service.
   * @param afterTimestamp  The timestamp from just after calling the service.
   * @param expectRawData   <tt>true</tt> if raw data should be expected,
   *                        otherwise <tt>false</tt>
   */
  public static void validateBasics(String                testInfo,
                                    SzResponseWithRawData response,
                                    SzHttpMethod          expectedHttpMethod,
                                    String                selfLink,
                                    long                  beforeTimestamp,
                                    long                  afterTimestamp,
                                    boolean               expectRawData)
  {
    String suffix = (testInfo != null && testInfo.trim().length() > 0)
        ? " ( " + testInfo + " )" : "";

    validateBasics(testInfo,
                   response,
                   expectedHttpMethod,
                   selfLink,
                   beforeTimestamp,
                   afterTimestamp);

    Object rawData = response.getRawData();
    if (expectRawData) {
      assertNotNull(rawData, "Raw data unexpectedly non-null" + suffix);
    } else {
      assertNull(rawData, "Raw data unexpectedly null" + suffix);
    }
  }

  /**
   * Validates the raw data and ensures the expected JSON property keys are
   * present and that no unexpected keys are present.
   *
   * @param rawData      The raw data to validate.
   * @param expectedKeys The zero or more expected property keys.
   */
  public static void validateRawDataMap(Object rawData, String... expectedKeys)
  {
    validateRawDataMap(null,
                            rawData,
                            true,
                            expectedKeys);
  }

  /**
   * Validates the raw data and ensures the expected JSON property keys are
   * present and that no unexpected keys are present.
   *
   * @param testInfo     Additional test information to be logged with failures.
   * @param rawData      The raw data to validate.
   * @param expectedKeys The zero or more expected property keys.
   */
  public static void validateRawDataMap(String    testInfo,
                                        Object    rawData,
                                        String... expectedKeys)
  {
    validateRawDataMap(testInfo, rawData, true, expectedKeys);
  }

  /**
   * Validates the raw data and ensures the expected JSON property keys are
   * present and that, optionally, no unexpected keys are present.
   *
   * @param rawData      The raw data to validate.
   * @param strict       Whether or not property keys other than those specified are
   *                     allowed to be present.
   * @param expectedKeys The zero or more expected property keys -- these are
   *                     either a minimum or exact set depending on the
   *                     <tt>strict</tt> parameter.
   */
  public static void validateRawDataMap(Object    rawData,
                                        boolean   strict,
                                        String... expectedKeys)
  {
    validateRawDataMap(null, rawData, strict, expectedKeys);
  }

  /**
   * Validates the raw data and ensures the expected JSON property keys are
   * present and that, optionally, no unexpected keys are present.
   *
   *
   * @param testInfo     Additional test information to be logged with failures.
   * @param rawData      The raw data to validate.
   * @param strict       Whether or not property keys other than those specified are
   *                     allowed to be present.
   * @param expectedKeys The zero or more expected property keys -- these are
   *                     either a minimum or exact set depending on the
   *                     <tt>strict</tt> parameter.
   */
  public static void validateRawDataMap(String    testInfo,
                                        Object    rawData,
                                        boolean   strict,
                                        String... expectedKeys)
  {
    String suffix = (testInfo != null && testInfo.trim().length() > 0)
        ? " ( " + testInfo + " )" : "";

    if (rawData == null) {
      fail("Expected raw data but got null value" + suffix);
    }

    if (!(rawData instanceof Map)) {
      fail("Raw data is not a JSON object: " + rawData + suffix);
    }

    Map<String, Object> map = (Map<String, Object>) rawData;
    Set<String> expectedKeySet = new HashSet<>();
    Set<String> actualKeySet = map.keySet();
    for (String key : expectedKeys) {
      expectedKeySet.add(key);
      if (!actualKeySet.contains(key)) {
        fail("JSON property missing from raw data: " + key + " / " + map
                 + suffix);
      }
    }
    if (strict && expectedKeySet.size() != actualKeySet.size()) {
      Set<String> extraKeySet = new HashSet<>(actualKeySet);
      extraKeySet.removeAll(expectedKeySet);
      fail("Unexpected JSON properties in raw data: " + extraKeySet + suffix);
    }

  }


  /**
   * Validates the raw data and ensures it is a collection of objects and the
   * expected JSON property keys are present in the array objects and that no
   * unexpected keys are present.
   *
   * @param rawData      The raw data to validate.
   * @param expectedKeys The zero or more expected property keys.
   */
  public static void validateRawDataMapArray(Object     rawData,
                                             String...  expectedKeys)
  {
    validateRawDataMapArray(null, rawData, true, expectedKeys);
  }

  /**
   * Validates the raw data and ensures it is a collection of objects and the
   * expected JSON property keys are present in the array objects and that no
   * unexpected keys are present.
   *
   * @param testInfo     Additional test information to be logged with failures.
   * @param rawData      The raw data to validate.
   * @param expectedKeys The zero or more expected property keys.
   */
  public static void validateRawDataMapArray(String     testInfo,
                                             Object     rawData,
                                             String...  expectedKeys)
  {
    validateRawDataMapArray(testInfo, rawData, true, expectedKeys);
  }

  /**
   * Validates the raw data and ensures it is a collection of objects and the
   * expected JSON property keys are present in the array objects and that,
   * optionally, no unexpected keys are present.
   *
   * @param rawData      The raw data to validate.
   * @param strict       Whether or not property keys other than those specified are
   *                     allowed to be present.
   * @param expectedKeys The zero or more expected property keys for the array
   *                     objects -- these are either a minimum or exact set
   *                     depending on the <tt>strict</tt> parameter.
   */
  public static void validateRawDataMapArray(Object     rawData,
                                             boolean    strict,
                                             String...  expectedKeys)
  {
    validateRawDataMapArray(null, rawData, strict, expectedKeys);
  }

  /**
   * Validates the raw data and ensures it is a collection of objects and the
   * expected JSON property keys are present in the array objects and that,
   * optionally, no unexpected keys are present.
   *
   * @param testInfo     Additional test information to be logged with failures.
   * @param rawData      The raw data to validate.
   * @param strict       Whether or not property keys other than those specified are
   *                     allowed to be present.
   * @param expectedKeys The zero or more expected property keys for the array
   *                     objects -- these are either a minimum or exact set
   *                     depending on the <tt>strict</tt> parameter.
   */
  public static void validateRawDataMapArray(String     testInfo,
                                             Object     rawData,
                                             boolean    strict,
                                             String...  expectedKeys)
  {
    String suffix = (testInfo != null && testInfo.trim().length() > 0)
        ? " ( " + testInfo + " )" : "";

    if (rawData == null) {
      fail("Expected raw data but got null value" + suffix);
    }

    if (!(rawData instanceof Collection)) {
      fail("Raw data is not a JSON array: " + rawData + suffix);
    }

    Collection<Object> collection = (Collection<Object>) rawData;
    Set<String> expectedKeySet = new HashSet<>();
    for (String key : expectedKeys) {
      expectedKeySet.add(key);
    }

    for (Object obj : collection) {
      if (!(obj instanceof Map)) {
        fail("Raw data is not a JSON array of JSON objects: " + rawData + suffix);
      }

      Map<String, Object> map = (Map<String, Object>) obj;

      Set<String> actualKeySet = map.keySet();
      for (String key : expectedKeySet) {
        if (!actualKeySet.contains(key)) {
          fail("JSON property missing from raw data array element: "
                   + key + " / " + map + suffix);
        }
      }
      if (strict && expectedKeySet.size() != actualKeySet.size()) {
        Set<String> extraKeySet = new HashSet<>(actualKeySet);
        extraKeySet.removeAll(expectedKeySet);
        fail("Unexpected JSON properties in raw data: " + extraKeySet + suffix);
      }
    }
  }

  /**
   * Compares two collections to ensure they have the same elements.
   *
   */
  public static void assertSameElements(Collection expected,
                                        Collection actual,
                                        String     description)
  {
    if (expected != null) {
      expected = upperCase(expected);
      actual   = upperCase(actual);
      assertNotNull(actual, "Unexpected null " + description);
      if (!actual.containsAll(expected)) {
        Set missing = new HashSet(expected);
        missing.removeAll(actual);
        fail("Missing one or more expected " + description + ".  missing=[ "
                 + missing + " ], actual=[ " + actual + " ]");
      }
      if (!expected.containsAll(actual)) {
        Set extras = new HashSet(actual);
        extras.removeAll(expected);
        fail("One or more extra " + description + ".  extras=[ "
                 + extras + " ], actual=[ " + actual + " ]");
      }
    }
  }

  /**
   * Converts the {@link String} elements in the specified {@link Collection}
   * to upper case and returns a {@link Set} contianing all values.
   *
   * @param c The {@link Collection} to process.
   *
   * @return The {@link Set} containing the same elements with the {@link
   *         String} elements converted to upper case.
   */
  protected static Set upperCase(Collection c) {
    Set set = new LinkedHashSet();
    for (Object obj : c) {
      if (obj instanceof String) {
        obj = ((String) obj).toUpperCase();
      }
      set.add(obj);
    }
    return set;
  }

  /**
   * Validates an entity
   */
  public static void validateEntity(
      String                              testInfo,
      SzResolvedEntity                    entity,
      List<SzRelatedEntity>               relatedEntities,
      Boolean                             forceMinimal,
      SzFeatureMode                       featureMode,
      boolean                             withFeatureStats,
      boolean                             withInternalFeatures,
      Integer                             expectedRecordCount,
      Set<SzRecordId>                     expectedRecordIds,
      Boolean                             relatedSuppressed,
      Integer                             relatedEntityCount,
      Boolean                             relatedPartial,
      Map<String,Integer>                 expectedFeatureCounts,
      Map<String,Set<String>>             primaryFeatureValues,
      Map<String,Set<String>>             duplicateFeatureValues,
      Map<SzAttributeClass, Set<String>>  expectedDataValues,
      Set<String>                         expectedOtherDataValues)
  {
    if (expectedRecordCount != null) {
      assertEquals(expectedRecordCount, entity.getRecords().size(),
                   "Unexpected number of records: " + testInfo);
    }
    if (expectedRecordIds != null) {
      // get the records and convert to record ID set
      Set<SzRecordId> actualRecordIds = new HashSet<>();
      List<SzMatchedRecord> matchedRecords = entity.getRecords();
      for (SzMatchedRecord record : matchedRecords) {
        SzRecordId recordId = new SzRecordId(record.getDataSource(),
                                             record.getRecordId());
        actualRecordIds.add(recordId);
      }
      assertSameElements(expectedRecordIds, actualRecordIds,
                              "Unexpected record IDs: " + testInfo);
    }

    // check the features
    if (forceMinimal != null && forceMinimal) {
      assertEquals(0, entity.getFeatures().size(),
                   "Features included in minimal results: " + testInfo
                       + " / " + entity.getFeatures());
    } else if (featureMode != null && featureMode == NONE) {
      assertEquals(
          0, entity.getFeatures().size(),
          "Features included despite NONE feature mode: " + testInfo
              + " / " + entity.getFeatures());

    } else {
      assertNotEquals(0, entity.getFeatures().size(),
                      "Features not present for entity: " + testInfo);

      Set<String> featureKeys = entity.getFeatures().keySet();
      if (withInternalFeatures) {
        if (featureKeys.contains("NAME") && !featureKeys.contains("NAME_KEY")) {
          fail("Missing NAME_KEY, but found NAME with internal features "
                   + "requested: " + testInfo + " / " + featureKeys);
        }
        if (featureKeys.contains("ADDRESS")
            && !featureKeys.contains("ADDR_KEY"))
        {
          fail("Missing ADDR_KEY, but found ADDRESS with internal features "
                   + "requested: " + testInfo + " / " + featureKeys);
        }
      } else {
        if (featureKeys.contains("NAME_KEY")) {
          fail("Found NAME_KEY with internal features suppressed: "
                   + testInfo + " / " + featureKeys);
        }
        if (featureKeys.contains("ADDR_KEY")) {
          fail("Found ADDR_KEY with internal features suppressed: "
                   + testInfo + " / " + featureKeys);
        }
      }
      // validate representative feature mode
      if (featureMode == REPRESENTATIVE) {
        entity.getFeatures().entrySet().forEach(entry -> {
          String                featureKey    = entry.getKey();
          List<SzEntityFeature> featureValues = entry.getValue();
          featureValues.forEach(featureValue -> {
            if (featureValue.getDuplicateValues().size() != 0) {
              fail("Duplicate feature values present for " + featureKey
                       + " feature despite REPRESENTATIVE feature mode: "
                       + testInfo + " / " + featureValue);
            }
          });
        });
      }

      // check if statistics are present
      entity.getFeatures().entrySet().forEach(entry -> {
        String                featureKey    = entry.getKey();
        List<SzEntityFeature> featureValues = entry.getValue();
        featureValues.forEach(featureValue -> {
          List<SzEntityFeatureDetail> list = featureValue.getFeatureDetails();
          for (SzEntityFeatureDetail detail: list) {
            if (withFeatureStats) {
              assertNotNull(detail.getStatistics(),
                            "Expected feature statistics: " + testInfo
                                + " / " + detail);
            } else {
              assertNull(detail.getStatistics(),
                         "Unexpected feature statistics: " + testInfo
                         + " / " + detail);
            }
          }
        });
      });

      // validate the feature counts (if any)
      if (expectedFeatureCounts != null) {
        expectedFeatureCounts.entrySet().forEach(entry -> {
          String featureKey = entry.getKey();
          int expectedCount = entry.getValue();
          List<SzEntityFeature> featureValues
              = entity.getFeatures().get(featureKey);
          assertEquals(expectedCount, featureValues.size(),
                       "Unexpected feature count for " + featureKey
                           + " feature: " + testInfo + " / " + featureValues);
        });
      }

      // validate the feature values (if any)
      if (primaryFeatureValues != null) {
        primaryFeatureValues.entrySet().forEach(entry -> {
          String      featureKey    = entry.getKey();
          Set<String> primaryValues = entry.getValue();

          List<SzEntityFeature> featureValues
              = entity.getFeatures().get(featureKey);

          primaryValues.forEach(primaryValue -> {
            boolean found = false;
            for (SzEntityFeature featureValue : featureValues) {
              if (primaryValue.equalsIgnoreCase(featureValue.getPrimaryValue())) {
                found = true;
                break;
              }
            }
            if (!found) {
              fail("Could not find \"" + primaryValue + "\" among the "
                       + featureKey + " primary feature values: " + testInfo
                       + " / " + featureValues);
            }
          });
        });
      }
      if (duplicateFeatureValues != null && (featureMode != REPRESENTATIVE)) {
        duplicateFeatureValues.entrySet().forEach(entry -> {
          String      featureKey      = entry.getKey();
          Set<String> duplicateValues = entry.getValue();

          List<SzEntityFeature> featureValues
              = entity.getFeatures().get(featureKey);

          duplicateValues.forEach(expectedDuplicate -> {
            boolean found = false;
            for (SzEntityFeature featureValue : featureValues) {
              for (String duplicateValue : featureValue.getDuplicateValues()) {
                if (expectedDuplicate.equalsIgnoreCase(duplicateValue)) {
                  found = true;
                  break;
                }
              }
            }
            if (!found) {
              fail("Could not find \"" + expectedDuplicate + "\" among the "
                       + featureKey + " duplicate feature values: " + testInfo
                       + " / " + featureValues);
            }
          });
        });
      }

      if (forceMinimal == null || !forceMinimal) {
        Date lastSeenTimestamp = entity.getLastSeenTimestamp();
        assertNotNull(lastSeenTimestamp,
                      "Last-seen timestamp is null: " + testInfo);
        long now = System.currentTimeMillis();
        long lastSeen = lastSeenTimestamp.getTime();
        assertTrue(now > lastSeen,
                   "Last-seen timestamp in the future: " + lastSeenTimestamp
                       + " / " + (new Date(now)) + " / " + testInfo);
      }

      // validate the features versus the data elements
      SzApiProvider provider = SzApiProvider.Factory.getProvider();
      entity.getFeatures().entrySet().forEach(entry -> {
        String featureKey = entry.getKey();
        List<SzEntityFeature> featureValues = entry.getValue();

        SzAttributeClass attrClass = SzAttributeClass.parseAttributeClass(
            provider.getAttributeClassForFeature(featureKey));

        if (attrClass == null) {
          // skip this feature if working with internal features
          if (withInternalFeatures) return;

          // otherwise fail
          fail("Unrecognized feature key (" + featureKey + "): " + testInfo
               + " / " + entity.getFeatures());
        }

        List<String> dataSet = getDataElements(entity, attrClass);
        if (dataSet == null) return;

        for (SzEntityFeature feature : featureValues) {
          String featureValue = feature.getPrimaryValue().trim().toUpperCase();
          boolean found = false;
          for (String dataValue : dataSet) {
            if (dataValue.toUpperCase().indexOf(featureValue) >= 0) {
              found = true;
              break;
            }
          }
          if (!found) {
            fail(featureKey + " feature value (" + featureValue
                     + ") not found in " + attrClass + " data values: "
                     + dataSet + " (" + testInfo + ")");
          }
        }
      });
    }

    // check if related entities are provided to validate
    if (relatedEntities != null) {
      if (relatedSuppressed == null || !relatedSuppressed) {
        // check if verifying the number of related entities
        if (relatedEntityCount != null) {
          assertEquals(relatedEntityCount, relatedEntities.size(),
                       "Unexpected number of related entities: "
                           + testInfo);
        }

        // check if verifying if related entities are partial
        if (relatedPartial != null || (forceMinimal != null && forceMinimal)) {
          boolean partial = ((relatedPartial != null && relatedPartial)
              || (forceMinimal != null && forceMinimal)
              || (featureMode == NONE));

          for (SzRelatedEntity related : relatedEntities) {
            if (related.isPartial() != partial) {
              if (partial) {
                fail("Entity " + entity.getEntityId() + " has a complete "
                         + "related entity (" + related.getEntityId()
                         + ") where partial entities were expected: " + testInfo);
              } else {
                fail("Entity " + entity.getEntityId() + " has a partial "
                         + "related entity (" + related.getEntityId()
                         + ") where complete entities were expected: " + testInfo);
              }
            }
          }
        }
      }
    }

    if (expectedDataValues != null
        && (forceMinimal == null || !forceMinimal)
        && (featureMode == null || featureMode != NONE))
    {
      expectedDataValues.entrySet().forEach(entry -> {
        SzAttributeClass attrClass      = entry.getKey();
        Set<String>      expectedValues = entry.getValue();
        List<String>     actualValues   = getDataElements(entity, attrClass);
        assertSameElements(expectedValues,
                                actualValues,
                                attrClass.toString() + " (" + testInfo + ")");
      });
    }
    if (expectedOtherDataValues != null
        && (forceMinimal == null || !forceMinimal) )
    {
      List<String> actualValues = entity.getOtherData();
      assertSameElements(expectedOtherDataValues, actualValues,
                              "OTHER DATA (" + testInfo + ")");
    }
  }

  /**
   * Gets the data elements from the specified entity for the given attribute
   * class.
   *
   * @param entity The entity to get the data from.
   * @param attrClass The attribute class identifying the type of data
   * @return The {@link List} of data elements.
   */
  public static List<String> getDataElements(SzResolvedEntity entity,
                                             SzAttributeClass attrClass)
  {
    switch (attrClass) {
      case NAME:
        return entity.getNameData();
      case CHARACTERISTIC:
        return entity.getCharacteristicData();
      case PHONE:
        return entity.getPhoneData();
      case IDENTIFIER:
        return entity.getIdentifierData();
      case ADDRESS:
        return entity.getAddressData();
      case RELATIONSHIP:
        return entity.getRelationshipData();
      default:
        return null;
    }
  }

  /**
   * Validates an {@link SzDataSourcesResponse} instance.
   *
   * @param response The response to validate.
   * @param selfLink The HTTP request URI
   * @param beforeTimestamp The timestamp before executing the request.
   * @param afterTimestamp The timestamp after executing the request and
   *                       concluding timers on the response.
   * @param expectRawData Whether or not to expect raw data.
   * @param expectedDataSources The expected data sources.
   */
  public static void validateDataSourcesResponse(
      SzDataSourcesResponse     response,
      SzHttpMethod              httpMethod,
      String                    selfLink,
      long                      beforeTimestamp,
      long                      afterTimestamp,
      boolean                   expectRawData,
      Map<String, SzDataSource> expectedDataSources)
  {
    validateDataSourcesResponse(null,
                                response,
                                httpMethod,
                                selfLink,
                                beforeTimestamp,
                                afterTimestamp,
                                expectRawData,
                                expectedDataSources);
  }

  /**
   * Validates an {@link SzDataSourcesResponse} instance.
   *
   * @param response The response to validate.
   * @param testInfo The optional test info describing the test.
   * @param selfLink The HTTP request URI
   * @param beforeTimestamp The timestamp before executing the request.
   * @param afterTimestamp The timestamp after executing the request and
   *                       concluding timers on the response.
   * @param expectRawData Whether or not to expect raw data.
   * @param expectedDataSources The expected data sources.
   */
  public static void validateDataSourcesResponse(
      String                    testInfo,
      SzDataSourcesResponse     response,
      SzHttpMethod              httpMethod,
      String                    selfLink,
      long                      beforeTimestamp,
      long                      afterTimestamp,
      boolean                   expectRawData,
      Map<String, SzDataSource> expectedDataSources)
  {
    validateBasics(testInfo,
                   response,
                   httpMethod,
                   selfLink,
                   beforeTimestamp,
                   afterTimestamp,
                   expectRawData);

    String testSuffix = (testInfo == null) ? "" : ": " + testInfo;
    String info = (testInfo == null) ? "" : "testInfo=[ " + testInfo + " ], ";

    SzDataSourcesResponse.Data data = response.getData();

    assertNotNull(data, "Response data is null" + testSuffix);

    Set<String> sources = data.getDataSources();
    Map<String, SzDataSource> details = data.getDataSourceDetails();

    assertNotNull(sources, "Data sources set is null" + testSuffix);
    assertNotNull(details, "Data source details map is null" + testSuffix);

    assertEquals(expectedDataSources.keySet(), sources,
                 "Unexpected or missing data sources in set.  "
                     + info
                     + "unexpected=[ "
                     + diffSets(sources, expectedDataSources.keySet())
                     + " ], missing=[ "
                     + diffSets(expectedDataSources.keySet(), sources)
                     + " ]" + testSuffix);

    assertEquals(expectedDataSources.keySet(), details.keySet(),
                 "Unexpected or missing data source details");

    details.entrySet().forEach(entry -> {
      String code = entry.getKey();
      SzDataSource source = entry.getValue();
      assertEquals(code, source.getDataSourceCode(),
                   "Data source code property key ("
                       + code + ") in details does not match the data source "
                       + "code in the corresponding detail object: "
                       + info + "detail=[ " + source.toString() + " ]");
    });

    expectedDataSources.values().forEach(expected -> {
      String code = expected.getDataSourceCode();
      SzDataSource actual = details.get(code);
      if (expected.getDataSourceId() != null) {
        assertEquals(expected, actual,
                     "Unexpected data source details" + testSuffix);
      }
    });

    if (expectRawData) {
      validateRawDataMap(testInfo, response.getRawData(), "DATA_SOURCES");
      Object array = ((Map) response.getRawData()).get("DATA_SOURCES");
      validateRawDataMapArray(
          testInfo, array,false,"DSRC_CODE", "DSRC_ID");
    }
  }

  /**
   * Validates an {@link SzDataSourceResponse} instance.
   *
   * @param response The response to validate.
   * @param expectedHttpMethod The expected HTTP method.
   * @param selfLink The HTTP request URI
   * @param beforeTimestamp The timestamp before executing the request.
   * @param afterTimestamp The timestamp after executing the request and
   *                       concluding timers on the response.
   * @param expectRawData Whether or not to expect raw data.
   * @param expectedDataSource The expected data source.
   */
  public static void validateDataSourceResponse(
      SzDataSourceResponse    response,
      SzHttpMethod            expectedHttpMethod,
      String                  selfLink,
      long                    beforeTimestamp,
      long                    afterTimestamp,
      boolean                 expectRawData,
      SzDataSource            expectedDataSource)
  {
    validateBasics(response,
                   expectedHttpMethod,
                   selfLink,
                   beforeTimestamp,
                   afterTimestamp,
                   expectRawData);

    SzDataSourceResponse.Data data = response.getData();

    assertNotNull(data, "Response data is null");

    SzDataSource dataSource = data.getDataSource();

    assertNotNull(dataSource, "Data source is null");

    assertEquals(expectedDataSource, dataSource,
                 "Unexpected data source");

    if (expectRawData) {
      validateRawDataMap(
          response.getRawData(), "DSRC_CODE", "DSRC_ID");
    }
  }

  /**
   * Validates an {@link SzEntityClassesResponse} instance.
   *
   * @param response The response to validate.
   * @param selfLink The HTTP request URI
   * @param beforeTimestamp The timestamp before executing the request.
   * @param afterTimestamp The timestamp after executing the request and
   *                       concluding timers on the response.
   * @param expectRawData Whether or not to expect raw data.
   * @param expectedEntityClasses The expected entity classes.
   */
  public static void validateEntityClassesResponse(
      String                      testInfo,
      SzEntityClassesResponse     response,
      SzHttpMethod                httpMethod,
      String                      selfLink,
      long                        beforeTimestamp,
      long                        afterTimestamp,
      boolean                     expectRawData,
      Boolean                     defaultResolving,
      Map<String, SzEntityClass>  expectedEntityClasses)
  {
    validateBasics(testInfo,
                   response,
                   httpMethod,
                   selfLink,
                   beforeTimestamp,
                   afterTimestamp,
                   expectRawData);

    String testSuffix = (testInfo == null) ? "" : ": " + testInfo;
    String info = (testInfo == null) ? "" : "testInfo=[ " + testInfo + " ], ";

    SzEntityClassesResponse.Data data = response.getData();

    assertNotNull(data, "Response data is null" + testSuffix);

    Set<String> classes = data.getEntityClasses();
    Map<String, SzEntityClass> details = data.getEntityClassDetails();

    assertNotNull(classes, "Entity classes set is null" + testSuffix);
    assertNotNull(details, "Entity class details map is null" + testSuffix);

    assertEquals(expectedEntityClasses.keySet(), classes,
                 "Unexpected or missing entity classes in set.  "
                     + info
                     + "unexpected=[ "
                     + diffSets(classes, expectedEntityClasses.keySet())
                     + " ], missing=[ "
                     + diffSets(expectedEntityClasses.keySet(), classes)
                     + " ]" + testSuffix);

    assertEquals(expectedEntityClasses.keySet(), details.keySet(),
                 "Unexpected or missing entity class details"
                     + testSuffix);

    details.entrySet().forEach(entry -> {
      String code = entry.getKey();
      SzEntityClass entityClass = entry.getValue();
      assertEquals(code, entityClass.getEntityClassCode(),
                   "Entity class code property key ("
                       + code + ") in details does not match the entity class "
                       + "code in the corresponding detail object: "
                       + info + "detail=[ " + entityClass.toString() + " ]");
    });

    expectedEntityClasses.values().forEach(expected -> {
      String code = expected.getEntityClassCode();
      SzEntityClass actual = details.get(code);
      if (expected.getEntityClassId() != null)
      {
        assertEquals(expected.getEntityClassId(), actual.getEntityClassId(),
                     "Unexpected entity class ID for " + code
                         + testSuffix);
      }
      Boolean expectedResolving = expected.isResolving();
      if (expectedResolving == null) expectedResolving = defaultResolving;
      if (expectedResolving == null) expectedResolving = true;
      assertEquals(expectedResolving, actual.isResolving(),
                   "Unexpected resolving flag for entity class ("
                       + actual.getEntityClassCode() + ")" + info
                       + "expectedEntityClass=[ " + expected + " ]");
    });

    if (expectRawData) {
      validateRawDataMap(testInfo, response.getRawData(), "ENTITY_CLASSES");
      Object array = ((Map) response.getRawData()).get("ENTITY_CLASSES");
      validateRawDataMapArray(
          testInfo, array,false,
          "ECLASS_CODE", "ECLASS_ID", "RESOLVE");
    }
  }

  /**
   * Validates an {@link SzEntityClassResponse} instance.
   *
   * @param response The response to validate.
   * @param selfLink The HTTP request URI
   * @param beforeTimestamp The timestamp before executing the request.
   * @param afterTimestamp The timestamp after executing the request and
   *                       concluding timers on the response.
   * @param expectRawData Whether or not to expect raw data.
   * @param expectedEntityClass The expected entity class.
   */
  public static void validateEntityClassResponse(
      SzEntityClassResponse   response,
      String                  selfLink,
      long                    beforeTimestamp,
      long                    afterTimestamp,
      boolean                 expectRawData,
      SzEntityClass           expectedEntityClass)
  {
    validateBasics(
        response, selfLink, beforeTimestamp, afterTimestamp, expectRawData);

    SzEntityClassResponse.Data data = response.getData();

    assertNotNull(data, "Response data is null");

    SzEntityClass entityClass = data.getEntityClass();

    assertNotNull(entityClass, "Entity class is null");

    assertEquals(expectedEntityClass, entityClass,
                 "Unexpected entity class");

    if (expectRawData) {
      validateRawDataMap(
          response.getRawData(),
          "ECLASS_CODE", "ECLASS_ID", "RESOLVE");
    }
  }

  /**
   * Validates an {@link SzEntityClassesResponse} instance.
   *
   * @param response The response to validate.
   * @param selfLink The HTTP request URI
   * @param beforeTimestamp The timestamp before executing the request.
   * @param afterTimestamp The timestamp after executing the request and
   *                       concluding timers on the response.
   * @param expectRawData Whether or not to expect raw data.
   * @param expectedEntityTypes The expected entity types.
   */
  public static void validateEntityTypesResponse(
      String                      testInfo,
      SzEntityTypesResponse       response,
      SzHttpMethod                httpMethod,
      String                      selfLink,
      long                        beforeTimestamp,
      long                        afterTimestamp,
      boolean                     expectRawData,
      Map<String, SzEntityType>   expectedEntityTypes)
  {
    validateBasics(testInfo,
                   response,
                   httpMethod,
                   selfLink,
                   beforeTimestamp,
                   afterTimestamp,
                   expectRawData);

    String testSuffix = (testInfo == null) ? "" : ": " + testInfo;
    String info = (testInfo == null) ? "" : "testInfo=[ " + testInfo + " ], ";

    SzEntityTypesResponse.Data data = response.getData();

    assertNotNull(data, "Response data is null" + testSuffix);

    Set<String> types = data.getEntityTypes();
    Map<String, SzEntityType> details = data.getEntityTypeDetails();

    assertNotNull(types, "Entity type set is null" + testSuffix);
    assertNotNull(details, "Entity type details map is null" + testSuffix);

    assertEquals(expectedEntityTypes.keySet(), types,
                 "Unexpected or missing entity types in set.  "
                     + info
                     + "unexpected=[ "
                     + diffSets(types, expectedEntityTypes.keySet())
                     + " ], missing=[ "
                     + diffSets(expectedEntityTypes.keySet(), types)
                     + " ]");

    expectedEntityTypes.values().forEach(expected -> {
      String code = expected.getEntityTypeCode();
      SzEntityType actual = details.get(code);
      if (expected.getEntityTypeId() != null)
      {
        assertEquals(expected.getEntityTypeId(), actual.getEntityTypeId(),
                     "Unexpected entity type ID for " + code + testSuffix);
      }

      assertEquals(expected.getEntityClassCode(), actual.getEntityClassCode(),
                   "Unexpected entity class code for entity type ("
                       + expected.getEntityTypeCode() + "): " + info
                       + "expectedEntityType=[ " + expected + " ]");
    });

    if (expectRawData) {
      validateRawDataMap(testInfo, response.getRawData(), "ENTITY_TYPES");
      Object array = ((Map) response.getRawData()).get("ENTITY_TYPES");
      validateRawDataMapArray(
          testInfo, array,false,
          "ETYPE_CODE", "ETYPE_ID", "ECLASS_CODE");
    }
  }

  /**
   * Validates an {@link SzEntityClassResponse} instance.
   *
   * @param response The response to validate.
   * @param selfLink The HTTP request URI
   * @param beforeTimestamp The timestamp before executing the request.
   * @param afterTimestamp The timestamp after executing the request and
   *                       concluding timers on the response.
   * @param expectRawData Whether or not to expect raw data.
   * @param expectedEntityType The expected entity type.
   */
  public static void validateEntityTypeResponse(
      SzEntityTypeResponse    response,
      String                  selfLink,
      long                    beforeTimestamp,
      long                    afterTimestamp,
      boolean                 expectRawData,
      SzEntityType            expectedEntityType)
  {
    validateBasics(
        response, selfLink, beforeTimestamp, afterTimestamp, expectRawData);

    SzEntityTypeResponse.Data data = response.getData();

    assertNotNull(data, "Response data is null");

    SzEntityType entityType = data.getEntityType();

    assertNotNull(entityType, "Entity type is null");

    assertEquals(expectedEntityType, entityType,
                 "Unexpected entity type");

    if (expectRawData) {
      validateRawDataMap(
          response.getRawData(),
          "ETYPE_CODE", "ETYPE_ID", "ECLASS_CODE");
    }
  }

  private static Set diffSets(Set s1, Set s2) {
    Set diff = new LinkedHashSet<>(s1);
    diff.removeAll(s2);
    return diff;
  }

  /**
   * Validates an {@link SzAttributeTypesResponse} instance.
   *
   * @param response The response to validate.
   * @param selfLink The expected meta data self link.
   * @param beforeTimestamp The timestamp before executing the request.
   * @param afterTimestamp The timestamp after executing the request and
   *                       concluding timers on the response.
   * @param expectRawData Whether or not to expect raw data.
   * @param expectedAttrTypeCodes The expected attribute type codes.
   */
  public static void validateAttributeTypesResponse(
      String                    testInfo,
      SzAttributeTypesResponse  response,
      String                    selfLink,
      long                      beforeTimestamp,
      long                      afterTimestamp,
      Boolean                   expectRawData,
      Set<String>               expectedAttrTypeCodes)
  {
    if (expectRawData == null) {
      expectRawData = false;
    }

    validateBasics(testInfo,
                   response,
                   selfLink,
                   beforeTimestamp,
                   afterTimestamp,
                   expectRawData);

    SzAttributeTypesResponse.Data data = response.getData();

    assertNotNull(data, "Response data is null: " + testInfo);

    List<SzAttributeType> attrTypes = data.getAttributeTypes();

    assertNotNull(attrTypes, "List of attribute types is null: " + testInfo);

    Map<String, SzAttributeType> map = new LinkedHashMap<>();
    for (SzAttributeType attrType : attrTypes) {
      map.put(attrType.getAttributeCode(), attrType);
    }

    assertEquals(expectedAttrTypeCodes, map.keySet(),
                 "Unexpected or missing attribute types: "
                     + "unexpected=[ "
                     + diffSets(map.keySet(), expectedAttrTypeCodes)
                     + " ], missing=[ "
                     + diffSets(expectedAttrTypeCodes, map.keySet())
                     + " ], testInfo=[ " + testInfo + " ]");

    if (expectRawData) {
      validateRawDataMap(
          response.getRawData(), true, "CFG_ATTR");

      Object attrs = ((Map) response.getRawData()).get("CFG_ATTR");

      validateRawDataMapArray(testInfo,
                              attrs,
                              false,
                              "DEFAULT_VALUE",
                              "ATTR_CODE",
                              "FELEM_REQ",
                              "ATTR_CLASS",
                              "INTERNAL",
                              "ATTR_ID",
                              "FTYPE_CODE",
                              "FELEM_CODE",
                              "ADVANCED");
    }
  }

  /**
   /**
   * Validates an {@link SzAttributeTypeResponse} instance.
   *
   * @param response The response to validate.
   * @param attributeCode The requested attribute code.
   * @param beforeTimestamp The timestamp before executing the request.
   * @param afterTimestamp The timestamp after executing the request and
   *                       concluding timers on the response.
   * @param expectRawData Whether or not to expect raw data.
   */
  public static void validateAttributeTypeResponse(
      SzAttributeTypeResponse response,
      String                  selfLink,
      String                  attributeCode,
      long                    beforeTimestamp,
      long                    afterTimestamp,
      Boolean                 expectRawData)
  {
    if (expectRawData == null) {
      expectRawData = false;
    }

    validateBasics(
        response, selfLink, beforeTimestamp, afterTimestamp, expectRawData);

    SzAttributeTypeResponse.Data data = response.getData();

    assertNotNull(data, "Response data is null");

    SzAttributeType attrType = data.getAttributeType();

    assertNotNull(attrType, "Attribute Type is null");

    assertEquals(attributeCode, attrType.getAttributeCode(),
                 "Unexpected attribute type code");

    if (expectRawData) {
      validateRawDataMap(response.getRawData(),
                              "DEFAULT_VALUE",
                              "ATTR_CODE",
                              "FELEM_REQ",
                              "ATTR_CLASS",
                              "INTERNAL",
                              "ATTR_ID",
                              "FTYPE_CODE",
                              "FELEM_CODE",
                              "ADVANCED");
    }
  }

  /**
   * Validates an {@link SzConfigResponse} instance.
   *
   * @param response The response to validate.
   * @param selfLink The expected meta data self link.
   * @param beforeTimestamp The timestamp before executing the request.
   * @param afterTimestamp The timestamp after executing the request and
   *                       concluding timers on the response.
   * @param expectedDataSources The expected data sources.
   */
  public static void validateConfigResponse(
      SzConfigResponse        response,
      String                  selfLink,
      long                    beforeTimestamp,
      long                    afterTimestamp,
      Set<String>             expectedDataSources)
  {
    validateBasics(
        response, selfLink, beforeTimestamp, afterTimestamp, true);

    Object rawData = response.getRawData();

    validateRawDataMap(rawData, true, "G2_CONFIG");

    Object g2Config = ((Map) rawData).get("G2_CONFIG");

    validateRawDataMap(g2Config,
                       false,
                       "CFG_ATTR",
                       "CFG_FELEM",
                       "CFG_DSRC");

    Object cfgDsrc = ((Map) g2Config).get("CFG_DSRC");

    validateRawDataMapArray(cfgDsrc,
                            false,
                            "DSRC_ID",
                            "DSRC_DESC",
                            "DSRC_CODE");

    Set<String> actualDataSources = new LinkedHashSet<>();
    for (Object dsrc : ((Collection) cfgDsrc)) {
      Map dsrcMap = (Map) dsrc;
      String dsrcCode = (String) dsrcMap.get("DSRC_CODE");
      actualDataSources.add(dsrcCode);
    }

    assertEquals(expectedDataSources, actualDataSources,
                 "Unexpected set of data sources in config.");
  }

  /**
   * Validates an {@link SzRecordResponse} instance.
   *
   * @param response The response to validate.
   * @param dataSourceCode The data source code for the requested record.
   * @param expectedRecordId The record ID for the requested record.
   * @param expectedNameData The expected name data or <tt>null</tt> if not
   *                         validating the name data.
   * @param expectedAddressData The expected address data or <tt>null</tt> if
   *                            not validating the address data.
   * @param expectedPhoneData The expected phone data or <tt>null</tt> if not
   *                          validating the phone data.
   * @param expectedIdentifierData The expected identifier data or <tt>null</tt>
   *                               if not validating the identifier data.
   * @param expectedAttributeData The expected attribute data or <tt>null</tt>
   *                              if not validating the attribute data.
   * @param expectedOtherData The expected other data or <tt>null</tt>
   *                          if not validating the other data.
   * @param beforeTimestamp The timestamp before executing the request.
   * @param afterTimestamp The timestamp after executing the request and
   *                       concluding timers on the response.
   * @param expectRawData Whether or not to expect raw data.
   */
  public static void validateRecordResponse(
      SzRecordResponse  response,
      SzHttpMethod      httpMethod,
      String            selfLink,
      String            dataSourceCode,
      String            expectedRecordId,
      Set<String>       expectedNameData,
      Set<String>       expectedAddressData,
      Set<String>       expectedPhoneData,
      Set<String>       expectedIdentifierData,
      Set<String>       expectedAttributeData,
      Set<String>       expectedRelationshipData,
      Set<String>       expectedOtherData,
      long              beforeTimestamp,
      long              afterTimestamp,
      Boolean           expectRawData)
  {
    if (expectRawData == null) {
      expectRawData = false;
    }

    validateBasics(
        response, httpMethod, selfLink, beforeTimestamp, afterTimestamp);

    SzRecordResponse.Data data = response.getData();

    assertNotNull(data, "Response data is null");

    SzEntityRecord record = data.getRecord();

    assertNotNull(record, "Response record is null");

    String dataSource = record.getDataSource();
    assertNotNull(dataSource, "Data source is null");
    assertEquals(dataSourceCode, dataSource, "Unexpected data source value");

    String recordId = record.getRecordId();
    assertNotNull(recordId, "Record ID is null");
    assertEquals(expectedRecordId, recordId, "Unexpected record ID value");

    Date lastSeenTimestamp = record.getLastSeenTimestamp();
    assertNotNull(lastSeenTimestamp, "Last-seen timestamp is null: "
                  + record + " / " + response.getRawData());
    long now = System.currentTimeMillis();
    long lastSeen = lastSeenTimestamp.getTime();
    assertTrue(now > lastSeen,
               "Last-seen timestamp in the future: " + lastSeenTimestamp
                   + " / " + (new Date(now)));

    assertSameElements(
        expectedNameData, record.getNameData(), "names");
    assertSameElements(
        expectedAddressData, record.getAddressData(), "addresses");
    assertSameElements(
        expectedPhoneData, record.getPhoneData(), "phone numbers");
    assertSameElements(
        expectedIdentifierData, record.getIdentifierData(), "identifiers");
    assertSameElements(
        expectedAttributeData, record.getCharacteristicData(), "characteristics");
    assertSameElements(
        expectedRelationshipData, record.getRelationshipData(), "relationships");
    assertSameElements(
        expectedOtherData, record.getOtherData(), "other");

    if (expectRawData) {
      validateRawDataMap(response.getRawData(),
                              false,
                              "JSON_DATA",
                              "NAME_DATA",
                              "ATTRIBUTE_DATA",
                              "IDENTIFIER_DATA",
                              "ADDRESS_DATA",
                              "PHONE_DATA",
                              "RELATIONSHIP_DATA",
                              "ENTITY_DATA",
                              "OTHER_DATA",
                              "DATA_SOURCE",
                              "RECORD_ID");
    }

  }

  /**
   * Validates an {@link SzEntityResponse} instance.
   *
   * @param testInfo The test information describing the test.
   * @param response The response to validate.
   * @param selfLink The expected meta data self link.
   * @param withRaw <tt>true</tt> if requested with raw data, <tt>false</tt>
   *                if requested without raw data and <tt>null</tt> if this is
   *                not being validated.
   * @param withRelated The {@link SzRelationshipMode} value or <tt>null</tt>
   *                    if this aspect is not being validated.
   * @param forceMinimal <tt>true</tt> if requested with minimal data,
   *                     <tt>false</tt> if requested with standard data and
   *                     <tt>null</tt> if this aspect is not being validated.
   * @param featureMode The {@link SzFeatureMode} requested or
   *                    <tt>null</tt> if this is not being validated.
   * @param withFeatureStats <tt>true</tt> if request with feature statistics,
   *                         otherwise <tt>false</tt>.
   * @param withInternalFeatures <tt>true</tt> if request with internal features,
   *                            otherwise <tt>false</tt>.
   * @param expectedRecordCount The number of expected records for the entity,
   *                            or <tt>null</tt> if this is not being validated.
   * @param expectedRecordIds The expected record IDs for the entity to have or
   *                          <tt>null</tt> if this is not being validated.
   * @param relatedEntityCount The expected number of related entities or
   *                           <tt>null</tt> if this is not being validated.
   * @param expectedFeatureCounts The expected number of features by feature
   *                              type, or <tt>null</tt> if this is not being
   *                              validated.
   * @param primaryFeatureValues The expected primary feature values by feature
   *                             type, or <tt>null</tt> if this is not being
   *                             validated.
   * @param duplicateFeatureValues The expected duplicate fature values by
   *                               feature type, or <tt>null</tt> if this is not
   *                               being validated.
   * @param expectedDataValues The expected data values by attribute class, or
   *                           <tt>null</tt> if this is not being validated.
   * @param beforeTimestamp The timestamp before executing the request.
   * @param afterTimestamp The timestamp after executing the request and
   *                       concluding timers on the response.
   */
  public static void validateEntityResponse(
      String                              testInfo,
      SzEntityResponse                    response,
      SzHttpMethod                        httpMethod,
      String                              selfLink,
      Boolean                             withRaw,
      SzRelationshipMode                  withRelated,
      Boolean                             forceMinimal,
      SzFeatureMode                       featureMode,
      boolean                             withFeatureStats,
      boolean                             withInternalFeatures,
      Integer                             expectedRecordCount,
      Set<SzRecordId>                     expectedRecordIds,
      Integer                             relatedEntityCount,
      Map<String,Integer>                 expectedFeatureCounts,
      Map<String,Set<String>>             primaryFeatureValues,
      Map<String,Set<String>>             duplicateFeatureValues,
      Map<SzAttributeClass, Set<String>>  expectedDataValues,
      Set<String>                         expectedOtherDataValues,
      long                                beforeTimestamp,
      long                                afterTimestamp)
  {
    validateBasics(testInfo,
                   response,
                   httpMethod,
                   selfLink,
                   beforeTimestamp,
                   afterTimestamp);

    SzEntityData entityData = response.getData();

    assertNotNull(entityData, "Response data is null: " + testInfo);

    SzResolvedEntity resolvedEntity = entityData.getResolvedEntity();

    assertNotNull(resolvedEntity, "Resolved entity is null: " + testInfo);

    List<SzRelatedEntity> relatedEntities = entityData.getRelatedEntities();

    assertNotNull(relatedEntities,
                  "Related entities list is null: " + testInfo);

    validateEntity(
        testInfo,
        resolvedEntity,
        relatedEntities,
        forceMinimal,
        featureMode,
        withFeatureStats,
        withInternalFeatures,
        expectedRecordCount,
        expectedRecordIds,
        (withRelated == SzRelationshipMode.NONE),
        (withRelated == SzRelationshipMode.NONE ? 0 : relatedEntityCount),
        (withRelated != SzRelationshipMode.FULL),
        expectedFeatureCounts,
        primaryFeatureValues,
        duplicateFeatureValues,
        expectedDataValues,
        expectedOtherDataValues);

    if (withRaw != null && withRaw) {
      if ((withRelated == FULL) && (forceMinimal == null || !forceMinimal))
      {
        validateRawDataMap(testInfo,
                           response.getRawData(),
                           true,
                           "ENTITY_PATHS", "ENTITIES");

        Object entities = ((Map) response.getRawData()).get("ENTITIES");
        validateRawDataMapArray(testInfo,
                                entities,
                                false,
                                "RESOLVED_ENTITY",
                                "RELATED_ENTITIES");


        if (featureMode == NONE || (forceMinimal != null && forceMinimal)) {
          for (Object entity : ((Collection) entities)) {
            validateRawDataMap(testInfo,
                               ((Map) entity).get("RESOLVED_ENTITY"),
                               false,
                               "ENTITY_ID",
                               "RECORDS");
          }

        } else {
          for (Object entity : ((Collection) entities)) {
            validateRawDataMap(testInfo,
                               ((Map) entity).get("RESOLVED_ENTITY"),
                               false,
                               "ENTITY_ID",
                               "FEATURES",
                               "RECORD_SUMMARY",
                               "RECORDS");
          }
        }


      } else {
        if (withRelated == PARTIAL) {
          validateRawDataMap(testInfo,
                             response.getRawData(),
                             false,
                             "RESOLVED_ENTITY",
                             "RELATED_ENTITIES");
        } else {
          validateRawDataMap(testInfo,
                             response.getRawData(),
                             false,
                             "RESOLVED_ENTITY");
        }

        Object entity = ((Map) response.getRawData()).get("RESOLVED_ENTITY");
        if (featureMode == NONE || (forceMinimal != null && forceMinimal)) {
          validateRawDataMap(testInfo,
                             entity,
                             false,
                             "ENTITY_ID",
                             "RECORDS");

        } else {
          validateRawDataMap(testInfo,
                             entity,
                             false,
                             "ENTITY_ID",
                             "FEATURES",
                             "RECORD_SUMMARY",
                             "RECORDS");
        }
      }
    }
  }

  /**
   * Validate an {@link SzAttributeSearchResponse} instance.
   *
   * @param testInfo The test information describing the test.
   * @param response The response to validate.
   * @param selfLink The expected meta data self link.
   * @param expectedCount The number of expected matching entities for the
   *                      search, or <tt>null</tt> if this is not being
   *                      validated.
   * @param withRelationships <tt>true</tt> if requested with relationship
   *                          information should be included with the entity
   *                          results, <tt>false</tt> or <tt>null</tt> if the
   *                          relationship information should be excluded.
   * @param forceMinimal <tt>true</tt> if requested with minimal data,
   *                     <tt>false</tt> if requested with standard data and
   *                     <tt>null</tt> if this aspect is not being validated.
   * @param featureInclusion The {@link SzFeatureMode} requested or
   *                         <tt>null</tt> if this is not being validated.
   * @param withFeatureStats <tt>true</tt> if request with feature statistics,
   *                         otherwise <tt>false</tt>.
   * @param withInternalFeatures <tt>true</tt> if request with internal features,
   *                            otherwise <tt>false</tt>.
   * @param beforeTimestamp The timestamp before executing the request.
   * @param afterTimestamp The timestamp after executing the request and
   *                       concluding timers on the response.
   * @param expectRawData Whether or not to expect raw data.
   */
  public static void validateSearchResponse(
      String                    testInfo,
      SzAttributeSearchResponse response,
      SzHttpMethod              httpMethod,
      String                    selfLink,
      Integer                   expectedCount,
      Boolean                   withRelationships,
      Boolean                   forceMinimal,
      SzFeatureMode featureInclusion,
      boolean                   withFeatureStats,
      boolean                   withInternalFeatures,
      long                      beforeTimestamp,
      long                      afterTimestamp,
      Boolean                   expectRawData)
  {
    if (expectRawData == null) {
      expectRawData = false;
    }

    validateBasics(
        testInfo, response, selfLink, beforeTimestamp, afterTimestamp);

    SzAttributeSearchResponse.Data data = response.getData();

    assertNotNull(data, "Response data is null: " + testInfo);

    List<SzAttributeSearchResult> results = data.getSearchResults();

    assertNotNull(results, "Result list is null: " + testInfo);

    if (expectedCount != null) {
      assertEquals(expectedCount, results.size(),
                   "Unexpected number of results: " + testInfo);
    }

    for (SzAttributeSearchResult result : results) {

      validateEntity(testInfo,
                     result,
                     result.getRelatedEntities(),
                     forceMinimal,
                     featureInclusion,
                     withFeatureStats,
                     withInternalFeatures,
                     null,
                     null,
                     (withRelationships == null ? false : withRelationships),
                     null,
                     true,
                     null,
                     null,
                     null,
                     null,
                     null);

      Map<String, List<SzSearchFeatureScore>> featureScores
          = result.getFeatureScores();
      assertNotNull(featureScores, "Feature scores was null for entity "
                    + result.getEntityId() + ": " + testInfo);
      if (featureScores.containsKey("NAME")) {
        Integer bestNameScore = result.getBestNameScore();
        assertNotNull(bestNameScore, "Best name score is null for "
            + "entity " + result.getEntityId() + " even though NAME feature "
            + "scores exist (" + featureScores.get("NAME") + "): " + testInfo);

        List<SzSearchFeatureScore> nameScores = featureScores.get("NAME");
        int expectedBestNameScore = -1;
        for (SzSearchFeatureScore nameScore : nameScores) {
          SzNameScoring nameScoringDetails = nameScore.getNameScoringDetails();
          assertEquals(nameScore.getScore(),
                       nameScoringDetails.asFullScore(),
                       "Overall score not equal to overall name score "
                       + "for entity " + result.getEntityId() + " with name "
                       + "scoring details (" + nameScoringDetails
                       + "): " + testInfo);
          Integer fullNameScore = nameScoringDetails.getFullNameScore();
          Integer orgNameScore  = nameScoringDetails.getOrgNameScore();
          if (fullNameScore == null) fullNameScore = -1;
          if (orgNameScore == null) orgNameScore = -1;
          int maxScore = Integer.max(fullNameScore, orgNameScore);
          if (maxScore > expectedBestNameScore) {
            expectedBestNameScore = maxScore;
          }
        }
        assertEquals(bestNameScore, expectedBestNameScore,
          "Unexpected best name score for entity "
              + result.getEntityId() + " with name feature scores ("
              + nameScores + "): " + testInfo);

      }
    }

    if (expectRawData) {
      validateRawDataMap(testInfo,
                              response.getRawData(),
                              false,
                              "RESOLVED_ENTITIES");

      Object entities = ((Map) response.getRawData()).get("RESOLVED_ENTITIES");
      validateRawDataMapArray(testInfo,
                                   entities,
                                   false,
                                   "MATCH_INFO", "ENTITY");
      for (Object obj : ((Collection) entities)) {
        Object matchInfo = ((Map) obj).get("MATCH_INFO");
        validateRawDataMap(testInfo,
                                matchInfo,
                                false,
                                "MATCH_LEVEL",
                                "MATCH_KEY",
                                "MATCH_SCORE",
                                "ERRULE_CODE",
                                "REF_SCORE",
                                "FEATURE_SCORES");
        Object entity = ((Map) obj).get("ENTITY");
        Object resolvedEntity = ((Map) entity).get("RESOLVED_ENTITY");
        if (featureInclusion == NONE || (forceMinimal != null && forceMinimal)) {
          validateRawDataMap(testInfo,
                                  resolvedEntity,
                                  false,
                                  "ENTITY_ID",
                                  "RECORDS");

        } else {
          validateRawDataMap(testInfo,
                                  resolvedEntity,
                                  false,
                                  "ENTITY_ID",
                                  "FEATURES",
                                  "RECORD_SUMMARY",
                                  "RECORDS");

        }
      }

    }
  }

  /**
   * Validates an {@Link SzLoadRecordResponse} instance.
   *
   * @param response The response to validate.
   * @param httpMethod The HTTP method used to load the record.
   * @param dataSourceCode The data source code fo the loaded record.
   * @param expectedRecordId The record ID of the loaded record.
   * @param beforeTimestamp The timestamp before executing the request.
   * @param afterTimestamp The timestamp after executing the request and
   *                       concluding timers on the response.
   */
  public static void validateLoadRecordResponse(
      SzLoadRecordResponse  response,
      SzHttpMethod          httpMethod,
      String                selfLink,
      String                dataSourceCode,
      String                expectedRecordId,
      Boolean               withInfo,
      Boolean               withRaw,
      Integer               expectedAffectedCount,
      Integer               expectedFlaggedCount,
      Set<String>           expectedFlags,
      long                  beforeTimestamp,
      long                  afterTimestamp)
  {
    try {
      String testInfo = "method=[ " + httpMethod + " ], path=[ " + selfLink
          + " ], dataSource=[ " + dataSourceCode + " ], expectedRecordId=[ "
          + expectedRecordId + " ], withInfo=[ " + withInfo + " ], withRaw=[ "
          + withRaw + " ]";

      validateBasics(
          testInfo, response, httpMethod, selfLink, beforeTimestamp, afterTimestamp);

      SzLoadRecordResponse.Data data = response.getData();

      assertNotNull(data, "Response data is null: " + testInfo);

      String recordId = data.getRecordId();

      assertNotNull(recordId, "Record ID is null: " + testInfo);

      if (expectedRecordId != null) {
        assertEquals(expectedRecordId, recordId,
                     "Unexpected record ID value: " + testInfo);
      }

      // if withInfo is null then don't check the info at all (or raw data)
      if (withInfo == null) return;

      // check for info
      SzResolutionInfo info = data.getInfo();
      if (withInfo) {
        assertNotNull(info, "Info requested, but was null: " + testInfo);
      } else {
        assertNull(info, "Info not requested, but was found: " + testInfo);
      }

      if (withInfo) {
        if (expectedRecordId != null) {
          assertEquals(expectedRecordId, info.getRecordId(),
                       "Unexpected record ID in info: " + testInfo);
        }
        if (dataSourceCode != null) {
          assertEquals(dataSourceCode, info.getDataSource(),
                       "Unexpected data source in info: " + testInfo);
        }
        // check the affected entities
        if (expectedAffectedCount != null && expectedAffectedCount > 0) {
          Set<Long> affected = info.getAffectedEntities();
          assertNotNull(affected,
                        "Affected entities set is null: " + testInfo);
          assertEquals(expectedAffectedCount, affected.size(),
                       "Affected entities set is the wrong size: "
                           + affected);
        }

        // check the interesting entites
        if (expectedFlaggedCount != null && expectedFlaggedCount > 0) {
          List<SzFlaggedEntity> flagged = info.getFlaggedEntities();
          assertNotNull(flagged,
                        "Flagged entities list is null: " + testInfo);
          assertEquals(expectedAffectedCount, flagged.size(),
                       "Flagged entities set is the wrong size: "
                           + flagged);

          if (expectedFlags != null && expectedFlags.size() > 0) {
            Set<String> entityFlags = new LinkedHashSet<>();
            for (SzFlaggedEntity flaggedEntity : flagged) {
              entityFlags.addAll(flaggedEntity.getFlags());
            }
            assertEquals(expectedFlags, entityFlags,
                         "Unexpected flags for flagged entities: "
                             + flagged);

            Set<String> recordFlags = new LinkedHashSet<>();
            for (SzFlaggedEntity flaggedEntity : flagged) {
              for (SzFlaggedRecord flaggedRecord : flaggedEntity.getSampleRecords()) {
                recordFlags.addAll(flaggedRecord.getFlags());
              }
            }
            assertEquals(expectedFlags, recordFlags,
                         "Unexpected flags for flagged records: "
                             + flagged);
          }
        }
      }

      // check for raw data
      if (withInfo && withRaw != null) {
        Object rawData = response.getRawData();
        if (withRaw) {
          assertNotNull(rawData, "Raw data requested, but was null: "
              + testInfo);

          validateRawDataMap(
              rawData,
              false,
              "DATA_SOURCE", "RECORD_ID");

          // check the raw data affected entities
          if (expectedAffectedCount != null && expectedAffectedCount > 0) {
            validateRawDataMap(
                rawData,
                false,
                "AFFECTED_ENTITIES");

            Object array = ((Map) response.getRawData()).get("AFFECTED_ENTITIES");
            validateRawDataMapArray(
                testInfo, array, false, "ENTITY_ID", "LENS_CODE");
          }

          // check the raw data interesting entities
          if (expectedFlaggedCount != null && expectedFlaggedCount > 0) {
            validateRawDataMap(
                rawData,
                false,
                "INTERESTING_ENTITIES");

            Object array = ((Map) response.getRawData()).get("INTERESTING_ENTITIES");
            validateRawDataMapArray(
                testInfo, array, false,
                "ENTITY_ID", "LENS_CODE", "DEGREES", "FLAGS",
                "SAMPLE_RECORDS");
          }

        } else {
          assertNull(rawData, "Raw data not requested, but was found: "
              + testInfo);
        }
      }
    } catch (RuntimeException e) {
      e.printStackTrace();
      throw e;
    }
  }

  /**
   * Validates an {@Link SzReevaluateRecordResponse} instance.
   *
   * @param response The response to validate.
   * @param httpMethod The HTTP method used to load the record.
   * @param dataSourceCode The data source code fo the loaded record.
   * @param beforeTimestamp The timestamp before executing the request.
   * @param afterTimestamp The timestamp after executing the request and
   *                       concluding timers on the response.
   */
  public static void validateReevaluateResponse(
      SzReevaluateResponse  response,
      SzHttpMethod          httpMethod,
      String                selfLink,
      Boolean               withInfo,
      Boolean               withRaw,
      String                dataSourceCode,
      String                expectedRecordId,
      Integer               expectedAffectedCount,
      Integer               expectedFlaggedCount,
      Set<String>           expectedFlags,
      long                  beforeTimestamp,
      long                  afterTimestamp)
  {
    try {
      String testInfo = "method=[ " + httpMethod + " ], path=[ " + selfLink
          + " ], dataSource=[ " + dataSourceCode + " ], expectedRecordId=[ "
          + expectedRecordId + " ], withInfo=[ " + withInfo + " ], withRaw=[ "
          + withRaw + " ]";

      validateBasics(
          testInfo, response, httpMethod, selfLink, beforeTimestamp, afterTimestamp);

      SzReevaluateResponse.Data data = response.getData();

      assertNotNull(data, "Response data is null: " + testInfo);

      // if withInfo is null then don't check the info at all (or raw data)
      if (withInfo == null) return;

      // check for info
      SzResolutionInfo info = data.getInfo();
      if (withInfo) {
        assertNotNull(info, "Info requested, but was null: " + testInfo);
      } else {
        assertNull(info, "Info not requested, but was found: " + testInfo);
      }

      if (withInfo) {
        if (expectedRecordId != null) {
          assertEquals(expectedRecordId, info.getRecordId(),
                       "Unexpected record ID in info: " + testInfo);
        }
        if (dataSourceCode != null) {
          assertEquals(dataSourceCode, info.getDataSource(),
                       "Unexpected data source in info: " + testInfo);
        }
        // check the affected entities
        if (expectedAffectedCount != null && expectedAffectedCount > 0) {
          Set<Long> affected = info.getAffectedEntities();
          assertNotNull(affected,
                        "Affected entities set is null: " + testInfo);
          assertEquals(expectedAffectedCount, affected.size(),
                       "Affected entities set is the wrong size: "
                           + affected);
        }

        // check the interesting entites
        if (expectedFlaggedCount != null && expectedFlaggedCount > 0) {
          List<SzFlaggedEntity> flagged = info.getFlaggedEntities();
          assertNotNull(flagged,
                        "Flagged entities list is null: " + testInfo);
          assertEquals(expectedAffectedCount, flagged.size(),
                       "Flagged entities set is the wrong size: "
                           + flagged);

          if (expectedFlags != null && expectedFlags.size() > 0) {
            Set<String> entityFlags = new LinkedHashSet<>();
            for (SzFlaggedEntity flaggedEntity : flagged) {
              entityFlags.addAll(flaggedEntity.getFlags());
            }
            assertEquals(expectedFlags, entityFlags,
                         "Unexpected flags for flagged entities: "
                             + flagged);

            Set<String> recordFlags = new LinkedHashSet<>();
            for (SzFlaggedEntity flaggedEntity : flagged) {
              for (SzFlaggedRecord flaggedRecord : flaggedEntity.getSampleRecords()) {
                recordFlags.addAll(flaggedRecord.getFlags());
              }
            }
            assertEquals(expectedFlags, recordFlags,
                         "Unexpected flags for flagged records: "
                             + flagged);
          }
        }
      }

      // check for raw data
      if (withInfo && withRaw != null) {
        Object rawData = response.getRawData();
        if (withRaw) {
          assertNotNull(rawData, "Raw data requested, but was null: "
              + testInfo);

          validateRawDataMap(
              rawData,
              false,
              "DATA_SOURCE", "RECORD_ID");

          // check the raw data affected entities
          if (expectedAffectedCount != null && expectedAffectedCount > 0) {
            validateRawDataMap(
                rawData,
                false,
                "AFFECTED_ENTITIES");

            Object array = ((Map) response.getRawData()).get("AFFECTED_ENTITIES");
            validateRawDataMapArray(
                testInfo, array, false, "ENTITY_ID", "LENS_CODE");
          }

          // check the raw data interesting entities
          if (expectedFlaggedCount != null && expectedFlaggedCount > 0) {
            validateRawDataMap(
                rawData,
                false,
                "INTERESTING_ENTITIES");

            Object array = ((Map) response.getRawData()).get("INTERESTING_ENTITIES");
            validateRawDataMapArray(
                testInfo, array, false,
                "ENTITY_ID", "LENS_CODE", "DEGREES", "FLAGS",
                "SAMPLE_RECORDS");
          }

        } else {
          assertNull(rawData, "Raw data not requested, but was found: "
              + testInfo);
        }
      }
    } catch (RuntimeException e) {
      e.printStackTrace();
      throw e;
    }
  }


  /**
   * Validates an {@Link SzDeleteRecordResponse} instance.
   *
   * @param response The response to validate.
   * @param httpMethod The HTTP method used to load the record.
   * @param dataSourceCode The data source code fo the loaded record.
   * @param beforeTimestamp The timestamp before executing the request.
   * @param afterTimestamp The timestamp after executing the request and
   *                       concluding timers on the response.
   */
  public static void validateDeleteRecordResponse(
      SzDeleteRecordResponse  response,
      SzHttpMethod            httpMethod,
      String                  selfLink,
      Boolean                 withInfo,
      Boolean                 withRaw,
      String                  dataSourceCode,
      String                  expectedRecordId,
      Integer                 expectedAffectedCount,
      Integer                 expectedFlaggedCount,
      Set<String>             expectedFlags,
      long                    beforeTimestamp,
      long                    afterTimestamp)
  {
    try {
      String testInfo = "method=[ " + httpMethod + " ], path=[ " + selfLink
          + " ], dataSource=[ " + dataSourceCode + " ], expectedRecordId=[ "
          + expectedRecordId + " ], withInfo=[ " + withInfo + " ], withRaw=[ "
          + withRaw + " ]";

      validateBasics(
          testInfo, response, httpMethod, selfLink, beforeTimestamp, afterTimestamp);

      SzDeleteRecordResponse.Data data = response.getData();

      assertNotNull(data, "Response data is null: " + testInfo);

      // if withInfo is null then don't check the info at all (or raw data)
      if (withInfo == null) return;

      // check for info
      SzResolutionInfo info = data.getInfo();
      if (withInfo) {
        assertNotNull(info, "Info requested, but was null: " + testInfo);
      } else {
        assertNull(info, "Info not requested, but was found: " + testInfo);
      }

      if (withInfo) {
        if (expectedRecordId != null) {
          assertEquals(expectedRecordId, info.getRecordId(),
                       "Unexpected record ID in info: " + testInfo);
        }
        if (dataSourceCode != null) {
          assertEquals(dataSourceCode, info.getDataSource(),
                       "Unexpected data source in info: " + testInfo);
        }
        // check the affected entities
        if (expectedAffectedCount != null && expectedAffectedCount > 0) {
          Set<Long> affected = info.getAffectedEntities();
          assertNotNull(affected,
                        "Affected entities set is null: " + testInfo);
          assertEquals(expectedAffectedCount, affected.size(),
                       "Affected entities set is the wrong size: "
                           + affected);
        }

        // check the interesting entites
        if (expectedFlaggedCount != null && expectedFlaggedCount > 0) {
          List<SzFlaggedEntity> flagged = info.getFlaggedEntities();
          assertNotNull(flagged,
                        "Flagged entities list is null: " + testInfo);
          assertEquals(expectedAffectedCount, flagged.size(),
                       "Flagged entities set is the wrong size: "
                           + flagged);

          if (expectedFlags != null && expectedFlags.size() > 0) {
            Set<String> entityFlags = new LinkedHashSet<>();
            for (SzFlaggedEntity flaggedEntity : flagged) {
              entityFlags.addAll(flaggedEntity.getFlags());
            }
            assertEquals(expectedFlags, entityFlags,
                         "Unexpected flags for flagged entities: "
                             + flagged);

            Set<String> recordFlags = new LinkedHashSet<>();
            for (SzFlaggedEntity flaggedEntity : flagged) {
              for (SzFlaggedRecord flaggedRecord : flaggedEntity.getSampleRecords()) {
                recordFlags.addAll(flaggedRecord.getFlags());
              }
            }
            assertEquals(expectedFlags, recordFlags,
                         "Unexpected flags for flagged records: "
                             + flagged);
          }
        }
      }

      // check for raw data
      if (withInfo && withRaw != null) {
        Object rawData = response.getRawData();
        if (withRaw) {
          assertNotNull(rawData, "Raw data requested, but was null: "
              + testInfo);

          validateRawDataMap(
              rawData,
              false,
              "DATA_SOURCE", "RECORD_ID");

          // check the raw data affected entities
          if (expectedAffectedCount != null && expectedAffectedCount > 0) {
            validateRawDataMap(
                rawData,
                false,
                "AFFECTED_ENTITIES");

            Object array = ((Map) response.getRawData()).get("AFFECTED_ENTITIES");
            validateRawDataMapArray(
                testInfo, array, false, "ENTITY_ID", "LENS_CODE");
          }

          // check the raw data interesting entities
          if (expectedFlaggedCount != null && expectedFlaggedCount > 0) {
            validateRawDataMap(
                rawData,
                false,
                "INTERESTING_ENTITIES");

            Object array = ((Map) response.getRawData()).get("INTERESTING_ENTITIES");
            validateRawDataMapArray(
                testInfo, array, false,
                "ENTITY_ID", "LENS_CODE", "DEGREES", "FLAGS",
                "SAMPLE_RECORDS");
          }

        } else {
          assertNull(rawData, "Raw data not requested, but was found: "
              + testInfo);
        }
      }
    } catch (RuntimeException e) {
      e.printStackTrace();
      throw e;
    }
  }


  public static void validateLicenseResponse(
      SzLicenseResponse  response,
      String             selfLink,
      long               beforeTimestamp,
      long               afterTimestamp,
      Boolean            expectRawData,
      String             expectedLicenseType,
      long               expectedRecordLimit)
  {
    if (expectRawData == null) {
      expectRawData = false;
    }

    validateBasics(
        response, selfLink, beforeTimestamp, afterTimestamp, expectRawData);

    SzLicenseResponse.Data data = response.getData();

    assertNotNull(data, "Response data is null");

    SzLicenseInfo licenseInfo = data.getLicense();

    assertNotNull(licenseInfo, "License data is null");

    assertEquals(expectedRecordLimit,
                 licenseInfo.getRecordLimit(),
                 "Record limit wrong");

    assertEquals(expectedLicenseType,
                 licenseInfo.getLicenseType(),
                 "Unexpected license type");

    if (expectRawData) {
      validateRawDataMap(
          response.getRawData(),
          "customer", "contract", "issueDate", "licenseType",
          "licenseLevel", "billing", "expireDate", "recordLimit");

    }
  }

  public static void validateVersionResponse(
      SzVersionResponse  response,
      String             selfLink,
      long               beforeTimestamp,
      long               afterTimestamp,
      Boolean            expectRawData,
      String             repoInitJson)
  {
    if (expectRawData == null) {
      expectRawData = false;
    }

    validateBasics(
        response, selfLink, beforeTimestamp, afterTimestamp, expectRawData);

    SzVersionInfo info = response.getData();

    assertNotNull(info, "Response data is null");

    assertEquals(BuildInfo.MAVEN_VERSION,
                 info.getApiServerVersion(),
                 "API Server Version wrong");

    assertEquals(BuildInfo.REST_API_VERSION,
                 info.getRestApiVersion(),
                 "REST API Version wrong");

    // assume we can reinitialize the product API since it does not really do
    // anything when we initialize it
    G2Product product = NativeApiFactory.createProductApi();
    product.initV2("testApiServer", repoInitJson, false);
    try {
      String versionJson = product.version();

      JsonObject jsonObject = JsonUtils.parseJsonObject(versionJson);
      String expectedVersion = JsonUtils.getString(jsonObject, "VERSION");
      String expectedBuildNum = JsonUtils.getString(jsonObject, "BUILD_NUMBER");

      JsonObject subObject = JsonUtils.getJsonObject(
          jsonObject, "COMPATIBILITY_VERSION");

      String configCompatVers = JsonUtils.getString(subObject,
                                                    "CONFIG_VERSION");

      assertEquals(expectedVersion, info.getNativeApiVersion(),
                   "Native API Version wrong");

      assertEquals(expectedBuildNum, info.getNativeApiBuildNumber(),
                   "Native API Build Number wrong");

      assertEquals(configCompatVers, info.getConfigCompatibilityVersion(),
                   "Native API Config Compatibility wrong");
    } finally {
      product.destroy();

    }
    if (expectRawData) {
      validateRawDataMap(
          response.getRawData(),
          false,
          "VERSION", "BUILD_NUMBER", "BUILD_DATE", "COMPATIBILITY_VERSION");
    }
  }

}
