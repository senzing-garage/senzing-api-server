package com.senzing.util;

import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;
import javax.json.*;

public class JsonUtils {
  /**
   * Private constructor since this class only has static methods.
   */
  private JsonUtils() {
    // do nothing
  }

  /**
   * Gets a String value from the specified {@link JsonObject} using the
   * specified key.  If the specified key is missing or has a null value then
   * <tt>null</tt> is returned, otherwise the {@link String} value is
   * returned.
   *
   * @param obj The {@link JsonObject} to get the value from.
   *
   * @param key The non-null {@link String} key for the value.
   *
   * @return The {@link String} value for the key, or <tt>null</tt> if the
   *         JSON value is <tt>null</tt> or missing.
   */
  public static String getString(JsonObject obj, String key)
  {
    return getString(obj, key, null);
  }

  /**
   * Gets a String value from the specified {@link JsonObject} using the
   * specified key.  If the specified key is missing or has a null value then
   * <tt>null</tt> is returned, otherwise the {@link String} value is
   * returned.
   *
   * @param obj The {@link JsonObject} to get the value from.
   *
   * @param key The non-null {@link String} key for the value.
   *
   * @param defaultValue The value to return if the key is missing or its
   *                     value is <tt>null</tt>.
   *
   * @return The {@link String} value for the key, or the specified default
   *         value if the JSON value is <tt>null</tt> or missing.
   */
  public static String getString(JsonObject obj,
                                 String     key,
                                 String     defaultValue)
  {
    if (obj == null) return defaultValue;
    if (!obj.containsKey(key)) return defaultValue;
    return obj.isNull(key) ? defaultValue : obj.getString(key);
  }

  /**
   * Obtains the {@link JsonArray} for specified key from the specified
   * {@Link JsonObject}.  If there is no value associated with the specified
   * key or if the value is null, then <tt>null</tt> is returned.
   *
   * @param obj The {@link JsonObject} from which to obtain the array.
   *
   * @param key The key for the property to obtain.
   *
   * @return The {@link JsonArray} for the specified key, or <tt>null</tt>
   *         if not found or if the value is null.
   */
  public static JsonArray getJsonArray(JsonObject obj, String key) {
    if (obj == null) return null;
    if (!obj.containsKey(key) || obj.isNull(key)) return null;
    return obj.getJsonArray(key);
  }

  /**
   * Obtains a {@link List} of {@link String} instances from a {@link JsonArray}
   * of {@link JsonString} instances bound to the specified {@link JsonObject}
   * by the specified key.  If the specified key is missing or has a null value
   * then <tt>null</tt> is returned, otherwise a {@link List} containing the
   * {@link String} instances from the array is returned.
   *
   * @param obj The {@link JsonObject} from which to obtain the {@link String}
   *            values.
   *
   * @param key The non-null {@link String} key for the values.
   *
   * @return A {@link List} of {@link String} values for the values found in
   *         the array or <tt>null</tt> if the associated key is missing or its
   *         value is null.
   */
  public static List<String> getStrings(JsonObject obj, String key)
  {
    return getStrings(obj, key, null);
  }

  /**
   * Obtains a {@link List} of {@link String} instances from a {@link JsonArray}
   * of {@link JsonString} instances bound to the specified {@link JsonObject}
   * by the specified key.  If the specified key is missing or has a null value
   * then <tt>null</tt> is returned, otherwise a {@link List} containing the
   * {@link String} instances from the array is returned.
   *
   * @param obj The {@link JsonObject} from which to obtain the {@link String}
   *            values.
   *
   * @param key The non-null {@link String} key for the values.
   *
   * @param defaultValue The value to return if the key is missing or its
   *                     value is <tt>null</tt>.
   *
   * @return A {@link List} of {@link String} values for the values found in
   *         the array or the specified default value if the associated key is
   *         missing or its value is null.
   */
  public static List<String> getStrings(JsonObject    obj,
                                        String        key,
                                        List<String>  defaultValue)
  {
    if (obj == null) return defaultValue;
    if (!obj.containsKey(key) || obj.isNull(key)) return defaultValue;
    return Collections.unmodifiableList(
        obj.getJsonArray(key).getValuesAs(JsonString::getString));
  }

  /**
   * Gets an integer value from the specified {@link JsonObject} using the
   * specified key.  If the specified key is missing or has a null value then
   * <tt>null</tt> is returned, otherwise the {@link Integer} value is
   * returned.
   *
   * @param obj The {@link JsonObject} to get the value from.
   *
   * @param key The non-null {@link String} key for the value.
   *
   * @return The {@link Integer} value for the key, or <tt>null</tt> if the
   *         JSON value is <tt>null</tt> or missing.
   */
  public static Integer getInteger(JsonObject obj, String key)
  {
    return getInteger(obj, key, null);
  }

  /**
   * Gets an integer value from the specified {@link JsonObject} using the
   * specified key.  If the specified key is missing or has a null value then
   * <tt>null</tt> is returned, otherwise the {@link Integer} value is
   * returned.
   *
   * @param obj The {@link JsonObject} to get the value from.
   *
   * @param key The non-null {@link String} key for the value.
   *
   * @param defaultValue The value to return if the key is missing or its
   *                     value is <tt>null</tt>.
   *
   * @return The {@link Integer} value for the key, or the specified default
   *         value if the JSON value is <tt>null</tt> or missing.
   */
  public static Integer getInteger(JsonObject obj,
                                   String     key,
                                   Integer    defaultValue)
  {
    if (obj == null) return defaultValue;
    if (!obj.containsKey(key)) return defaultValue;
    return obj.isNull(key) ? defaultValue : obj.getJsonNumber(key).intValue();
  }

  /**
   * Gets a long integer value from the specified {@link JsonObject} using the
   * specified key.  If the specified key is missing or has a null value then
   * <tt>null</tt> is returned, otherwise the {@link Long} value is
   * returned.
   *
   * @param obj The {@link JsonObject} to get the value from.
   *
   * @param key The non-null {@link String} key for the value.
   *
   * @return The {@link Long} value for the key, or <tt>null</tt> if the
   *         JSON value is <tt>null</tt> or missing.
   */
  public static Long getLong(JsonObject obj, String key)
  {
    return getLong(obj, key, null);
  }

  /**
   * Gets a long integer value from the specified {@link JsonObject} using the
   * specified key.  If the specified key is missing or has a null value then
   * <tt>null</tt> is returned, otherwise the {@link Long} value is
   * returned.
   *
   * @param obj The {@link JsonObject} to get the value from.
   *
   * @param key The non-null {@link String} key for the value.
   *
   * @param defaultValue The value to return if the key is missing or its
   *                     value is <tt>null</tt>.
   *
   * @return The {@link Long} value for the key, or the specified default
   *         value if the JSON value is <tt>null</tt> or missing.
   */
  public static Long getLong(JsonObject obj,
                             String     key,
                             Long       defaultValue)
  {
    if (obj == null) return defaultValue;
    if (!obj.containsKey(key)) return defaultValue;
    return obj.isNull(key) ? defaultValue : obj.getJsonNumber(key).longValue();
  }

  /**
   * Gets a boolean value from the specified {@link JsonObject} using the
   * specified key.  If the specified key is missing or has a null value then
   * <tt>null</tt> is returned, otherwise the {@link Boolean} value is
   * returned.
   *
   * @param obj The {@link JsonObject} to get the value from.
   *
   * @param key The non-null {@link String} key for the value.
   *
   * @return The {@link Boolean} value for the key, or <tt>null</tt> if the
   *         JSON value is <tt>null</tt> or missing.
   */
  public static Boolean getBoolean(JsonObject obj, String key)
  {
    return getBoolean(obj, key, null);
  }

  /**
   * Gets an integer value from the specified {@link JsonObject} using the
   * specified key.  If the specified key is missing or has a null value then
   * <tt>null</tt> is returned, otherwise the {@link Boolean} value is
   * returned.
   *
   * @param obj The {@link JsonObject} to get the value from.
   *
   * @param key The non-null {@link String} key for the value.
   *
   * @param defaultValue The value to return if the key is missing or its
   *                     value is <tt>null</tt>.
   *
   * @return The {@link Boolean} value for the key, or the specified default
   *         value if the JSON value is <tt>null</tt> or missing.
   */
  public static Boolean getBoolean(JsonObject obj,
                                   String     key,
                                   Boolean    defaultValue)
  {
    if (obj == null) return defaultValue;
    if (!obj.containsKey(key)) return defaultValue;
    return obj.isNull(key) ? defaultValue : obj.getBoolean(key);
  }

  /**
   * Gets the {@link JsonValue} for the specified key in the specified object.
   * If the key is not found in the object then <tt>null</tt> is returned.
   *
   * @param obj The {@link JsonObject} from which to obtain the value.
   *
   * @param key The {@link String} key for the property to retrieve.
   *
   * @return The {@link JsonValue} for the key or <tt>null</tt> if the key is
   *         not found.
   */
  public static JsonValue getJsonValue(JsonObject obj, String key) {
    if (obj == null) return null;
    if (!obj.containsKey(key)) return null;
    return obj.getValue("/" + key);
  }

  /**
   * Gets the {@link JsonObject} for the specified key in the specified object.
   * If the key is not found in the object then <tt>null</tt> is returned.
   *
   * @param obj The {@link JsonObject} from which to obtain the value.
   *
   * @param key The {@link String} key for the property to retrieve.
   *
   * @return The {@link JsonObject} for the key or <tt>null</tt> if the key is
   *         not found.
   */
  public static JsonObject getJsonObject(JsonObject obj, String key) {
    if (obj == null) return null;
    if (!obj.containsKey(key)) return null;
    return obj.getJsonObject(key);
  }

  /**
   * Adds the specified key/value pair to the specified {@link
   * JsonObjectBuilder}.  If the specified value is <tt>null</tt> then
   * {@link JsonObjectBuilder#addNull(String)} is used, otherwise
   * {@link JsonObjectBuilder#add(String,String)} is used.
   *
   * @param job The {@link JsonObjectBuilder} to add the key/value pair to.
   *
   * @param key The {@link String} key.
   *
   * @param val The {@link String} value, or <tt>null</tt>.
   */
  public static JsonObjectBuilder add(JsonObjectBuilder job, String key, String val) {
    if (val == null) {
      job.addNull(key);
    } else {
      job.add(key, val);
    }
    return job;
  }

  /**
   * Adds the specified key/value pair to the specified {@link
   * JsonObjectBuilder}.  If the specified value is <tt>null</tt> then
   * {@link JsonObjectBuilder#addNull(String)} is used, otherwise
   * {@link JsonObjectBuilder#add(String,int)} is used.
   *
   * @param job The {@link JsonObjectBuilder} to add the key/value pair to.
   *
   * @param key The {@link String} key.
   *
   * @param val The {@link Integer} value, or <tt>null</tt>.
   */
  public static JsonObjectBuilder add(JsonObjectBuilder job, String key, Integer val) {
    if (val == null) {
      job.addNull(key);
    } else {
      job.add(key, val);
    }
    return job;
  }

  /**
   * Adds the specified key/value pair to the specified {@link
   * JsonObjectBuilder}.  If the specified value is <tt>null</tt> then
   * {@link JsonObjectBuilder#addNull(String)} is used, otherwise
   * {@link JsonObjectBuilder#add(String,long)} is used.
   *
   * @param job The {@link JsonObjectBuilder} to add the key/value pair to.
   *
   * @param key The {@link String} key.
   *
   * @param val The {@link Long} value, or <tt>null</tt>.
   */
  public static JsonObjectBuilder add(JsonObjectBuilder job, String key, Long val) {
    if (val == null) {
      job.addNull(key);
    } else {
      job.add(key, val);
    }
    return job;
  }

  /**
   * Adds the specified key/value pair to the specified {@link
   * JsonObjectBuilder}.  If the specified value is <tt>null</tt> then
   * {@link JsonObjectBuilder#addNull(String)} is used, otherwise
   * {@link JsonObjectBuilder#add(String,double)} is used.
   *
   * @param job The {@link JsonObjectBuilder} to add the key/value pair to.
   *
   * @param key The {@link String} key.
   *
   * @param val The {@link Double} value, or <tt>null</tt>.
   */
  public static JsonObjectBuilder add(JsonObjectBuilder job, String key, Double val) {
    if (val == null) {
      job.addNull(key);
    } else {
      job.add(key, val);
    }
    return job;
  }

  /**
   * Adds the specified key/value pair to the specified {@link
   * JsonObjectBuilder}.  If the specified value is <tt>null</tt> then
   * {@link JsonObjectBuilder#addNull(String)} is used, otherwise
   * {@link JsonObjectBuilder#add(String,double)} is used.
   *
   * @param job The {@link JsonObjectBuilder} to add the key/value pair to.
   *
   * @param key The {@link String} key.
   *
   * @param val The {@link Float} value, or <tt>null</tt>.
   */
  public static JsonObjectBuilder add(JsonObjectBuilder job, String key, Float val) {
    if (val == null) {
      job.addNull(key);
    } else {
      job.add(key, val.doubleValue());
    }
    return job;
  }

  /**
   * Adds the specified key/value pair to the specified {@link
   * JsonObjectBuilder}.  If the specified value is <tt>null</tt> then
   * {@link JsonObjectBuilder#addNull(String)} is used, otherwise
   * {@link JsonObjectBuilder#add(String,BigInteger)} is used.
   *
   * @param job The {@link JsonObjectBuilder} to add the key/value pair to.
   *
   * @param key The {@link String} key.
   *
   * @param val The {@link BigInteger} value, or <tt>null</tt>.
   */
  public static JsonObjectBuilder add(JsonObjectBuilder job, String key, BigInteger val)
  {
    if (val == null) {
      job.addNull(key);
    } else {
      job.add(key, val);
    }
    return job;
  }

  /**
   * Adds the specified key/value pair to the specified {@link
   * JsonObjectBuilder}.  If the specified value is <tt>null</tt> then
   * {@link JsonObjectBuilder#addNull(String)} is used, otherwise
   * {@link JsonObjectBuilder#add(String,BigDecimal)} is used.
   *
   * @param job The {@link JsonObjectBuilder} to add the key/value pair to.
   *
   * @param key The {@link String} key.
   *
   * @param val The {@link BigDecimal} value, or <tt>null</tt>.
   */
  public static JsonObjectBuilder add(JsonObjectBuilder job, String key, BigDecimal val)
  {
    if (val == null) {
      job.addNull(key);
    } else {
      job.add(key, val);
    }
    return job;
  }

  /**
   * Adds the specified key/value pair to the specified {@link
   * JsonObjectBuilder}.  If the specified value is <tt>null</tt> then
   * {@link JsonObjectBuilder#addNull(String)} is used, otherwise
   * {@link JsonObjectBuilder#add(String,boolean)} is used.
   *
   * @param job The {@link JsonObjectBuilder} to add the key/value pair to.
   *
   * @param key The {@link String} key.
   *
   * @param val The {@link Boolean} value, or <tt>null</tt>.
   */
  public static JsonObjectBuilder add(JsonObjectBuilder job, String key, Boolean val) {
    if (val == null) {
      job.addNull(key);
    } else {
      job.add(key, val);
    }
    return job;
  }

  /**
   * Parses JSON text as a {@link JsonObject}.  If the specified text is not
   * formatted as a JSON object then an exception will be thrown.
   *
   * @param jsonText The JSON text to be parsed.
   *
   * @return The parsed {@link JsonObject}.
   */
  public static JsonObject parseJsonObject(String jsonText) {
    if (jsonText == null) return null;
    StringReader sr = new StringReader(jsonText);
    JsonReader jsonReader = Json.createReader(sr);
    return jsonReader.readObject();
  }

  /**
   * Parses JSON text as a {@link JsonArray}.  If the specified text is not
   * formatted as a JSON array then an exception will be thrown.
   *
   * @param jsonText The JSON text to be parsed.
   *
   * @return The parsed {@link JsonObject}.
   */
  public static JsonArray parseJsonArray(String jsonText) {
    if (jsonText == null) return null;
    StringReader sr = new StringReader(jsonText);
    JsonReader jsonReader = Json.createReader(sr);
    return jsonReader.readArray();
  }

  /**
   * Same as {@link #normalizeJsonValue(JsonValue)}, but first parses the
   * specified text as a {@link JsonValue}.
   *
   * @param jsonText The JSON-formatted text to parse.
   *
   * @return The normalized object representation.
   *
   * @see #normalizeJsonValue(JsonValue)
   */
  public static Object normalizeJsonText(String jsonText) {
    if (jsonText == null) return null;
    jsonText = jsonText.trim();
    JsonValue jsonValue = null;
    if ((jsonText.indexOf("{") == 0) || (jsonText.indexOf("[") == 0)) {
      StringReader  sr          = new StringReader(jsonText);
      JsonReader    jsonReader  = Json.createReader(sr);
      jsonValue = jsonReader.read();
    } else if (jsonText.equals("true")) {
      jsonValue = JsonValue.TRUE;
    } else if (jsonText.equals("false")) {
      jsonValue = JsonValue.FALSE;
    } else if (jsonText.equals("null")) {
      jsonValue = JsonValue.NULL;
    } else {
      String harnessText = "{\"value\": " + jsonText + "}";
      StringReader sr         = new StringReader(harnessText);
      JsonReader   jsonReader = Json.createReader(sr);
      JsonObject   jsonObject = jsonReader.readObject();
      jsonValue = jsonObject.getValue("/value");
    }
    return normalizeJsonValue(jsonValue);
  }

  /**
   * Converts the specified {@link JsonValue} to a basic hierarchical object
   * representation.  The returned value depends on the {@link
   * JsonValue.ValueType}.
   * <ul>
   *   <li>{@link JsonValue.ValueType#NULL} yields <tt>null</tt></li>
   *   <li>{@link JsonValue.ValueType#TRUE} yields {@link Boolean#TRUE}</li>
   *   <li>{@link JsonValue.ValueType#FALSE} yields {@link Boolean#FALSE}</li>
   *   <li>{@link JsonValue.ValueType#STRING} yields a {@link String}</li>
   *   <li>{@link JsonValue.ValueType#NUMBER} yields a {@link Long}
   *        or {@link Double}</li>
   *   <li>{@link JsonValue.ValueType#ARRAY} yields a {@link List}</li>
   *   <li>{@link JsonValue.ValueType#OBJECT} yields a {@link Map}</li>
   * </ul>
   *
   * @param jsonValue The {@link JsonValue} to normalize.
   *
   * @return The normalized version of the {@link JsonValue}.
   */
  public static Object normalizeJsonValue(JsonValue jsonValue)
  {
    if (jsonValue == null) return null;
    switch (jsonValue.getValueType()) {
      case NULL: {
        return null;
      }
      case TRUE: {
        return Boolean.TRUE;
      }
      case FALSE: {
        return Boolean.FALSE;
      }
      case NUMBER: {
        JsonNumber jsonNumber = (JsonNumber) jsonValue;
        if (jsonNumber.isIntegral()) {
          return new Long(jsonNumber.longValue());
        } else {
          return new Double(jsonNumber.doubleValue());
        }
      }
      case STRING: {
        JsonString jsonString = (JsonString) jsonValue;
        return jsonString.getString();
      }
      case ARRAY: {
        JsonArray jsonArray = (JsonArray) jsonValue;
        int count = jsonArray.size();
        ArrayList result = new ArrayList(count);
        for (JsonValue jv : jsonArray) {
          result.add(normalizeJsonValue(jv));
        }
        return result;
      }
      case OBJECT: {
        LinkedHashMap<String, Object> result = new LinkedHashMap<>();
        JsonObject jsonObject = (JsonObject) jsonValue;

        for (Map.Entry entry : jsonObject.entrySet()) {
          String    key   = (String) entry.getKey();
          Object    value = normalizeJsonValue((JsonValue) entry.getValue());
          result.put(key, value);
        }
        return result;
      }
      default:
        throw new IllegalStateException(
            "Unrecognized JsonValue.ValueType: " + jsonValue.getValueType());
    }
  }

  /**
   * Converts the specified {@link JsonValue} to a JSON string.
   *
   * @param writer The {@link Writer} to write to.
   *
   * @param jsonValue The {@link JsonValue} describing the JSON.
   *
   * @return The specified {@link Writer}.
   *
   */
  public static <T extends Writer> T toJsonText(T writer, JsonValue jsonValue)
  {
    Objects.requireNonNull(writer, "Writer cannot be null");
    JsonWriter jsonWriter = Json.createWriter(writer);

    if (jsonValue != null) {
      jsonWriter.write(jsonValue);
    } else {
      jsonWriter.write(JsonValue.NULL);
    }

    return writer;
  }

  /**
   * Converts the specified {@link JsonValue} to a JSON string.
   *
   * @param jsonValue The {@link JsonValue} describing the JSON.
   *
   * @return The specified {@link JsonValue} converted to a JSON string.
   */
  public static String toJsonText(JsonValue jsonValue) {
    return JsonUtils.toJsonText(new StringWriter(), jsonValue).toString();
  }

  /**
   * Converts the specified {@link JsonObjectBuilder} to a JSON string.
   *
   * @param writer The {@link Writer} to write to.
   *
   * @param builder The {@link JsonObjectBuilder} describing the object.
   *
   * @return The specified {@link Writer}.
   *
   */
  public static <T extends Writer> T toJsonText(T                 writer,
                                                JsonObjectBuilder builder)
  {
    Objects.requireNonNull(writer, "Writer cannot be null");
    JsonWriter jsonWriter = Json.createWriter(writer);

    JsonObject jsonObject = builder.build();

    jsonWriter.writeObject(jsonObject);

    return writer;
  }

  /**
   * Converts the specified {@link JsonObjectBuilder} to a JSON string.
   *
   * @param builder The {@link JsonObjectBuilder} describing the object.
   *
   * @return The specified {@link JsonObjectBuilder} converted to a JSON string.
   */
  public static String toJsonText(JsonObjectBuilder builder) {
    return JsonUtils.toJsonText(new StringWriter(), builder).toString();
  }

  /**
   * Converts the specified {@link JsonArrayBuilder} to a JSON string.
   *
   * @param writer The {@link Writer} to write to.
   *
   * @param builder The {@link JsonArrayBuilder} describing the object.
   *
   * @return The specified {@link Writer}
   */
  public static <T extends Writer> T toJsonText(T                 writer,
                                                JsonArrayBuilder  builder)
  {
    Objects.requireNonNull(writer, "Writer cannot be null");
    JsonWriter jsonWriter = Json.createWriter(writer);

    JsonArray jsonArray = builder.build();

    jsonWriter.writeArray(jsonArray);

    return writer;
  }

  /**
   * Converts the specified {@link JsonArrayBuilder} to a JSON string.
   *
   * @param builder The {@link JsonArrayBuilder} describing the object.
   *
   * @return The specified {@link JsonArrayBuilder} converted to a JSON string.
   */
  public static String toJsonText(JsonArrayBuilder builder) {
    return JsonUtils.toJsonText(new StringWriter(), builder).toString();
  }


}
