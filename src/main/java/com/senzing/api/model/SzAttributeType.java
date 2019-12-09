package com.senzing.api.model;

import com.senzing.util.JsonUtils;

import javax.json.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SzAttributeType {
  /**
   * The unique attribute code identifying this attribute type.
   */
  private String attributeCode;

  /**
   * The default value associated with the attribute type when an attribute
   * value is not provided.
   */
  private String defaultValue;

  /**
   * Describes the {@linkplain SzAttributeNecessity necessity} for an attribute
   * of this type with the associated feature type.
   */
  private SzAttributeNecessity necessity;

  /**
   * The {@linkplain SzAttributeClass attribute class} associated with the
   * attribute.
   */
  private SzAttributeClass attributeClass;

  /**
   * The feature type to which this attribute type belongs (if any).  If this
   * is <tt>null</tt> then it is a stand-alone attribute.
   */
  private String featureType;

  /**
   * Whether or not the attribute type is considered to be "advanced". Advanced
   * attribute types usually require the user to have some knowledge of how the
   * data is mapped in the entity repository (e.g.: "RECORD_ID" or
   * "DATA_SOURCE"). An application may exclude displaying these as options if
   * these things are being auto-generated or automatically selected for the
   * user.
   */
  private boolean advanced;

  /**
   * Whether or not an attribute type that is typically generated internally
   * based on other attribute types.  These are not commonly used by the user
   * except in some rare cases.  Examples include pre-hashed versions of
   * attributes that are hashed.
   */
  private boolean internal;

  /**
   * Default constructor.
   */
  public SzAttributeType() {
    this.attributeCode    = null;
    this.defaultValue     = null;
    this.necessity        = null;
    this.attributeClass   = null;
    this.featureType      = null;
    this.advanced         = false;
    this.internal         = false;
  }

  /**
   * Returns the unique attribute code identifying this attribute type.
   *
   * @return The unique attribute code identifying this attribute type.
   */
  public String getAttributeCode() {
    return attributeCode;
  }

  /**
   * Sets the unique attribute code identifying this attribute type.
   *
   * @param attributeCode The unique attribute code identifying this
   *                      attribute type.
   */
  public void setAttributeCode(String attributeCode) {
    this.attributeCode = attributeCode;
  }

  /**
   * Gets the default value associated with the attribute type when an attribute
   * value is not provided.
   *
   * @return The default value associated with the attribute type when an
   *         attribute value is not provided.
   */
  public String getDefaultValue() {
    return defaultValue;
  }

  /**
   * Sets the default value associated with the attribute type when an attribute
   * value is not provided.
   *
   * @param defaultValue The default value associated with the attribute type
   *                     when an attribute value is not provided.
   */
  public void setDefaultValue(String defaultValue) {
    this.defaultValue = defaultValue;
  }

  /**
   * Gets the {@linkplain SzAttributeNecessity necessity} for an attribute
   * of this type with the associated feature type.
   *
   * @return The {@link SzAttributeNecessity} describing the neccessity.
   */
  public SzAttributeNecessity getNecessity() {
    return necessity;
  }

  /**
   * Sets the {@linkplain SzAttributeNecessity necessity} for an attribute
   * of this type with the associated feature type.
   *
   * @param necessity The {@link SzAttributeNecessity} describing the
   *                  neccessity.
   */
  public void setNecessity(SzAttributeNecessity necessity) {
    this.necessity = necessity;
  }

  /**
   * Gets the {@linkplain SzAttributeClass attribute class} associated with the
   * attribute type.
   *
   * @return The {@link SzAttributeClass} describing the attribute class
   *         associated with the attribute type.
   */
  public SzAttributeClass getAttributeClass() {
    return attributeClass;
  }

  /**
   * Sets the {@linkplain SzAttributeClass attribute class} associated with the
   * attribute type.
   *
   * @param attributeClass The {@link SzAttributeClass} describing the attribute
   *                       class associated with the attribute type.
   */
  public void setAttributeClass(SzAttributeClass attributeClass) {
    this.attributeClass = attributeClass;
  }

  /**
   * Gets the name of feature type to which this attribute type belongs (if
   * any).  If <tt>null</tt> is returned, then the attribute type is stand-alone
   * and not part of a feature type.
   *
   * @return The name of the feature type to which this attribute type belongs,
   *         or <tt>null</tt> if this is a stand-alone attribute type.
   */
  public String getFeatureType() {
    return featureType;
  }

  /**
   * Sets the name of feature type to which this attribute type belongs (if
   * any).  If <tt>null</tt> is specified, then the attribute type is
   * stand-alone and not part of a feature type.
   *
   * @param featureType The name of the feature type to which this attribute
   *                    type belongs, or <tt>null</tt> if this is a stand-alone
   *                    attribute type.
   */
  public void setFeatureType(String featureType) {
    this.featureType = featureType;
  }

  /**
   * Checks whether or not the attribute type is considered to be "advanced".
   * Advanced attribute types usually require the user to have some knowledge
   * of how the data is mapped in the entity repository (e.g.: "RECORD_ID" or
   * "DATA_SOURCE"). An application may exclude displaying these as options if
   * these things are being auto-generated or automatically selected for the
   * user.
   *
   * @return <tt>true</tt> if this attribute type is advanced, otherwise
   *         <tt>false</tt>
   */
  public boolean isAdvanced() {
    return advanced;
  }

  /**
   * Sets whether or not the attribute type is considered to be "advanced".
   * Advanced attribute types usually require the user to have some knowledge
   * of how the data is mapped in the entity repository (e.g.: "RECORD_ID" or
   * "DATA_SOURCE"). An application may exclude displaying these as options if
   * these things are being auto-generated or automatically selected for the
   * user.
   *
   * @param advanced <tt>true</tt> if this attribute type is advanced,
   *                 otherwise <tt>false</tt>.
   */
  public void setAdvanced(boolean advanced) {
    this.advanced = advanced;
  }

  /**
   * Checks whether or not an attribute type that is typically generated
   * internally based on other attribute types.  These are not commonly used by
   * the user except in some rare cases.  Examples include pre-hashed versions
   * of attributes that are hashed.
   *
   * @return <tt>true</tt> if this attribute type is internal, otherwise
   *         <tt>false</tt>.
   */
  public boolean isInternal() {
    return internal;
  }

  /**
   * Sets whether or not an attribute type that is typically generated
   * internally based on other attribute types.  These are not commonly used by
   * the user except in some rare cases.  Examples include pre-hashed versions
   * of attributes that are hashed.
   *
   * @param internal <tt>true</tt> if this attribute type is internal,
   *                 otherwise <tt>false</tt>.
   */
  public void setInternal(boolean internal) {
    this.internal = internal;
  }

  @Override
  public String toString() {
    return "SzAttributeType{" +
        "attributeCode='" + attributeCode + '\'' +
        ", defaultValue='" + defaultValue + '\'' +
        ", necessity=" + necessity +
        ", attributeClass=" + attributeClass +
        ", featureType='" + featureType + '\'' +
        ", advanced=" + advanced +
        ", internal=" + internal +
        '}';
  }

  /**
   * Parses a list of entity data instances from a {@link JsonArray}
   * describing a JSON array in the Senzing native API format for entity
   * features and populates the specified {@link List} or creates a new
   * {@link List}.
   *
   * @param list The {@link List} of {@link SzAttributeType} instances to
   *             populate, or <tt>null</tt> if a new {@link List}
   *             should be created.
   *
   * @param jsonArray The {@link JsonArray} describing the JSON in the
   *                  Senzing native API format.
   *
   * @return The populated (or created) {@link List} of {@link
   *         SzAttributeType} instances.
   */
  public static List<SzAttributeType> parseAttributeTypeList(
      List<SzAttributeType> list,
      JsonArray             jsonArray)
  {
    if (list == null) {
      list = new ArrayList<>(jsonArray.size());
    }
    for (JsonObject jsonObject : jsonArray.getValuesAs(JsonObject.class)) {
      list.add(parseAttributeType(null, jsonObject));
    }
    return list;
  }

  /**
   * Parses the attribute type data from a {@link JsonObject} describing
   * JSON from the Senzing CFG_ATTR native config format for an attribute type
   * and populates the specified {@link SzAttributeType} or creates a new
   * instance.
   *
   * @param attributeType The {@link SzAttributeType} instance to populate, or
   *                      <tt>null</tt> if a new instance should be created.
   *
   * @param jsonObject The {@link JsonObject} describing the JSON in the
   *                   Senzing native CFG_ATTR config format.
   *
   * @return The populated (or created) {@link SzAttributeType}.
   */
  public static SzAttributeType parseAttributeType(
      SzAttributeType   attributeType,
      JsonObject        jsonObject)
  {
    if (attributeType == null) attributeType = new SzAttributeType();

    String  attrCode     = jsonObject.getString("ATTR_CODE");
    String  defaultValue = JsonUtils.getString(jsonObject, "DEFAULT_VALUE");
    String  felemReq     = jsonObject.getString("FELEM_REQ");
    String  rawAttrClass  = jsonObject.getString("ATTR_CLASS");
    boolean internal      = interpretBoolean(jsonObject, "INTERNAL");
    String  ftypeCode     = JsonUtils.getString(jsonObject, "FTYPE_CODE");
    boolean advanced      = interpretBoolean(jsonObject,"ADVANCED");

    if (ftypeCode != null && ftypeCode.trim().length() == 0) {
      ftypeCode = null;
    }

    attributeType.setAttributeCode(attrCode);
    attributeType.setDefaultValue(defaultValue);
    attributeType.setNecessity(
        SzAttributeNecessity.parseAttributeNecessity(felemReq));
    attributeType.setAttributeClass(
        SzAttributeClass.parseAttributeClass(rawAttrClass));
    attributeType.setInternal(internal);
    attributeType.setFeatureType(ftypeCode);
    attributeType.setAdvanced(advanced);

    return attributeType;
  }

  /**
   * Interprets a 0/1, true/false, "Yes"/"No" or "true"/"false" value from
   * a JSON object attribute as a boolean value.
   *
   * @param jsonObject The {@link JsonObject} from which to extract the value.
   *
   * @param key The key for extracting the value from the {@link JsonObject}
   *
   * @return <tt>true</tt> or <tt>false</tt> depending on the interpreted value.
   */
  public static boolean interpretBoolean(JsonObject jsonObject, String key) {
    JsonValue jsonValue = jsonObject.getValue("/" + key);
    switch (jsonValue.getValueType()) {
      case NUMBER:
      {
        int num = ((JsonNumber) jsonValue).intValue();
        return (num != 0);
      }
      case TRUE:
        return true;
      case FALSE:
        return false;
      case STRING:
      {
        String text = ((JsonString) jsonValue).getString();
        if ("YES".equalsIgnoreCase(text))   return true;
        if ("TRUE".equalsIgnoreCase(text))  return true;
        return false;
      }
      default:
        throw new IllegalArgumentException(
            "The JsonValue does not appear to be a boolean: "
            + jsonValue.toString());
    }
  }
}
