package com.senzing.api.model;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Enumerates the various methods for retrieving features for entities.
 *
 */
public enum SzFeatureMode {
  /**
   * Do not include any feature values.  This option provides the fastest
   * performance because no feature values need to be retrieved.
   */
  NONE,

  /**
   * Include only a single representative value per "unique" value of a feature.
   * If there are multiple values that are near duplicates then only one value
   * is included and the others are suppressed.
   */
  REPRESENTATIVE,

  /**
   * Group near-duplicate feature values and return a representative value along
   * with its near duplicate values.
   */
  WITH_DUPLICATES;
}
