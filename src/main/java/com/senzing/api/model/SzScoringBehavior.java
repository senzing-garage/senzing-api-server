package com.senzing.api.model;

import java.util.Objects;

/**
 * Describes the scoring behavior for features of a given feature type.
 */
public class SzScoringBehavior {
  /**
   * The code for the scoring behavior.
   */
  private String code;

  /**
   * The scoring frequency for the behavior.
   */
  private SzScoringFrequency frequency;

  /**
   * Whether or not the feature is considered exclusive for scoring purposes.
   */
  private boolean exclusive;

  /**
   * Whether or not the feature is considered stable for scoring purposes.
   */
  private boolean stable;

  /**
   * Constructs with the specified {@link SzScoringFrequency}, exclusivity
   * and stability flags.
   *
   * @param frequency The {@link SzScoringFrequency} for the scoring behavior.
   * @param exclusive <tt>true</tt> if the feature value is considered
   *                  exclusive, and <tt>false</tt> if not.
   * @param stable <tt>true</tt> if the feature value is considered stable,
   *               and <tt>false</tt> if not.
   */
  public SzScoringBehavior(SzScoringFrequency frequency,
                           boolean            exclusive,
                           boolean            stable)
  {
    if (frequency == null) {
      throw new NullPointerException(
          "The specified frequency cannot be null");
    }
    this.code       = computeCode(frequency, exclusive, stable);
    this.frequency  = frequency;
    this.exclusive  = exclusive;
    this.stable     = stable;
  }

  /**
   * Private default constructor for JSON marshalling.
   */
  private SzScoringBehavior() {
    this.code       = null;
    this.frequency  = null;
    this.exclusive  = false;
    this.stable     = false;
  }

  /**
   * Parses the specified text as an {@link SzScoringBehavior}.
   *
   * @param text The text to be parsed.
   *
   * @return The {@link SzScoringBehavior} for the text.
   *
   * @throws IllegalArgumentException If the specified text does not match
   *                                  a known scoring behavior.
   */
  public static SzScoringBehavior parse(String text)
    throws IllegalArgumentException
  {
    String origText = text;
    text = text.trim().toUpperCase();
    SzScoringFrequency frequency = null;
    for (SzScoringFrequency freq : SzScoringFrequency.values()) {
      if (text.startsWith(freq.code())) {
        frequency = freq;
        break;
      }
    }
    if (frequency == null) return null;

    // check if there is remaining text
    int length = frequency.code().length();
    text = (length < text.length()) ? text.substring(length) : "";
    boolean exclusive = false;
    boolean stable    = false;

    switch (text) {
      case "ES":
        stable    = true;
      case "E":
        exclusive = true;
        break;
      case "":
        exclusive = false;
        stable    = false;
        break;
      default:
        throw new IllegalArgumentException(
            "Unrecognized scoring behavior: " + origText);
    }

    // create the instance
    return new SzScoringBehavior(frequency, exclusive, stable);
  }

  /**
   * Computers the scoring behavior code from the specified {@link
   * SzScoringFrequency}, exclusivity flag and stability flag.
   *
   * @param frequency The {@link SzScoringFrequency} for the scoring behavior.
   * @param exclusive <tt>true</tt> if the feature value is considered
   *                  exclusive, and <tt>false</tt> if not.
   * @param stable <tt>true</tt> if the feature value is considered stable,
   *               and <tt>false</tt> if not.
   *
   * @return The scoring behavior code.
   */
  private static String computeCode(SzScoringFrequency  frequency,
                                    boolean             exclusive,
                                    boolean             stable)
  {
    StringBuilder sb = new StringBuilder();
    sb.append(frequency.code());

    if (exclusive) sb.append("E");
    if (stable)    sb.append("S");

    return sb.toString();
  }

  /**
   * Returns the code for the scoring behavior.
   *
   * @return The code for the scoring behavior.
   */
  public String getCode() {
    return this.code;
  }

  /**
   * Private setter for JSON marshalling.
   */
  private void setCode(String code) {
    this.code = code;
  }

  /**
   * Gets the {@link SzScoringFrequency} for the scoring behavior.
   *
   * @return The {@link SzScoringFrequency} for the scoring behavior.
   */
  public SzScoringFrequency getFrequency() {
    return this.frequency;
  }

  /**
   * Private setter for JSON marshalling.
   */
  private void setFrequency(SzScoringFrequency frequency) {
    this.frequency = frequency;
  }

  /**
   * Checks if the scoring behavior is exclusive.
   *
   * @return <tt>true</tt> if the scoring behavior is exclusive,
   *         otherwise <tt>false</tt>
   */
  public boolean isExclusive() {
    return this.exclusive;
  }

  /**
   * Private setter for JSON marshalling.
   */
  private void setExclusive(boolean exclusive) {
    this.exclusive = exclusive;
  }

  /**
   * Checks if the scoring behavior is stable.
   *
   * @return <tt>true</tt> if the scoring behavior is stable,
   *         otherwise <tt>false</tt>
   */
  public boolean isStable() {
    return this.stable;
  }

  /**
   * Private setter for JSON marshalling.
   */
  private void setStable(boolean stable) {
    this.exclusive = stable;
  }

  /**
   * Implemented to check if two scoring behavior instances are equal.
   *
   * @param object The object to compare against.
   *
   * @return <tt>true</tt> if the objects are equal, otherwise <tt>false</tt>.
   */
  public boolean equals(Object object) {
    if (object == null) return false;
    if (this == object) return true;
    if (this.getClass() != object.getClass()) return false;
    SzScoringBehavior behavior = (SzScoringBehavior) object;
    return (this.getFrequency().equals(behavior.getFrequency())
            && (this.isExclusive() == behavior.isExclusive())
            && (this.isStable() == behavior.isStable()));
  }

  /**
   * Implemented to implement a hash code that is consistent with the
   * {@link #equals(Object)} implementation.
   *
   * @return The hash code for this instance.
   */
  public int hashCode() {
    return Objects.hash(this.getFrequency(),
                        this.isExclusive(),
                        this.isStable());
  }

  /**
   * Implemented to return the text code for this instance.
   *
   * @return The text code for this instance.
   */
  public String toString() {
    return this.getCode();
  }
}
