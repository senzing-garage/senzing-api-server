package com.senzing.util;

import java.util.*;

/**
 * Represents a semantic version described as a {@link String} containing
 * integer numbers separated by decimal points (e.g.: "1.4.5").
 */
public class SemanticVersion implements Comparable<SemanticVersion >{
  /**
   * The {@link List} of version parts.
   */
  private List<Integer> versionParts;

  /**
   * The version string representation.
   */
  private String versionString;

  /**
   * The normalized version of the {@link String} for calculating the hash code.
   */
  private String normalizedString;

  /**
   * Constructs with the specified version string (e.g.: "1.4.5").
   *
   * @param versionString The version string with which to construct.
   *
   * @throws NullPointerException If the specified parameter is <tt>null</tt>.
   *
   * @throws IllegalArgumentException If the specified parameter is not properly
   *                                  formated.
   */
  public SemanticVersion(String versionString)
    throws NullPointerException, IllegalArgumentException
  {
    Objects.requireNonNull(
        versionString, "Version string cannot be null");

    try {
      String[] tokens = versionString.split("\\.");
      this.versionParts = new ArrayList<>(tokens.length);
      for (String token : tokens) {
        int part = Integer.parseInt(token);
        if (part < 0) {
          throw new IllegalArgumentException(
              "Negative version part is not allowed: " + part);
        }
        this.versionParts.add(part);
      }

      // create a normalized list of version parts by removing trailing zeroes
      List<Integer> normalized = new LinkedList<>(this.versionParts);
      Collections.reverse(normalized);
      Iterator<Integer> iter = normalized.iterator();
      while (iter.hasNext()) {
        Integer part = iter.next();
        if (!part.equals(0)) break;
        iter.remove();
      }
      Collections.reverse(normalized);

      // set the version strings
      StringBuilder versionSB     = new StringBuilder();
      String        prefix        = "";
      for (Integer part: this.versionParts) {
        versionSB.append(prefix).append(part);
        prefix = ".";
      }
      this.versionString = versionSB.toString();

      StringBuilder normalizedSB  = new StringBuilder();
      prefix = "";
      for (Integer part: normalized) {
        normalizedSB.append(prefix).append(part);
        prefix = ".";
      }
      this.normalizedString = normalizedSB.toString();

    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Invalid semantic version string: " + versionString);
    }
  }

  /**
   * Overridden to return <tt>true</tt> if and only if the specified parameter
   * is a non-null reference to an object of the same class with equivalent
   * version parts.
   *
   * @param object The object to compare with.
   *
   * @return <tt>true</tt> if the objects are equal, otherwise <tt>false</tt>
   */
  @Override
  public boolean equals(Object object) {
    if (object == null) return false;
    if (this == object) return true;
    if (this.getClass() != object.getClass()) return false;
    SemanticVersion version = (SemanticVersion) object;
    return (this.compareTo(version) == 0);
  }

  /**
   * Implemented to return a hash code that is consistent with the {@link
   * #equals(Object)} implementation.
   *
   * @return The hash code for this instance.
   */
  @Override
  public int hashCode() {
    return this.normalizedString.hashCode();
  }

  /**
   * Implemented to compare the parts of the semantic version in order.
   */
  @Override
  public int compareTo(SemanticVersion other) {
    Objects.requireNonNull(
        other, "The specified parameter cannot be null");
    Iterator<Integer> iter1 = this.versionParts.iterator();
    Iterator<Integer> iter2 = other.versionParts.iterator();

    // iterate over the parts
    while (iter1.hasNext() || iter2.hasNext()) {
      // get the next version parts
      Integer part1 = iter1.hasNext() ? iter1.next() : 0;
      Integer part2 = iter2.hasNext() ? iter2.next() : 0;

      // get the diff between the parts
      int diff = part1 - part2;

      // if the diff is non-zero then return it
      if (diff != 0) return diff;
    }

    // if we have exhausted all version parts without a difference then we have
    // equality
    return 0;
  }

  /**
   * Returns the version string for this instance.  This is equivalent to
   * calling {@link #toString(boolean)} with <tt>false</tt> as the parameter.
   *
   * @return The version string for this instance.
   */
  @Override
  public String toString() {
    return this.toString(false);
  }

  /**
   * Returns a version string for this instance that is optionally nornmalized
   * to remove trailing zeroes.
   *
   * @param normalized <tt>true</tt> if trailing zeroes should be stripped,
   *                   otherwise <tt>false</tt>.
   *
   * @return A {@link String} representation of this instnace that is optionally
   *         normalized to remove trailing zeroes.
   */
  public String toString(boolean normalized) {
    return (normalized) ? this.normalizedString : this.versionString;
  }

  /**
   * Provides a test main method.
   */
  public static void main(String[] args) {
    if (args.length == 0) {
      System.err.println("USAGE: java " + SemanticVersion.class.getName()
                         + " <version> [compare-version]*");
      System.exit(1);
    }
    SemanticVersion version = new SemanticVersion(args[0]);
    System.out.println(
        "VERSION: " + version + " (" + version.toString(true) + ")");
    for (int index = 1; index < args.length; index++) {
      SemanticVersion version2 = new SemanticVersion(args[index]);
      System.out.println();
      System.out.println(
          "VERSUS " + version2 + " (" + version2.toString(true)
          + "): "
          + "COMPARE (" + version.compareTo(version2) + ") "
          + " / EQUALS (" + version.equals(version2) + ") "
          + " / HASH (" + version2.hashCode() + ") "
          + " / HASH-EQUALITY (" + (version.hashCode() == version2.hashCode())
          + ")");
    }
  }
}
