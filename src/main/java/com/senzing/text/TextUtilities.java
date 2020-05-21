package com.senzing.text;

import com.senzing.api.model.SzRecordId;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TextUtilities {
  /**
   * The random number generator to use for generating encryption keys.
   */
  private static final SecureRandom PRNG = new SecureRandom();

  /**
   * The minimum character in the range for generating random characters.
   */
  private static final int MIN_CHAR = (int) '!';

  /**
   * The maximum character in the range for generating random characters.
   */
  private static final int MAX_CHAR = (int) '~';

  /**
   * A list of printable characters.
   */
  private static final List<Character> PRINTABLE_CHARS;

  /**
   * A list of alphabetic characters.
   */
  private static final List<Character> ALPHA_CHARS;

  /**
   * A list of alpha-numeric characters.
   */
  private static final List<Character> ALPHANUMERIC_CHARS;

  static {
    try {
      int capacity = (MAX_CHAR - MIN_CHAR) + 1;
      List<Character> printableChars  = new ArrayList<>(capacity);
      List<Character> alphaChars      = new ArrayList<>(capacity);
      List<Character> alphaNumChars   = new ArrayList<>(capacity);

      for (int index = MIN_CHAR; index <= MAX_CHAR; index++) {
        Character c = (char) index;
        printableChars.add(c);
        if (Character.isAlphabetic(c)) {
          alphaChars.add(c);
          alphaNumChars.add(c);
        } else if (Character.isDigit(c)) {
          alphaNumChars.add(c);
        }
      }

      PRINTABLE_CHARS     = Collections.unmodifiableList(printableChars);
      ALPHA_CHARS         = Collections.unmodifiableList(alphaChars);
      ALPHANUMERIC_CHARS  = Collections.unmodifiableList(alphaNumChars);

    } catch (Exception e) {
      e.printStackTrace();
      throw new ExceptionInInitializerError(e);
    }
  }

  /**
   * Default constructor.
   */
  private TextUtilities() {
    // do nothing
  }

  /**
   * Utility function to generate random printable non-whitespace ASCII text
   * of a specified length.
   *
   * @param count The number of characters to generate.
   *
   * @return The generated text.
   */
  public static String randomPrintableText(int count) {
    return randomText(count, PRINTABLE_CHARS);
  }

  /**
   * Utility function to generate random ASCII alphabetic text of a specified
   * length.
   *
   * @param count The number of characters to generate.
   *
   * @return The generated text.
   */
  public static String randomAlphabeticText(int count) {
    return randomText(count, ALPHA_CHARS);
  }

  /**
   * Utility function to generate random ASCII alpha-numeric text of a
   * specified length.
   *
   * @param count The number of characters to generate.
   *
   * @return The generated text.
   */
  public static String randomAlphanumericText(int count) {
    return randomText(count, ALPHANUMERIC_CHARS);
  }

  /**
   * Internal functions to generate random text given a list of allowed
   * characters and a count of the number of desired characters.
   */
  private static String randomText(int count, List<Character> allowedChars) {
    StringBuilder sb = new StringBuilder();
    int max = allowedChars.size();
    for (int index = 0; index < count; index++) {
      sb.append(allowedChars.get(PRNG.nextInt(max)));
    }
    return sb.toString();
  }
}
