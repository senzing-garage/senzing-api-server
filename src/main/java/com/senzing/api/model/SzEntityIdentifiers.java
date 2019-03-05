package com.senzing.api.model;

import com.senzing.util.JsonUtils;

import javax.json.JsonArray;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.json.JsonValue;
import java.util.*;

/**
 * Used to represent a {@link List} of zero or more {@link SzEntityIdentifier}
 * instances.
 *
 */
public class SzEntityIdentifiers {
  /**
   * The {@link List} of {@link SzEntityIdentifier} instances.
   */
  private List<SzEntityIdentifier> identifiers;

  /**
   * Constructs with no {@link SzEntityIdentifier} instances.
   */
  public SzEntityIdentifiers() throws NullPointerException
  {
    this.identifiers = Collections.emptyList();
  }

  /**
   * Constructs with a single {@link SzEntityIdentifier} instance.
   *
   * @param identifier The single non-null {@link SzEntityIdentifier} instance.
   *
   * @throws NullPointerException If the specified parameter is null.
   */
  public SzEntityIdentifiers(SzEntityIdentifier identifier)
      throws NullPointerException
  {
    Objects.requireNonNull(identifier, "Identifier cannot be null.");
    this.identifiers = Collections.singletonList(identifier);
  }

  /**
   * Constructs with the specified {@link Collection} of {@link
   * SzEntityIdentifier} instances.  The specified {@link Collection} will be
   * copied.
   *
   * @param identifiers The non-null {@link Collection} of {@link
   *                    SzEntityIdentifier} instances.
   *
   * @throws NullPointerException If the specified parameter is null.
   */
  public SzEntityIdentifiers(Collection<SzEntityIdentifier> identifiers)
    throws NullPointerException
  {
    Objects.requireNonNull(identifiers, "Identifiers cannot be null.");
    this.identifiers = Collections.unmodifiableList(
        new ArrayList<>(identifiers));
  }

  /**
   * Private constructor to use when the collection of {@link
   * SzEntityIdentifier} instances may not need to be copied.
   *
   * @param identifiers The {@link List} of {@link SzEntityIdentifier}
   *                    instances.
   *
   * @param copy <tt>true</tt> if the specified list should be copied or
   *             used directly.
   */
  private SzEntityIdentifiers(List<SzEntityIdentifier>  identifiers,
                              boolean                   copy)
  {
    if (copy) {
      if (identifiers == null || identifiers.size() == 0) {
        this.identifiers = Collections.emptyList();
      } else {
        this.identifiers = Collections.unmodifiableList(
            new ArrayList<>(identifiers));
      }
    } else {
      this.identifiers = identifiers;
    }
  }

  /**
   * Checks if all the {@link SzEntityIdentifier} instances contained are of the
   * same type (e.g.: either {@link SzEntityId} or {@link SzRecordId}).
   *
   * @return <tt>true</tt> if the {@link SzEntityIdentifier} instances are
   *         of the same type otherwise <tt>false</tt>.
   */
  public boolean isHomogeneous() {
    Class<? extends SzEntityIdentifier> c = null;
    for (SzEntityIdentifier i : this.identifiers) {
      if (c == null) {
        c = i.getClass();
        continue;
      }
      if (c != i.getClass()) return false;
    }
    return true;
  }

  /**
   * Checks if there are no entity identifiers specified for this instance.
   *
   * @return <tt>true</tt> if no entity identifiers are specified, otherwise
   *         <tt>false</tt>.
   */
  public boolean isEmpty() {
    return (this.identifiers == null || this.identifiers.size() == 0);
  }

  /**
   * Returns the unmodifiable {@link List} of {@link SzEntityIdentifier}
   * instances that were specified.
   *
   * @return The unmodifiable {@link List} of {@link SzEntityIdentifier}
   *         instances that were specified.
   */
  public List<SzEntityIdentifier> getIdentifiers() {
    return this.identifiers;
  }

  /**
   * Parses the specified text as a {@link List} of homogeneous
   * {@link SzEntityIdentifier} instances.
   *
   * @param text The text to parse.
   *
   * @return The {@link SzEntityIdentifiers} instance representing the {@link
   *         List} of {@link SzEntityIdentifier} instances.
   */
  public static SzEntityIdentifiers valueOf(String text) {
    text = text.trim();
    int               length  = text.length();
    char              first   = text.charAt(0);
    char              last    = text.charAt(length-1);

    // check if no identifiers
    if (length == 0) {
      // no identifiers
      return new SzEntityIdentifiers();
    }

    // check if it looks like a JSON array
    if (first == '[' && last == ']') {
      try {
        return parseAsJsonArray(text);

      } catch (RuntimeException e) {
        // ignore
      }
    }

    // check if we have yet to find the identifiers
    if (first == '{' && last == '}') {
      try {
        // it appears we have a JSON object for a single entity identifier
        JsonObject jsonObject = JsonUtils.parseJsonObject(text);
        SzRecordId recordId = SzRecordId.parse(jsonObject);
        return new SzEntityIdentifiers(recordId);

      } catch (RuntimeException e) {
        // ignore
      }
    }

    // check if we have a comma-separated list of entity IDs
    if (text.matches("(-?[\\d]+\\s*,\\s*)+-?[\\d]+")) {
      String[] tokens = text.split("\\s*,\\s*");
      List<SzEntityIdentifier> identifiers = new ArrayList<>(tokens.length);
      for (String token : tokens) {
        token = token.trim();
        identifiers.add(SzEntityId.valueOf(token));
      }
      identifiers = Collections.unmodifiableList(identifiers);
      return new SzEntityIdentifiers(identifiers, false);
    }

    // try to convert it to a JSON array
    if (first != '[' && last != ']') {
      try {
        return parseAsJsonArray("[" + text + "]");

      } catch (RuntimeException e) {
        // ignore
      }
    }

    // try to parse as delimited records -- this assumes data source codes
    // and record IDs do not contain commas
    try {
      return parseAsDelimitedTokens(text);

    } catch (RuntimeException e) {
      // ignore
    }

    // if we get here then check for a failure
    throw new IllegalArgumentException(
        "Unable to interpret the text as a list of entity identifiers: "
        + text);
  }

  /**
   * Parses the specified text as a JSON array.
   *
   * @param text The text to parse
   * @return The {@link SzEntityIdentifiers} that was parsed.
   */
  private static SzEntityIdentifiers parseAsJsonArray(String text) {
    // it appears we have a JSON array of entity identifiers
    JsonArray jsonArray = JsonUtils.parseJsonArray(text);
    List<SzEntityIdentifier> identifiers
        = new ArrayList<>(jsonArray.size());
    JsonValue.ValueType valueType = null;
    for (JsonValue value : jsonArray) {
      JsonValue.ValueType vt = value.getValueType();
      SzEntityIdentifier identifier = null;
      switch (vt) {
        case NUMBER:
          identifier = new SzEntityId(((JsonNumber) value).longValue());
          break;

        case OBJECT:
          identifier = SzRecordId.parse((JsonObject) value);
          break;

        default:
          throw new IllegalArgumentException(
              "Unexpected element in entity identifier array: valueType=[ "
                  + valueType + " ], value=[ " + value + " ]");
      }
      identifiers.add(identifier);
    }

    // make the list unmodifiable
    return new SzEntityIdentifiers(
        Collections.unmodifiableList(identifiers), false);
  }

  /**
   * Parses the specified text as comma-delimited tokens to build an instance
   * of {@link SzEntityIdentifiers}.
   *
   * @param text The text to be parsed.
   * @return The {@link SzEntityIdentifiers} that was parsed.
   */
  private static SzEntityIdentifiers parseAsDelimitedTokens(String text) {
    String[]      rawTokens = text.split("\\s*,\\s*");
    List<String>  tokens    = new ArrayList<>(rawTokens.length);

    String jsonStart = null;
    for (String rawToken : rawTokens) {
      String tok = rawToken.trim();
      // check if we are starting a JSON token
      if (jsonStart == null && tok.startsWith("{") && tok.endsWith("\""))
      {
        // looks like JSON
        jsonStart = rawToken;
        continue;
      }

      // check if we are completing a JSON token
      if (jsonStart != null && tok.startsWith("\"") && tok.endsWith("}")) {
        try {
          String jsonText = jsonStart + "," + rawToken;
          JsonObject obj = JsonUtils.parseJsonObject(jsonText);

          tokens.add(jsonText);

        } catch (Exception e) {
          // not JSON
          tokens.add(jsonStart);
          tokens.add(rawToken);

        } finally {
          jsonStart = null;
        }
        continue;
      }

      // otherwise just take the raw token as-is
      if (jsonStart != null) tokens.add(jsonStart);
      tokens.add(rawToken);
      jsonStart = null;
    }

    List<SzEntityIdentifier> identifiers = new ArrayList<>(tokens.size());
    for (String token : tokens) {
      token = token.trim();
      identifiers.add(SzEntityIdentifier.valueOf(token));
    }
    identifiers = Collections.unmodifiableList(identifiers);
    return new SzEntityIdentifiers(identifiers, false);
  }

  /**
   * Test main function.
   */
  public static void main(String[] args) {
    for (String arg : args) {
      System.out.println();
      System.out.println("- - - - - - - - - - - - - - - - - - - - - ");
      System.out.println("PARSING: " + arg);
      try {
        SzEntityIdentifiers identifiers = SzEntityIdentifiers.valueOf(arg);
        System.out.println(identifiers.getIdentifiers());

      } catch (Exception e) {
        e.printStackTrace(System.out);
      }

    }
  }
}
