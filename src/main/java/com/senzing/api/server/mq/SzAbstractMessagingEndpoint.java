package com.senzing.api.server.mq;

import com.senzing.api.services.SzMessageSink;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.*;

import static com.senzing.io.IOUtilities.UTF_8;

/**
 * Provides an abstract implementation of {@link SzMessagingEndpoint}.
 */
public abstract class SzAbstractMessagingEndpoint
    implements SzMessagingEndpoint
{
  /**
   * The {@link SzMessageSink} interface to this instance (cached so it does not
   * have to be recreated each time it is created).
   */
  private SzMessageSink sink;

  /**
   * Default constructor.
   */
  protected SzAbstractMessagingEndpoint() {
    this.sink = (SzAbstractMessagingEndpoint.this::send);
  }

  /**
   * Returns the underlying {@link SzMessageSink} interface to this instance.
   *
   * @return The underlying {@link SzMessageSink} interface to this instance.
   */
  @Override
  public SzMessageSink asMessageSink() {
    return this.sink;
  }

  /**
   * Parses the specified query string into name/value pairs that are added to
   * the specified {@link Map}.
   *
   * @param queryText The query string to parse.
   */
  protected static Map<String, List<String>> parseQueryString(String queryText)
  {
    Map<String, List<String>> map = new LinkedHashMap<>();
    if (queryText != null) {
      queryText = queryText.substring(1);
      String[] pairs = queryText.split("&");
      for (String pair: pairs) {
        int index = pair.indexOf("=");
        String key = pair;
        String value = "";
        if (index >= 0) {
          key = pair.substring(0, index);
          if (index < pair.length() - 1) {
            value = pair.substring(index + 1);
          }
        }

        // URL decode the values
        try {
          key = URLDecoder.decode(key, UTF_8);
          value = URLDecoder.decode(value, UTF_8);

        } catch (UnsupportedEncodingException cannotHappen) {
          throw new IllegalStateException("UTF-8 Encoding Not Supported");
        }

        // add the value to the properties map
        List<String> list = map.get(key);
        if (list == null) {
          list = new LinkedList<>();
          map.put(key, list);
        }
        list.add(value);
      }
    }

    // return the map
    return map;
  }
}
