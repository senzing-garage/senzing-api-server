package com.senzing.api.model;

import com.senzing.util.JsonUtils;
import org.glassfish.json.JsonUtil;

import javax.ws.rs.PathParam;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;

/**
 * A tagging interface for entity identifiers.
 */
public interface SzEntityIdentifier {
  /**
   * Implemented to return either an instance of {@link SzRecordId}
   * or {@link SzEntityId}.
   *
   * @param text The text to parse.
   *
   * @return The {@link SzEntityIdentifier} for the specified text.
   */
  static SzEntityIdentifier valueOf(String text) {
    if (text.matches("-?[\\d]+")) {
      return SzEntityId.valueOf(text);
    } else {
      return SzRecordId.valueOf(text);
    }
  }
}
