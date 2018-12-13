package com.senzing.api.util;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class WorkbenchUtilities {
  private static final String FILENAME_DATE_TIME_FORMAT = "yyyyMMdd_HHmmss";

  private WorkbenchUtilities() {
    // do nothing
  }

  /**
   * Creates a unique name that can be used for temporary directories or filenames.
   * @param projectId The Project ID
   * @param exportType What is included in the file/directory.  (e.g. logs, search)
   * @return A unique string
   */
  public static String generateTimestampedName(long projectId, final String exportType) {
    Instant requestTime = Instant.now();
    String osCompatibleTimeString = DateTimeFormatter.ofPattern(FILENAME_DATE_TIME_FORMAT)
        .withZone(ZoneId.systemDefault())
        .format(requestTime);
    return "senzing-" + exportType + "-project_" + projectId + "-" + osCompatibleTimeString;
  }


}
