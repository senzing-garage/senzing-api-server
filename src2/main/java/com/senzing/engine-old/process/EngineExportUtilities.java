package com.senzing.api.engine.process;

import com.senzing.util.AccessToken;
import com.senzing.api.ResultsExport;
import com.senzing.api.ServerError;
import com.senzing.api.Workbench;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.senzing.g2.engine.G2Engine.*;
import static com.senzing.api.engine.process.EngineProcess.*;
import static com.senzing.api.ServerErrorLog.recordProjectError;
import static com.senzing.api.Workbench.*;

public class EngineExportUtilities {
  private static final Map<Long, Map<String, ExportReader>> EXPORT_READERS = new HashMap<>();
  private static final Set<Long> LOADED_PROJECTS = new HashSet<>();
  private static final Pattern FILENAME_FORMAT_LEVEL = Pattern.compile("entity-report-level-([1234])\\.(csv|json)\\.zip");
  private static final Pattern FILENAME_FORMAT_FLAGS = Pattern.compile("entity-report-flags-(\\d+)\\.(csv|json)\\.zip");
  static final String EXPORT_SUBDIR = "export-cache";

  static Map<String,ExportReader> getExportReaders(long projectId) {
    synchronized (EXPORT_READERS) {
      Map<String,ExportReader> map = EXPORT_READERS.get(projectId);
      if (map == null) {
        map = new HashMap();
        EXPORT_READERS.put(projectId, map);
      }
      return map;
    }
  }

  public static List<ResultsExport> getResultExports(long projectId, AccessToken token) {
    Map<String,ExportReader> projectReaders = getExportReaders(projectId);

    loadExistingExportFiles(projectId);
    synchronized (projectReaders) {
      return projectReaders.values().stream()
          .map(r -> r.getExport(token))
          .collect(Collectors.toList());
    }
  }

  static void unloadExistingExports(long projectId) {
    synchronized (EXPORT_READERS) {
      EXPORT_READERS.remove(projectId);
      LOADED_PROJECTS.remove(projectId);
    }
  }

  static void deleteExistingExportFiles(long projectId) {
    try {
      Path legacyExportDir = Workbench.getProjectDirectory(projectId).toPath();
      if (legacyExportDir.toFile().exists()) {
        Files.list(legacyExportDir)
            .filter(p -> p.toString().contains("entity-report-level"))
            .map(Path::toFile)
            .forEach(f -> {
              if (f.exists()) f.delete();
            });
      }

      Path exportDir = legacyExportDir.resolve(EXPORT_SUBDIR);
      if (exportDir.toFile().exists()) {
        Files.list(exportDir)
            .filter(p -> p.toString().contains("entity-report-flags"))
            .map(Path::toFile)
            .forEach(f -> {
              if (f.exists()) f.delete();
            });
      }

      unloadExistingExports(projectId);

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static void loadExistingExportFiles(long projectId) {
    try {
      synchronized (EXPORT_READERS) {
        if (LOADED_PROJECTS.contains(projectId)) return;
      }

      Map<String,ExportReader> projectReaders = getExportReaders(projectId);

      Path legacyExportDir = Workbench.getProjectDirectory(projectId).toPath();
      Files.list(legacyExportDir)
          .filter(p -> p.toString().contains("entity-report-level"))
          .map(Path::toFile)
          .map(f -> asExportReader(projectId, f))
          .forEach(r -> {
            synchronized (projectReaders) {
              projectReaders.put(r.getKey(), r);
              r.start();
            }
          });

      Path exportDir = legacyExportDir.resolve(EXPORT_SUBDIR);
      if (!exportDir.toFile().exists()) {
        exportDir.toFile().mkdirs();
      }
      Files.list(exportDir)
          .filter(p -> p.toString().contains("entity-report-flags"))
          .map(Path::toFile)
          .map(f -> asExportReader(projectId, f))
          .forEach(r -> {
            synchronized (projectReaders) {
              projectReaders.put(r.getKey(), r);
              r.start();
            }
          });

      synchronized (EXPORT_READERS) {
        LOADED_PROJECTS.add(projectId);
      }

    } catch (IOException e) {
      ServerError se = new ServerError(false, "export-results", e);
      recordProjectError(projectId, se);
      throw new RuntimeException(e);
    }
  }

  private static ExportReader asExportReader(long projectId, File f) {
    String filename = f.getName();
    String format = getFormat(filename);

    try {
      int matchLevelFlags = getMatchLevelFlags(filename);
      if (matchLevelFlags != -1) {
        return new ExportReader(projectId, matchLevelFlags, format, f);
      }
      else {
        int maxMatchLevel = getMaxMatchLevel(filename);
        return new ExportReader(projectId, asMatchLevelFlags(maxMatchLevel), format, f);
      }
    }
    catch (IOException e) {
      ServerError se = new ServerError(false, "export-results", e);
      recordProjectError(projectId, se);
      throw new RuntimeException(e);
    }
  }

  static int getMatchLevelFlags(String filename) {
    Matcher flagMatcher = FILENAME_FORMAT_FLAGS.matcher(filename);
    if (flagMatcher.matches()) {
      String matchLevelFlags = flagMatcher.group(1);
      return Integer.parseInt(matchLevelFlags);
    }
    return -1;
  }

  static int getMaxMatchLevel(String filename) {
    Matcher levelMatcher = FILENAME_FORMAT_LEVEL.matcher(filename);
    if (levelMatcher.matches()) {
      String matchLevel = levelMatcher.group(1);
      return Integer.parseInt(matchLevel);
    }
    return -1;
  }

  private static String getFormat(String filename) {
    String format;
    if (filename.contains(".json")) {
      format = "JSON";
    }
    else if (filename.contains(".csv")) {
      format = "CSV";
    }
    else {
      throw new IllegalStateException("Export File does not contain known format in filename");
    }
    return format;
  }

  private static ExportReader getExportReader(long projectId,
                                              int matchLevelFlags,
                                              String format) {
    String key = ExportReader.getKey(projectId, matchLevelFlags, format);

    ExportReader reader;
    Map<String,ExportReader> projectReaders = getExportReaders(projectId);
    synchronized (projectReaders) {
      reader = projectReaders.get(key);
      if (reader == null) {
        try {
          File projectDir = Workbench.getProjectDirectory(projectId);
          File exportFile = new File(projectDir, ExportReader.exportFileName(matchLevelFlags, format));

          if (exportFile.exists()) {
            reader = new ExportReader(projectId, matchLevelFlags, format, exportFile);
            projectReaders.put(key, reader);
            reader.start();
          }
        }
        catch (IOException e) {
          ServerError se = new ServerError(false, "export-results", e);
          recordProjectError(projectId, se);
          throw new RuntimeException(e);
        }
      }
      else {
        File exportFile = reader.getExportFile();
        // check if NOT busy and if the export file exists
        if (exportFile != null && !exportFile.exists()) {
          projectReaders.remove(key);
          reader = null;
        }
      }
    }
    return reader;
  }

  public static ResultsExport checkExport(long projectId,
                                          int matchLevelFlags,
                                          String format,
                                          AccessToken token) {
    ExportReader reader = getExportReader(projectId, matchLevelFlags, format);

    if (reader == null) {
      return null;
    }

    return reader.getExport(token);
  }

  public static File getExportedResultsFile(long projectId,
                                            int matchLevelFlags,
                                            String format) {
    ExportReader reader = getExportReader(projectId, matchLevelFlags, format);

    if (reader == null) {
      return null;
    }

    return reader.getExportFile();
  }

  public static ResultsExport exportResults(long projectId,
                                            int matchLevelFlags,
                                            String format,
                                            AccessToken token)
  {
    ExportReader reader = getExportReader(projectId, matchLevelFlags, format);
    String key = ExportReader.getKey(projectId, matchLevelFlags, format);

    // check if an export is already in progress
    if (reader != null) {
      ResultsExport export = reader.getExport(token);
      if (export.isInProgress()) {
        return export;
      }
    }

    // check if files have updated
    long mostRecentChange = getMostRecentTime(projectId, true);

    if (reader == null || mostRecentChange == 0L || mostRecentChange >= reader.getExportTime()) {
      EngineProcess engineProcess = EngineProcess.getInstance(projectId);
      if (engineProcess.isResolving()) {
        throw new IllegalStateException(
            "Cannot export results while resolving records.  projectId=[ " + projectId + "]");
      }

      Map<String,ExportReader> projectReaders = getExportReaders(projectId);
      synchronized (projectReaders) {
        if (reader != null) {
          log("REMOVING READER FOR KEY: " + key);
          projectReaders.remove(key);
        }

        reader = new ExportReader(engineProcess, matchLevelFlags, format);
        projectReaders.put(key, reader);
        reader.start();
      }
    }

    ResultsExport export = reader.getExport(token);
    return export;
  }

  public static int asMatchLevelFlags(Boolean includeSingletons, Boolean includeMatches, Boolean includePossibleMatches, Boolean includeDiscoveredRelationships, Boolean includeDisclosedRelationships) {
    int flags = G2_EXPORT_CSV_INCLUDE_FULL_DETAILS;
    if (includeSingletons) {
      flags |= G2_EXPORT_INCLUDE_ALL_ENTITIES;
    }
    if (includeMatches) {
      flags |= G2_EXPORT_INCLUDE_RESOLVED;
    }
    if (includePossibleMatches) {
      flags |= G2_EXPORT_INCLUDE_POSSIBLY_SAME;
    }
    if (includeDiscoveredRelationships) {
      flags |= G2_EXPORT_INCLUDE_POSSIBLY_RELATED;
    }
    if (includeDisclosedRelationships) {
      flags |= G2_EXPORT_INCLUDE_DISCLOSED;
    }
    return flags;
  }

  public static int asMatchLevelFlags(int maxMatchLevel) {
    return asMatchLevelFlags(false,
        maxMatchLevel >= 1,
        maxMatchLevel >= 2,
        maxMatchLevel >= 3,
        maxMatchLevel >= 4);
  }
}
