package com.senzing.api.engine.process;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.charset.Charset;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipOutputStream;
import java.util.zip.ZipEntry;

import com.senzing.util.Closer;
import com.senzing.beans.DataBeanUtilities;
import com.senzing.util.AccessToken;
import com.senzing.api.ResultsExport;
import com.senzing.api.ServerError;
import com.senzing.api.Workbench;
import com.senzing.api.engine.EngineRequest;
import com.senzing.api.engine.EngineResponse;
import com.senzing.api.engine.EngineOperation;
import com.senzing.api.engine.EngineException;
import com.senzing.api.engine.EngineResourceHandle;

import static com.senzing.g2.engine.G2Engine.*;
import static java.nio.file.StandardCopyOption.*;
import static com.senzing.api.Workbench.*;
import static com.senzing.api.ServerErrorLog.*;
import static com.senzing.api.engine.EngineOperation.*;
import static com.senzing.api.engine.EnginePriority.*;
import static com.senzing.api.engine.process.EngineExportUtilities.*;

class ExportReader extends Thread {
  private EngineProcess         engineProcess;
  private long                  projectId;
  private EngineResourceHandle  exportHandle;
  private boolean               complete;
  private int                   matchLevelFlags;
  private String                format;
  private String                key;
  private Object                monitor;
  private ResultsExport         export;
  private File                  tempExportFile;
  private File                  exportFile;
  private PrintWriter           writer;
  private ZipOutputStream       zipStream;
  private AccessToken           token;
  private boolean               success = false;

  public long getProjectId() {
    return this.projectId;
  }

  public static String getKey(long    projectId,
                              int     matchLevelFlags,
                              String  format)
  {
    return projectId + ":" + matchLevelFlags + ":" + format.toUpperCase();
  }

  public String getKey() {
    return projectId + ":" + matchLevelFlags + ":" + format.toUpperCase();
  }

  public static String zipEntryName(int matchLevelFlags, String format)
  {
    return "entity-report-flags-" + matchLevelFlags
            + "." + format.toLowerCase();
  }

  public static String exportFileName(int matchLevelFlags, String format)
  {
    return zipEntryName(matchLevelFlags, format) + ".zip";
  }

  ExportReader(EngineProcess engineProcess,
               int matchLevelFlags,
               String        format)
  {
    this.token          = new AccessToken();
    this.engineProcess  = engineProcess;
    this.projectId      = engineProcess.getProjectId();
    this.matchLevelFlags = matchLevelFlags;
    this.exportHandle   = null;
    this.complete       = false;
    this.format         = format;
    this.monitor        = new Object();
    this.export         = new ResultsExport(this.token);
    this.key            = ExportReader.getKey(projectId, matchLevelFlags, format);

    this.export.initValue("projectId", this.projectId, token);
    this.export.initValue("format", this.format.toUpperCase(), token);
    this.export.initValue("inProgress", true, token);
    this.export.initValue("includeSingletons", hasFlag(G2_EXPORT_INCLUDE_ALL_ENTITIES), token);
    this.export.initValue("includeMatches", hasFlag(G2_EXPORT_INCLUDE_RESOLVED), token);
    this.export.initValue("includePossibleMatches", hasFlag(G2_EXPORT_INCLUDE_POSSIBLY_SAME), token);
    this.export.initValue("includeDiscoveredRelationships", hasFlag(G2_EXPORT_INCLUDE_POSSIBLY_RELATED), token);
    this.export.initValue("includeDisclosedRelationships", hasFlag(G2_EXPORT_INCLUDE_DISCLOSED), token);

    this.setName("ExportReader-" + this.projectId + "-" + matchLevelFlags + "-" + format);
  }

  public ExportReader(long      projectId,
                      int       matchLevelFlags,
                      String    format,
                      File      exportFile)
    throws IOException
  {
    this.token          = new AccessToken();
    this.engineProcess  = null;
    this.projectId      = projectId;
    this.matchLevelFlags = matchLevelFlags;
    this.exportHandle   = null;
    this.complete       = true;
    this.format         = format.toUpperCase();
    this.monitor        = new Object();
    this.exportFile     = exportFile;
    this.export         = new ResultsExport(this.token);
    this.key            = ExportReader.getKey(projectId, matchLevelFlags, format);

    BasicFileAttributes attr = Files.readAttributes(
      exportFile.toPath(), BasicFileAttributes.class);

    Date createdOn = new Date(attr.creationTime().toMillis());
    Date lastModified = new Date(exportFile.lastModified());

    this.export.initValue("projectId", this.projectId, token);
    this.export.initValue("format", this.format.toUpperCase(), token);
    this.export.initValue("inProgress", false, token);
    this.export.initValue("fileSize", exportFile.length(), token);
    this.export.initValue("lastModified", lastModified, token);
    this.export.initValue("createdOn", createdOn, token);
    this.export.initValue("includeSingletons", hasFlag(G2_EXPORT_INCLUDE_ALL_ENTITIES), token);
    this.export.initValue("includeMatches", hasFlag(G2_EXPORT_INCLUDE_RESOLVED), token);
    this.export.initValue("includePossibleMatches", hasFlag(G2_EXPORT_INCLUDE_POSSIBLY_SAME), token);
    this.export.initValue("includeDiscoveredRelationships", hasFlag(G2_EXPORT_INCLUDE_POSSIBLY_RELATED), token);
    this.export.initValue("includeDisclosedRelationships", hasFlag(G2_EXPORT_INCLUDE_DISCLOSED), token);

    this.setName("ExportReader-" + this.projectId + "-" + matchLevelFlags + "-" + format);
  }

  private boolean hasFlag(int flag) {
    return (this.matchLevelFlags & flag) == flag;
  }

  public void complete() throws IOException {
    synchronized (this.monitor) {
      if (this.complete) return;
      this.complete = true;

      // cleanup the export handle
      if (this.exportHandle != null) {
        try {
          long requestId = this.engineProcess.getNextRequestId();

          EngineRequest request = new EngineRequest(CLOSE_EXPORT,
                                                    requestId,
                                                    SYNC);

          request.setParameter(EXPORT_HANDLE, this.exportHandle.getHandleId());
          request.setEngineAuthenticationId(this.exportHandle.getEngineId());

          EngineResponse response
            = this.engineProcess.sendSyncRequest(request);

          if (!response.isSuccessful()) {
            EngineException e = response.getException();
            ServerError se = new ServerError(false, "export-results", e);
            recordProjectError(this.projectId, se);
          }

        } finally {
          this.exportHandle = null;
        }
      }

      // check if any errors
      this.writer = Closer.close(this.writer);
      if (this.success && this.export.getExportError() == null) {
        long projectId = this.projectId;
        File exportDir = Workbench.getProjectDirectory(projectId)
            .toPath().resolve(EXPORT_SUBDIR).toFile();

        this.exportFile = new File(exportDir, exportFileName(matchLevelFlags, format));

        // delete the old file for good measure
        if (this.exportFile.exists()) {
          this.exportFile.delete();
        }

        // move the temp file into place
        Files.move(this.tempExportFile.toPath(),
                   this.exportFile.toPath(),
                   REPLACE_EXISTING);

        AccessToken tok = this.token;
        this.export.setValue("inProgress", false, tok);
        this.export.setValue("fileSize", this.exportFile.length(), tok);
        this.export.setValue(
          "lastModified", new Date(this.exportFile.lastModified()), tok);

      } else {
        this.tempExportFile.delete();
      }
      this.tempExportFile = null;
    }
  }

  public boolean isComplete() {
    synchronized (this.monitor) {
      return this.complete;
    }
  }

  public ResultsExport getExport() {
    return this.getExport(null);
  }

  public ResultsExport getExport(AccessToken token) {
    synchronized (this.monitor) {
      return DataBeanUtilities.cloneBean(this.export, token);
    }
  }

  public long getExportTime() {
    synchronized (this.monitor) {
      return this.export.getCreatedOn().getTime();
    }
  }

  public File getExportFile() {
    synchronized (this.monitor) {
      if (this.export.isInProgress()) return null;
      return this.exportFile;
    }
  }

  public void run()
  {
    try {
      this.doRun();
      this.complete();

    } catch (Exception e) {
      log(e);
      ServerError se = new ServerError(false, "export-results", e);
      recordProjectError(this.projectId, se);
      synchronized (this.monitor) {
        if (this.export.getExportError() == null) {
          this.export.setValue("exportError", se);
        }
      }

    } finally {
      try {
        this.complete();
      } catch (Exception ignore) {
        log(ignore);
      }
    }
  }

  public void doRun()
    throws IOException
  {
    if (this.isComplete()) return;
    EngineOperation exportOperation = null;
    if (this.format.equalsIgnoreCase("JSON")) {
      exportOperation = EXPORT_JSON_ENTITY_REPORT;
    } else {
      exportOperation = EXPORT_CSV_ENTITY_REPORT;
    }

    long requestId = this.engineProcess.getNextRequestId();

    EngineRequest request = new EngineRequest(exportOperation, requestId, SYNC);

    request.setParameter(MATCH_LEVEL_FLAGS, this.matchLevelFlags);
    request.setParameter(EXPORT_FLAGS, 0);

    EngineResponse response = this.engineProcess.sendSyncRequest(request);

    if (!response.isSuccessful()) {
      EngineException e = response.getException();
      log(e);
      ServerError se = new ServerError(false, "export-results", e);
      this.export.setValue("exportError", se);
      recordProjectError(this.projectId, se);
      this.complete();
      return;
    }

    // get the export handle
    long handleId = (Long) response.getResult();
    long engineId = response.getEngineAuthenticationId();

    this.exportHandle = new EngineResourceHandle(handleId, engineId);

    this.export.initValue("createdOn", (new Date()), this.token);
    File projectDir = Workbench.getProjectDirectory(this.projectId);
    File exportDir = projectDir.toPath().resolve(EXPORT_SUBDIR).toFile();

    String prefix = "temp-entity-report-flags-" + matchLevelFlags + "-";
    String suffix = "." + this.format.toLowerCase() + ".zip";
    this.tempExportFile = File.createTempFile(prefix, suffix, USER_TEMP_DIR);

    this.zipStream = new ZipOutputStream(
      new FileOutputStream(this.tempExportFile), Charset.forName("UTF-8"));

    String    entryName = zipEntryName(this.matchLevelFlags, this.format);
    ZipEntry  zipEntry  = new ZipEntry(entryName);
    this.zipStream.putNextEntry(zipEntry);

    this.writer = new PrintWriter(
      new OutputStreamWriter(this.zipStream, "UTF-8"));

    while (!this.isComplete())
    {
      // ensure this is still the active reader
      ExportReader reader = null;
      Map<String,ExportReader> exportReaders = getExportReaders(this.projectId);
      synchronized (exportReaders) {
        reader = exportReaders.get(this.key);
      }
      if (reader != this) {
        log("EXPORT READER HAS BEEN REPLACED BY ANOTHER: " + reader);
        this.complete();
        continue;
      }
      int fetchCount = 10000;
      requestId = this.engineProcess.getNextRequestId();
      request = new EngineRequest(EXPORT_FETCH_NEXT, requestId, SYNC);

      request.setParameter(EXPORT_HANDLE, this.exportHandle.getHandleId());
      request.setParameter(FETCH_COUNT, fetchCount);
      request.setEngineAuthenticationId(this.exportHandle.getEngineId());

      response = this.engineProcess.sendSyncRequest(request);

      if (!response.isSuccessful()) {
        EngineException e = response.getException();
        log(e);
        ServerError se = new ServerError(false, "export-results", e);
        this.export.setValue("exportError", se);
        recordProjectError(this.projectId, se);
        this.complete();
        continue;
      }

      List<String> result = (List<String>) response.getResult();
      if (result != null) {
        for (String line: result) {
          this.writer.println(line);
          this.writer.flush();
          synchronized (this.monitor) {
            this.export.setValue("lastModified", new Date(), this.token);
          }
        }
      }
      if (result == null || result.size() < fetchCount) {
        this.success = true;
        this.writer.flush();
        this.zipStream.closeEntry();
        this.zipStream.finish();
        this.complete();
        continue;
      }
    }
  }
}
