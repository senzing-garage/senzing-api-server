package com.senzing.api.engine.process;

import com.senzing.api.Project;

import java.io.*;
import java.util.LinkedHashSet;
import java.util.Set;

import static com.senzing.api.Workbench.*;

public class ConfigurationFileUtilities {
  /**
   * Creates the INI file for the engine.  If the file already exists and
   * the <tt>"PRESERVE_G2_INI"</tt> file system property is set to
   * <tt>"true"</tt> then the file is <b>not</b> overwritten.
   *
   * @param iniFile The File providing the path to the file.
   * @param projectDir The Project directory.
   * @param configFile The path to the G2 config file.
   * @param logCfgFile The path to the logging config file.
   *
   * @return <tt>true</tt> if the file was created and <tt>false</tt> if the
   *         existing file was preserved and not overwritten.
   *
   * @throws IOException If an I/O failure occurs.
   */
  public static boolean createIniFile(File     iniFile,
                                      File     projectDir,
                                      File     configFile,
                                      File     logCfgFile)
      throws IOException
  {
    // check if we are preserving the existing file
    String prop = System.getProperty("PRESERVE_G2_INI");
    boolean preserve = false;
    if (prop != null) {
      preserve = Boolean.valueOf(prop);
    }

    // if preserving and it exists then return
    if (iniFile.exists() && preserve) return false;

    prop = System.getProperty("G2_ENGINE_DB_CLUSTERED");
    boolean defaultClustered = true;
    if (prop != null) {
      defaultClustered = Boolean.valueOf(prop);
    }

    File g2RepoDB = new File(projectDir, "G2C.db");
    File g2ResDB  = new File(projectDir, "G2_RES.db");
    boolean clustered = ((!g2RepoDB.exists() && defaultClustered)
        || g2ResDB.exists());

    String pdir = projectDir.toString();

    try (FileOutputStream fos = new FileOutputStream(iniFile);
         PrintWriter pw = new PrintWriter(new OutputStreamWriter(fos)))
    {
      String sqlitePrefix = "sqlite3://na:na@" + pdir + FILE_SEP;
      if (clustered) {
        pw.println("[PIPELINE]");
        pw.println(" SUPPORTPATH=" + DATA_PATH);
        pw.println(" TAQFILES=customOn.txt");
        pw.println(" ONFILES=customOn.txt");
        pw.println(" LICENSEFILE=" + LICENSE_FILE.getCanonicalPath());
        pw.println();
        pw.println("[SQL]");
        pw.println(" BACKEND=HYBRID");
        pw.println(" CONNECTION=" + sqlitePrefix + "G2C.db");
        pw.println(" G2CONFIGFILE=" + configFile.toString());
        pw.println();
        pw.println("[HYBRID]");
        pw.println(" RES_FEAT=C1");
        pw.println(" RES_FEAT_EKEY=C1");
        pw.println(" RES_FEAT_LKEY=C1");
        pw.println(" RES_FEAT_STAT=C1");
        pw.println();
        pw.println(" LIB_FEAT=C2");
        pw.println(" LIB_FEAT_HKEY=C2");
        pw.println();
        pw.println("[C1]");
        pw.println(" CLUSTER_SIZE=1");
        pw.println(" DB_1=" + sqlitePrefix + "G2_RES.db");
        pw.println();
        pw.println("[C2]");
        pw.println(" CLUSTER_SIZE=1");
        pw.println(" DB_1=" + sqlitePrefix + "G2_LIB_FEAT.db");
        pw.println();
        pw.println("[LOGGING]");
        pw.println(" CONFIGFILE=" + logCfgFile.toString());
        pw.flush();

      } else {
        pw.println("[PIPELINE]");
        pw.println(" SUPPORTPATH=" + DATA_PATH);
        pw.println(" TAQFILES=customOn.txt");
        pw.println(" ONFILES=customOn.txt");
        pw.println();
        pw.println("[SQL]");
        pw.println(" CONNECTION=" + sqlitePrefix + "G2C.db");
        pw.println(" G2CONFIGFILE=" + configFile.toString());
        pw.println();
        pw.println("[LOGGING]");
        pw.println(" CONFIGFILE=" + logCfgFile.toString());
        pw.flush();
      }
    }
    return true;
  }

  /**
   * Creates the G2 engine logging config file.  If the file already exists and
   * the <tt>"PRESERVE_G2_LOGGING_CFG"</tt> file system property is set to
   * <tt>"true"</tt> then the file is <b>not</b> overwritten.
   *
   * @param logCfgFile The logging config file path to write to.
   * @param projectDir The project config directory.
   *
   * @return <tt>true</tt> if the file was created and <tt>false</tt> if the
   *         existing file was preserved and not overwritten.
   *
   * @throws IOException If an I/O failure occurs.
   */
  public static boolean createLogConfigFile(File logCfgFile, File projectDir)
      throws IOException
  {
    // check if we are preserving the existing file
    String prop = System.getProperty("PRESERVE_G2_LOGGING_CFG");
    boolean preserve = false;
    if (prop != null) {
      preserve = Boolean.valueOf(prop);
    }

    // if the file exists and preserving then return
    if (logCfgFile.exists() && preserve) return false;

    String projectURI = projectDir.toURI().toString();
    if (projectURI.endsWith("/")) {
      projectURI = projectURI.substring(0, projectURI.length()-1);
    }
    if (!projectURI.startsWith("file:///")) {
      if (projectURI.startsWith("file://")) {
        projectURI = "file:///" + projectURI.substring("file://".length());
      } else if (projectURI.startsWith("file:/")) {
        projectURI = "file:///" + projectURI.substring("file:/".length());
      }
    }

    try (FileOutputStream fos = new FileOutputStream(logCfgFile);
         PrintWriter pw = new PrintWriter(new OutputStreamWriter(fos)))
    {
      pw.println("console://stdout *.CRIT;*.ERR");
      pw.println(projectURI + "/$NODE_NAME.err *.CRIT;*.ERR");
      pw.println(projectURI + "/$NODE_NAME.nte *.NOTE");
      pw.flush();
    }

    return true;
  }

  /**
   * Creates the INI file and logging configuration file for an externally
   * linked project.  This is created once and not overwritten.
   *
   * @param project The {@link Project} describing the external project.
   * @param is The {@link InputStream} for the provided INI file.
   */
  public static void createExternalIniFile(Project project, InputStream is)
  {
    // get the project directory and path to the INI file path
    File projectDir;
    File outIniFile;
    File logCfgFile;
    final String utf8 = "UTF-8";

    try {
      projectDir = getProjectDirectory(project.getId());
      outIniFile = new File(projectDir, "g2.ini");
      logCfgFile = new File(projectDir, "g2-logging.cfg");

      createLogConfigFile(logCfgFile, projectDir);

    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    boolean pipelineSegment       = false;
    boolean sqlSegment            = false;
    boolean loggingSegment        = false;
    boolean licenseConfigured     = false;
    boolean loggingConfigured     = false;
    boolean supportPathConfigured = false;
    int     sqlSegmentCount       = 0;
    int     pipelineSegmentCount  = 0;
    int     loggingSegmentCount   = 0;

    try (InputStreamReader  isr = new InputStreamReader(is, utf8);
         BufferedReader     br  = new BufferedReader(isr);
         FileOutputStream   fos = new FileOutputStream(outIniFile);
         OutputStreamWriter osw = new OutputStreamWriter(fos, utf8);
         PrintWriter        pw  = new PrintWriter(osw))
    {
      for (String line = br.readLine(); line != null; line = br.readLine()) {
        line = line.trim();
        boolean segmentHeader = line.startsWith("[") && line.endsWith("]");

        if (line.length() == 0) continue;

        // start the new segment header
        if (segmentHeader) {
          // add a new LICENSEFILE entry at the end of the [PIPELINE] segment
          if (pipelineSegment && !licenseConfigured) {
            // add our own license file file line
            pw.println(" LICENSEFILE=" + LICENSE_FILE.getCanonicalPath());
            licenseConfigured = true;
          }
          if (loggingSegment && !loggingConfigured) {
            pw.println(" CONFIGFILE=" + logCfgFile.toString());
            loggingConfigured = true;
          }

          pw.println();
          pw.println(line);

          pipelineSegment = (line.equals("[PIPELINE]"));
          sqlSegment      = (line.equals("[SQL]"));
          loggingSegment  = (line.equals("[LOGGING]"));

          if (pipelineSegment)  pipelineSegmentCount++;
          if (sqlSegment)       sqlSegmentCount++;
          if (loggingSegment)   loggingSegmentCount++;

          if ((pipelineSegmentCount > 1) || (sqlSegmentCount > 1)
              || (loggingSegmentCount > 1))
          {
            throw new IllegalArgumentException(
                "Improperly formatted INI file (repeated segments).  "
                + "pipelineSegmentCount=[ " + pipelineSegmentCount
                + " ], sqlSegmentCount=[ " + sqlSegmentCount
                + " ], loggingSegmentCount=[ " + loggingSegmentCount + " ]");
          }

          if (sqlSegment) {
            pw.println(" G2CONFIGFILE=path-does-not-exist-to-avoid-default.json");
          }

        } else {
          // replace any license file specified in this file
          if (pipelineSegment && line.startsWith("LICENSEFILE=")) {
            pw.println(" LICENSEFILE=" + LICENSE_FILE.getCanonicalPath());
            licenseConfigured = true;
            continue;
          }

          // skip any G2 config file specified in this file -- we don't need it
          if (sqlSegment && line.startsWith("G2CONFIGFILE=")) {
            continue;
          }

          // override the SUPPORTPATH entry
          if (pipelineSegment && line.startsWith("SUPPORTPATH=")) {
            pw.println(" SUPPORTPATH=" + DATA_PATH);
            supportPathConfigured = true;
            continue;
          }

          // replace the logging segment config file
          if (loggingSegment && line.startsWith("CONFIGFILE=")) {
            pw.println(" CONFIGFILE=" + logCfgFile.toString());
            loggingConfigured = true;
            continue;
          }

          pw.println(" " + line);
        }
      }

      // check if license was configured
      if (!licenseConfigured || !supportPathConfigured) {
        if (!pipelineSegment) {
          if (pipelineSegmentCount == 0) {
            pw.println();
            pw.println("[PIPELINE]");
            pipelineSegmentCount++;
          } else {
            throw new IllegalStateException(
                "Completed pipeline segment without configuring license and "
                + "support path.  licenseConfigured=[ " + licenseConfigured
                + " ], supportPathConfigured=[ " + supportPathConfigured
                + " ]");
          }
        }

        if (!supportPathConfigured) {
          pw.println(" SUPPORTPATH=" + DATA_PATH);
          supportPathConfigured = true;
        }

        if (!licenseConfigured) {
          pw.println(" LICENSEFILE=" + LICENSE_FILE.getCanonicalPath());
          licenseConfigured = true;
        }
      }

      // check if logging was configured
      if (!loggingConfigured) {
        if (!loggingSegment) {
          if (loggingSegmentCount == 0) {
            pw.println();
            pw.println("[LOGGING]");
            loggingSegmentCount++;
          } else {
            throw new IllegalStateException(
                "Completed logging segment without configuring logging.  "
                + "licenseConfigured=[ " + licenseConfigured
                + " ], supportPathConfigured=[ " + supportPathConfigured
                + " ]");
          }
        }
        pw.println(" CONFIGFILE=" + logCfgFile.toString());
        loggingConfigured = true;
      }

      if ((pipelineSegmentCount == 0) || (sqlSegmentCount == 0)
          || (loggingSegmentCount == 0))
      {
        throw new IllegalArgumentException(
            "Improperly formatted INI file (missing segments).  "
                + "pipelineSegmentCount=[ " + pipelineSegmentCount
                + " ], sqlSegmentCount=[ "+ sqlSegmentCount
                + " ], loggingSegmentCount=[ " + loggingSegmentCount + " ]");
      }

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }


  /**
   * Validates the INI file for an externally linked project.
   *
   * @param is The {@link InputStream} for the provided INI file.
   */
  public static Set<String> validateExternalIniFile(InputStream is)
  {
    final String utf8 = "UTF-8";
    Set<String> errors = new LinkedHashSet<>();

    boolean pipelineSegment       = false;
    boolean sqlSegment            = false;
    boolean loggingSegment        = false;
    int     sqlSegmentCount       = 0;
    int     pipelineSegmentCount  = 0;
    int     loggingSegmentCount   = 0;

    try (InputStreamReader  isr = new InputStreamReader(is, utf8);
         BufferedReader     br  = new BufferedReader(isr))
    {
      for (String line = br.readLine(); line != null; line = br.readLine()) {
        line = line.trim();
        boolean segmentHeader = line.startsWith("[") && line.endsWith("]");

        if (line.length() == 0) continue;

        // start the new segment header
        if (segmentHeader) {
          pipelineSegment = (line.equals("[PIPELINE]"));
          sqlSegment      = (line.equals("[SQL]"));
          loggingSegment  = (line.equals("[LOGGING]"));

          if (pipelineSegment)  pipelineSegmentCount++;
          if (sqlSegment)       sqlSegmentCount++;
          if (loggingSegment)   loggingSegmentCount++;

          if (pipelineSegmentCount > 1) {
            errors.add("ini-file-multi-pipeline-error");
          }
          if (sqlSegmentCount > 1) {
            errors.add("ini-file-multi-sql-error");
          }
          if (loggingSegmentCount > 1) {
            errors.add("ini-file-multi-logging-error");
          }

        }
      }

      if (sqlSegmentCount == 0) {
        errors.add("ini-file-no-sql-error");
      }

      return errors;

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}
