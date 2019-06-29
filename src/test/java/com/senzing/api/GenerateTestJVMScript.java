package com.senzing.api;

import com.senzing.util.OperatingSystemFamily;

import java.io.*;

import static com.senzing.util.OperatingSystemFamily.*;

public class GenerateTestJVMScript {
  public static void main(String[] args) {
    String targetFileName = "target/java-wrapper/bin/java-wrapper.bat";
    if (args.length > 0) {
      targetFileName = args[0];
    }
    File targetFile = new File(targetFileName);
    File targetDir = targetFile.getParentFile();
    if (targetDir != null && !targetDir.exists()) {
      targetDir.mkdirs();
    }
    String  senzingDirPath  = System.getProperty("senzing.install.dir");
    String  javaHome        = System.getProperty("java.home");
    File    javaHomeDir     = new File(javaHome);
    File    javaBinDir      = new File(javaHomeDir, "bin");
    File    javaExecutable  = new File(javaBinDir, "java");

    System.err.println("*********************************************");
    System.err.println();
    System.err.println("senzing.install.dir = " + senzingDirPath);
    System.err.println("java.home = " + javaHome);
    System.err.println();
    System.err.println("*********************************************");

    File senzingDir = null;
    File libDir = null;
    File platformLibDir = null;
    String pathSep = ":";
    if (senzingDirPath != null && senzingDirPath.trim().length() > 0) {
      senzingDir = new File(senzingDirPath);
    }
    switch (RUNTIME_OS_FAMILY) {
      case WINDOWS:
        if (senzingDir == null) {
          senzingDir = new File("C:\\Program Files\\Senzing\\g2");
        }
        libDir = new File(senzingDir, "lib");
        pathSep = ";";
        break;
      case MAC_OS:
        if (senzingDir == null) {
          senzingDir = new File("/Applications/Senzing.app/Contents/Resources/app/g2/");
        }
        libDir = new File(senzingDir, "lib");
        platformLibDir = new File(libDir, "macos");
        break;
      case UNIX:
        if (senzingDir == null) {
          senzingDir = new File("/opt/senzing/g2");
        }
        libDir = new File("/opt/senzing/g2/lib");
        platformLibDir = new File(libDir, "debian");
        break;
    }

    String libraryPath = libDir.toString()
        + ((platformLibDir != null) ? pathSep + platformLibDir.toString() : "");

    try {
      try (FileOutputStream fos = new FileOutputStream(targetFile);
           OutputStreamWriter osw = new OutputStreamWriter(fos, "UTF-8");
           PrintWriter pw = new PrintWriter(osw))
      {
        if (RUNTIME_OS_FAMILY == WINDOWS) {
          pw.println("@echo off");

        } else {
          pw.println("#!/bin/sh");
          pw.println("export DYLD_LIBRARY_PATH=\"" + libraryPath + "\"");
          pw.println("export LD_LIBRARY_PATH=\"" + libraryPath + "\"");
          pw.println(javaExecutable.toString() + " \"$@\"");
        }
      }
      targetFile.setExecutable(true);

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
