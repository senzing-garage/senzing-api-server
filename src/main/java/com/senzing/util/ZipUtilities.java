package com.senzing.util;

import java.io.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import static com.senzing.io.IOUtilities.*;

/**
 * Utilities for working with ZIP files.
 */
public class ZipUtilities {
  /**
   * Creates a ZIP archive of the specified directory or file.
   *
   * @param directoryOrFile THe directory to be zipped.
   *
   * @param targetZipFile The ZIP file to be created.
   *
   * @throws IOException If a failure occurs.
   */
  public static void zip(File directoryOrFile, File targetZipFile)
    throws IOException
  {
    try (FileOutputStream     fos = new FileOutputStream(targetZipFile);
         BufferedOutputStream bos = new BufferedOutputStream(fos);
         ZipOutputStream      zos = new ZipOutputStream(bos, UTF_8_CHARSET))
    {
      zip(directoryOrFile, zos);
      zos.finish();
    }
  }

  /**
   * Creates a ZIP archive of the specified directory or file.
   *
   * @param directoryOrFile THe directory to be zipped.
   *
   * @param targetZipStream The ZIP file to be created.
   *
   * @throws IOException If a failure occurs.
   */
  public static void zip(File directoryOrFile, ZipOutputStream targetZipStream)
      throws IOException {
    String basePath = directoryOrFile.getParentFile().getPath();
    doZip(basePath, directoryOrFile, targetZipStream);
  }

  /**
   * Recursively creates the ZIP file.
   */
  private static void doZip(String          basePath,
                            File            directoryOrFile,
                            ZipOutputStream targetZip)
    throws IOException
  {
    // get the name sans the base path
    String name = directoryOrFile.getPath().substring(basePath.length());
    if (name.startsWith(File.separator)) {
      name = name.substring(1);
    }

    // make sure the we use a forward separator for the file separator
    if (File.separatorChar != '/') {
      name.replace(File.separatorChar, '/');
    }

    // check if we have a directory
    if (directoryOrFile.isDirectory()) {
      String dirName = name;
      if (!dirName.endsWith("/")) dirName = dirName + "/";
      ZipEntry zipEntry = new ZipEntry(dirName);
      targetZip.putNextEntry(zipEntry);
      targetZip.closeEntry();
      File[] children = directoryOrFile.listFiles();
      for (File child : children) {
        doZip(basePath,  child, targetZip);
      }

    } else {
      ZipEntry zipEntry = new ZipEntry(name);
      targetZip.putNextEntry(zipEntry);
      try (FileInputStream fis = new FileInputStream(directoryOrFile)) {
        byte[] bytes = new byte[8192];
        for (int count = fis.read(bytes); count >= 0; count = fis.read(bytes)) {
          targetZip.write(bytes, 0, count);
        }
      }
      targetZip.closeEntry();
    }
  }

  /**
   * Unzips a ZIP archive and places in the contents in the specified
   * directory.
   *
   * @param zipFile The ZIP file to extract.
   * @param targetDirectory The directory to extract the archive to.
   *
   * @throws IOException If a failure occurs.
   */
  public static void unzip(File zipFile, File targetDirectory)
    throws IOException
  {
    try (FileInputStream  fis = new FileInputStream(zipFile);
         ZipInputStream   zis = new ZipInputStream(fis, UTF_8_CHARSET))
    {
      unzip(zis, targetDirectory);
    }
  }

  /**
   * Unzips a ZIP archive and places in the contents in the specified
   * directory.
   *
   * @param zipStream The ZIP file to extract.
   * @param targetDirectory The directory to extract the archive to.
   *
   * @throws IOException If a failure occurs.
   */
  public static void unzip(ZipInputStream zipStream, File targetDirectory)
      throws IOException
  {
    byte[] buffer = new byte[8192];
    for (ZipEntry zipEntry = zipStream.getNextEntry();
         zipEntry != null;
         zipEntry = zipStream.getNextEntry())
    {
      String  name = zipEntry.getName();
      long    size = zipEntry.getSize();
      boolean directory = (size <= 0L && name.endsWith("/"));
      if (File.separatorChar != '/') {
        name = name.replace('/', File.separatorChar);
      }
      File targetFile = new File(targetDirectory, name);
      if (directory) {
        System.out.println(targetFile);
        targetFile.mkdirs();
      } else {
        try (FileOutputStream fos = new FileOutputStream(targetFile)) {
          for (int count = zipStream.read(buffer);
               count >= 0;
               count = zipStream.read(buffer))
          {
            fos.write(buffer, 0, count);
          }
        }
      }
    }
  }

  /**
   *
   */
  public static void main(String[] args) {
    try {
      if (args.length != 2) {
        System.err.println("Expected two arguments.");
        System.exit(1);
      }
      File source = new File(args[0]);
      File target = new File(args[1]);
      if (!source.exists()) {
        System.err.println("The source file (first argument) must exist: ");
        System.err.println("   " + source);
        System.exit(1);
      }
      boolean sourceZip = (source.getName().toLowerCase().endsWith(".zip")
                           && !source.isDirectory());
      boolean targetZip = (target.getName().toLowerCase().endsWith(".zip")
                           && !target.isDirectory());

      // check if extracting
      if (target.exists() && target.isDirectory() && sourceZip) {
        unzip(source, target);
      } else if (targetZip) {
        zip(source, target);
      } else {
        System.err.println("At least one argument must be a ZIP file");
        System.exit(1);
      }

    } catch (Exception e) {
      e.printStackTrace();
      System.exit(1);
    }
  }

}
