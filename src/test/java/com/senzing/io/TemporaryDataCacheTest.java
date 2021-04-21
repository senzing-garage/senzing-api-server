package com.senzing.io;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.aop.interceptor.ConcurrencyThrottleInterceptor;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link TemporaryDataCache}.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.CONCURRENT)
public class TemporaryDataCacheTest {
  /**
   * The test file
   */
  private List<File> testFiles;

  /**
   * The base file size for the test files that get generated.
   */
  private static final int BASE_FILE_SIZE = 100;

  /**
   * The test file size.
   */
  private static final int TEST_FILE_COUNT = 4;

  @BeforeAll
  public void setup() throws IOException {
    this.testFiles = new ArrayList<>(TEST_FILE_COUNT);

    char[] characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789".toCharArray();
    Random random = new Random(System.currentTimeMillis());

    int exponent = 0;
    for (int index = 0; index < TEST_FILE_COUNT; index++) {
      File testFile = File.createTempFile("test-file-", ".dat");
      this.testFiles.add(testFile);
      exponent = index + Math.max(0, index - 1);
      int fileSize = (int) Math.pow(25.0, (double) exponent);

      try (FileOutputStream     fos = new FileOutputStream(testFile);
           BufferedOutputStream bos = new BufferedOutputStream(fos, 8192))
      {
        long writeCount = 0L;
        while (writeCount < fileSize) {
          double nextValue = random.nextDouble();
          int nextIndex = (int) (((double) characters.length) * nextValue);
          bos.write((int) characters[nextIndex]);
          writeCount++;
        }
        bos.flush();
      }
      testFile.deleteOnExit();
    }
    this.testFiles = Collections.unmodifiableList(this.testFiles);
  }

  @AfterAll
  public void teardown() throws IOException {
    for (File file: this.testFiles) {
      file.delete();
    }
  }

  public List<File> getTestFiles() {
    return this.testFiles;
  }

  @ParameterizedTest
  @MethodSource("getTestFiles")
  public void verifyContentAccurate(File file) throws IOException {
    File tempFile = File.createTempFile("test-file-", ".dat");
    FileInputStream fis = new FileInputStream(file);
    try {
      TemporaryDataCache tdc = new TemporaryDataCache(fis);
      try (InputStream          is  = tdc.getInputStream();
           BufferedInputStream  bis = new BufferedInputStream(is, 8192);
           FileOutputStream     fos = new FileOutputStream(tempFile);
           BufferedOutputStream bos = new BufferedOutputStream(fos, 8192))
      {
        for (int byteRead = bis.read(); byteRead >= 0; byteRead = bis.read()) {
          bos.write(byteRead);
        }
        bos.flush();

      } finally {
        tdc.delete();
      }
    } catch (RuntimeException|IOException e) {
      e.printStackTrace();
      throw e;
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    assertFalse(IOUtilities.checkFilesDiffer(file, tempFile),
                "Read data differs from source file.");
  }

  @Test
  public void testCustomDirectory() throws IOException {
    File    tempFile      = File.createTempFile("TempDataCache-", ".dat");
    String  tempFileName  = tempFile.getName();
    int     length        = tempFileName.length();
    String  tempDirName   = tempFileName.substring(0, length - ".dat".length());
    File    tempDir       = new File(tempFile.getParent(), tempDirName);

    // create the directory
    tempDir.mkdirs();

    File    inputFile     = this.testFiles.get(0);
    int     preCount      = tempDir.listFiles().length;
    int     maxCount      = 0;

    FileInputStream fis = new FileInputStream(inputFile);
    try {
      TemporaryDataCache tdc = new TemporaryDataCache(fis, tempDir);

      assertEquals(tempDir, tdc.getDirectory(),
                   "TemporaryDataCache.getDirectory() has unexpected "
                    + "directory.");

      try (InputStream          is  = tdc.getInputStream();
           BufferedInputStream  bis = new BufferedInputStream(is))
      {
        for (int byteRead = bis.read(); byteRead >= 0; byteRead = bis.read()) {
          int count = tempDir.listFiles().length;
          if (count > maxCount) maxCount = count;
        }

      } finally {
        tdc.delete();
      }

      int postCount = tempDir.listFiles().length;
      if (preCount > 0) {
        fail("Files already existed in specified directory: " + tempDir);
      }
      if (maxCount == 0) {
        fail("Files were never created in specified directory: " + tempDir);
      }
      if (postCount > 0) {
        fail("Files were never deleted from specified directory: " + tempDir);
      }
    } catch (RuntimeException|IOException e) {
      e.printStackTrace();
      throw e;
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testFilePrefix() throws IOException {
    File    tempFile      = File.createTempFile("TempDataCache-", ".dat");
    String  tempFileName  = tempFile.getName();
    int     length        = tempFileName.length();
    String  tempDirName   = tempFileName.substring(0, length - ".dat".length());
    File    tempDir       = new File(tempFile.getParent(), tempDirName);

    // create the directory
    tempDir.mkdirs();

    File    inputFile     = this.testFiles.get(0);
    int     preCount      = tempDir.listFiles().length;
    int     maxCount      = 0;

    FileInputStream fis = new FileInputStream(inputFile);
    try {
      TemporaryDataCache tdc
          = new TemporaryDataCache(fis, tempDir, "TempDataCache-");

      try (InputStream          is  = tdc.getInputStream();
           BufferedInputStream  bis = new BufferedInputStream(is))
      {
        for (int byteRead = bis.read(); byteRead >= 0; byteRead = bis.read()) {
          int count = tempDir.listFiles(
              f -> f.getName().startsWith("TempDataCache-")).length;
          if (count > maxCount) maxCount = count;
        }

      } finally {
        tdc.delete();
      }

      int postCount = tempDir.listFiles().length;
      if (preCount > 0) {
        fail("Files already existed in specified directory: " + tempDir);
      }
      if (maxCount == 0) {
        fail("Files with prefix were never created in specified directory: "
                 + tempDir);
      }
      if (postCount > 0) {
        fail("Files were never deleted from specified directory: " + tempDir);
      }
    } catch (RuntimeException|IOException e) {
      e.printStackTrace();
      throw e;
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testConsuming() throws IOException {
    File    tempFile      = File.createTempFile("TempDataCache-", ".dat");
    String  tempFileName  = tempFile.getName();
    int     length        = tempFileName.length();
    String  tempDirName   = tempFileName.substring(0, length - ".dat".length());
    File    tempDir       = new File(tempFile.getParent(), tempDirName);

    // create the directory
    tempDir.mkdirs();

    File    inputFile     = this.testFiles.get(this.testFiles.size() - 1);
    int     preCount      = tempDir.listFiles().length;
    int     maxCount      = 0;

    FileInputStream fis = new FileInputStream(inputFile);
    try {
      TemporaryDataCache tdc
          = new TemporaryDataCache(fis, tempDir, "TempDataCache-");

      tdc.waitUntilAppendingComplete();

      assertFalse(tdc.isAppending(),
                  "TemporaryDataCache is still appending.");

      maxCount = tempDir.listFiles(
          f -> f.getName().startsWith("TempDataCache-")).length;

      try (InputStream          is  = tdc.getInputStream(true);
           BufferedInputStream  bis = new BufferedInputStream(is))
      {
        for (int byteRead = bis.read(); byteRead >= 0; byteRead = bis.read()) {
          // do nothing
        }
        int count = tempDir.listFiles(
            f -> f.getName().startsWith("TempDataCache-")).length;
        if (count > maxCount) {
          fail("File count increased after consuming TemporaryDataCache: "
                   + tempDir);
        }
        assertEquals(0,count,
                     "File count did not reduce to zero after consuming.");

      } finally {
        int count = tempDir.listFiles(
            f -> f.getName().startsWith("TempDataCache-")).length;
        if (count > 0) {
          fail("Files remaining after consuming entire TemporaryDataCache: "
               + tempDir);
        }
        assertTrue(tdc.isDeleted(),
                   "TemporaryDataCache NOT marked deleted after "
                   + "consuming all data");
        tdc.delete();
      }

    } catch (RuntimeException|IOException e) {
      e.printStackTrace();
      throw e;
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testAppending() throws IOException {
    File inputFile = this.testFiles.get(this.testFiles.size() - 1);
    FileInputStream fis = new FileInputStream(inputFile);
    try {
      TemporaryDataCache tdc = new TemporaryDataCache(fis);
      boolean appended = false;
      try (InputStream          is  = tdc.getInputStream();
           BufferedInputStream  bis = new BufferedInputStream(is))
      {
        for (int byteRead = bis.read(); byteRead >= 0; byteRead = bis.read())
        {
          if (!appended) appended = tdc.isAppending();
        }

      } finally {
        assertFalse(tdc.isAppending(),
                    "Instance still appending after read completed");
        tdc.delete();
      }

      if (!appended) {
        fail("Never transitioned to 'appending' state.");
      }

    } catch (RuntimeException|IOException e) {
      e.printStackTrace();
      throw e;
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  /**
   *
   * @throws IOException
   */
  private void backgroundRead(InputStream         is,
                              TemporaryDataCache  tdc,
                              int                 byteCount,
                              long                waitTime)
  {
    // read the bytes
    Thread thread = new Thread(() -> {
      try {
        for (int index = 0; index < byteCount; index++) {
          is.read();
        }
      } catch (IOException ignore) {
        // ignore
      }
    });
    thread.start();
    try {
      // give it 5 seconds to complete
      thread.join(waitTime);
    } catch (InterruptedException ignore) {
      // do nothing
    }
    boolean stillAlive = thread.isAlive();
    if (stillAlive) {
      tdc.delete();
      thread.interrupt();
      fail("Reader thread is still blocked in reading "
               + byteCount + " bytes after " + waitTime + "ms.");
    }
  }

  @Test
  public void testSplitAppending() throws IOException {
    PipedInputStream  pis = new PipedInputStream();
    PipedOutputStream pos = new PipedOutputStream(pis);

    try {
      TemporaryDataCache tdc = new TemporaryDataCache(pis);
      int writeCount = 128;

      // write a small number of bytes
      for (int index = 0; index < writeCount; index++) {
        pos.write('A');
      }
      pos.flush();
      Thread.sleep(3000L);

      try (InputStream is = tdc.getInputStream()) {
        // read the bytes
        backgroundRead(is, tdc, writeCount, 5000L);

        // write more bytes
        for (int index = 0; index < writeCount; index++) {
          pos.write('A');
        }
        pos.flush();
        pos.close(); // close the pipe

        backgroundRead(is, tdc, writeCount, 5000L);

      } finally {
        tdc.delete();
      }

    } catch (RuntimeException|IOException e) {
      e.printStackTrace();
      throw e;
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testRepeatedRead() throws IOException {
    File inputFile = this.testFiles.get(0);
    int fileSize = (int) inputFile.length();
    FileInputStream fis = new FileInputStream(inputFile);
    try {
      TemporaryDataCache tdc = new TemporaryDataCache(fis);

      ByteArrayOutputStream baos1 = new ByteArrayOutputStream(fileSize);
      ByteArrayOutputStream baos2 = new ByteArrayOutputStream(fileSize);

      try (InputStream          is  = tdc.getInputStream();
           BufferedInputStream  bis = new BufferedInputStream(is))
      {
        for (int byteRead = bis.read(); byteRead >= 0; byteRead = bis.read()) {
          baos1.write(byteRead);
        }
      }

      try (InputStream          is  = tdc.getInputStream();
           BufferedInputStream  bis = new BufferedInputStream(is))
      {
        for (int byteRead = bis.read(); byteRead >= 0; byteRead = bis.read()) {
          baos2.write(byteRead);
        }
      } finally {
        tdc.delete();
      }

      byte[] bytes1 = baos1.toByteArray();
      byte[] bytes2 = baos2.toByteArray();
      for (int index = 0; index < bytes1.length; index++) {
        if (bytes1[index] != bytes2[index]) {
          fail("TemporaryDataCache data is different after reading it twice");
        }
      }

    } catch (RuntimeException|IOException e) {
      e.printStackTrace();
      throw e;
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testRepeatedReadWithConsume() throws IOException {
    File inputFile = this.testFiles.get(0);
    int fileSize = (int) inputFile.length();
    FileInputStream fis = new FileInputStream(inputFile);
    try {
      TemporaryDataCache tdc = new TemporaryDataCache(fis);

      ByteArrayOutputStream baos1 = new ByteArrayOutputStream(fileSize);
      ByteArrayOutputStream baos2 = new ByteArrayOutputStream(fileSize);

      try (InputStream          is  = tdc.getInputStream();
           BufferedInputStream  bis = new BufferedInputStream(is))
      {
        for (int byteRead = bis.read(); byteRead >= 0; byteRead = bis.read()) {
          baos1.write(byteRead);
        }
      }

      try (InputStream          is  = tdc.getInputStream(true);
           BufferedInputStream  bis = new BufferedInputStream(is))
      {
        for (int byteRead = bis.read(); byteRead >= 0; byteRead = bis.read()) {
          baos2.write(byteRead);
        }
      }

      byte[] bytes1 = baos1.toByteArray();
      byte[] bytes2 = baos2.toByteArray();
      for (int index = 0; index < bytes1.length; index++) {
        if (bytes1[index] != bytes2[index]) {
          fail("TemporaryDataCache data is different after reading it twice");
        }
      }

    } catch (RuntimeException|IOException e) {
      e.printStackTrace();
      throw e;
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testDeleted() throws IOException {
    File  inputFile = this.testFiles.get(this.testFiles.size() - 1);
    int   fileSize  = (int) inputFile.length();
    FileInputStream fis = new FileInputStream(inputFile);
    try {
      TemporaryDataCache tdc = new TemporaryDataCache(fis);
      int readCount = 0;
      try (InputStream          is  = tdc.getInputStream();
           BufferedInputStream  bis = new BufferedInputStream(is))
      {
        for (int byteRead = bis.read(); byteRead >= 0; byteRead = bis.read()) {
          readCount++;
          assertFalse(tdc.isDeleted(),
                      "TemporaryDataCache prematurely marked deleted!");
          if (readCount > fileSize / 2) break;
        }

        tdc.delete();

        // try to read after a delete
        try {
          for (int byteRead = bis.read(); byteRead >= 0; byteRead = bis.read()) {
            readCount++;
            assertTrue(tdc.isDeleted(),
                        "TemporaryDataCache should be marked deleted!");
          }

          fail("Unexpectedly succeeded to read from TemporaryDataCache after "
               + "deletion.");

        } catch (IOException e) {
          // all good
        }

      } finally {
        tdc.delete();
      }

    } catch (RuntimeException|IOException e) {
      e.printStackTrace();
      throw e;
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }
}
