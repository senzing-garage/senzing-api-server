package com.senzing.io;

import com.senzing.util.LoggingUtilities;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.CipherOutputStream;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.*;
import java.security.*;
import java.util.LinkedList;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static com.senzing.text.TextUtilities.*;
import static com.senzing.util.LoggingUtilities.*;
import static com.senzing.io.IOUtilities.*;

/**
 * Provides an {@link InputStream} implementation that will read data from
 * a source stream in the background and as it becomes available makes it
 * possible to concurrently read that data from the beginning of the stream
 * multiple times.
 */
public class TemporaryDataCache {
  /**
   * How long to wait for more data before closing out a file part.
   */
  private static final long FILE_PART_TIMEOUT = 500L;

  /**
   * The random number generator to use for generating encryption keys.
   */
  private static final SecureRandom PRNG = new SecureRandom();

  /**
   * The algorithm to use for the cipher when encrypting.
   */
  private static final String CIPHER_ALGORITHM = "AES/CBC/PKCS5PADDING";

  /**
   * The algorithm to use for key generation.
   */
  private static final String KEY_ALGORITHM = "AES";

  /**
   * The sync flush flag for the GZIP stream when compressing.
   */
  private static final boolean SYNC_FLUSH = true;

  /**
   * The flush threshold for the GZIP stream when compressing.
   */
  private static final int FLUSH_THRESHOLD = 1024 * 512;

  /**
   * The size of the cache file part.
   */
  private static final int MIN_CACHE_FILE_SIZE = 1024;

  /**
   * The size of the cache file part.
   */
  private static final int MAX_CACHE_FILE_SIZE = 1024 * 1024 * 4;

  /**
   * The default prefix to use for the file parts.
   */
  private static final String DEFAULT_PREFIX = "sz-file-part-";

  /**
   * The list of file parts.
   */
  private final List<CacheFilePart> fileParts = new LinkedList<>();

  /**
   * The base file name.
   */
  private String baseFileName;

  /**
   * The directory in which the file parts are stored.
   */
  private File directory;

  /**
   * The {@link ConsumerThread} for this instance.
   */
  private ConsumerThread consumerThread;

  /**
   * Flag indicating if this instance has had its backing files deleted.
   */
  private boolean deleted;

  /**
   * The generated initialization vector for this instance.
   */
  private byte[] initVector;

  /**
   * The generated AES key for this instance.
   */
  private byte[] aesKey;

  /**
   * The {@link IvParameterSpec} for the initialization vector.
   */
  private IvParameterSpec ivSpec;

  /**
   * The key to use for encrypting and decrypting.
   */
  private SecretKeySpec keySpec;

  /**
   * The failure (if any) that occurred during consumption.
   */
  private Exception failure = null;

  /**
   * Constructs an instance that stores its temporary files in the system
   * temporary directory using the default file name prefix.
   *
   * @param sourceStream The source input stream
   * @throws IOException If an I/O failure occurs.
   */
  public TemporaryDataCache(InputStream sourceStream)
      throws IOException {
    this(sourceStream, null, null);
  }

  /**
   * Constructs an instance that stores its temporary files in the specified
   * directory using the default file name prefix.
   *
   * @param sourceStream The source input stream
   * @param directory    The directory where to store the temporary files.
   * @throws IOException If an I/O failure occurs.
   */
  public TemporaryDataCache(InputStream sourceStream, File directory)
      throws IOException {
    this(sourceStream, directory, null);
  }

  /**
   * Constructs an instance that stores its temporary files in the specified
   * directory using the default file name prefix.
   *
   * @param sourceStream   The source input stream
   * @param directory      The directory where to store the temporary files.
   * @param fileNamePrefix The name prefix to use for the temporary files.
   * @throws IOException If an I/O failure occurs.
   */
  public TemporaryDataCache(InputStream sourceStream,
                            File directory,
                            String fileNamePrefix)
      throws IOException
  {
    // figure out the base file name, suffix and directory
    if (fileNamePrefix == null) {
      fileNamePrefix = DEFAULT_PREFIX;
    }

    File tempFile = null;
    if (directory == null) {
      tempFile = File.createTempFile(fileNamePrefix, "-0.dat");
      directory = tempFile.getParentFile();
    } else {
      tempFile = File.createTempFile(fileNamePrefix, "-0.dat",
                                     directory);
    }
    String fileName = tempFile.getName();
    int length = fileName.length();

    fileNamePrefix = fileName.substring(0, length - "-0.dat".length());

    // check the base file name
    if (fileNamePrefix.endsWith("-")) {
      fileNamePrefix = fileNamePrefix + "-";
    }

    // check if encrypted
    this.aesKey = randomPrintableText(16).getBytes(UTF_8);
    this.initVector = randomPrintableText(16).getBytes(UTF_8);

    this.keySpec = new SecretKeySpec(this.aesKey, KEY_ALGORITHM);
    this.ivSpec = new IvParameterSpec(this.initVector);

    this.baseFileName = fileNamePrefix;
    this.directory = directory;
    this.deleted = false;
    this.consumerThread = new ConsumerThread(sourceStream);
    this.consumerThread.start();
  }

  /**
   * Immediately deletes the associated file parts.  The instance is unusable
   * after deleted.
   *
   * @return The number of deleted file parts.
   */
  public int delete() {
    int count = 0;
    synchronized (this.fileParts) {
      for (CacheFilePart filePart : this.fileParts) {
        if (filePart.file.delete()) count++;
      }
      this.fileParts.clear();
      this.deleted = true;
      this.fileParts.notifyAll();
    }
    return count;
  }

  /**
   * Checks if this instance has had its backing
   */
  public boolean isDeleted() {
    synchronized (this.fileParts) {
      return this.deleted;
    }
  }

  /**
   * Gets the directory that the file parts are stored in.
   *
   * @return The directory that the file parts are stored in.
   */
  public File getDirectory() {
    return this.directory;
  }

  /**
   * Checks if data is still be read from the source stream specified in the
   * constructor.
   *
   * @return <tt>true</tt> if data is still being appended to the stream,
   * otherwise <tt>false</tt>
   */
  public boolean isAppending() {
    synchronized (this.fileParts) {
      return this.consumerThread.isAlive() && this.consumerThread.isAppending();
    }
  }

  /**
   * Waits until the instance is completed appending.
   *
   * @throws InterruptedException If interrupted.
   */
  public void waitUntilAppendingComplete()
      throws InterruptedException
  {
    while (this.consumerThread.isAlive()) {
      synchronized (this.fileParts) {
        this.fileParts.wait(2000L);
      }
    }
  }

  /**
   * Sets the failure for this instance if one occurs.
   */
  private void setFailure(Exception e) {
    synchronized (this.fileParts) {
      this.failure = e;
    }
  }

  /**
   * Checks if a failure has occurred and if so, throws an exception.
   */
  private void checkFailure() throws RuntimeException {
    synchronized (this.fileParts) {
      if (this.failure != null) throw new RuntimeException(this.failure);
    }
  }

  /**
   * Waits until the instance is completed appending.
   *
   * @param maxWait The maximum amount of time to wait.
   *
   * @return <tt>true</tt> if appending completed, otherwise <tt>false</tt>
   *
   * @throws InterruptedException If interrupted.
   */
  public boolean waitUntilAppendingComplete(long maxWait)
      throws InterruptedException
  {
    if (maxWait <= 0L) {
      this.waitUntilAppendingComplete();
      return true;
    }

    long remaining = maxWait;
    while (this.consumerThread.isAlive() && remaining > 0L) {
      synchronized (this.fileParts) {
        long startWait  = System.currentTimeMillis();
        this.fileParts.wait(remaining < 2000L ? remaining: 2000L);
        long endWait = System.currentTimeMillis();
        remaining -= (endWait - startWait);
      }
    }

    // check if still alive
    return (! this.consumerThread.isAlive());
  }

  /**
   * Returns an input stream that will read the data as it becomes available
   * from the source stream.
   *
   * @return An input stream that will read the data as it becomes available
   *         from the source stream.
   */
  public InputStream getInputStream() {
    return this.getInputStream(false);
  }

  /**
   * Returns an input stream that will read the data as it becomes available
   * from the source stream.
   *
   * @param consume <tt>true</tt> if the file parts should be deleted
   *                as they are read, and <tt>false</tt> if not.
   *
   * @return An input stream that will read the data as it becomes available
   *         from the source stream.
   */
  public InputStream getInputStream(boolean consume) {
    return new ChainFileInputStream(consume);
  }

  /**
   * Provides the sink for the consumed data that will break the consumed data
   * into file parts rather than store them in memory to avoid exceeding
   * memory limitations.
   */
  private class FilePartSink extends Thread {
    /**
     * The current file.
     */
    private File currentFile = null;

    /**
     * The current file output stream.
     */
    private FileOutputStream currentFOS = null;

    /**
     * The current cipher output stream.
     */
    private CipherOutputStream currentCOS = null;

    /**
     * The current GZIP output stream.
     */
    private GZIPOutputStream currentGOS = null;

    /**
     * The offset relative to the whole for the current file part.
     */
    private int currentOffset = 0;

    /**
     * The number of bytes written to the current file part.
     */
    private int currentWriteCount = 0;

    /**
     * The total number of bytes written.
     */
    private int totalWriteCount = 0;

    /**
     * The timestamp of the last write operation.
     */
    private long lastWriteTime = -1L;

    /**
     * Flag indicating if this thread has been shutdown.
     */
    private boolean closed = false;

    /**
     * The current file part index.
     */
    private int partIndex = 0;

    /**
     * The current maximum length for a file part.
     */
    private int maxPartLength = MIN_CACHE_FILE_SIZE;

    /**
     * Default constructor.
     */
    private FilePartSink() {
      // do nothing
    }

    /**
     * Flags this instance for shutdown.
     */
    private void close() {
      TemporaryDataCache owner = TemporaryDataCache.this;
      synchronized (owner.fileParts) {
        this.closed = true;
        this.completeCurrentFilePart();
      }
    }

    /**
     * Checks if the file sink has been shutdown.
     */
    private boolean isClosed() {
      TemporaryDataCache owner = TemporaryDataCache.this;
      synchronized (owner.fileParts) {
        return this.closed;
      }
    }

    /**
     * Closes the current file and adds the file part to the queue.
     */
    private void completeCurrentFilePart() {
      TemporaryDataCache owner = TemporaryDataCache.this;

      synchronized (owner.fileParts) {
        try {
          File  completedFile   = null;
          int   completedLength = 0;
          int   completedOffset = 0;

          // check if we do not have a current file
          if ((this.currentFOS == null) || (this.currentCOS == null)
              || (this.currentGOS == null) || (this.currentFile == null)) {
            return;
          }

          // keep track of the current file information
          completedFile   = this.currentFile;
          completedOffset = this.currentOffset;
          completedLength = this.currentWriteCount;

          // if we have written at least one byte then flush/finish
          if (this.currentWriteCount > 0) {
            this.currentGOS.flush();
            this.currentGOS.finish();
            this.currentGOS.flush();
            this.currentCOS.flush();
          }

          // close (don't cross) the streams
          IOUtilities.close(this.currentGOS);
          IOUtilities.close(this.currentCOS);
          IOUtilities.close(this.currentFOS);

          debugLog("Completed file part: " + this.currentFile + " ("
                       + this.currentWriteCount + " bytes / "
                       + this.currentFile.length() + " compressed)");

          // reinitialize the current file fields
          this.currentGOS         = null;
          this.currentCOS         = null;
          this.currentFOS         = null;
          this.currentFile        = null;
          this.currentOffset      = 0;
          this.currentWriteCount  = 0;
          this.lastWriteTime      = -1L;

          this.partIndex++;

          // increment the size of the next file part
          if (completedLength > 0) {
            this.maxPartLength = this.maxPartLength * 16;
            if (this.maxPartLength > MAX_CACHE_FILE_SIZE) {
              this.maxPartLength = MAX_CACHE_FILE_SIZE;
            }
          }

          // push the file part
          if (completedLength > 0) {
            if (!owner.isDeleted()) {
              CacheFilePart cfp = new CacheFilePart(completedFile,
                                                    completedOffset,
                                                    completedLength);
              owner.fileParts.add(cfp);
              owner.fileParts.notifyAll();
            }
          }

        } catch (IOException e) {
          owner.setFailure(e);
          throw new RuntimeException(e);
        }
      }
    }

    /**
     * Creates the next file part.
     */
    private void beginNextFilePart() {
      final TemporaryDataCache  owner     = TemporaryDataCache.this;
      final int                 gzSize    = FLUSH_THRESHOLD + 8192;
      final boolean             syncFlush = SYNC_FLUSH;

      synchronized (owner.fileParts) {
        try {
          if ((this.currentFOS != null) || (this.currentCOS != null)
              || (this.currentGOS != null) || (this.currentFile != null)) {
            throw new IllegalStateException("A current file is already open.");
          }
          File directory = owner.directory;
          String baseFileName = owner.baseFileName;
          String fileName = baseFileName + "-" + this.partIndex + ".dat";

          Cipher cipher = Cipher.getInstance(CIPHER_ALGORITHM);
          cipher.init(Cipher.ENCRYPT_MODE, owner.keySpec, owner.ivSpec);

          this.currentOffset      = this.totalWriteCount;
          this.currentWriteCount  = 0;
          this.lastWriteTime      = System.nanoTime();
          this.currentFile        = new File(directory, fileName);
          this.currentFOS         = new FileOutputStream(this.currentFile);
          this.currentCOS         = new CipherOutputStream(this.currentFOS,
                                                           cipher);
          this.currentGOS         = new GZIPOutputStream(this.currentCOS,
                                                         gzSize,
                                                         syncFlush);

          // flag the file for deletion on exit
          this.currentFile.deleteOnExit();

          debugLog("Beginning file part: " + this.currentFile);

        } catch (RuntimeException e) {
          owner.setFailure(e);
          throw e;

        } catch (Exception e) {
          owner.setFailure(e);
          throw new RuntimeException(e);
        }
      }
    }

    /**
     * Writes the next byte to the current file part.
     *
     * @param data The byte to write.
     */
    public void writeByte(byte data) throws IOException {
      final TemporaryDataCache owner = TemporaryDataCache.this;
      synchronized (owner.fileParts) {
        // check if shutdown
        if (this.closed) {
          throw new IllegalStateException("Sink thread is already shutdown");
        }

        // check if there is a current file open and if not, open one
        if (this.currentGOS == null) {
          this.beginNextFilePart();
        }

        // write the byte and increment the counters
        this.currentGOS.write(data);
        this.totalWriteCount++;
        this.currentWriteCount++;
        this.lastWriteTime = System.nanoTime();

        // log every 5K bytes
        if (LoggingUtilities.isDebugLogging()) {
          double countLog10 = Math.floor(
              Math.log10((double) this.currentWriteCount));

          int logInterval = (int) (Math.max(100, Math.pow(10, countLog10)
              * Math.max(1, countLog10 - 1)));

          if ((this.currentWriteCount % logInterval) == 0) {
            debugLog("Bytes written to file: " + this.currentFile + " ("
                         + this.currentWriteCount + " current part / "
                         + this.totalWriteCount + " total bytes)");
          }
        }

        // check the write count
        if (this.currentWriteCount >= this.maxPartLength) {
          this.completeCurrentFilePart();
        }
      }
    }

    /**
     * Periodically checks if we have a partial
     */
    public void run() {
      final TemporaryDataCache owner = TemporaryDataCache.this;
      long waitTime = FILE_PART_TIMEOUT;
      while (!this.isClosed()) {
        synchronized (owner.fileParts) {
          try {
            owner.fileParts.wait(waitTime);
          } catch (InterruptedException ignore) {
            // ignore
          }
          // check if shutdown
          if (!this.isClosed()) {
            // get the current nano time
            long now = System.nanoTime();

            // check if there is a current file part and the last write time
            if (this.lastWriteTime > 0L && this.currentWriteCount > 0) {
              // calculate the duration from nanoseconds to milliseconds
              long duration = (now - this.lastWriteTime) / 1000000L;

              // check if the duration exceeds the timeout
              if (duration >= FILE_PART_TIMEOUT) {
                // if it the timeout is exceeded then complete the file part
                this.completeCurrentFilePart();
                waitTime = FILE_PART_TIMEOUT;
              } else {
                // otherwise wait for at least the remaining amount of time
                waitTime = FILE_PART_TIMEOUT - duration;
              }
            }
          }
        }
      }
    }
  }

  /**
   * The consumer thread for consuming the data from the source stream.
   */
  private class ConsumerThread extends Thread {
    /**
     * The source stream to read from.
     */
    private InputStream sourceStream;

    /**
     * Flag indicating if still appending.
     */
    private boolean appending = true;

    /**
     * Constructs with the specified source stream.
     *
     * @param sourceStream The source {@link InputStream} to read from.
     */
    public ConsumerThread(InputStream sourceStream) {
      this.sourceStream = sourceStream;
      this.appending = true;
    }

    /**
     * Checks if still appending.
     */
    public synchronized boolean isAppending() {
      return this.appending;
    }

    /**
     * Reads the data from the source stream and writes it to the underlying
     * file parts.
     */
    public void run() {
      TemporaryDataCache owner = TemporaryDataCache.this;

      InputStream is = this.sourceStream;

      FilePartSink sink = new FilePartSink();
      sink.start();

      int byteCount = 0;
      try (InputStream bis = new BufferedInputStream(is,8192)) {
        int readByte = 0;
        for (readByte = bis.read();
             readByte >= 0 && !owner.isDeleted();
             readByte = bis.read())
        {
          sink.writeByte((byte) readByte);
          byteCount++;
        }

        // close the sink
        sink.close();

        if (readByte < 0) {
          synchronized (this) {
            this.appending = false;
          }
        }

      } catch (RuntimeException e) {
        owner.setFailure(e);
        throw e;
      } catch (Exception e) {
        owner.setFailure(e);
        throw new RuntimeException(e);
      }
    }
  }

  private static class CacheFilePart implements Comparable<CacheFilePart> {
    private final File file;
    private final long offset;
    private final long length;

    CacheFilePart(File file, long offset, long length) {
      this.file   = file;
      this.offset = offset;
      this.length = length;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      CacheFilePart filePart = (CacheFilePart) o;
      return offset == filePart.offset &&
          length == filePart.length;
    }

    public int hashCode() {
      return ((int)(this.offset^this.length));
    }

    public int compareTo(CacheFilePart p) {
      if (this.offset == p.offset) {
        if (this.length == p.length) return 0;
        return (this.length < p.length) ? -1 : 1;
      }
      return (this.offset < p.offset) ? -1 : 1;
    }
  }

  /**
   * The consumer thread for consuming the data from the source stream.
   */
  private class ChainFileInputStream extends InputStream {

    private boolean consuming;

    private boolean eof;

    private boolean closed;

    private int currentFileIndex;

    private InputStream currentIS;

    private CacheFilePart currentFilePart;

    private long currentOffset;

    private ChainFileInputStream(boolean consuming) {
      this.consuming        = consuming;
      this.eof              = false;
      this.closed           = false;
      this.currentIS        = null;
      this.currentFilePart  = null;
      this.currentFileIndex = 0;
      this.currentOffset    = 0;
    }

    private void closeInputStream() throws IOException {
      if (this.currentIS != null) {
        this.currentIS.close();
        this.currentIS = null;
      }
    }

    public void close() throws IOException {
      if (this.closed) return;
      this.closeInputStream();
      this.currentFilePart = null;
      this.currentFileIndex = -1;
      this.eof              = true;
      this.closed           = true;
    }

    public long skip(long n) throws IOException {
      TemporaryDataCache owner = TemporaryDataCache.this;
      owner.checkFailure();
      if (this.closed) {
        throw new IOException("Cannot skip: stream already closed.");
      }
      if (n < 0) return 0L;
      if (this.eof) return 0L;

      long totalSkipped = 0L;
      long remainingSkip = n;

      while (remainingSkip > 0L) {
        // advance to the next file if necessary
        while (this.currentFilePart == null) {
          synchronized (owner.fileParts) {
            if (owner.isDeleted()) {
              this.closeInputStream();
              throw new IOException("Cannot skip: Backing files deleted");

            } else if (owner.fileParts.size() > this.currentFileIndex) {
              this.attachStream();

            } else if (owner.isAppending()) {
              // data is still be appended -- so wait for it
              try {
                owner.fileParts.wait(5000L);

              } catch (InterruptedException e) {
                throw new IOException(
                    "Interrupted while waiting for an available file.", e);
              }
            } else {
              return totalSkipped;
            }
          }
        }

        // check if the remaining number of bytes in the file is less than
        // the remaining number to skip
        long remaining = this.currentFilePart.length - this.currentOffset;
        if (remainingSkip < remaining) {
          long skipped = this.currentIS.skip(remainingSkip);
          totalSkipped += skipped;
          remainingSkip -= skipped;
          this.currentOffset += skipped;
        } else {
          totalSkipped += remaining;
          remainingSkip -= remaining;
          this.advanceFile();
        }
      }

      // return the number of skipped bytes
      return totalSkipped;
    }

    private void advanceFile() throws IOException {
      if (this.consuming && this.currentFilePart != null) {
        this.currentFilePart.file.delete();
      }
      this.currentFilePart = null;
      this.currentFileIndex++;
      if (this.currentIS != null) {
        this.currentIS.close();
        this.currentIS = null;
      }
      this.currentOffset = 0L;
    }

    private void attachStream() throws IOException {
      try {
        TemporaryDataCache owner = TemporaryDataCache.this;
        Cipher cipher = Cipher.getInstance(CIPHER_ALGORITHM);
        cipher.init(Cipher.DECRYPT_MODE, owner.keySpec, owner.ivSpec);

        synchronized (owner.fileParts) {
          this.currentFilePart = owner.fileParts.get(this.currentFileIndex);

          // dump the contents of the file part
          if (isDebugLogging()) {
            File    filePart  = this.currentFilePart.file;
            Cipher  tmpCipher = Cipher.getInstance(CIPHER_ALGORITHM);
            tmpCipher.init(Cipher.DECRYPT_MODE, owner.keySpec, owner.ivSpec);
            try (FileInputStream    fis = new FileInputStream(filePart);
                 CipherInputStream  cis = new CipherInputStream(fis, tmpCipher);
                 GZIPInputStream    gis = new GZIPInputStream(cis);
                 InputStreamReader  isr = new InputStreamReader(gis, UTF_8))
            {
              char[]        buffer    = new char[2048];
              StringBuilder sb        = new StringBuilder();
              for (int readCount = isr.read(buffer);
                   readCount >= 0;
                   readCount = isr.read(buffer))
              {
                sb.append(buffer, 0, readCount);
                if (sb.length() >= buffer.length * 2) break;
              }
              boolean truncated = (isr.read() >= 0);

              debugLog("Reading file part " + this.currentFileIndex
                       + ": " + filePart,
                       (truncated ? "CONTENTS:" : "PREVIEW"),
                       "-------------------------------------",
                       sb.toString(),
                       "-------------------------------------");
            }
          }

          this.currentIS =  new BufferedInputStream(
              new FileInputStream(this.currentFilePart.file), 8192);

          try {
            this.currentIS = new CipherInputStream(this.currentIS, cipher);
            this.currentIS = new GZIPInputStream(this.currentIS);

          } catch (IOException e) {
            this.currentIS.close();
            this.currentIS = null;
            throw e;

          } catch (Exception e) {
            this.currentIS.close();
            this.currentIS = null;
            throw new IOException(e);
          }

          this.currentOffset = 0L;
        }

      } catch (IOException e) {
        throw e;
      } catch (GeneralSecurityException e) {
        throw new IOException("Failed decryption of backing file", e);
      }
    }

    public int read() throws IOException {
      TemporaryDataCache owner = TemporaryDataCache.this;

      owner.checkFailure();

      if (this.closed) {
        throw new IOException("Cannot read: stream already closed.");
      }

      // check for EOF
      if (this.eof) {
        return -1;
      }

      String prefix = "" + System.identityHashCode(this) + ": ";
      // check if the current file has bytes left to read
      if (this.currentFilePart == null
          || ((this.currentFilePart.length - this.currentOffset) <= 0L))
      {
        // advance the file if the current one is exhausted
        if (this.currentFilePart != null) {
          this.advanceFile();
        }

        // ensure the current file is set
        while (this.currentFilePart == null) {
          synchronized (owner.fileParts) {
            if (owner.isDeleted()) {
              this.closeInputStream();
              throw new IOException("Cannot read: Backing files deleted");

            } else if (owner.fileParts.size() > this.currentFileIndex) {
              this.attachStream();

            } else if (owner.isAppending()) {
              // data is still be appended -- so wait for it
              try {
                long start = System.currentTimeMillis();
                owner.fileParts.wait(5000L);
              } catch (InterruptedException e) {
                throw new IOException(
                    "Interrupted while waiting for an available file.", e);
              }
            } else {
              this.eof = true;
              if (this.consuming) owner.delete();
              return -1; // EOF
            }
          }
        }
      }

      // read the next byte
      int byteRead = this.currentIS.read();
      if (byteRead < 0L) {
        throw new IOException(
            "Unexpected EOF from backing input stream.  offset=[ "
                + this.currentOffset + " ], fileSize=[ "
                + this.currentFilePart.length + " ]");
      }
      this.currentOffset++;

      return byteRead;
    }
  }
}
