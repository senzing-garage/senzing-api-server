package com.senzing.api.engine.process;

import com.senzing.util.FileBackedAppendList;

import java.io.File;
import java.io.IOException;
import java.util.*;
import static com.senzing.api.Workbench.*;

/**
 *
 */
public class ReportIndex {
  private static final int INDEX_LIST_CAPACITY = 10000;
  private static final int INDEX_LIST_SEGMENT_SIZE = 5000;
  private static final int MAX_ACTIVE_MERGERS = 20;
  private static final long SCHEDULER_LINGER_TIME = 5000L;

  /**
   * An interface used for registering callbacks to be executed after
   * the {@link ReportIndex} is completed.
   */
  interface Callback {
    void execute(ReportIndex reportIndex);
  }

  /**
   * This is used to ensure notification occurs when dependent sources are
   * modified so dependent targets (i.e.: for merging) can be notified.
   */
  private static IdentityHashMap<ReportIndex,Set<IndexMerger>>
      DEPENDENT_MERGERS = new IdentityHashMap<>();

  /**
   * The queue of pending {@link IndexMerger} instances that need to be
   * started.
   */
  private static List<IndexMerger> PENDING_MERGERS
      = new LinkedList<IndexMerger>();

  /**
   * The number of active mergers.
   */
  private static int active_merger_count = 0;

  /**
   * The number of completed mergers.
   */
  private static int completed_merger_count = 0;

  /**
   * The current background merge scheduler thread.
   */
  private static MergeScheduler merge_scheduler = null;

  /**
   * The {@link List} of records indexes that are hits.
   */
  private FileBackedAppendList<Integer> recordIndexes;

  /**
   * The number of records that have been evaluated.
   */
  private int evaluatedCount;

  /**
   * The maximum value in the {@link #recordIndexes} list.
   */
  private int maxRecordIndex;

  /**
   * The minimum value int he {@link #recordIndexes} list.
   */
  private int minRecordIndex;

  /**
   * The <tt>boolean</tt> flag indicating if this instance is complete.
   */
  private boolean complete;

  /**
   * The <tt>boolean</tt> flag indicating if this instance has been flagged
   * invalid.
   */
  private boolean invalid;

  /**
   * The {@link List} of {@link Callback} instances to execute upon completion.
   */
  private List<Callback> callbacks;

  /**
   * The {@link IndexMerger} associated with this instance.
   */
  private IndexMerger indexMerger = null;

  /**
   * Default constructor.
   */
  ReportIndex() {
    this.evaluatedCount = 0;
    this.complete       = false;
    this.minRecordIndex = -1;
    this.maxRecordIndex = -1;
    this.callbacks      = new LinkedList<>();
    String baseName     = "report-index-" + System.identityHashCode(this);
    this.recordIndexes  = new FileBackedAppendList<>(USER_TEMP_DIR,
                                                     baseName,
                                                    true,
                                                     INDEX_LIST_CAPACITY,
                                                     INDEX_LIST_SEGMENT_SIZE);
  }

  /**
   * Private constructor used when loading the data from backing files.
   *
   * @param backingList The backing list that was loaded.
   */
  private ReportIndex(FileBackedAppendList<Integer> backingList)
  {
    Map<String,String> metaData = backingList.getOpaqueMetaData();
    this.evaluatedCount = Integer.parseInt(metaData.get("evaluatedCount"));
    this.minRecordIndex = Integer.parseInt(metaData.get("minRecordIndex"));
    this.maxRecordIndex = Integer.parseInt(metaData.get("maxRecordIndex"));
    this.recordIndexes  = backingList;
    this.complete       = true;
  }

  /**
   * Marks this index as {@linkplain #isComplete() "complete"}, indicating that
   * no more records will be included in this index and evaluation of this index
   * against the data set is complete.  If this instance has already been marked
   * {@linkplain #isComplete() "complete"} then calling this method has no
   * effect.  Any attempt to modify this instance after calling this
   * method will result in an {@link UnsupportedOperationException}.
   */
  void markCompleted() {
    synchronized (this) {
      if (this.complete) return;
      this.complete = true;
      this.notifyAll();
    }
    this.notifyDependents(true);
    this.indexMerger = null;

    // execute all the registered callbacks
    for (Callback callback : this.callbacks) {
      try {
        callback.execute(this);
      } catch (Exception ignore) {
        log(ignore);
      }
    }
  }

  /**
   * Marks this index as {@linkplain #isComplete() "complete"}, indicating that
   * no more records will be included in this index and evaluation of this index
   * against the data set is complete <b>and</b> increases the evaluated count
   * to the specified value.  This indicates that all records with indexes
   * greater than the {@linkplain #getMaxRecordIndex() "max record index"}
   * and less than the specified evaluated count are "misses" for the index.
   *
   * If the specified evaluated count value is less than zero or less than the
   * current evaluated count then an {@link IllegalArgumentException} is thrown.
   *
   * If this instance has already been marked {@linkplain #isComplete()
   * "complete"} then calling this method has no effect (other than verifying
   * the specified parameter).  Any attempt to modify this instance after
   * calling this method will result in an {@link
   * UnsupportedOperationException}.
   *
   * @param evaluatedCount The value to increase the evaluated count to.
   *
   * @throws IllegalArgumentException If the specified evaluated count is less
   *                                  then zero <b>or</b> less than the previous
   *                                  evaluated count.
   */
  void markCompleted(int evaluatedCount) {
    if (evaluatedCount < 0) {
      throw new IllegalArgumentException(
          "The specified evaluated count cannot be less than zero: "
          + evaluatedCount);
    }
    synchronized (this) {
      if (evaluatedCount < this.evaluatedCount) {
        throw new IllegalArgumentException(
            "The specified evaluated count cannot be less than the current "
            + "evaluated count.  specified=[ " + evaluatedCount
            + " ], currentEvalCount=[ " + this.getEvaluatedCount() + " ]");
      }
      if (this.complete) return;
      this.evaluatedCount = evaluatedCount;
      this.complete = true;
      this.recordIndexes.complete();
      this.notifyAll();
    }
    this.notifyDependents(true);

    // execute all the registered callbacks
    for (Callback callback : this.callbacks) {
      try {
        callback.execute(this);
      } catch (Exception ignore) {
        log(ignore);
      }
    }
  }

  /**
   * Prioritizes the background task of completing this index.
   */
  public synchronized void prioritize() {
    if (this.indexMerger != null) {
      prioritizeMerger(this.indexMerger);
    }
  }

  /**
   * Invalidates this instance.
   */
  void invalidate() {
    synchronized (this) {
      if (this.invalid) return;
      this.invalid = true;
      this.notifyAll();
    }
    this.notifyDependents(true);
  }

  /**
   * Checks if this instance has been invalidated.
   *
   * @return <tt>true</tt> if invalidated, otherwise <tt>false</tt>
   */
  boolean isInvalidated() {
    synchronized (this) {
      return this.invalid;
    }
  }

  /**
   * Checks if this instance is "complete".  An incomplete index is one that is
   * still being evaluated and may contain more records at a later time as
   * it is evaluated in the background.
   *
   * @return <tt>true</tt> if the record index is complete, otherwise
   *         <tt>false</tt>.
   */
  synchronized boolean isComplete() {
    return this.complete;
  }

  /**
   * Asserts that this instance is not marked as complete.
   */
  private void assertNotComplete() {
    if (this.complete) {
      throw new UnsupportedOperationException(
          "Cannot modify a " + this.getClass().getSimpleName() + " that has "
          + "already been marked as \"complete\"");
    }
  }

  /**
   * Returns the number of records that have been evaluated by this index.
   * This helps determine that if the last record in the index is, for example,
   * 20, but the evaluated count is 200, then we know that records
   * 21 through 199 are <b>not</b> included in the index.  This method initially
   * returns zero (0) for an {@linkplain #isComplete() "incomplete"} index.
   *
   * @return The number of records in the source data set that hasve been
   *         evaluated for inclusion in this index.
   */
  synchronized int getEvaluatedCount() {
    return this.evaluatedCount;
  }

  /**
   * Returns the number of records that have been recorded as hits for this
   * index.  This is essentially the size of the index.
   *
   * @return The number of records that have been recorded as hits for this
   *         index.
   */
  synchronized int getHitCount() {
    return this.recordIndexes.size();
  }

  /**
   * This is the minimum record index value that has been included in this
   * index.
   *
   * @return The minimum record index that been included in this index.
   *
   */
  synchronized int getMinRecordIndex() {
    return this.minRecordIndex;
  }

  /**
   * This is the maximum record index value that has been included in this
   * index.  A value less than this value may not be indexed because the
   * record index is append-only and must be in order.
   *
   * @return The maximum record index that been included in this index.
   */
  synchronized int getMaxRecordIndex() {
    return this.maxRecordIndex;
  }

  /**
   * Marks that the next record being evaluated does <b>not</b> match this
   * index.  This increments the {@linkplain #getEvaluatedCount()
   * "evaluated count"} by one (1) while <b>not</b> modifying the {@linkplain
   * #getMaxRecordIndex() "maximum record index"} and <b>not</b> recording
   * an record index in the list of matched indexes.
   *
   * @throws UnsupportedOperationException If this instance has already been
   *                                       marked complete.
   */
  void recordMiss() throws UnsupportedOperationException {
    synchronized (this) {
      this.assertNotComplete();
      this.evaluatedCount++;
      this.notifyAll();
    }
    this.notifyDependents(false);
  }

  /**
   * Records the miss for the specified record index and advances the
   * evaluated count to the specified index plus one.  The records for all
   * indexes greater than the {@link #getMaxRecordIndex() "max record index"}
   * and less than the specified record index are <b>also</b> considered to
   * be misses for the index.  The specified index must be greater than the
   * {@linkplain #getMaxRecordIndex() "max record index"} and greater-than or
   * equal-to the {@linkplain #getEvaluatedCount() "evaluated count"}.
   *
   * @param recordIndex The index of the record to mark as a hit.
   *
   * @throws IllegalArgumentException If the specified index is less-than
   *                                  the {@linkplain #getMaxRecordIndex()
   *                                  "max record index"} prior to calling
   *                                  <b>or</b> if it is less-than zero (0).
   */
  void recordMiss(int recordIndex) {
    if (recordIndex < 0) {
      throw new IllegalArgumentException(
          "The specified record index cannot be negative.  recordIndex=[ "
              + recordIndex + " ]");
    }
    synchronized (this) {
      this.assertNotComplete();
      int max       = this.getMaxRecordIndex();
      int evalCount = this.getEvaluatedCount();
      if (recordIndex <= max) {
        throw new IllegalArgumentException(
            "The specified record index must be strictly greater than the "
                + "previous max record index.  recordIndex=[ " + recordIndex
                + " ], maxRecordIndex=[ " + max + " ]");
      }
      if (recordIndex < evalCount) {
        throw new IllegalArgumentException(
            "The specified record index cannot be less-than the previous "
                + "evaluated count.  recordIndex=[ " + recordIndex
                + " ], evaluatedCount=[ " + evalCount + " ]");
      }
      this.evaluatedCount = recordIndex + 1;
      this.notifyAll();
    }
    this.notifyDependents(false);
  }

  /**
   * Marks that the next record being evaluated (i.e.: the one with its
   * record index equal to {@link #getEvaluatedCount()}) b>does</b> match this
   * index.  This then increments the {@link #getEvaluatedCount()
   * "evaluated count"} by one (1) and sets the {@linkplain #getMaxRecordIndex()
   * "maximum record index"} accordingly to the previous value of the
   * {@linkplain #getEvaluatedCount() "evaluated count"}.  This also adds the
   * matching record index to the backing list of matching record indexes.
   *
   * @throws UnsupportedOperationException If this instance has already been
   *                                       marked complete.
   */
  void recordHit() {
    synchronized (this) {
      this.assertNotComplete();
      int recordIndex = this.evaluatedCount++;
      this.recordIndexes.add(recordIndex);
      if (this.minRecordIndex < 0) this.minRecordIndex = recordIndex;
      this.maxRecordIndex = recordIndex;
      this.notifyAll();
    }

    this.notifyDependents(false);
  }

  /**
   * Records the hit for the specified record index and advances the
   * evaluated count to the specified index plus one.  All records with
   * evaluated count to the specified index plus one.  All records with
   * indexes greater than the previous {@linkplain #getMaxRecordIndex()
   * "max record index"} and less-than the specified record index are considered
   * to be misses for the index.  The specified index must be greater than the
   * {@linkplain #getMaxRecordIndex() "max record index"} and greater-than or
   * equal-to the {@linkplain #getEvaluatedCount() "evaluated count"}.
   *
   * @param recordIndex The index of the record to mark as a hit.
   *
   * @throws IllegalArgumentException If the specified index is less-than
   *                                  the {@linkplain #getMaxRecordIndex()
   *                                  "max record index"} prior to calling
   *                                  or if it is less-than zero (0).
   */
  void recordHit(int recordIndex) {
    if (recordIndex < 0) {
      throw new IllegalArgumentException(
          "The specified record index cannot be negative.  recordIndex=[ "
          + recordIndex + " ]");
    }
    synchronized (this) {
      this.assertNotComplete();
      int max       = this.getMaxRecordIndex();
      int evalCount = this.getEvaluatedCount();
      if (recordIndex <= max) {
        throw new IllegalArgumentException(
            "The specified record index must be strictly greater than the "
            + "previous max record index.  recordIndex=[ " + recordIndex
            + " ], maxRecordIndex=[ " + max + " ]");
      }
      if (recordIndex < evalCount) {
        throw new IllegalArgumentException(
            "The specified record index cannot be less-than the previous "
            + "evaluated count.  recordIndex=[ " + recordIndex
            + " ], evaluatedCount=[ " + evalCount + " ]");
      }
      this.recordIndexes.add(recordIndex);
      if (this.minRecordIndex < 0) this.minRecordIndex = recordIndex;
      this.maxRecordIndex = recordIndex;
      this.evaluatedCount = recordIndex + 1;
      this.notifyAll();
    }
    this.notifyDependents(false);
  }

  /**
   * Returns the list of record indexes in the specified range of hit indexes.
   * The specified range has to do with the hits, for example, if the from
   * index is zero (0) and the to index was one (1), that would mean to return
   * the first hit which may have a record index of <tt>50000</tt>.  So the
   * specified range does not constrain the values of the record indexes
   * returned, but rather the size of the list and the position of the hits
   * in the overall list of hits.
   *
   * @param fromHit The hit index (inclusive) of the first hit to be returned.
   *
   * @param toHit The hit index (exclusive) of the last hit to be returned.
   *
   * @return The <b>unmodifiable</b> {@link List} of record indexes for
   *         the hits.
   */
  public List<Integer> getHits(int fromHit, int toHit) {
    if ((fromHit < 0) || (toHit < 0)) {
      throw new IllegalArgumentException(
          "The specified indexes cannot be negative.  from=[ "
          + fromHit + " ], to=[ " + toHit + " ]");
    }
    if (fromHit >= toHit) {
      throw new IllegalArgumentException(
          "The specified from index must be strictly less-than the "
          + "specified to index.  from=[ " + fromHit + " ], to=[ "
              + toHit + " ]");
    }
    // check if complete
    synchronized (this) {
      if (this.isInvalidated()) {
        throw new IllegalStateException(
            "This " + this.getClass().getSimpleName()
                + " has been invalidated");
      }
      int count = this.getHitCount();
      if (fromHit >= count || toHit > count) {
        throw new IllegalArgumentException(
            "The specified from index must be strictly less than the hit count "
            + "and the specified to index must be less-than or equal-to the "
            + "hit count.  hitCount=[ " + count + " ], from=[ " + fromHit
            + " ], to=[ " + toHit + " ]");
      }
      return Collections.unmodifiableList(
          this.recordIndexes.subList(fromHit, toHit));
    }
  }

  /**
   * Saves the data from this instance to a set of multiple files in the
   * specified directory using the specified base name for the file (see
   * {@link FileBackedAppendList}).  If this method is called for this
   * instance before it is {@linkplain #markCompleted() marked as complete}
   * then an {@link UnsupportedOperationException}.
   *
   * @param directory The {@link File} representing the directory to save in.
   *
   * @param baseName The base name to use for the file parts for the index.
   *
   * @throws IOException If an I/O failure occurs.
   *
   * @throws UnsupportedOperationException If this instance has not yet been
   *                                       {@linkplain #markCompleted()}
   *                                       marked as complete}.
   */
  synchronized void save(File directory, String baseName)
    throws IOException, UnsupportedOperationException
  {
    if (!this.complete) {
      throw new UnsupportedOperationException(
          "Cannot save an index that is incomplete.");
    }
    Map<String,String> metaData = new LinkedHashMap<>();
    metaData.put("evaluatedCount", String.valueOf(this.evaluatedCount));
    metaData.put("maxRecordIndex", String.valueOf(this.maxRecordIndex));
    metaData.put("minRecordIndex", String.valueOf(this.minRecordIndex));

    this.recordIndexes.saveCopyTo(directory, baseName, metaData);
  }


  /**
   * Executes the specified {@link Callback} when this instance is
   * {@linkplain #markCompleted() marked completed}.  If this instance has
   * is already {@linkplain #isComplete() complete} then the callback is
   * immediately executed.
   */
  void onComplete(Callback callback)
  {
    Objects.requireNonNull(callback, "The specified callback cannot be null");
    boolean registered = false;
    synchronized (this) {
      if (!this.isComplete()) {
        this.callbacks.add(callback);
        registered = true;
      }
    }
    if (!registered) {
      try {
        callback.execute(this);
      } catch (Exception ignore) {
        log(ignore);
      }
    }
  }

  /**
   * Constructs with the directory and base file name to load the data from.
   *
   * @param directory The directory where to look for the files
   *
   * @param baseName The base name of the backing files.
   *
   * @throws IOException If an I/O failure occurs.
   */
  static ReportIndex load(File directory, String baseName)
      throws IOException
  {
    FileBackedAppendList<Integer> backingList
        = FileBackedAppendList.load(directory, baseName);

    return new ReportIndex(backingList);
  }

  /**
   * Checks if the backing files for the index already exists with the specified
   * base file name in the specified directory.
   *
   * @param directory The directory where to look for the files
   *
   * @param baseName The base name of the backing files.
   *
   * @return <tt>true</tt> if the file exists, otherwise <tt>false</tt>
   */
  static boolean checkExists(File directory, String baseName)
  {
    return FileBackedAppendList.checkExists(directory, baseName);
  }

  /**
   * Finds all dependent monitors for this instance and notifies them that
   * changes have occurred.
   */
  private void notifyDependents(boolean clear) {
    Set<IndexMerger> mergers;
    synchronized (DEPENDENT_MERGERS) {
      if (clear) {
        mergers = DEPENDENT_MERGERS.remove(this);
      } else {
        mergers = DEPENDENT_MERGERS.get(this);
      }
    }
    if (mergers == null) return;
    synchronized (mergers) {
      for (Object merger : mergers) {
        synchronized (merger) {
          merger.notifyAll();
        }
      }
    }
  }

  /**
   * Intersects the specified {@link ReportIndex} instances.
   *
   * @param first The first {@link ReportIndex} to intersect.
   *
   * @param second The second {@link ReportIndex} to intersect.
   *
   * @param additionalIndexes The zero or more additionaly {@link ReportIndex}
   *                          instances.
   *
   * @return The intersected {@link ReportIndex} which may not be complete
   *         until some later time.
   */
  static ReportIndex intersect(ReportIndex    first,
                               ReportIndex    second,
                               ReportIndex... additionalIndexes)
  {
    int count = additionalIndexes.length + 2;
    List<ReportIndex> list = new ArrayList<>(count);
    list.add(first);
    list.add(second);
    for (ReportIndex index : additionalIndexes) {
      list.add(index);
    }

    return intersect(list);
  }

  /**
   * Intersects the {@link List} of {@link ReportIndex} instances.
   *
   * @param reportIndexes The {@link Collection} of {@link ReportIndex}
   *                      instances to intersect.
   *
   * @return The intersected {@link ReportIndex} which may not be complete
   *         until some later time.
   */
  static ReportIndex intersect(Collection<ReportIndex> reportIndexes)
  {
    // check the specified collection
    if (reportIndexes.size() < 2) {
      throw new IllegalArgumentException(
          "The specified collection of ReportIndex instances must contain at "
          + "least two (2) ReportIndex instances.  count=[ "
          + reportIndexes.size() + " ]");
    }

    // copy the collection to a list
    List<ReportIndex> list = new ArrayList<>(reportIndexes);

    IndexMerger merger = new IndexMerger(MergeType.INTERSECTION, list);
    enqueueMerger(merger);
    return merger.getTarget();
  }

  /**
   * Unions the specified {@link ReportIndex} instances.
   *
   * @param first The first {@link ReportIndex} to intersect.
   *
   * @param second The second {@link ReportIndex} to intersect.
   *
   * @param additionalIndexes The zero or more additionaly {@link ReportIndex}
   *                          instances.
   *
   * @return The unioned {@link ReportIndex} which may not be complete
   *         until some later time.
   */
  static ReportIndex union(ReportIndex    first,
                           ReportIndex    second,
                           ReportIndex... additionalIndexes)
  {
    int count = additionalIndexes.length + 2;
    List<ReportIndex> list = new ArrayList<>(count);
    list.add(first);
    list.add(second);
    for (ReportIndex index : additionalIndexes) {
      list.add(index);
    }

    return union(list);
  }

  /**
   * Unions the {@link List} of {@link ReportIndex} instances.
   *
   * @param reportIndexes The {@link Collection} of {@link ReportIndex}
   *                      instances to intersect.
   *
   * @return The unioned {@link ReportIndex} which may not be complete
   *         until some later time.
   */
  static ReportIndex union(Collection<ReportIndex> reportIndexes)
  {
    // check the specified collection
    if (reportIndexes.size() < 2) {
      throw new IllegalArgumentException(
          "The specified collection of ReportIndex instances must contain at "
          + "least two (2) ReportIndex instances.  count=[ "
          + reportIndexes.size() + " ]");
    }

    // copy the collection to a list
    List<ReportIndex> list = new ArrayList<>(reportIndexes);

    IndexMerger merger = new IndexMerger(MergeType.UNION, list);
    enqueueMerger(merger);
    return merger.getTarget();
  }

  /**
   * Enqueues the specified {@link IndexMerger} to be started (at most 10 at a
   * time unless prioritized).
   *
   * @param merger The {@link IndexMerger} to enqueue.
   */
  private static void enqueueMerger(IndexMerger merger) {
    synchronized (PENDING_MERGERS) {
      PENDING_MERGERS.add(merger);
      if (merge_scheduler == null) {
        merge_scheduler = new MergeScheduler();
        merge_scheduler.start();
      }
//      log(completed_merger_count + " INDEX MERGERS COMPLETED WITH "
//              + PENDING_MERGERS.size() + " PENDING");
      PENDING_MERGERS.notifyAll();
    }
  }

  /**
   * Finds the specified {@link IndexMerger} and if still pending moves it
   * out of the pending state and starts it.
   *
   * @param merger The {@link IndexMerger} to prioritize.
   */
  private static void prioritizeMerger(IndexMerger merger) {
    synchronized (PENDING_MERGERS) {
      Iterator<IndexMerger> iter = PENDING_MERGERS.iterator();
      while (iter.hasNext()) {
        IndexMerger im = iter.next();
        if (im == merger) {
          iter.remove();
          merger.start();
          active_merger_count++;
          PENDING_MERGERS.notifyAll();
          break;
        }
      }
//      log(completed_merger_count + " INDEX MERGERS COMPLETED WITH "
//              + PENDING_MERGERS.size() + " PENDING");
    }
  }

  /**
   * Enumerates the various ways in which indexes can be merged.
   */
  private enum MergeType {
    INTERSECTION,
    UNION;
  }

  /**
   *
   */
  private static class MergeState {
    private int place;
    private int value;
    private boolean done;
    private ReportIndex index;

    /**
     * Constructs a new {@link MergeState} with the specified {@link
     * ReportIndex}
     *
     * @param index The {@link ReportIndex} with which to construct the state.
     */
    private MergeState(ReportIndex index) {
      this.index = index;
      this.place = -1;
      this.value = -1;
    }

    /**
     * Advances the merge state if possible.  If not possible, then this method
     * does nothing.
     */
    private void advance() {
      synchronized (this.index) {
        if ((this.place+1) < this.index.recordIndexes.size()) {
          this.place++;
          this.value = this.index.recordIndexes.get(this.place);

        } else if (this.index.isComplete()) {
          this.done   = true;
          this.value  = -1;
        }
      }
    }

    /**
     * Checks if this state has exhausted available record indexes and is
     * pending the source index to obtain more record indexes.  If the source
     * index is complete and no more record indexes become available then this
     * method returns <tt>false</tt>.
     *
     * @return <tt>true</tt> if pending, otherwise <tt>false</tt>
     */
    private boolean pending() {
      synchronized (this.index) {
        return ((this.index.recordIndexes.size() == (this.place+1))
                && !this.index.isComplete());
      }
    }

    /**
     * Checks if the state is done producing record indexes for the merger.
     *
     * @return <tt>true</tt> if this instance has exhausted all available
     *         record indexes and no more are available, otherwise
     *         <tt>false</tt>
     */
    private boolean done() {
      return this.done;
    }

    /**
     * Returns the evaluated count for the associated source index.
     *
     * @return The evaluated count for the associated source index.
     */
    private int count() {
      return this.index.getEvaluatedCount();
    }

    /**
     * Overridden to return a diagnostic {@link String} describing this
     * instance.
     *
     * @return a diagnostic {@link String} describing this instance.
     */
    public String toString() {
      return ("{place=" + this.place + ",value=" + this.value
              + ",done=" + this.done() + ",pending=" + this.pending() + "}");
    }
  }

  /**
   * Background thread for merging multiple indexes into one by intersection
   * or union.
   */
  private static class IndexMerger extends Thread {
    private ReportIndex target;
    private List<ReportIndex> sources;
    private MergeType mergeType;

    /**
     * Constructs with the specified {@link MergeType} and {@link List} of
     * {@link ReportIndex} instances to merge.
     *
     * @param mergeType The {@link MergeType} to determine how to merge.
     *
     * @param indexes The {@link List} of {@link ReportIndex} instances.
     */
    private IndexMerger(MergeType mergeType, List<ReportIndex> indexes) {
      this.target     = new ReportIndex();
      this.sources    = indexes;
      this.mergeType  = mergeType;

      // sort the lists by identity hash code to ensure consistent locking
      this.sources.sort((l1, l2) -> {
        int h1 = System.identityHashCode(l1);
        int h2 = System.identityHashCode(l2);
        return h1 - h2;
      });

      this.target.indexMerger = this;
    }

    /**
     * Returns the target {@link ReportIndex}.
     *
     * @return The target {@link ReportIndex}.
     */
    ReportIndex getTarget() {
      return this.target;
    }

    /**
     * Final implementation using system identity hash code.
     *
     * @return The system identity hash code.
     */
    final public int hashCode() {
      return System.identityHashCode(this);
    }

    /**
     * Final implementation using referential equality
     *
     * @param obj The object to compare against.
     *
     * @return <tt>true</tt> if referentially equal, otherwise <tt>false</tt>
     */
    final public boolean equals(Object obj) {
      return (this == obj);
    }

    /**
     * Implemented to background merge the specified indexes and then cleanup
     * after this merger.
     */
    public void run() {
      try {
        this.doRun();
      } finally {
        // clear any dependencies pointing at this instance
        for (ReportIndex source : this.sources) {
          synchronized (DEPENDENT_MERGERS) {
            Set<IndexMerger> mergers = DEPENDENT_MERGERS.get(source);
            if (mergers != null) {
              synchronized (mergers) {
                mergers.remove(this);
                if (mergers.size() == 0) {
                  DEPENDENT_MERGERS.remove(source);
                }
              }
            }
          }
        }

        // clear out the index merger for the target
        synchronized (this.target) {
          this.target.indexMerger = null;
        }

        // decrement the active merger count
        synchronized (PENDING_MERGERS) {
          active_merger_count--;
          completed_merger_count++;
//          log(completed_merger_count + " INDEX MERGERS COMPLETED WITH "
//                  + PENDING_MERGERS.size() + " PENDING");
          PENDING_MERGERS.notifyAll();
        }
      }
    }

    /**
     * Implemented to background merge the specified indexes.
     */
    private void doRun() {
      // setup the dependencies
      this.sources.forEach(index -> {
        if (!index.isComplete()) {
          Set<IndexMerger> mergers;
          synchronized (DEPENDENT_MERGERS) {
            mergers = DEPENDENT_MERGERS.get(index);
            if (mergers == null) {
              mergers = new HashSet<>();
              DEPENDENT_MERGERS.put(index, mergers);
            }
          }
          synchronized (mergers) {
            mergers.add(this);
          }
        }
      });

      final MergeType mt = this.mergeType;
      List<MergeState> states = new ArrayList<>(this.sources.size());
      this.sources.forEach(s -> {
        states.add(new MergeState(s));
      });

      List<MergeState> least = new ArrayList<>(states.size());

      // loop until any one of our source indexes is done producing values
      // if one is done producing values then we cannot get ALL to match
      // which is required for intersection
      while (!this.checkDone(states) && !this.target.isInvalidated()) {
        // get the minimum value
        OptionalInt val = states.stream().filter(s -> !s.done())
            .mapToInt(s -> s.value).min();

        // check if the value is not present because there is a race condition
        // where the sources can transition to a "done" state after the
        // guard on the while loop
        if (!val.isPresent()) continue;
        int v = val.getAsInt();

        // determine which instances have the least value
        least.clear();

        // find all states that have that least value
        states.stream().filter(s -> !s.done() && (s.value == v))
            .forEach(s -> least.add(s));

        // check if we have a hit
        if ((v != -1)
            && ((mt == MergeType.UNION) || (least.size() == states.size())))
        {
          // all states have the same value (not -1) -- that's hit
          synchronized (this.target) {
            this.target.recordIndexes.add(v);
            if (this.target.minRecordIndex < 0) this.target.minRecordIndex = v;
            this.target.maxRecordIndex = v;
            this.target.evaluatedCount = v + 1;
            this.target.notifyAll();
          }
          this.target.notifyDependents(false);
        }

        // wait until the ones in the least position are all in a ready state
        synchronized (this) {
          while (least.stream().anyMatch(s -> s.pending())) {
            try {
              this.wait(5000L);
            } catch (InterruptedException ignore) {
              log(ignore);
            }
          }
        }

        // advance the least ones
        least.forEach(s -> {
          if (!s.done()) s.advance();
        });
      }

      // check if invalidated
      if (!this.target.isInvalidated()) {
        // determine the evaluated count
        int evalCount;
        switch (mt) {
          case INTERSECTION:
            evalCount = states.stream().filter(s->s.done())
                .mapToInt(s->s.count()).min().getAsInt();
            break;
          case UNION:
            evalCount = states.stream().filter(s->s.done())
                .mapToInt(s->s.count()).max().getAsInt();
            break;
          default:
            throw new IllegalStateException("Unrecognized MergeType: " + mt);
        }

        // finalize the target index
        synchronized (this.target) {
          this.target.evaluatedCount = evalCount;
        }

        // mark the target complete
        this.target.markCompleted();
      }
    }

    /**
     * Checks if the merge is done given the specified {@link List} of
     * {@link MergeState} instances.  The condition for completion is
     * different depending on the associated {@link MergeType} for this
     * instance.
     *
     * @param states The {@link List} of {@link MergeState} instances.
     * @return <tt>true</tt> if done merging, otherwise <tt>false</tt>
     */
    private boolean checkDone(List<MergeState> states) {
      switch (this.mergeType) {
        case INTERSECTION:
          return states.stream().anyMatch(s->s.done());
        case UNION:
          return states.stream().allMatch(s->s.done());
        default:
          throw new IllegalStateException(
              "Unrecognized merge type: " + this.mergeType);
      }
    }
  }

  public static void main(String[] args) {
    ReportIndex index1 = new ReportIndex();
    ReportIndex index2 = new ReportIndex();
    ReportIndex index3 = new ReportIndex();
    for (int recordIndex = 0; recordIndex < 10; recordIndex++) {
      if (recordIndex < 7) {
        index1.recordHit(recordIndex);
      }
      if (recordIndex > 3) {
        index2.recordHit(recordIndex);
      }
      if (recordIndex % 3 == 0) {
        index3.recordHit(recordIndex);
      }
    }
    index1.markCompleted(10);
    index2.markCompleted(10);
    index3.markCompleted(10);
    ReportIndex union = ReportIndex.union(index1, index2, index3);
    synchronized (union) {
      while (!union.isComplete()) {
        try {
          union.wait(1000);
        } catch (InterruptedException ignore) {
          // ignore
        }
      }
    }
    ReportIndex intersection = ReportIndex.intersect(index1, index2, index3);
    synchronized (intersection) {
      while (!intersection.isComplete()) {
        try {
          intersection.wait(1000);
        } catch (InterruptedException ignore) {
          // ignore
        }
      }
    }
    System.out.println("INDEX1       : " + index1.getHits(0, index1.getHitCount()));
    System.out.println("INDEX2       : " + index2.getHits(0, index2.getHitCount()));
    System.out.println("INDEX3       : " + index3.getHits(0, index3.getHitCount()));
    System.out.println("UNION        : " + union.getHits(0, union.getHitCount()));
    System.out.println("INTERSECTION : " + intersection.getHits(0, intersection.getHitCount()));
  }

  /**
   * Merge scheduler thread class.
   */
  private static class MergeScheduler extends Thread {
    private long zeroStart = -1L;

    public boolean isDone() {
      synchronized (PENDING_MERGERS) {
        // check if we have active mergers
        if (active_merger_count > 0) return false;

        // check if we have any pending mergers
        long now = System.currentTimeMillis();
        if (PENDING_MERGERS.size() == 0 && this.zeroStart < 0L) {
          this.zeroStart = now;
        } else if (PENDING_MERGERS.size() > 0) {
          this.zeroStart = -1L;
        }

        // check if we have had no mergers for 5 seconds or more
        final long lingerTime = SCHEDULER_LINGER_TIME;
        if (this.zeroStart > 0L && ((now - this.zeroStart) > lingerTime))
        {
          return true;
        }

        // pending mergers or 5 seconds has not yet elapsed
        return false;
      }
    }

    public void run() {
      synchronized (PENDING_MERGERS) {
        while (!this.isDone()) {
          // start mergers (up to 10 at a time)
          if (PENDING_MERGERS.size() > 0
              && active_merger_count < MAX_ACTIVE_MERGERS)
          {
            IndexMerger merger = PENDING_MERGERS.remove(0);
            merger.start();
            active_merger_count++;
//            log(completed_merger_count + " INDEX MERGERS COMPLETED WITH "
//                    + PENDING_MERGERS.size() + " PENDING");
            continue;
          }

          // wait for notification
          try {
            PENDING_MERGERS.wait(5000L);
          } catch (InterruptedException ignore) {
            // do nothing
          }
        }

        // we are done for now, clear out the merge scheduler
        merge_scheduler = null;
      }
    }
  }
}

