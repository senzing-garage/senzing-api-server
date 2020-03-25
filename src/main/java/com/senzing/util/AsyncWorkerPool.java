package com.senzing.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * Provides a pool of worker threads to execute asynchronous tasks and
 * provide the results.
 *
 * @param <T> The return type from the asynchronous task.
 */
public class AsyncWorkerPool<T> {
  /**
   * The list of available {@link AsyncWorker} instances.
   */
  private List<AsyncWorker> available;

  /**
   * The list of all {@link AsyncWorker} instances whether available or not.
   */
  private List<AsyncWorker> allThreads;

  /**
   * Flag indicating if the pool has been marked closed.
   */
  private boolean closed;

  /**
   * A task to be executed.
   */
  public interface Task<T> {
    T execute() throws Exception;
  }

  /**
   * The asynchronous result associated with a previously executed task.
   */
  public static class AsyncResult<T> {
    private T value;
    private Exception failure;

    private AsyncResult(T value, Exception failure) {
      this.value      = value;
      this.failure    = failure;
    }

    /**
     * Returns the value produced by the completed task.
     * If an exception is thrown by the task and it does not complete
     * then this throws the associated failure.
     *
     * @return The value produced by the completed task.
     */
    public T getValue() throws Exception {
      if (this.failure != null) throw this.failure;
      return this.value;
    }

    /**
     * Returns a diagnostic string describing this instance.
     *
     * @return A diagnostic string describing this instance.
     */
    public String toString() {
      return "{ value=[ " + this.value + " ]"
              + ((this.failure != null)
                  ? ", failure=[ " + this.failure + " ]" : "")
              + " }";
    }
  }

  /**
   * Constructs with the specified number of threads in the pool.
   * @param size The number of threads to create in the pool.
   */
  public AsyncWorkerPool(int size) {
    this("AsyncWorker", size);
  }

  /**
   * Constructs with the specified thread base name and the number of threads
   * to create.
   *
   * @param baseName The base name to use as a prefix when naming the
   *                 async worker threads in the pool.
   *
   * @param size The number of worker threads to create.
   */
  public AsyncWorkerPool(String baseName, int size) {
    this.available    = new LinkedList<>();
    this.allThreads   = new LinkedList<>();
    this.closed       = false;

    // if baseName ends with "-" then strip it off since we will add it back
    if (baseName.endsWith("-")) {
      baseName = baseName.substring(0, baseName.length() - 1);
    }

    int identityHashCode = System.identityHashCode(this);
    for (int index = 0; index < size; index++) {
      AsyncWorker aw = new AsyncWorker();
      aw.setName(baseName + "-" + identityHashCode + "-" + index);
      this.available.add(aw);
      this.allThreads.add(aw);
      aw.start();
    }
    this.allThreads = Collections.unmodifiableList(this.allThreads);
  }

  /**
   * Executes the specified {@link Task} asynchronously and returns the {@link
   * AsyncResult} (if any) produced by a previous execution of the assigned
   * worker.  The returned {@link AsyncResult} is <tt>null</tt> if the assigned
   * worker's result has already been consumed or has not exceuted a previous
   * task.
   *
   * @param task The task to execute.
   * @return The {@link AsyncResult} of the previous
   */
  public AsyncResult<T> execute(Task<T> task) {
    synchronized (this.available) {
      AsyncWorker<T> worker = null;
      // wait for an available worker
      while (worker == null && !this.isClosed()) {
        if (this.available.size() == 0) {
          try {
            this.available.wait(10000L);
          } catch (InterruptedException ignore) {
            // do nothing
          }
        }
        if (this.available.size() > 0) {
          worker = this.available.remove(0);
        }
      }

      // check if closed
      if (worker == null || this.isClosed()) {
        throw new IllegalStateException(
            "Pool closed while attempting to execute a task.");
      }

      // execute the task
      return worker.enlist(task);
    }
  }

  /**
   * Returns the size of the worker thread pool.
   *
   * @return The size of the worker thread pool.
   */
  public int size() {
    return this.allThreads.size();
  }

  /**
   * Checks if this pool has been closed.  Once closed, the pool can no longer
   * be used to execute any further tasks.
   */
  public boolean isClosed() {
    synchronized (this.available) {
      return this.closed;
    }
  }

  /**
   * Closes this pool so no further tasks can be executed against it.
   *
   * @return The {@link List} of non-null {@link AsyncResult} instances
   *         describing the results from the concluding tasks.
   */
  public List<AsyncResult<T>> close()
  {
    // mark this pool as closed and notify
    synchronized (this.available) {
      this.closed = true;
      this.available.notifyAll();
    }

    List<AsyncResult<T>> results = new ArrayList<>(this.allThreads.size());
    // mark all the threads complete
    for (AsyncWorker thread: this.allThreads) {
      AsyncResult<T> result = thread.retire();
      if (result != null) {
        results.add(result);
      }
    }
    return results;
  }

  /**
   * A worker thread for performing asynchronous tasks.
   */
  private class AsyncWorker<T> extends Thread {
    /**
     * Flag to indicate if the worker thread is complete.
     */
    private boolean complete;

    /**
     * The task to execute.
     */
    private Task<T> currentTask = null;

    /**
     * Describes the result previous task.
     */
    private AsyncResult<T> previousResult;

    /**
     * Default constructor.
     */
    private AsyncWorker() {
      this.previousResult = null;
      this.complete = false;
    }

    /**
     * Resets the thread so it is ready to execute the next task.
     */
    private synchronized AsyncResult<T> reset() {
      AsyncResult<T> prevResult = this.previousResult;
      this.currentTask    = null;
      this.previousResult = null;
      return prevResult;
    }

    /**
     * Sets the current task for the thread and returns the result from the
     * previous execution.
     */
    private synchronized AsyncResult<T> enlist(Task<T> task) {
      AsyncResult<T> prevResult = this.reset();
      this.currentTask = task;
      this.notifyAll();
      return prevResult;
    }

    /**
     * Checks if the thread has been marked complete and should stop
     * processing tasks.
     *
     * @return <tt>true</tt> if this instance has been marked complete,
     *         otherwise <tt>false</tt>.
     */
    private synchronized boolean isComplete() {
      return this.complete;
    }

    /**
     * If this thread is busy, this waits for it to no longer be busy, then
     * marks this thread as completed, notifies, and joins against the thread.
     * Finally, this method returns the {@link AsyncResult} from the previous
     * task that was executed (if not yet consumed).
     */
    private AsyncResult<T> retire() {
      synchronized (this) {
        while (this.isBusy()) {
          try {
            this.wait(2000L);
          } catch (InterruptedException ignore) {
            // do nothing
          }
        }
        this.complete = true;
        this.notifyAll();
      }
      try {
        this.join();
      } catch (InterruptedException ignore) {
        // do nothing
      }
      return this.reset();
    }

    /**
     * Checks if this thread is currently busy executing a task.
     *
     * @return <tt>true</tt> if this thread is currently busy executing a
     *         task, otherwise <tt>false</tt>.
     */
    private synchronized boolean isBusy() {
      return (this.currentTask != null);
    }

    /**
     * Implement the run method to wait for the next task and execute it.
     * This continues until this thread is marked complete.
     */
    public void run()
    {
      AsyncWorkerPool pool = AsyncWorkerPool.this;

      // loop while not complete
      while (!this.isComplete() && !pool.isClosed()) {
        // get the next task to perform
        Task<T> task = null;
        synchronized (this) {
          // loop while not complete and no task
          while (this.currentTask == null && !this.isComplete()) {
            try {
              this.wait(10000L);
            } catch (InterruptedException ignore) {
              // do nothing
            }
          }
          task = this.currentTask;
        }

        // check if we have a task to do and if so, get the result
        AsyncResult<T> result = null;
        if (task != null) {
          try {
            // execute the task and record the result
            result = new AsyncResult<>(this.currentTask.execute(), null);

          } catch (Exception e) {
            // record any failure for the task
            result = new AsyncResult<>(null, e);
          }

          // update worker fields to clear the task and store the result
          synchronized (this) {
            this.currentTask = null;
            this.previousResult = result;
            // make sure to notify when done
            this.notifyAll();
          }
        }

        // return this instance to the pool
        synchronized (pool.available) {
          if (!pool.isClosed()) {
            pool.available.add(this);
            pool.available.notifyAll();
          }
        }
      }
    }
  }
}
