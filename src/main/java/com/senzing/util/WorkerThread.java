package com.senzing.util;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;

/**
 * Provides a simple worker thread that can be pooled and can execute a task
 * within the thread.
 */
public class WorkerThread extends Thread
{
  private boolean complete;

  private Task task = null;

  private Object result = null;

  private Exception failure = null;

  private boolean busy = false;

  public WorkerThread() {
    this.complete = false;
  }

  private void reset() {
    this.task = null;
    this.result = null;
    this.failure = null;
    this.busy = false;
  }

  public synchronized boolean isComplete() {
    return this.complete;
  }

  public synchronized void markComplete() {
    this.complete = true;
    this.notifyAll();
  }

  public synchronized boolean isBusy() {
    return this.busy;
  }

  public synchronized Object execute(Task task)
    throws Exception
  {
    try {
      if (this.isBusy()) {
        throw new IllegalStateException("Already busy with another task.");
      }

      // flag as busy
      this.busy = true;

      // set the runnable
      this.task = task;

      // notify
      this.notifyAll();

      // wait for completion (releasing the lock)
      while (this.task != null) {
        try {
          this.wait(2000L);

        } catch (InterruptedException ignore) {
          // ignore the exception
        }
      }

      // check for a failure
      if (this.failure != null) {
        Exception e = this.failure;
        this.reset();
        throw e;
      }

      // get the result
      Object result = this.result;

      // reset the worker thread
      this.reset();
      return result;

    } finally {
      this.reset();
    }
  }

  public void run() {
    synchronized (this) {
      // loop while not complete
      while (!this.isComplete()) {

        // loop while not complete and no task
        while (this.task == null && !this.isComplete()) {
          try {
            this.wait(10000L);
          } catch (InterruptedException ignore) {
            // do nothing
          }
        }

        // check if we have a task to do
        if (this.task != null) {
          try {
            // execute the task and record the result
            this.result = this.task.execute();

          } catch (Exception e) {
            // record any failure for the task
            this.failure = e;
          }
          // clear the task
          this.task = null;
        }
      }
    }
  }

  /**
   * The interface describing the tasks that can be performed by the
   * WorkerThread.
   */
  public static interface Task {
    Object execute() throws Exception;
  }

  /**
   *
   */
  public static class Pool {
    private List<WorkerThread> available;

    private IdentityHashMap<WorkerThread,WorkerThread> unavailable;

    public Pool(int count) {
      this("WorkerThread", count);
    }

    public Pool(String baseName, int count) {
      this.available = new ArrayList<>(count);
      this.unavailable = new IdentityHashMap<>();

      for (int index = 0; index < count; index++) {
        WorkerThread wt = new WorkerThread();
        wt.setName(baseName + "-" + index);
        this.available.add(wt);
      }
    }

    public Object execute(Task task)
        throws Exception
    {
      synchronized (this.available) {
        while (this.available.size() == 0) {
          try {
            this.wait(2000L);
          } catch (InterruptedException ignore) {
            // do nothing
          }
        }


      }
      return null;
    }
  }

}
