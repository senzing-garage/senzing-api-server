package com.senzing.api.engine.process;

import com.senzing.util.AccessToken;
import com.senzing.api.engine.EngineRequest;
import com.senzing.api.engine.EngineResponse;

import java.util.*;

import static com.senzing.api.Workbench.*;

public class SendPrioritizer extends Thread {
  /**
   * Internal container class for wrapping the enqueued {@link EngineRequest}
   * instances and providing a focal point for synchronizing and waiting for
   * a response via notify() calls without having to wake all waiting threads.
   */
  private static class ThreadRequest {
    private Thread thread;
    private EngineRequest request;
    private EngineResponse response;
    private boolean complete;

    ThreadRequest(EngineRequest request) {
      this.thread   = Thread.currentThread();
      this.request  = request;
      this.response = null;
      this.complete = false;
    }
  }

  /**
   * The object to use for synchronization.
   */
  private final Object monitor = new Object();

  /**
   * The backing {@link EngineProcess}.
   */
  private EngineProcess engineProcess;

  /**
   * The ranks for each thread that is registered.
   */
  private Map<Thread,Long> threadRanks;

  /**
   * The queue of {@link ThreadRequest} instances sorted for highest priority
   * first.
   */
  private List<ThreadRequest> requestQueue;

  /**
   * The rank value to assign to the next thread that is prioritized.
   */
  private long nextRank;

  /**
   * Flag indicating if this instance has completed.
   */
  private boolean completed;

  /**
   * The pool of {@link Executor} instances.
   */
  private List<Executor> executorPool;

  /**
   * The {@link AccessToken} to use to authorize privileged access.
   */
  private final AccessToken accessToken;

  /**
   * Constructs with the specified {@link EngineProcess}
   * 
   * @param engineProcess The {@link EngineProcess} to use for sending.
   *
   * @param concurrency The max number of concurrent requests.
   *
   * @param token The {@link AccessToken} used by the owner to gain privilege to
   *              mark completion of this instannce.
   */
  public SendPrioritizer(EngineProcess  engineProcess,
                         int            concurrency,
                         AccessToken    token)
  {
    if (concurrency < 1) {
      throw new IllegalArgumentException(
          "Concurrency must be greater than zero: " + concurrency);
    }
    this.engineProcess  = engineProcess;
    this.threadRanks    = new IdentityHashMap<>();
    this.requestQueue   = new LinkedList<>();
    this.nextRank       = Long.MAX_VALUE - 1;
    this.completed      = false;
    this.accessToken    = token;
    if (concurrency > 1) {
      this.executorPool = new ArrayList<>(concurrency);
      for (int index = 0; index < concurrency; index++) {
        Executor executor = new Executor();
        this.executorPool.add(executor);
        executor.start();
      }
    } else{
      this.executorPool = null;
    }
  }

  /**
   * Registers the specified thread (if not yet registered) and gives it
   * the highest priority.
   *
   * @param thread The thread to register with highest priority.
   */
  public void prioritizeThread(Thread thread) {
    synchronized (this.monitor) {
      // check if our "top rank" value has not been overly decremented
      if (this.nextRank > Long.MIN_VALUE) {
        // mark the thread as top-ranked -- this condition should always be true
        this.threadRanks.put(thread, this.nextRank--);

      } else {
        // we somehow used up the whole range of long numbers -- we assume we
        // cannot possibly have done so by having that many registered threads
        List<Thread> threads = new ArrayList<>(this.threadRanks.keySet());

        // sort them in reverse order of rank
        threads.sort((t1, t2) -> {
          long r1 = this.getThreadRank(t1);
          long r2 = this.getThreadRank(t2);
          if (r1 > r2) return -1;
          if (r2 > r1) return 1;
          return 0;
        });

        // reset to the top rank
        this.nextRank = Long.MAX_VALUE - 1;

        // re-insert the values with new rank values
        for (Thread t : threads) {
          this.threadRanks.put(t, this.nextRank--);
        }
      }
    }
  }

  /**
   * Returns the rank for the specified thread.  If the specified {@link Thread}
   * is not registered then {@link Long#MAX_VALUE} is returned.
   *
   * @param thread The {@link Thread} to unregister.
   */
  private long getThreadRank(Thread thread) {
    synchronized (this.monitor) {
      Long rank = this.threadRanks.get(thread);
      if (rank == null) return Long.MAX_VALUE;
      return rank;
    }
  }

  /**
   * Unregisters the specified thread and forces it to lowest priority (along
   * with all other unregistered threads).  This also removes the reference to
   * the specified thread for garbage collection purposes.
   *
   * @param thread The thread to unregister.
   */
  public void unregisterThread(Thread thread) {
    synchronized (this.monitor) {
      this.assertNotComplete();
      this.threadRanks.remove(thread);
    }
  }

  /**
   * Sends the specified request and returns the response.  This method may
   * take a long while to return if the thread sending the request is lower
   * priority.
   *
   * @param request The {@link EngineRequest} to send.
   *
   * @return The {@link EngineResponse} for the specified request.
   */
  public EngineResponse send(EngineRequest request) {
    // create the wrapper object
    ThreadRequest tr = new ThreadRequest(request);

    // sync on the monitor
    synchronized (this.monitor) {
      this.assertNotComplete();

      // add the new item
      this.requestQueue.add(tr);

      // sort the requests by priority
      this.requestQueue.sort((r1, r2) -> {
        // get the thread ranks
        long rank1 = this.getThreadRank(r1.thread);
        long rank2 = this.getThreadRank(r2.thread);

        // lower rank values come first
        if (rank1 < rank2) return -1;
        if (rank2 > rank1) return 1;

        // if ranks are equal then sort by ID -- should not happen
        long id1 = r1.request.getRequestId();
        long id2 = r2.request.getRequestId();
        if (id1 < id2) return -1;
        if (id2 > id1) return 1;

        // all be equal then return zero -- should not happen
        return 0;
      });

      // notify listeners
      this.monitor.notifyAll();
    }

    // sync on the wrapper object we just created
    synchronized (tr) {
      // wait for completion (i.e.: a response is received)
      while (!tr.complete) {
        try {
          tr.wait(5000L);
        } catch (InterruptedException ignore) {
          ignore.printStackTrace();
          // ignore
        }
      }
      
      // return the response
      return tr.response;
    }
  }

  private void assertNotComplete() {
    synchronized (this.monitor) {
      if (this.isComplete()) {
        throw new IllegalStateException(
            "This instance has already been marked completed.");
      }
    }
  }

  public boolean isComplete() {
    synchronized (this.monitor) {
      // checks if this instance is completed
      return this.completed;
    }
  }

  public void complete(AccessToken accessToken) {
    if (this.accessToken != null && this.accessToken != accessToken) {
      throw new UnsupportedOperationException(
          "Cannot mark completed unless the proper access token is provided.");
    }
    synchronized (this.monitor) {
      // mark completion
      this.completed = true;

      // we don't care about priority any more since no new requests are coming
      this.threadRanks.clear();

      // notify any listeners
      this.monitor.notifyAll();
    }
  }

  /**
   * Implemented to send the enqueued requests according to the thread priority.
   *
   */
  public void run() {
    while (!this.isComplete()) {
      // sync on the request queue to wait for a request
      synchronized (this.monitor) {
        // wait for the queue to have something to send
        while (this.requestQueue.size() == 0) {
          try {
            this.monitor.wait(5000L);
          } catch (InterruptedException ignore) {
            ignore.printStackTrace();
            // ignore
          }
        }
      }

      // get an executor
      Executor executor = null;
      if (this.executorPool != null) {
        synchronized (this.executorPool) {
          if (this.executorPool.size() == 0) {
            try {
              this.executorPool.wait(5000L);
            } catch (InterruptedException ignore) {
              log(ignore);
            }
          }
          executor = this.executorPool.remove(0);
        }
        if (executor == null) continue;
      }

      // dequeue the request
      ThreadRequest tr;
      synchronized (this.monitor) {
        tr = this.requestQueue.remove(0);
      }

      // execute the request
      if (executor != null) {
        // if we have an executor use it to achieve concurrency
        executor.execute(tr);

      } else {
        // for concurrency of one, just execute the request without overhead
        EngineResponse response = this.engineProcess.sendSyncRequest(tr.request);

        // synchronize and mark completion
        synchronized (tr) {
          tr.response = response;
          tr.complete = true;
          tr.notifyAll();
        }
      }
    }
  }

  /**
   * Internal class to fascilitate concurrency.
   */
  private class Executor extends Thread {
    private ThreadRequest currentRequest = null;

    private Executor() {
      this.currentRequest = null;
    }

    private synchronized void execute(ThreadRequest request) {
      if (this.currentRequest != null) {
        throw new IllegalStateException(
            "Cannot execute a request while one is pending.");
      }
      this.currentRequest = request;
      this.notifyAll();
    }

    public void run() {
      final SendPrioritizer prioritizer = SendPrioritizer.this;
      while (!prioritizer.isComplete()) {
        synchronized (this) {
          // wait until a request is available
          if (this.currentRequest == null)
          {
            try {
              this.wait(15000L);
            } catch (InterruptedException ignore) {
              log(ignore);
            }
          }
          if (this.currentRequest == null) continue;
        }

        // get the request to execute
        ThreadRequest tr = this.currentRequest;

        // clear the field holding the reference to the request
        this.currentRequest = null;

        // get the engine process
        EngineProcess engineProcess = prioritizer.engineProcess;

        // get the response
        EngineResponse response = engineProcess.sendSyncRequest(tr.request);

        // synchronize and mark completion
        synchronized (tr) {
          tr.response = response;
          tr.complete = true;
          tr.notifyAll();
        }

        synchronized (prioritizer.executorPool) {
          executorPool.add(this);
          executorPool.notifyAll();
        }
      }
    }

  }
}
