package com.senzing.util;

import java.util.LinkedHashMap;
import java.util.Map;

public class Timers {
  private static long now() {
    return System.currentTimeMillis();
  }

  private static class TimerInfo {
    private Long    start        = null;
    private long    accumulated  = 0;

    private TimerInfo(long startTime) {
      this.start = startTime;
    }

    private TimerInfo() {
      this(now());
    }

    private TimerInfo(TimerInfo other) {
      this(other, now());
    }

    private TimerInfo(TimerInfo other, long atTime) {
      this.start        = null;
      this.accumulated  = other.getDuration(atTime);
    }

    private long getDuration() {
      return this.getDuration(now());
    }

    private long getDuration(long atTime) {
      if (this.start == null) return this.accumulated;

      return this.accumulated + (atTime - this.start);
    }

    private boolean isRunning() {
      return (this.start != null);
    }

    private boolean isPaused() {
      return (this.start == null);
    }

    private boolean pause() {
      return this.pause(now());
    }

    private boolean pause(long atTime) {
      if (this.start == null) return false;
      long duration = atTime - this.start;
      this.accumulated += duration;
      this.start = null;
      return true;
    }

    private boolean resume() {
      return this.resume(now());
    }

    private boolean resume(long atTime) {
      if (this.start != null) return false;
      this.start = atTime;
      return true;
    }

    private void mergeWith(TimerInfo timerInfo) {
      this.accumulated += timerInfo.getDuration();
    }

    private void mergeWith(TimerInfo timerInfo, long now) {
      this.accumulated += timerInfo.getDuration(now);
    }
  }

  private Map<String, TimerInfo> timerInfos;

  /**
   * Constructs with zero or more timer names that represent the initial
   * timer keys.  All of these timers will start immediately with the same
   * timestamp.
   *
   * @param initialTimers The zero or more names of the initial timers.
   */
  public Timers(String... initialTimers) {
    this.timerInfos = new LinkedHashMap<>();
    long startTime = now();
    if (initialTimers != null) {
      for (String initialTimer : initialTimers) {
        this.timerInfos.put(initialTimer, new TimerInfo(startTime));
      }
    }
  }

  /**
   * Checks if the specified timer exists.
   *
   * @return <tt>true</tt> if the timer exists, otherwise <tt>false</tt>.
   */
  public boolean hasTimer(String timerName) {
    return this.timerInfos.containsKey(timerName);
  }

  /**
   * Checks if the specified timer exists and is paused.
   *
   * @return <tt>true</tt> if the timer exists and is paused, otherwise
   *         <tt>false</tt>.
   */
  public boolean isPaused(String timerName) {
    TimerInfo info = this.timerInfos.get(timerName);
    return (info != null && info.isPaused());
  }

  /**
   * Checks if the specified timer exists and is running.
   *
   * @return <tt>true</tt> if the timer exists and is running, otherwise
   *         <tt>false</tt>.
   */
  public boolean isRunning(String timerName) {
    TimerInfo info = this.timerInfos.get(timerName);
    return (info != null && info.isRunning());
  }

  /**
   * Starts one or more new timers with the specified names.  This method
   * checks each of the timer names to see if they already exist and if a
   * timer by the specified name already exists and is running then it is
   * skipped (ignored).  If it exists and is paused then it is resumed.  This
   * method returns the number of timers that were created or resumed.
   *
   * @param timerName The name of the first timer to start.
   *
   * @param moreTimerNames Additional timer names to be started with the same
   *                       start time.
   *
   * @return The number of timer names that were for new timers that were
   *         created.
   */
  public int start(String timerName, String... moreTimerNames) {
    int count = 0;
    long now = now();
    TimerInfo info = this.timerInfos.get(timerName);
    if (info == null) {
      this.timerInfos.put(timerName, new TimerInfo(now));
      count++;
    } else if (info.resume()) {
      count++;
    }

    if (moreTimerNames != null) {
      for (String addlTimerName : moreTimerNames) {
        info = this.timerInfos.get(addlTimerName);
        if (info == null) {
          this.timerInfos.put(addlTimerName, new TimerInfo(now));
          count++;
        } else if (info.resume()) {
          count++;
        }
      }
    }

    return count;
  }

  /**
   * Pauses the one or more named timers.  If a named timer does not exist or
   * is not running it is skipped.  This method returns the number of timers
   * that were found and in a running state and therefore successfully paused.
   *
   * @param timerName The name of the timer.
   *
   * @param moreTimerNames Additional timer names to pause at the same time.
   *
   * @return The number of timers that were successfully paused.
   */
  public int pause(String timerName, String... moreTimerNames) {
    int count = 0;
    long now = now();
    TimerInfo info = this.timerInfos.get(timerName);
    if (info != null && info.pause(now)) count++;
    if (moreTimerNames != null) {
      for (String addlTimerName : moreTimerNames) {
        info = this.timerInfos.get(addlTimerName);
        if (info != null && info.pause(now)) count++;
      }
    }
    return count;
  }

  /**
   * Resumes the one or more named timers.  If a named timer does not exist or
   * is not paused it is skipped.  This method returns the number of timers
   * that were found in a paused state and therefore successfully resumed.
   *
   * @param timerName The name of the timer.
   *
   * @param moreTimerNames Additional timer names to pause at the same time.
   *
   * @return <tt>null</tt> if the timer name is not recognized, <tt>false</tt>
   *         if the timer is found, but was already running, and <tt>true</tt>
   *         if the timer is found and was paused and was successfully resumed.
   */
  public int resume(String timerName, String... moreTimerNames) {
    int count = 0;
    long now = now();
    TimerInfo info = this.timerInfos.get(timerName);
    if (info != null && info.resume(now)) count++;
    if (moreTimerNames != null) {
      for (String addlTimerName: moreTimerNames) {
        info = this.timerInfos.get(addlTimerName);
        if (info != null && info.resume(now)) count++;
      }
    }
    return count;
  }

  /**
   * Pauses all the timers that were running and returns the number of timers
   * that were running that were successfully paused.
   *
   * @return The number of timers that were running and were successfully
   *         paused.
   */
  public int pauseAll() {
    int count = 0;
    for (TimerInfo info : this.timerInfos.values()) {
      if (info.pause()) count++;
    }
    return count;
  }

  /**
   * Resumes all the timers that were paused and returns the number of timers
   * that were paused that were successfully resumed.
   *
   * @return The number of timers that were paused and were successfully
   *         resumed.
   */
  public int resumeAll() {
    int count = 0;
    for (TimerInfo info : this.timerInfos.values()) {
      if (info.resume()) count++;
    }
    return count;
  }

  /**
   * Returns a {@link Map} of all timer names to the their current durations.
   * If a timer is currently running it remains running, but the duration
   * is returned as if it was paused at the time this method was called.  If
   * the timer is currently paused it remains paused and the duration
   * accumulated at the time it was paused is returned.
   *
   * @return A {@link Map} of {@link String} timer name keys to {@link Long}
   *         values representing the current timings.
   */
  public Map<String, Long> getTimings() {
    Map<String,Long> result = new LinkedHashMap<>();
    long now = now();
    this.timerInfos.entrySet().forEach(e -> {
      result.put(e.getKey(), e.getValue().getDuration(now));
    });
    return result;
  }

  /**
   * Merges the durations from the specified {@link Timers} with this {@link
   * Timers} instance.  All the durations and timers from the specified
   * {@link Timers} instance are added to this one.
   *
   * @param timers The {@link Timers} to merge with.
   */
  public void mergeWith(Timers timers) {
    if (timers == null) return;
    long now = now();
    timers.timerInfos.entrySet().forEach(e -> {
      String    key    = e.getKey();
      TimerInfo info1  = this.timerInfos.get(key);
      TimerInfo info2  = e.getValue();
      if (info1 == null) {
        info1 = new TimerInfo(info2, now);
        this.timerInfos.put(key, info1);
      } else {
        info1.mergeWith(info2, now);
      }
    });
  }
}
