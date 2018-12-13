package com.senzing.api.engine.process;

/**
 * Represents possible states of {@link EngineProcess}
 *
 */
public enum EngineProcessState {
  /**
   * Indicates if the project is active (i.e.: has an EngineProcess peer)
   */
  ACTIVE,

  /**
   * Indicates we are currently resolving.
   */
  RESOLVING,

  /**
   * Indicates we are currently priming the audit.
   */
  PRIMING_AUDIT,

  /**
   * Indicates we are current priming the audit summary.  This step will
   * complete <b>BEFORE</b> {@link #PRIMING_AUDIT} will complete.
   */
  PRIMING_AUDIT_SUMMARY;
}
