package com.senzing.api.model;

/**
 * The state of a bulk load.
 */
public enum SzBulkLoadStatus {
  /**
   * The bulk load has not yet started.
   */
  NOT_STARTED,

  /**
   * The bulk load is in progress.
   */
  IN_PROGRESS,

  /**
   * The bulk load has been aborted.
   */
  ABORTED,

  /**
   * The bulk load has completed without being aborted.
   */
  COMPLETED;
}
