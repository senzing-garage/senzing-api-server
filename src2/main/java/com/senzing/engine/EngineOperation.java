package com.senzing.engine;

import static com.senzing.engine.EngineType.*;

public enum EngineOperation {

  /**
   * Test a connection with the Engine process
   */
  NO_OP(null),

  /**
   * The reinitialize request is a special request to trigger reinitializing
   * all the engines that are currently initialized.
   */
  REINITIALIZE(null),

  /**
   * The purge-and-reinitialize request is a special request to trigger purging
   * all data and reinitializing all the engines that are currently initialized.
   */
  PURGE_AND_REINITIALIZE(null),

  /**
   *
   */
  PRIME_ENGINE(RESOLVER),

  /**
   *
   */
  GET_ACTIVE_CONFIG_ID(RESOLVER),

  /**
   *
   */
  GET_REPOSITORY_LAST_MODIFIED(RESOLVER),

  /**
   *
   */
  EXPORT_CONFIG(RESOLVER),

  /**
   *
   */
  PROCESS(RESOLVER),

  /**
   *
   */
  LICENSE(RESOLVER),

  /**
   *
   */
  DESTROY(RESOLVER, true),

  /**
   *
   */
  STATS(RESOLVER),

  /**
   *
   */
  ADD_RECORD(RESOLVER),

  /**
   *
   */
  GET_RECORD(RESOLVER),

  /**
   *
   */
  GET_ENTITY_BY_ENTITY_ID(RESOLVER),

  /**
   *
   */
  GET_ENTITY_BY_RECORD_ID(RESOLVER),

  /**
   *
   */
  FIND_NETWORK_BY_ENTITY_ID(RESOLVER),

  /**
   *
   */
  EXPORT_JSON_ENTITY_REPORT(RESOLVER),

  /**
   *
   */
  EXPORT_CSV_ENTITY_REPORT(RESOLVER),

  /**
   *
   */
  EXPORT_FETCH_NEXT(RESOLVER, true, true),

  /**
   *
   */
  CLOSE_EXPORT(RESOLVER, true),

  /**
   * Searches for resolved entities
   */
  SEARCH_ENTITIES(RESOLVER),

  /**
   *
   */
  AUDIT_OPEN_SESSION(AUDITOR),

  /**
   *
   */
  AUDIT_CANCEL_SESSION(AUDITOR, true),

  /**
   *
   */
  AUDIT_CLOSE_SESSION(AUDITOR, true),

  /**
   *
   */
  AUDIT_SUMMARY_DATA(AUDITOR, true),

  /**
   *
   */
  AUDIT_USED_MATCH_KEYS(AUDITOR, true),

  /**
   *
   */
  AUDIT_USED_PRINCIPLES(AUDITOR, true),

  /**
   *
   */
  AUDIT_OPEN_REPORT(AUDITOR, true),

  /**
   *
   */
  AUDIT_FETCH_NEXT(AUDITOR, true, true),

  /**
   *
   */
  AUDIT_CLOSE_REPORT(AUDITOR, true),

  /**
   *
   */
  AUDIT_DESTROY(AUDITOR, true),

  /**
   *
   */
  QUERY_DESTROY(QUERIST, true),

  /**
   *
   */
  QUERY_ENTITIES(QUERIST);

  private EngineType engineType;

  private boolean requiresReset;

  private boolean cancelOnRestart;

  public EngineType getEngineType() {
    return this.engineType;
  }

  public boolean isDestroyOperation() {
    switch (this) {
      case DESTROY:
      case AUDIT_DESTROY:
      case QUERY_DESTROY:
        return true;
      default:
        return false;
    }
  }

  public boolean isCancelledOnEngineRestart() {
    return this.cancelOnRestart;
  }

  private EngineOperation(EngineType engineType) {

    this(engineType, false, false);
  }

  private EngineOperation(EngineType engineType, boolean cancelOnRestart) {
    this(engineType, cancelOnRestart, false);
  }

  private EngineOperation(EngineType  engineType,
                          boolean     cancelOnRestart,
                          boolean     requiresReset)
  {
    this.engineType       = engineType;
    this.cancelOnRestart  = cancelOnRestart;
    this.requiresReset    = requiresReset;
  }

  public boolean isResetRequired() {
    return this.requiresReset;
  }

  public static final String ENTITY_ID          = "ENTITY_ID";
  public static final String DATA_SOURCE        = "DATA_SOURCE";
  public static final String RECORD_ID          = "RECORD_ID";
  public static final String LOAD_ID            = "LOAD_ID";
  public static final String SESSION_ID         = "SESSION_ID";
  public static final String REPORT_ID          = "REPORT_ID";
  public static final String FROM_DATA_SOURCE   = "FROM_DATA_SOURCE";
  public static final String TO_DATA_SOURCE     = "TO_DATA_SOURCE";
  public static final String MATCH_LEVEL        = "MATCH_LEVEL";
  public static final String EXPORT_HANDLE      = "EXPORT_HANDLE";
  public static final String MATCH_LEVEL_FLAGS  = "MATCH_LEVEL_FLAGS";
  public static final String EXPORT_FLAGS       = "EXPORT_FLAGS";
  public static final String FETCH_COUNT        = "FETCH_COUNT";
  public static final String ENTITY_IDS         = "ENTITY_IDS";
  public static final String MAX_DEGREES        = "MAX_DEGREES";
  public static final String BUILD_OUT_DEGREES  = "BUILD_OUT_DEGREES";
  public static final String MAX_ENTITY_COUNT   = "MAX_ENTITY_COUNT";
}
