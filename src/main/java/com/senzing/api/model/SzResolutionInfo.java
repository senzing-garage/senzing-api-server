package com.senzing.api.model;

import java.util.List;

public class SzResolutionInfo {
  /**
   *
   */
  private String dataSource;

  /**
   *
   */
  private String recordId;

  /**
   *
   */
  private List<Long> affectedEntities;

  /**
   *
   */
  private List<SzFlaggedEntity> flaggedEntities;

  /**
   * Default constructor.
   */
  public SzResolutionInfo() {
  }
}
