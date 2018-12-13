package com.senzing.api.model;

import java.util.Objects;

public class SzRecordId {
  private String dataSourceCode;

  private String recordId;

  public SzRecordId() {
    this.dataSourceCode = null;
    this.recordId = null;
  }

  public SzRecordId(String dataSourceCode, String recordId) {
    this.dataSourceCode = dataSourceCode;
    this.recordId = recordId;
  }

  public String getDataSourceCode() {
    return dataSourceCode;
  }

  public void setDataSourceCode(String dataSourceCode) {
    this.dataSourceCode = dataSourceCode;
  }

  public String getRecordId() {
    return recordId;
  }

  public void setRecordId(String recordId) {
    this.recordId = recordId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SzRecordId recordId1 = (SzRecordId) o;
    return Objects.equals(getDataSourceCode(), recordId1.getDataSourceCode()) &&
        Objects.equals(getRecordId(), recordId1.getRecordId());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getDataSourceCode(), getRecordId());
  }

  @Override
  public String toString() {
    return "SzRecordId{" +
        "dataSourceCode='" + dataSourceCode + '\'' +
        ", recordId='" + recordId + '\'' +
        '}';
  }
}
