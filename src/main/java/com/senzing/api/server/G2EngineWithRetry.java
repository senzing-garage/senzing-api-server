package com.senzing.api.server;

import com.senzing.g2.engine.G2ConfigMgr;
import com.senzing.g2.engine.G2Engine;
import com.senzing.g2.engine.Result;

class G2EngineWithRetry implements G2Engine {
  /**
   * The backing {@link G2Engine} instance.
   */
  private G2Engine engineApi;

  /**
   * The {@link G2ConfigMgr} to use for checking the current default config.
   */
  private G2ConfigMgr configMgrApi;

  /**
   * The owning {@link SzApiServer}.
   */
  private SzApiServer apiServer;

  /**
   * Constructs with the specified {@link G2Engine} instance and {@link 
   * G2ConfigMgr} instance.
   * @param g2Engine The backing {@link G2Engine} instance.
   * @param apiServer The {@link SzApiServer} that owns this instance.
   */
  G2EngineWithRetry(G2Engine g2Engine, SzApiServer apiServer)
  {
    this.engineApi    = g2Engine;
    this.apiServer    = apiServer;
  }

  /**
   * Checks if a retry is needed by comparing the active config ID to the
   * default config ID.  If they are the same then <tt>false</tt> is returned,
   * but if they differ then the {@link G2Engine} is reinitialized and 
   * <tt>true</tt> is returned.
   *
   * @param retried Indicates if we have already retried once.
   * @return <tt>true</tt> if the last operation should be retried, otherwise
   *         <tt>false</tt>.
   */
  private boolean checkRetryNeeded(boolean retried) {
    if (retried) return false;

    Boolean result = this.apiServer.ensureConfigCurrent(false);
    if (result == null) return false;
    return result;
  }

  @Override
  public int init(String s, String s1, boolean b) {
    throw new UnsupportedOperationException(
        "Cannot initialize an instance of G2EngineWithRetry with an INI file: "
        + s1);
  }

  @Override
  public int init(String s, String s1, boolean b, Result<Long> result) {
    throw new UnsupportedOperationException(
        "Cannot initialize an instance of G2EngineWithRetry with an INI file: "
        + s1);
  }

  @Override
  public int initV2(String s, String s1, boolean b) {
    return this.engineApi.initV2(s, s1, b);
  }

  @Override
  public int initWithConfigIDV2(String s, String s1, long l, boolean b) {
    return this.engineApi.initWithConfigIDV2(s, s1, l, b);
  }

  @Override
  public int reinitV2(long l) {
    return this.engineApi.reinitV2(l);
  }

  @Override
  public int destroy() {
    return this.engineApi.destroy();
  }

  @Override
  public int primeEngine() {
    return this.engineApi.primeEngine();
  }

  @Override
  public int purgeRepository() {
    return this.engineApi.purgeRepository();
  }

  @Override
  public String stats() {
    return this.engineApi.stats();
  }

  @Override
  public int exportConfig(StringBuffer sb) {
    return this.engineApi.exportConfig(sb);
  }

  @Override
  public int exportConfig(StringBuffer sb, Result<Long> result) {
    return this.engineApi.exportConfig(sb, result);
  }

  @Override
  public int getActiveConfigID(Result<Long> result) {
    return this.engineApi.getActiveConfigID(result);
  }

  @Override
  public int getRepositoryLastModifiedTime(Result<Long> result) {
    return this.engineApi.getRepositoryLastModifiedTime(result);
  }

  @Override
  public int addRecord(String s, String s1, String s2, String s3) {
    int     returnCode;
    boolean retried = false;
    do {
      returnCode = this.engineApi.addRecord(s, s1, s2, s3);
    } while (returnCode != 0 && (retried = this.checkRetryNeeded(retried)));
    return returnCode;
  }

  @Override
  public int replaceRecord(String s, String s1, String s2, String s3) {
    int     returnCode;
    boolean retried = false;
    do {
      returnCode = this.engineApi.replaceRecord(s, s1, s2, s3);
    } while (returnCode != 0 && (retried = this.checkRetryNeeded(retried)));
    return returnCode;
  }

  @Override
  public int addRecordWithReturnedRecordID(String s, StringBuffer sb, String s1, String s2) {
    int     returnCode;
    boolean retried = false;
    do {
      returnCode = this.engineApi.addRecordWithReturnedRecordID(s, sb, s1, s2);
    } while (returnCode != 0 && (retried = this.checkRetryNeeded(retried)));
    return returnCode;
  }

  @Override
  public int deleteRecord(String s, String s1, String s2) {
    int     returnCode;
    boolean retried = false;
    do {
      returnCode = this.engineApi.deleteRecord(s, s1, s2);
    } while (returnCode != 0 && (retried = this.checkRetryNeeded(retried)));
    return returnCode;
  }

  @Override
  public int reevaluateRecord(String s, String s1, int i) {
    int     returnCode;
    boolean retried = false;
    do {
      returnCode = this.engineApi.reevaluateRecord(s, s1, i);
    } while (returnCode != 0 && (retried = this.checkRetryNeeded(retried)));
    return returnCode;
  }

  @Override
  public int reevaluateEntity(long l, int i) {
    int     returnCode;
    boolean retried = false;
    do {
      returnCode = this.engineApi.reevaluateEntity(l, i);
    } while (returnCode != 0 && (retried = this.checkRetryNeeded(retried)));
    return returnCode;
  }

  @Override
  public int searchByAttributes(String s, StringBuffer sb) {
    int     returnCode;
    boolean retried = false;
    do {
      returnCode = this.engineApi.searchByAttributes(s, sb);
    } while (returnCode != 0 && (retried = this.checkRetryNeeded(retried)));
    return returnCode;
  }

  @Override
  public int searchByAttributesV2(String s, int i, StringBuffer sb) {
    int     returnCode;
    boolean retried = false;
    do {
      returnCode = this.engineApi.searchByAttributesV2(s, i, sb);
    } while (returnCode != 0 && (retried = this.checkRetryNeeded(retried)));
    return returnCode;
  }

  @Override
  public int getEntityByEntityID(long l, StringBuffer sb) {
    int     returnCode;
    boolean retried = false;
    do {
      returnCode = this.engineApi.getEntityByEntityID(l, sb);
    } while (returnCode != 0 && (retried = this.checkRetryNeeded(retried)));
    return returnCode;
  }

  @Override
  public int getEntityByEntityIDV2(long l, int i, StringBuffer sb) {
    int     returnCode;
    boolean retried = false;
    do {
      returnCode = this.engineApi.getEntityByEntityIDV2(l, i, sb);
    } while (returnCode != 0 && (retried = this.checkRetryNeeded(retried)));
    return returnCode;
  }

  @Override
  public int getEntityByRecordID(String s, String s1, StringBuffer sb) {
    int     returnCode;
    boolean retried = false;
    do {
      returnCode = this.engineApi.getEntityByRecordID(s, s1, sb);
    } while (returnCode != 0 && (retried = this.checkRetryNeeded(retried)));
    return returnCode;
  }

  @Override
  public int getEntityByRecordIDV2(String s, String s1, int i, StringBuffer sb) {
    int     returnCode;
    boolean retried = false;
    do {
      returnCode = this.engineApi.getEntityByRecordIDV2(s, s1, i, sb);
    } while (returnCode != 0 && (retried = this.checkRetryNeeded(retried)));
    return returnCode;
  }

  @Override
  public int findPathByEntityID(long l, long l1, int i, StringBuffer sb) {
    int     returnCode;
    boolean retried = false;
    do {
      returnCode = this.engineApi.findPathByEntityID(l, l1, i, sb);
    } while (returnCode != 0 && (retried = this.checkRetryNeeded(retried)));
    return returnCode;
  }

  @Override
  public int findPathByEntityIDV2(long l, long l1, int i, int i1, StringBuffer sb) {
    int     returnCode;
    boolean retried = false;
    do {
      returnCode = this.engineApi.findPathByEntityIDV2(l, l1, i, i1, sb);
    } while (returnCode != 0 && (retried = this.checkRetryNeeded(retried)));
    return returnCode;
  }

  @Override
  public int findPathByRecordID(String s, String s1, String s2, String s3, int i, StringBuffer sb) {
    int     returnCode;
    boolean retried = false;
    do {
      returnCode = this.engineApi.findPathByRecordID(s, s1, s2, s3, i, sb);
    } while (returnCode != 0 && (retried = this.checkRetryNeeded(retried)));
    return returnCode;
  }

  @Override
  public int findPathByRecordIDV2(String s, String s1, String s2, String s3, int i, int i1, StringBuffer sb) {
    int     returnCode;
    boolean retried = false;
    do {
      returnCode = this.engineApi.findPathByRecordIDV2(s, s1, s2, s3, i, i1, sb);
    } while (returnCode != 0 && (retried = this.checkRetryNeeded(retried)));
    return returnCode;
  }

  @Override
  public int findPathExcludingByEntityID(long l, long l1, int i, String s, int i1, StringBuffer sb) {
    int     returnCode;
    boolean retried = false;
    do {
      returnCode = this.engineApi.findPathExcludingByEntityID(l, l1, i, s, i1, sb);
    } while (returnCode != 0 && (retried = this.checkRetryNeeded(retried)));
    return returnCode;
  }

  @Override
  public int findPathExcludingByRecordID(String s, String s1, String s2, String s3, int i, String s4, int i1, StringBuffer sb) {
    int     returnCode;
    boolean retried = false;
    do {
      returnCode = this.engineApi.findPathExcludingByRecordID(s, s1, s2, s3, i, s4, i1, sb);
    } while (returnCode != 0 && (retried = this.checkRetryNeeded(retried)));
    return returnCode;
  }

  @Override
  public int findPathIncludingSourceByEntityID(long l, long l1, int i, String s, String s1, int i1, StringBuffer sb) {
    int     returnCode;
    boolean retried = false;
    do {
      returnCode = this.engineApi.findPathIncludingSourceByEntityID(l, l1, i, s, s1, i1, sb);
    } while (returnCode != 0 && (retried = this.checkRetryNeeded(retried)));
    return returnCode;
  }

  @Override
  public int findPathIncludingSourceByRecordID(String s, String s1, String s2, String s3, int i, String s4, String s5, int i1, StringBuffer sb) {
    int     returnCode;
    boolean retried = false;
    do {
      returnCode = this.engineApi.findPathIncludingSourceByRecordID(s, s1, s2, s3, i, s4, s5, i1, sb);
    } while (returnCode != 0 && (retried = this.checkRetryNeeded(retried)));
    return returnCode;
  }

  @Override
  public int findNetworkByEntityID(String s, int i, int i1, int i2, StringBuffer sb) {
    int     returnCode;
    boolean retried = false;
    do {
      returnCode = this.engineApi.findNetworkByEntityID(s, i, i1, i2, sb);
    } while (returnCode != 0 && (retried = this.checkRetryNeeded(retried)));
    return returnCode;
  }

  @Override
  public int findNetworkByEntityIDV2(String s, int i, int i1, int i2, int i3, StringBuffer sb) {
    int     returnCode;
    boolean retried = false;
    do {
      returnCode = this.engineApi.findNetworkByEntityIDV2(s, i, i1, i2, i3, sb);
    } while (returnCode != 0 && (retried = this.checkRetryNeeded(retried)));
    return returnCode;
  }

  @Override
  public int findNetworkByRecordID(String s, int i, int i1, int i2, StringBuffer sb) {
    int     returnCode;
    boolean retried = false;
    do {
      returnCode = this.engineApi.findNetworkByRecordID(s, i, i1, i2, sb);
    } while (returnCode != 0 && (retried = this.checkRetryNeeded(retried)));
    return returnCode;
  }

  @Override
  public int findNetworkByRecordIDV2(String s, int i, int i1, int i2, int i3, StringBuffer sb) {
    int     returnCode;
    boolean retried = false;
    do {
      returnCode = this.engineApi.findNetworkByRecordIDV2(s, i, i1, i2, i3, sb);
    } while (returnCode != 0 && (retried = this.checkRetryNeeded(retried)));
    return returnCode;
  }

  @Override
  public int getRecord(String s, String s1, StringBuffer sb) {
    int     returnCode;
    boolean retried = false;
    do {
      returnCode = this.engineApi.getRecord(s, s1, sb);
    } while (returnCode != 0 && (retried = this.checkRetryNeeded(retried)));
    return returnCode;
  }

  @Override
  public int getRecordV2(String s, String s1, int i, StringBuffer sb) {
    int     returnCode;
    boolean retried = false;
    do {
      returnCode = this.engineApi.getRecordV2(s, s1, i, sb);
    } while (returnCode != 0 && (retried = this.checkRetryNeeded(retried)));
    return returnCode;
  }

  @Override
  public long exportJSONEntityReport(int i) {
    return this.engineApi.exportJSONEntityReport(i);
  }

  @Override
  public long exportCSVEntityReport(int i) {
    return this.engineApi.exportCSVEntityReport(i);
  }

  @Override
  public long exportCSVEntityReportV2(String s, int i) {
    return this.engineApi.exportCSVEntityReportV2(s, i);
  }

  @Override
  public String fetchNext(long l) {
    return this.engineApi.fetchNext(l);
  }

  @Override
  public void closeExport(long l) {
    this.engineApi.closeExport(l);
  }

  @Override
  public int processRedoRecord(StringBuffer sb) {
    int     returnCode;
    boolean retried = false;
    do {
      returnCode = this.engineApi.processRedoRecord(sb);
    } while (returnCode != 0 && (retried = this.checkRetryNeeded(retried)));
    return returnCode;
  }

  @Override
  public int getRedoRecord(StringBuffer sb) {
    return this.engineApi.getRedoRecord(sb);
  }

  @Override
  public long countRedoRecords() {
    return this.engineApi.countRedoRecords();
  }

  @Override
  public int process(String s) {
    int     returnCode;
    boolean retried = false;
    do {
      returnCode = this.engineApi.process(s);
    } while (returnCode != 0 && (retried = this.checkRetryNeeded(retried)));
    return returnCode;
  }

  @Override
  public int process(String s, StringBuffer sb) {
    int     returnCode;
    boolean retried = false;
    do {
      returnCode = this.engineApi.process(s, sb);
    } while (returnCode != 0 && (retried = this.checkRetryNeeded(retried)));
    return returnCode;
  }

  @Override
  public String getLastException() {
    return this.engineApi.getLastException();
  }

  @Override
  public int getLastExceptionCode() {
    return this.engineApi.getLastExceptionCode();
  }

  @Override
  public void clearLastException() {
    this.engineApi.clearLastException();
  }
}
