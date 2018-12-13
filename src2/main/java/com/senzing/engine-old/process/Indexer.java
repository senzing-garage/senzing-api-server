package com.senzing.api.engine.process;

//import com.senzing.g2.query.DocumentInfo;
//import com.senzing.g2.query.G2Indexer;
//import com.senzing.api.ObservedEntityId;

//import static com.senzing.api.ServerErrorLog.*;
//import static com.senzing.api.Workbench.*;
//import static com.senzing.api.engine.process.EngineProcess.*;

class Indexer extends Thread {
  private static final int FLUSH_THRESHOLD = 2000;
  private static final long FLUSH_PERIOD   = 10000L;
  private static final int MAX_BATCH_SIZE  = 200;

  private String                  clusterName;
  private String                  hostName;
  private int                     portNumber;
  private String                  indexName;
  private long                    lastFlush;
  private int                     totalIndexCount;
  private long                    totalIndexTime;
  private int                     totalPrepCount;
  private long                    totalPrepTime;

  //private G2Indexer               g2Indexer;
  private boolean                 complete;
  private EngineProcess           engineProcess;
  private int                     unflushedCount;
//  private List<ObservedEntityId>  recordIds;

  public Indexer(String         clusterName,
                 String         hostName,
                 int            portNumber,
                 String         indexName,
                 EngineProcess  engineProcess)
  {
    this.clusterName      = clusterName;
    this.hostName         = hostName;
    this.portNumber       = portNumber;
    this.indexName        = indexName;
    this.complete         = false;
    this.unflushedCount   = 0;
    this.lastFlush        = 0L;
    this.totalIndexTime   = 1L;
    this.totalIndexCount  = 0;
    this.totalPrepCount   = 0;
    this.totalPrepTime    = 1L;
    //this.g2Indexer        = null;
//    this.recordIds        = new LinkedList<ObservedEntityId>();
    this.engineProcess    = engineProcess;
    this.start();
  }

//  public void enqueueRecord(ObservedEntityId obsEntId) {
//    synchronized (this.recordIds) {
//      if (this.complete) {
//        throw new IllegalStateException(
//          "Cannot enqueue record IDs after completion has been flagged.");
//      }
//      this.recordIds.add(obsEntId);
//      this.recordIds.notifyAll();
//    }
//  }

  public void complete() {
//    synchronized (this.recordIds) {
//      this.complete = true;
//      this.recordIds.notifyAll();
//    }
  }

//  public boolean isComplete() {
//    synchronized (this.recordIds) {
//      return (this.complete && this.recordIds.size() == 0);
//    }
//  }

  public void run() {
    /*
    try {
      this.doRun();
      if (this.g2Indexer != null) {
        // DEACTIVATE ELASTIC SEARCH
        //this.g2Indexer.flushDataToIndex();
        //this.g2Indexer.destroy();
      }
      this.g2Indexer = null;

    } catch (Exception e) {
      log(e);

    } finally {
      log("MARKING INDEXER COMPLETE...");
      this.complete();
      log("INDEXER COMPLETED.");
    }
    */
  }

//  public boolean hasRecords() {
//    synchronized (this.recordIds) {
//      return this.recordIds.size() > 0;
//    }
//  }

  protected void doRun() {
/*  FIX COMPILE FOR NOW
    int remaining = 0;
    while (!this.isComplete()) {
      List<ObservedEntityId> currentBatch = null;
      synchronized (this.recordIds) {
        while (this.recordIds.size() == 0 && !this.isComplete()) {
          try {
            this.recordIds.wait(3000L);

          } catch (InterruptedException ignore) {
            // do nothing
          }
          // check if no records to index
          if (this.recordIds.size() == 0 && this.g2Indexer != null) {
            long now = System.currentTimeMillis();
            long lapse = now - this.lastFlush;
            // check if we should do a periodic flush of the index
            if (this.unflushedCount > 0 && lapse > FLUSH_PERIOD) {
              log("FLUSHING " + this.unflushedCount + " RECORDS TO INDEX " + this.indexName + "....");
              this.g2Indexer.flushDataToIndex();
              log("FLUSHED " + this.unflushedCount + " RECORDS TO INDEX " + this.indexName + ".");
              this.lastFlush = System.currentTimeMillis();
              this.unflushedCount = 0;
            }
            continue;
          }
        }
        int batchSize = this.recordIds.size();
        if (batchSize > MAX_BATCH_SIZE) {
          batchSize = MAX_BATCH_SIZE;
        }
        currentBatch = new ArrayList<ObservedEntityId>(batchSize);
        Iterator<ObservedEntityId> iter = this.recordIds.iterator();
        for (int index = 0; index < batchSize; index++) {
          ObservedEntityId obsEntId = iter.next();
          iter.remove();
          currentBatch.add(obsEntId);
        }
        remaining = this.recordIds.size();
      }

      if (this.g2Indexer == null) {
        log("INITIALIZING INDEXER");
        // DEACTIVATE ELASTIC SEARCH
        //this.g2Indexer = new G2Indexer();
        //this.g2Indexer.init(clusterName, hostName, portNumber, indexName);
        //if (!this.g2Indexer.indexExists()) {
        //  log("INDEX " + this.indexName + " DOES NOT EXIST.  CREATING....");
        //  this.g2Indexer.createIndex();
        //  log("INDEX " + this.indexName + " CREATED");
        //}
      }

      int count = currentBatch.size();
      log("CURRENT INDEX BATCH SIZE: " + count);
      List<DocumentInfo> documents = new ArrayList<DocumentInfo>(count);
      long prepStart = System.currentTimeMillis();
      Map<ObservedEntityId,String> recordJsonMap = null;

      try {
        recordJsonMap = this.engineProcess.getRecords(currentBatch);

      } catch (Exception e) {
        log("FAILED TO RETRIEVE RECORD JSON FOR RECORD IDS: " + currentBatch);
        log(e);
        continue;
      }

      long prepElapsed = System.currentTimeMillis() - prepStart;
      this.totalPrepTime += prepElapsed;
      this.totalPrepCount += count;

      for (Map.Entry<ObservedEntityId,String> ent: recordJsonMap.entrySet()) {
        ObservedEntityId obsEntId = ent.getKey();
        String recordJson = ent.getValue();

        if (recordJson == null || recordJson.length() == 0) {
          log("DID NOT FIND ENTITY FOR RECORD ID: " + obsEntId);
          continue;
        }

        // convert the raw text to a JSON object (from recordJson)
        StringReader sr = new StringReader(recordJson);
        JsonReader    jsonReader  = Json.createReader(sr);
        JsonObject    jsonObject  = jsonReader.readObject();

        // get the "JSON_DATA" sub-object -- which is a string
        String        jsonData    = jsonObject.getString("JSON_DATA");

        // now parse the JSON_DATA string as a JSON object
        StringReader  sr2         = new StringReader(jsonData);
        JsonReader    jsonReader2 = Json.createReader(sr2);
        JsonObject    jsonDataObj = jsonReader2.readObject();

        // setup a JSON writer and JsonObjectBuilder for the result
        StringWriter  sw          = new StringWriter();
        JsonWriter    jsonWriter  = Json.createWriter(sw);

        JsonObjectBuilder objBuilder = Json.createObjectBuilder();

        // create a map of keys to JsonValue instances to use for final result
        Map<String,JsonValue> map = new LinkedHashMap<String,JsonValue>();

        // iterate over JSON_DATA fields and add those to our map
        jsonDataObj.entrySet().stream().forEach((entry) -> {
          String key = entry.getKey();
          JsonValue val = entry.getValue();
          map.put(key, val);
        });

        // iterate over top-level record JSON fields and add to map (overwrite
        // the JSON_DATA fields if any duplicate keys)
        jsonObject.entrySet().stream().forEach((entry) -> {
          String key = entry.getKey();

          // ignore JSON_DATA string -- we are not indexing this
          if (key.equalsIgnoreCase("JSON_DATA")) return;
          JsonValue val = entry.getValue();
          map.put(key, val);
        });

        // for each key/value pair in merged map, add to the JsonObjectBuilder
        map.entrySet().stream().forEach((entry) -> {
          objBuilder.add(entry.getKey(), entry.getValue());
        });

        // build the JSON object to be indexed
        JsonObject indexObj = objBuilder.build();

        // convert the JSON object to a string using our JSON writer
        jsonWriter.write(indexObj);
        recordJson = sw.toString();

        // create a DocumentInfo for the string
        DocumentInfo doc = new DocumentInfo(obsEntId.toString(), recordJson);
        documents.add(doc);
      }
      if (count == 0) continue;
      log("INDEXING BATCH OF " + count + " TO " + this.indexName + "....");
      long start = System.currentTimeMillis();
      this.g2Indexer.addBulkDataToIndex(documents);
      long elapsed = System.currentTimeMillis() - start;
      this.totalIndexTime += elapsed;
      this.totalIndexCount += count;
      this.unflushedCount += count;
      log("INDEXED BATCH OF " + count + " TO " + this.indexName + ": " + elapsed + " milliseconds");
      log("AVERAGE RATE OF INDEXING: " + (((double)this.totalIndexCount*1000.0)/(double)this.totalIndexTime) + " records per second");
      log("AVERAGE RATE OF PREPARATION: " + (((double)this.totalPrepCount*1000.0)/(double)this.totalPrepTime) + " records per second");
      log("REMAINING TO BE INDEXED: " + remaining);
      if (this.unflushedCount > FLUSH_THRESHOLD) {
        log("FLUSHING " + this.unflushedCount + " RECORDS TO INDEX " + this.indexName + "....");
        this.g2Indexer.flushDataToIndex();
        log("FLUSHED " + this.unflushedCount + " RECORDS TO INDEX " + this.indexName + ".");
        this.lastFlush = System.currentTimeMillis();
        this.unflushedCount = 0;
      }
    }
    */
  }
}
