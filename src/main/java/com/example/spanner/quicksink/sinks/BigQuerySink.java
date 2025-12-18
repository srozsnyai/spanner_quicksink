package com.example.spanner.quicksink.sinks;

import com.example.spanner.quicksink.util.Log;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.TableId;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class BigQuerySink implements DataSink {

    private final BigQuery bigQuery;
    private final String datasetName;
    private final Gson gson = new Gson();
    private final Map<String, String> tableMap;
    
    private final int batchSize;
    private final int numThreads;
    private final List<List<InsertAllRequest.RowToInsert>> buffers;
    private final List<Map<String, List<InsertAllRequest.RowToInsert>>> shardedBuffers; // Table -> Rows mapping per shard
    private final List<ExecutorService> executors;
    private final ScheduledExecutorService scheduler;
    private final AtomicInteger roundRobinCounter = new AtomicInteger(0);
    private boolean closed = false;

    public BigQuerySink(BigQuery bigQuery, String datasetName, Map<String, String> tableMap, int batchSize, int numThreads) {
        this.bigQuery = bigQuery;
        this.datasetName = datasetName;
        this.tableMap = tableMap != null ? tableMap : new HashMap<>();
        this.batchSize = batchSize > 0 ? batchSize : 500;
        this.numThreads = Math.max(1, numThreads);

        this.shardedBuffers = new ArrayList<>(this.numThreads);
        this.buffers = null; // Unused in this design, we use shardedBuffers which maps Table -> Rows
        this.executors = new ArrayList<>(this.numThreads);

        for (int i = 0; i < this.numThreads; i++) {
            this.shardedBuffers.add(new HashMap<>());
            ExecutorService exec = Executors.newSingleThreadExecutor(r -> {
                Thread t = new Thread(r, "BQSink-Shard-" + r.hashCode());
                t.setDaemon(true);
                return t;
            });
            this.executors.add(exec);
            // Warm-up thread to ensure it appears in monitoring immediately
            exec.submit(() -> {});
        }
        
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "BQSink-scheduler");
            t.setDaemon(true);
            return t;
        });
        // Auto-flush every 1 second to ensure data doesn't get stuck if < batchSize
        this.scheduler.scheduleAtFixedRate(this::flushAsync, 1, 1, TimeUnit.SECONDS);
    }
    
    // Legacy/Test constructor
    public BigQuerySink(BigQuery bigQuery, String datasetName, Map<String, String> tableMap) {
        this(bigQuery, datasetName, tableMap, 500, 1);
    }
    
    public BigQuerySink(BigQuery bigQuery, String datasetName) {
        this(bigQuery, datasetName, new HashMap<>(), 500, 1);
    }

    @Override
    public void write(SinkRecord record) {
        if (closed || !record.isDataChange()) return;

        try {
            com.google.cloud.spanner.Struct struct = record.getStruct();
            if (struct == null) return;

            String commitTimestamp = record.getCommitTimestamp();
            
            // We interpret "Write" as adding to a buffer. 
            // We round-robin across shards to distribute load.
            int shardId = (roundRobinCounter.getAndIncrement() & Integer.MAX_VALUE) % numThreads;
            
            // We need to parse the record first to know WHAT to add.
            // Note: Parsing happens on the CALLER thread (Partition Reader).
            // This is good because it distributes CPU load of JSON parsing.
            
            Map<String, List<Map<String, Object>>> rowsPerTable = new HashMap<>();

            for (com.google.cloud.spanner.Struct dcr : struct.getStructList("data_change_record")) {
                String sourceTableName = dcr.getString("table_name");
                String tableName = tableMap.getOrDefault(sourceTableName, sourceTableName);
                String modType = dcr.getString("mod_type");
                
                List<com.google.cloud.spanner.Struct> mods = dcr.getStructList("mods");
                List<Map<String, Object>> rows = rowsPerTable.computeIfAbsent(tableName, k -> new ArrayList<>());

                for (com.google.cloud.spanner.Struct mod : mods) {
                    Map<String, Object> row = new HashMap<>();
                    
                    String keysJson = mod.getJson("keys");
                    JsonObject keys = gson.fromJson(keysJson, JsonObject.class);
                    flattenJsonToMap(keys, row);

                    // 2. New Values (only for INSERT/UPDATE)
                    if (!mod.isNull("new_values")) {
                        String newValuesJson = mod.getJson("new_values");
                        JsonObject newValues = gson.fromJson(newValuesJson, JsonObject.class);
                        flattenJsonToMap(newValues, row);
                    }

                    // 3. Metadata Injection
                    row.put("_spanner_op", modType);
                    row.put("_spanner_commit_timestamp", commitTimestamp);
                    
                    rows.add(row);
                }
            }
            
            // Now add to the shard buffer (Synchronized per shard)
            addToShard(shardId, rowsPerTable);

        } catch (Exception e) {
             Log.error("Error processing record for BigQuery: " + e.getMessage());
             e.printStackTrace(); 
        }
    }

    private synchronized void addToShard(int shardId, Map<String, List<Map<String, Object>>> rowsPerTable) {
        Map<String, List<InsertAllRequest.RowToInsert>> buffer = shardedBuffers.get(shardId);
        
        synchronized (buffer) {
            for (Map.Entry<String, List<Map<String, Object>>> entry : rowsPerTable.entrySet()) {
                String tableName = entry.getKey();
                List<InsertAllRequest.RowToInsert> targetList = buffer.computeIfAbsent(tableName, k -> new ArrayList<>());
                
                for (Map<String, Object> rowMap : entry.getValue()) {
                    targetList.add(InsertAllRequest.RowToInsert.of(rowMap));
                }
                
                // Check batch size per table
                if (targetList.size() >= batchSize) {
                    submitFlush(shardId, tableName, targetList);
                    // Create new list for this table in the buffer map
                    buffer.put(tableName, new ArrayList<>());
                }
            }
        }
    }
    
    // Must be called while holding lock on buffer for shardId? No, submitFlush extracts the list.
    // Actually addToShard holds the lock.
    private void submitFlush(int shardId, String tableName, List<InsertAllRequest.RowToInsert> rowsToFlush) {
        if (rowsToFlush.isEmpty()) return;
        
        // Copy? No, we replaced the list in the map with a new one, so 'rowsToFlush' is detached.
        // But we are inside synchronized block, so it's safe to submit.
        
        executors.get(shardId).submit(() -> {
            insertRows(tableName, rowsToFlush);
        });
    }

    private void flattenJsonToMap(JsonObject json, Map<String, Object> target) {
        for (Map.Entry<String, JsonElement> entry : json.entrySet()) {
            JsonElement val = entry.getValue();
            if (val.isJsonNull()) {
                target.put(entry.getKey(), null);
            } else if (val.isJsonPrimitive()) {
                if (val.getAsJsonPrimitive().isBoolean()) {
                    target.put(entry.getKey(), val.getAsBoolean());
                } else if (val.getAsJsonPrimitive().isNumber()) {
                    target.put(entry.getKey(), val.getAsNumber());
                } else {
                    target.put(entry.getKey(), val.getAsString());
                }
            } else {
                target.put(entry.getKey(), val.toString());
            }
        }
    }

    private void insertRows(String tableName, List<InsertAllRequest.RowToInsert> rows) {
        if (rows == null || rows.isEmpty()) return;

        try {
            String projectId = bigQuery.getOptions().getProjectId();
            TableId tableId = TableId.of(projectId, datasetName, tableName);
            InsertAllResponse response = bigQuery.insertAll(
                InsertAllRequest.newBuilder(tableId)
                    .setRows(rows)
                    .build()
            );

            if (response.hasErrors()) {
                for (Map.Entry<Long, List<BigQueryError>> entry : response.getInsertErrors().entrySet()) {
                     // Log only first few errors to avoid spamming
                    if (entry.getKey() < 5) {
                        Log.error("Error inserting into " + tableName + " row " + entry.getKey() + ": " + entry.getValue());
                    }
                }
                Log.error("Total errors in batch for " + tableName + ": " + response.getInsertErrors().size());
            } else {
                // Should we count here? Yes.
                // Assuming processLag=0 and dataLag=0 for now as we don't track start time per row easily in this batching model without object overhead
                com.example.spanner.quicksink.util.Metrics.markSinked(0, 0, rows.size());
            }
        } catch (Exception e) {
            Log.error("Failed to insert to BigQuery table " + tableName + ": " + e.getMessage());
        }
    }
    
    // Call this to submit all pending tasks. Returns Futures for those submissions.
    private List<Future<?>> flushAsync() {
        if (closed) return Collections.emptyList();
        
        List<Future<?>> submissions = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            Map<String, List<InsertAllRequest.RowToInsert>> buffer = shardedBuffers.get(i);
            final int shard = i;
            synchronized (buffer) {
                for (Map.Entry<String, List<InsertAllRequest.RowToInsert>> entry : buffer.entrySet()) {
                    if (!entry.getValue().isEmpty()) {
                        final String table = entry.getKey();
                        final List<InsertAllRequest.RowToInsert> rows = entry.getValue();
                        // Clear the buffer
                        entry.setValue(new ArrayList<>());
                        
                        // Submit
                        submissions.add(executors.get(shard).submit(() -> insertRows(table, rows)));
                    }
                }
            }
        }
        return submissions;
    }

    @Override
    public void flush() {
        if (closed) return;
        
        // 1. Submit everything pending
        List<Future<?>> pendingFutures = flushAsync();
        
        // 2. Submit Barrier to all executors
        List<Future<?>> barrierFutures = new ArrayList<>();
        for (ExecutorService exec : executors) {
            barrierFutures.add(exec.submit(() -> { return null; }));
        }
        
        // 3. Wait for all
        try {
             for (Future<?> f : pendingFutures) f.get(60, TimeUnit.SECONDS);
             for (Future<?> f : barrierFutures) f.get(60, TimeUnit.SECONDS);
        } catch (Exception e) {
             Log.error("Flush interrupted/failed: " + e.getMessage());
        }
    }

    @Override
    public void close() {
        if (closed) return;
        flush();
        closed = true;
        
        if (scheduler != null) {
            scheduler.shutdownNow();
        }

        for (ExecutorService exec : executors) {
            exec.shutdown();
            try {
                if (!exec.awaitTermination(5, TimeUnit.SECONDS)) {
                    exec.shutdownNow();
                }
            } catch (InterruptedException e) {
                exec.shutdownNow();
            }
        }
    }
}
