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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class BigQuerySink implements DataSink {

    private final BigQuery bigQuery;
    private final String datasetName;
    private final Gson gson = new Gson();
    
    private final Map<String, List<InsertAllRequest.RowToInsert>> buffer = new ConcurrentHashMap<>();
    private final int batchSize = 500; // Configurable? Using safe default.

    private final Map<String, String> tableMap;

    public BigQuerySink(BigQuery bigQuery, String datasetName, Map<String, String> tableMap) {
        this.bigQuery = bigQuery;
        this.datasetName = datasetName;
        this.tableMap = tableMap != null ? tableMap : new HashMap<>();
    }
    
    // For backward compatibility or tests
    public BigQuerySink(BigQuery bigQuery, String datasetName) {
        this(bigQuery, datasetName, new HashMap<>());
    }

    @Override
    public void write(SinkRecord record) {
        if (!record.isDataChange()) return;

        try {
            com.google.cloud.spanner.Struct struct = record.getStruct();
            if (struct == null) return;

            String commitTimestamp = record.getCommitTimestamp();

            for (com.google.cloud.spanner.Struct dcr : struct.getStructList("data_change_record")) {
                String sourceTableName = dcr.getString("table_name");
                // Reset/Resolve table name based on map, default to source name if not found
                String tableName = tableMap.getOrDefault(sourceTableName, sourceTableName);
                String modType = dcr.getString("mod_type");
                
                List<com.google.cloud.spanner.Struct> mods = dcr.getStructList("mods");
                
                for (com.google.cloud.spanner.Struct mod : mods) {
                    Map<String, Object> row = new HashMap<>();
                    
                    // 1. Keys
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

                    addToBuffer(tableName, row);
                }
            }
        } catch (Exception e) {
             Log.error("Error processing record for BigQuery: " + e.getMessage());
             e.printStackTrace(); // TODO: Better error handling?
        }
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

    private synchronized void addToBuffer(String tableName, Map<String, Object> row) {
        buffer.computeIfAbsent(tableName, k -> new ArrayList<>()).add(InsertAllRequest.RowToInsert.of(row));
        
        if (buffer.get(tableName).size() >= batchSize) {
            flushTable(tableName);
        }
    }

    private void flushTable(String tableName) {
        List<InsertAllRequest.RowToInsert> rows = buffer.remove(tableName);
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
                    Log.error("Error inserting into " + tableName + " row " + entry.getKey() + ": " + entry.getValue());
                }
            }
        } catch (Exception e) {
            Log.error("Failed to flush to BigQuery table " + tableName + ": " + e.getMessage());
            // TODO: Retry?
        }
    }

    @Override
    public void flush() {
        List<String> tables = new ArrayList<>(buffer.keySet());
        for (String table : tables) {
            flushTableAtomic(table);
        }
    }

    private synchronized void flushTableAtomic(String tableName) {
        flushTable(tableName);
    }

    @Override
    public void close() {
        flush();
    }
}
