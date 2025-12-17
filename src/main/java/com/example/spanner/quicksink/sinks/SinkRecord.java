package com.example.spanner.quicksink.sinks;

import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Statement;
import com.example.spanner.quicksink.util.ChangeRecordJsonFormatter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.List;

public class SinkRecord {
    private final Struct struct;
    private final String sql;
    private final Statement statement;
    private final String shardKey;
    private final long captureTimeMs;
    private String jsonCache;
    
    // Metadata cache
    private String txId;
    private String commitTimestamp;
    private String recordSequence;
    private Boolean isDataChange;
    private Integer modCount;

    public SinkRecord(Struct struct) {
        this.struct = struct;
        this.sql = null;
        this.statement = null;
        this.shardKey = null;
        this.captureTimeMs = System.currentTimeMillis();
    }

    public SinkRecord(String sql) {
        this.struct = null;
        this.sql = sql;
        this.statement = null;
        this.shardKey = null;
        this.captureTimeMs = System.currentTimeMillis();
    }

    public SinkRecord(Statement statement) {
        this.struct = null;
        this.sql = statement.getSql(); 
        this.statement = statement;
        this.shardKey = null;
        this.captureTimeMs = System.currentTimeMillis();
    }

    public SinkRecord(Statement statement, String shardKey) {
        this.struct = null;
        this.sql = statement.getSql();
        this.statement = statement;
        this.shardKey = shardKey;
        this.captureTimeMs = System.currentTimeMillis();
    }

    // For testing
    public SinkRecord(String txId, String commitTimestamp, String recordSequence) {
        this.struct = null;
        this.sql = null;
        this.statement = null;
        this.shardKey = null;
        this.txId = txId;
        this.commitTimestamp = commitTimestamp;
        this.recordSequence = recordSequence;
        this.isDataChange = true;
        this.captureTimeMs = System.currentTimeMillis();
    }

    public long getCaptureTimeMs() {
        return captureTimeMs;
    }

    public String getSql() {
        return sql;
    }

    public Statement getStatement() {
        return statement;
    }

    public String getShardKey() {
        return shardKey;
    }

    public synchronized String toJson() {
        if (jsonCache == null) {
            if (struct != null) {
                ChangeRecordJsonFormatter formatter = new ChangeRecordJsonFormatter();
                jsonCache = formatter.format(struct);
            } else {
                return "{\"sql\": \"" + sql + "\"}"; 
            }
        }
        return jsonCache;
    }

    public Struct getStruct() {
        return struct;
    }

    public boolean isDataChange() {
        if (struct == null) return false;
        if (isDataChange == null) {
             isDataChange = hasField(struct, "data_change_record") && 
                           !struct.isNull("data_change_record") &&
                           !struct.getStructList("data_change_record").isEmpty();
        }
        return isDataChange;
    }
    
    public String getTransactionId() {
        if (txId == null) {
            if (isDataChange()) {
                Struct dcr = struct.getStructList("data_change_record").get(0);
                 if (hasField(dcr, "server_transaction_id") && !dcr.isNull("server_transaction_id")) {
                    txId = dcr.getString("server_transaction_id");
                 }
            }
            if (txId == null) txId = "unknown";
        }
        return txId;
    }

    public String getCommitTimestamp() {
        if (commitTimestamp == null) {
            if (isDataChange()) {
                 Struct dcr = struct.getStructList("data_change_record").get(0);
                 if (hasField(dcr, "commit_timestamp") && !dcr.isNull("commit_timestamp")) {
                     commitTimestamp = dcr.getTimestamp("commit_timestamp").toString();
                 }
            }
            if (commitTimestamp == null) commitTimestamp = "";
        }
        return commitTimestamp;
    }

    public String getRecordSequence() {
        if (recordSequence == null) {
            if (isDataChange()) {
                Struct dcr = struct.getStructList("data_change_record").get(0);
                if (hasField(dcr, "record_sequence") && !dcr.isNull("record_sequence")) {
                    recordSequence = dcr.getString("record_sequence");
                }
            }
             if (recordSequence == null) recordSequence = "00000000";
        }
        return recordSequence;
    }



    public int getModCount() {
        if (modCount == null) {
            if (isDataChange()) {
                Struct dcr = struct.getStructList("data_change_record").get(0);
                if (hasField(dcr, "mods") && !dcr.isNull("mods")) {
                    modCount = dcr.getStructList("mods").size();
                } else {
                    modCount = 0;
                }
            } else {
                modCount = 1; 
            }
        }
        return modCount;
    }

    private boolean hasField(Struct s, String field) {
        try {
            s.getColumnIndex(field);
            return true;
        } catch (IllegalArgumentException ignore) {
            return false;
        }
    }
}
