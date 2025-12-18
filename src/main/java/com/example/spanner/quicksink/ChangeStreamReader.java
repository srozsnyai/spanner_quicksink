package com.example.spanner.quicksink;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.CountDownLatch;

import com.example.spanner.quicksink.sinks.*;
import com.example.spanner.quicksink.util.*;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.TimestampBound;

import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.BigQuery;

public class ChangeStreamReader {
    // Track active partition tokens to avoid duplicate readers
    private static final ConcurrentHashMap<String, AtomicBoolean> activePartitions = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, Integer> partitionIdMap = new ConcurrentHashMap<>();
    private static final java.util.concurrent.atomic.AtomicInteger partitionIdCounter = new java.util.concurrent.atomic.AtomicInteger(0);

    private static final ExecutorService partitionExecutor = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "Partition-Reader-" + System.nanoTime()); 
        return t;
    });
    private static volatile boolean shutdown = false;
    private static DataSink dataSink = null;
    private static final CountDownLatch shutdownLatch = new CountDownLatch(1);

    public static void main(String[] args) throws InterruptedException {
        String configPath = parseConfigPath(args);
        Properties props = loadProperties(configPath);

        String projectId = require(props, "projectId");
        String instanceId = require(props, "instanceId");
        String databaseId = require(props, "databaseId");
        String changeStreamName = require(props, "changeStream");

        long heartbeatMs = getLong(props, "heartbeatMs", 10000L);
        long startOffsetMinutes = getLong(props, "startOffsetMinutes", 5L);
        String dialect = props.getProperty("dialect", "google").trim();

        Instant startInstant = Instant.now().minus(Duration.ofMinutes(startOffsetMinutes));
        Timestamp startTimestamp = Timestamp.ofTimeSecondsAndNanos(startInstant.getEpochSecond(), startInstant.getNano());

        System.out.printf("Reading change stream '%s' from %s (heartbeat %dms)\n",
                changeStreamName, startTimestamp.toString(), heartbeatMs);
        System.out.println("Press Ctrl+C to stop...\n");

        // Initialize Data Sink
        String sinkType = props.getProperty("sink.type");
        if ("file".equalsIgnoreCase(sinkType)) {
            String sinkPath = props.getProperty("sink.path");
            if (sinkPath != null && !sinkPath.isBlank()) {
                dataSink = new FileDataSink(sinkPath);
                System.out.println("Writing data changes to: " + sinkPath);
            }
        } else if ("sql".equalsIgnoreCase(sinkType)) {
            String sinkPath = props.getProperty("sink.path");
             if (sinkPath != null && !sinkPath.isBlank()) {
                dataSink = new SqlSink(new FileDataSink(sinkPath));
                System.out.println("Writing SQL statements to: " + sinkPath);
            }
        } else if ("spanner".equalsIgnoreCase(sinkType)) {
             String targetProjectId = props.getProperty("sink.spanner.projectId");
             String targetInstanceId = props.getProperty("sink.spanner.instanceId");
             String targetDatabaseId = props.getProperty("sink.spanner.databaseId");

             int batchSize = Integer.parseInt(props.getProperty("sink.spanner.batchSize", "50").trim());
             int parallelism = Integer.parseInt(props.getProperty("sink.spanner.parallelism", "1").trim());
             int flushTimeout = Integer.parseInt(props.getProperty("sink.spanner.flushTimeoutSeconds", "30").trim());
             
             if (targetProjectId != null && targetInstanceId != null && targetDatabaseId != null) {
                 SpannerOptions options = SpannerOptions.newBuilder().setProjectId(targetProjectId).build();
                 com.google.cloud.spanner.Spanner spannerSinkClient = options.getService();
                 DatabaseId dbId = DatabaseId.of(targetProjectId, targetInstanceId, targetDatabaseId);
                 DatabaseClient dbClient = spannerSinkClient.getDatabaseClient(dbId);
                 
                 // Chain: SqlSink -> SpannerExecutorSink
                 // Note: SqlSink wraps SpannerExecutorSink
                 dataSink = new SqlSink(new SpannerExecutorSink(dbClient, batchSize, parallelism, flushTimeout));
                 System.out.println("Executing SQL against Spanner: " + dbId + " (Max Batch Size: " + batchSize + ", Parallelism: " + parallelism + ", FlushTimeout: " + flushTimeout + "s)");
             } else {
                 System.err.println("Missing configuration for Spanner sink (projectId, instanceId, databaseId)");
             }
        } else if ("bigquery".equalsIgnoreCase(sinkType)) {
             String bqProjectId = require(props, "sink.bigquery.projectId");
             String bqDataset = require(props, "sink.bigquery.dataset");
             
             BigQuery bigQuery = BigQueryOptions.newBuilder().setProjectId(bqProjectId).build().getService();
             
             // Parse table mapping: sink.bigquery.table.Source=Target
             java.util.Map<String, String> tableMap = new java.util.HashMap<>();
             for (String key : props.stringPropertyNames()) {
                 if (key.startsWith("sink.bigquery.table.")) {
                     String sourceTable = key.substring("sink.bigquery.table.".length());
                     String targetTable = props.getProperty(key);
                     if (targetTable != null && !targetTable.isBlank()) {
                         tableMap.put(sourceTable, targetTable);
                         System.out.println("Mapping Spanner table '" + sourceTable + "' to BigQuery table '" + targetTable + "'");
                     }
                 }
             }
             
             int bqBatchSize = Integer.parseInt(props.getProperty("sink.bigquery.batchSize", "500").trim());
             int bqParallelism = Integer.parseInt(props.getProperty("sink.bigquery.parallelism", "4").trim());

             dataSink = new BigQuerySink(bigQuery, bqDataset, tableMap, bqBatchSize, bqParallelism);
             System.out.println("Streaming to BigQuery Dataset: " + bqDataset + " (Project: " + bqProjectId + ", Batch: " + bqBatchSize + ", Threads: " + bqParallelism + ")");
        } else {
            System.err.println("Unknown sink type: " + sinkType + ". Defaulting to raw JSON file sink.");
        }

        // Initialize Buffer Config
        String bufferEnabled = props.getProperty("buffer.enabled");
        if (bufferEnabled != null && Boolean.parseBoolean(bufferEnabled)) {
            long windowMs = Long.parseLong(props.getProperty("buffer.windowMs", "5000"));
            long flushIntervalMs = Long.parseLong(props.getProperty("buffer.flushIntervalMs", "200"));
            dataSink = new TransactionBufferSink(dataSink, windowMs, flushIntervalMs);
            System.out.println("Buffering enabled with window: " + windowMs + "ms, flush interval: " + flushIntervalMs + "ms");
        } else {
            // If NOT buffering, wrap in MonitoredSink to track basic latency/throughput
            dataSink = new MonitoredSink(dataSink);
        }
        
        // Start Metrics Reporter
        Metrics.startReporter();

        // Initialize Logging
        int logLevel = 1; // Default to INFO
        String levelProp = props.getProperty("console.logLevel");
        if (levelProp != null) {
            try {
                logLevel = Integer.parseInt(levelProp);
            } catch (NumberFormatException e) {
                // ignore
            }
        }
        // if console.printRecords is true, force level 3
        String printRecords = props.getProperty("console.printRecords");
        if (printRecords != null && Boolean.parseBoolean(printRecords)) {
            logLevel = 3;
        }
        Log.setLevel(logLevel);
        System.out.println("Log level set to: " + logLevel);

        // Register shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            shutdown = true;
            partitionExecutor.shutdownNow();
            if (dataSink != null) {
                dataSink.close();
            }
            shutdownLatch.countDown();
        }));

        SpannerOptions options = SpannerOptions.newBuilder().setProjectId(projectId).build();
        try (Spanner spanner = options.getService()) {
            DatabaseClient dbClient = spanner.getDatabaseClient(DatabaseId.of(projectId, instanceId, databaseId));

            // Start initial query with partition_token=NULL to discover partitions
            readPartition(dbClient, null, startTimestamp, changeStreamName, dialect, heartbeatMs);

            // Keep main thread alive until shutdown
            try {
                shutdownLatch.await();
            } catch (InterruptedException e) {
                // Main thread interrupted, check shutdown flag
                if (!shutdown) {
                    System.err.println("Main thread interrupted unexpectedly");
                }
            }
        } finally {
            partitionExecutor.shutdown();
            Metrics.stop();
        }
    }

    /*
     * Reads a partition continuously. When child partitions are detected, spawns new readers.
     */
    private static void readPartition(DatabaseClient dbClient, String partitionToken, 
            Timestamp startTimestamp, String changeStreamName, String dialect, long heartbeatMs) {
        partitionExecutor.submit(() -> {
            try {
                String sql;
                Statement statement;
                if (dialect.equalsIgnoreCase("postgres") || dialect.equalsIgnoreCase("postgresql") || dialect.equalsIgnoreCase("pg")) {
                    String functionName = "spanner.read_change_stream";
                    if (partitionToken == null) {
                        sql = "SELECT * FROM " + functionName + "(@changeStreamName, " +
                                "start_timestamp => @startTs, end_timestamp => NULL, partition_token => NULL, " +
                                "heartbeat_milliseconds => @hbMs, read_options => NULL)";
                        statement = Statement.newBuilder(sql)
                                .bind("changeStreamName").to(changeStreamName)
                                .bind("startTs").to(startTimestamp)
                                .bind("hbMs").to(heartbeatMs)
                                .build();
                    } else {
                        sql = "SELECT * FROM " + functionName + "(@changeStreamName, " +
                                "start_timestamp => @startTs, end_timestamp => NULL, partition_token => @partitionToken, " +
                                "heartbeat_milliseconds => @hbMs, read_options => NULL)";
                        statement = Statement.newBuilder(sql)
                                .bind("changeStreamName").to(changeStreamName)
                                .bind("startTs").to(startTimestamp)
                                .bind("partitionToken").to(partitionToken)
                                .bind("hbMs").to(heartbeatMs)
                                .build();
                    }
                } else {
                    String perStreamFn = "READ_" + changeStreamName;
                    if (partitionToken == null) {
                        sql = "SELECT * FROM " + perStreamFn + "(start_timestamp => @startTs, end_timestamp => NULL, " +
                                "partition_token => NULL, heartbeat_milliseconds => @hbMs, read_options => NULL)";
                        statement = Statement.newBuilder(sql)
                                .bind("startTs").to(startTimestamp)
                                .bind("hbMs").to(heartbeatMs)
                                .build();
                    } else {
                        sql = "SELECT * FROM " + perStreamFn + "(start_timestamp => @startTs, end_timestamp => NULL, " +
                                "partition_token => @partitionToken, heartbeat_milliseconds => @hbMs, read_options => NULL)";
                        statement = Statement.newBuilder(sql)
                                .bind("startTs").to(startTimestamp)
                                .bind("partitionToken").to(partitionToken)
                                .bind("hbMs").to(heartbeatMs)
                                .build();
                    }
                }

                int pId = getPartitionId(partitionToken);
                String partitionLabel = "Partition " + pId;
                Log.info("[" + partitionLabel + "] Starting reader...");

                // The query streams continuously until cancelled or connection closes
                try (ResultSet rs = dbClient.singleUse(TimestampBound.strong()).executeQuery(statement)) {
                    while (rs.next() && !shutdown) {
                        try {
                            processRow(rs);
                        } catch (Exception e) {
                             Log.error("[" + partitionLabel + "] Failed to process row: " + e.getMessage());
                        }
                        // Check for child partitions and spawn new readers
                        handleChildPartitions(rs, dbClient, startTimestamp, changeStreamName, dialect, heartbeatMs);
                    }
                } catch (com.google.cloud.spanner.SpannerException e) {
                    if (shutdown) {
                        Log.info("[" + partitionLabel + "] Shutting down...");
                        return;
                    }
                    String msg = e.getMessage();
                    if (msg != null && msg.contains("Table-valued function not found")) {
                        Log.error("Change stream function not found. For GoogleSQL, the function is READ_<streamName>(...). For PostgreSQL, set 'dialect=postgres' to use spanner.read_change_stream(...). If you're using the emulator, change streams are not supported.");
                    } else if (e.getErrorCode() == com.google.cloud.spanner.ErrorCode.INVALID_ARGUMENT && msg != null && msg.contains("Please specify a start_timestamp within")) {
                         // retry: Extract valid timestamp
                         try {
                             java.util.regex.Matcher m = java.util.regex.Pattern.compile("within (\\S+) and").matcher(msg);
                             if (m.find()) {
                                 String validStartStr = m.group(1);
                                 Timestamp validStart = Timestamp.parseTimestamp(validStartStr);
                                 Log.info("[" + partitionLabel + "] Adjusted start_timestamp from " + startTimestamp + " to " + validStart + " to match partition validity.");
                                 if (!shutdown) {
                                     readPartition(dbClient, partitionToken, validStart, changeStreamName, dialect, heartbeatMs);
                                     return; 
                                 }
                             }
                         } catch (Exception parseEx) {
                             Log.error("[" + partitionLabel + "] Failed to parse valid timestamp from error: " + parseEx.getMessage());
                         }
                         // Fallback if parsing fails
                         Log.error("[" + partitionLabel + "] Error: " + e.getMessage());
                         try { Thread.sleep(5000); } catch (InterruptedException ie) {}
                         if (!shutdown) readPartition(dbClient, partitionToken, startTimestamp, changeStreamName, dialect, heartbeatMs);
                    } else {
                        Log.error("[" + partitionLabel + "] Error: " + e.getMessage());
                        // Retry after a delay
                        try { Thread.sleep(5000); } catch (InterruptedException ie) {}
                        if (!shutdown) {
                            readPartition(dbClient, partitionToken, startTimestamp, changeStreamName, dialect, heartbeatMs);
                        }
                    }
                }
            } catch (Exception e) {
                Log.error("Fatal error in partition reader: " + e.getMessage());
                e.printStackTrace();
            } finally {
                if (partitionToken != null) {
                    activePartitions.remove(partitionToken);
                    int pId = getPartitionId(partitionToken);
                    Log.info("[Partition " + pId + "] Reader stopped");
                }
            }
        });
    }

    /*
     * Detects child partition records and spawns new partition readers.
     * Extracts from the ChangeRecord structure.
     */
    private static void handleChildPartitions(ResultSet rs, DatabaseClient dbClient, 
            Timestamp startTimestamp, String changeStreamName, String dialect, long heartbeatMs) {
        try {
            // Get the ChangeRecord column
            String changeRecordColumn = getChangeRecordColumnName(rs);
            if (changeRecordColumn == null) return;

            // Get the array of structs from the ChangeRecord column
            java.util.List<com.google.cloud.spanner.Struct> changeRecordArray = rs.getStructList(changeRecordColumn);
            if (changeRecordArray == null || changeRecordArray.isEmpty()) return;

            // Get the first (and only) element from the array
            com.google.cloud.spanner.Struct changeRecord = changeRecordArray.get(0);

            // Check if this is a child_partitions_record
            if (hasField(changeRecord, "child_partitions_record") && 
                !changeRecord.isNull("child_partitions_record") &&
                !changeRecord.getStructList("child_partitions_record").isEmpty()) {
                
                // Extract the child_partitions_record array and get the first (only) element
                java.util.List<com.google.cloud.spanner.Struct> childPartitionsArray = changeRecord.getStructList("child_partitions_record");
                com.google.cloud.spanner.Struct childPartitionsRecord = childPartitionsArray.get(0);
                
                // Extract the child_partitions array
                if (hasField(childPartitionsRecord, "child_partitions") && 
                    !childPartitionsRecord.isNull("child_partitions")) {
                    
                    java.util.List<com.google.cloud.spanner.Struct> children = childPartitionsRecord.getStructList("child_partitions");
                    for (com.google.cloud.spanner.Struct child : children) {
                        String token = child.getString("token");
                        if (token != null && !token.isEmpty()) {
                            // Only start a reader if we haven't seen this partition token before
                            AtomicBoolean alreadyActive = activePartitions.putIfAbsent(token, new AtomicBoolean(true));
                            if (alreadyActive == null) {
                                // New partition - start reading it
                                Timestamp childStartTs = startTimestamp;
                                if (hasField(child, "start_timestamp") && !child.isNull("start_timestamp")) {
                                    childStartTs = child.getTimestamp("start_timestamp");
                                }
                                int pId = getPartitionId(token);
                                System.err.println("[Partition " + pId + "] Spawning new reader...");
                                readPartition(dbClient, token, childStartTs, changeStreamName, dialect, heartbeatMs);
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            // Log but don't fail the entire partition reader
            System.err.println("Error handling child partitions: " + e.getMessage());
        }
    }

    private static void processRow(ResultSet rs) {
        try {
            // Get the ChangeRecord column
            String changeRecordColumn = getChangeRecordColumnName(rs);
            if (changeRecordColumn == null) {
                return;
            }

            // Get the array of structs
            java.util.List<com.google.cloud.spanner.Struct> changeRecordArray = rs.getStructList(changeRecordColumn);
            if (changeRecordArray == null || changeRecordArray.isEmpty()) {
                return;
            }

            // Get the first element
            com.google.cloud.spanner.Struct changeRecord = changeRecordArray.get(0);

            // Format the record
            SinkRecord record = new SinkRecord(changeRecord);
            String jsonPreview = ""; // only for logging if needed, or we rely on record.isDataChange()

            boolean isDataChange = record.isDataChange();
            
            boolean isHeartbeat = hasField(changeRecord, "heartbeat_record") && 
                                 !changeRecord.isNull("heartbeat_record") &&
                                 !changeRecord.getStructList("heartbeat_record").isEmpty();

            // 1. Write to Data Sink (only data changes)
            if (isDataChange && dataSink != null) {
                Metrics.markReceived(record.getModCount());
                dataSink.write(record);
            }

            // 2. Console Output (Debug/Status)
            // Suppress heartbeats from console
            if (!isHeartbeat) {
                StringBuilder status = new StringBuilder();
                status.append("[Partitions: ").append(activePartitions.size()).append("] ");
                
                // Extract commit timestamp for status if available
                if (isDataChange) {
                    status.append("[LastCommit: ").append(record.getCommitTimestamp()).append("] ");
                }

                if (Log.isEnabled(Log.LEVEL_TRACE)) {
                    // Only format full JSON if trace is enabled
                    Log.trace(status.toString() + record.toJson());
                }
            }

        } catch (Exception e) {
            System.err.println("Error processing row: " + e.getMessage());
            e.printStackTrace();
            // Rethrow to stop processing and avoid data loss
            throw new RuntimeException("Fatal error in processRow", e);
        }
    }

    private static String getChangeRecordColumnName(ResultSet rs) {
        String[] possibleNames = {"ChangeRecord", "change_record", "CHANGERECORD"};
        for (String name : possibleNames) {
            if (hasColumn(rs, name)) return name;
        }
        if (rs.getColumnCount() > 0) return possibleNames[0];
        return "ChangeRecord";
    }

    private static boolean hasColumn(ResultSet rs, String columnName) {
        try {
            rs.getColumnIndex(columnName);
            return true;
        } catch (IllegalArgumentException ignore) {
            return false;
        }
    }

    // Helper to check fields on Struct (used by handleChildPartitions)
    private static boolean hasField(com.google.cloud.spanner.Struct s, String field) {
        try {
            s.getColumnIndex(field);
            return true;
        } catch (IllegalArgumentException ignore) {
            return false;
        }
    }

    private static String parseConfigPath(String[] args) {
        // Accept: "--config=path", "--config path", "-c path", or single positional arg as path
        if (args != null && args.length > 0) {
            for (int i = 0; i < args.length; i++) {
                String a = args[i];
                if (a == null || a.isBlank()) continue;
                if (a.startsWith("--config=")) {
                    String p = a.substring("--config=".length());
                    if (!p.isBlank()) return p;
                }
                if ("--config".equals(a) || "-c".equals(a)) {
                    if (i + 1 < args.length && args[i + 1] != null && !args[i + 1].isBlank()) {
                        return args[i + 1];
                    }
                }
            }
            // Single positional path (no leading dashes)
            if (args.length == 1 && args[0] != null && !args[0].isBlank() && !args[0].startsWith("-")) {
                return args[0];
            }
        }
        usageAndExit();
        return null; // unreachable
    }

    private static Properties loadProperties(String pathStr) {
        Path path = Path.of(pathStr);
        if (!Files.exists(path)) {
            System.err.println("Config file not found: " + path.toAbsolutePath());
            System.exit(2);
        }
        Properties props = new Properties();
        try (InputStream in = Files.newInputStream(path)) {
            props.load(in);
        } catch (IOException e) {
            System.err.println("Failed to read config file: " + e.getMessage());
            System.exit(2);
        }
        return props;
    }

    private static String require(Properties props, String key) {
        String v = props.getProperty(key);
        if (v == null || v.isBlank()) {
            System.err.println("Missing required property: " + key);
            usageAndExit();
        }
        return v.trim();
    }

    private static long getLong(Properties props, String key, long defaultVal) {
        String v = props.getProperty(key);
        if (v == null || v.isBlank()) return defaultVal;
        try {
            return Long.parseLong(v.trim()); 
        } catch (NumberFormatException e) {
            return defaultVal;
        }
    }

    private static int getPartitionId(String token) {
        if (token == null) return 0; // root
        return partitionIdMap.computeIfAbsent(token, k -> partitionIdCounter.incrementAndGet());
    }

    private static void usageAndExit() {
        System.err.println("Usage: java ... ChangeStreamReader --config=path/to/config.properties");
        System.err.println("   or: mvn exec:java -Dexec.args=\"--config path/to/config.properties\"");
        System.err.println("Required properties: projectId, instanceId, databaseId, changeStream");
        System.err.println("Optional properties: startOffsetMinutes (default 5), heartbeatMs (default 10000)");
        System.err.println("                     sink.type (file), sink.path, console.printRecords (true/false)");
        System.err.println("                     buffer.enabled (true/false), buffer.windowMs (default 5000)");
        System.exit(1);
    }
}


