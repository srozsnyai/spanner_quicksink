package com.example.spanner.quicksink.integration;

import com.google.cloud.bigquery.*;
import com.google.cloud.spanner.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class BigQuerySystemTest {

    private String projectId;
    private String instanceId;
    private String bqDataset;
    private String sourceDbId;
    
    // Services
    private Spanner spanner;
    private DatabaseAdminClient dbAdminClient;
    private DatabaseClient sourceClient;
    private BigQuery bigQuery;
    
    private Process appProcess;
    private Path configPath;

    @Before
    public void setUp() throws Exception {
        projectId = resolveConfig("test.projectId", "SPANNER_PROJECT_ID");
        instanceId = resolveConfig("test.instanceId", "SPANNER_INSTANCE_ID");
        bqDataset = resolveConfig("test.bqDataset", "BQ_DATASET");
        
        if (projectId == null || instanceId == null || bqDataset == null) {
            throw new IllegalStateException("Test configuration missing. Require: projectId, instanceId, bqDataset");
        }
        
        String uuid = UUID.randomUUID().toString().substring(0, 8);
        sourceDbId = "users_src_" + uuid;

        // 1. Spanner Setup
        SpannerOptions options = SpannerOptions.newBuilder().setProjectId(projectId).build();
        spanner = options.getService();
        dbAdminClient = spanner.getDatabaseAdminClient();
        
        System.out.println("=== Test Setup ===");
        System.out.println("Source DB: " + sourceDbId);
        System.out.println("Target BQ: " + bqDataset + ".Users");

        createDatabase(sourceDbId, getSourceSchema());
        sourceClient = spanner.getDatabaseClient(DatabaseId.of(projectId, instanceId, sourceDbId));
        
        // 2. BigQuery Setup
        bigQuery = BigQueryOptions.newBuilder().setProjectId(projectId).build().getService();
        setupBigQueryTable();
    }
    
    private void setupBigQueryTable() {
        TableId tableId = TableId.of(projectId, bqDataset, "Users");
        
        // Drop if exists
        bigQuery.delete(tableId);
        System.out.println("Dropped BQ Table (if existed): " + tableId);
        
        // Create
        Schema schema = Schema.of(
            Field.of("_spanner_op", StandardSQLTypeName.STRING),
            Field.of("_spanner_commit_timestamp", StandardSQLTypeName.STRING), // Requested as STRING
            Field.of("user_id", StandardSQLTypeName.STRING),
            Field.of("name", StandardSQLTypeName.STRING)
        );
        
        TableDefinition tableDefinition = StandardTableDefinition.of(schema);
        TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
        bigQuery.create(tableInfo);
        System.out.println("Created BQ Table: " + tableId);
    }
    
    // Priority: System Property > Env Var
    private String resolveConfig(String sysProp, String envVar) {
        String val = System.getProperty(sysProp);
        if (val != null && !val.isBlank()) return val;
        val = System.getenv(envVar);
        if (val != null && !val.isBlank()) return val;
        return null;
    }

    @After
    public void tearDown() {
        System.out.println("=== Test Teardown ===");
        if (appProcess != null) {
            appProcess.destroy(); 
            try {
                if (!appProcess.waitFor(5, TimeUnit.SECONDS)) appProcess.destroyForcibly();
            } catch (Exception e) {}
        }
        
        try {
            dropDatabase(sourceDbId);
        } catch (Exception e) {}
        
        if (spanner != null) spanner.close();
        
        try {
            if (configPath != null) Files.deleteIfExists(configPath);
        } catch (Exception e) {}
    }

    @Test
    public void testBigQueryReplication() throws Exception {
        // 2. Config
        String configContent = String.format("""
            projectId=%s
            instanceId=%s
            databaseId=%s
            changeStream=streamall
            
            startOffsetMinutes=0
            heartbeatMs=1000
            
            sink.type=bigquery
            sink.bigquery.projectId=%s
            sink.bigquery.dataset=%s
            sink.bigquery.batchSize=10
            sink.bigquery.parallelism=2
            
            buffer.enabled=true
            buffer.windowMs=1000
            buffer.flushIntervalMs=500
            
            console.logLevel=1
            """, projectId, instanceId, sourceDbId, projectId, bqDataset);
            
        configPath = Files.createTempFile("quicksink-bq-", ".properties");
        Files.writeString(configPath, configContent);
        
        // 3. Start App
        System.out.println("Starting QuickSink App...");
        ProcessBuilder pb = new ProcessBuilder(
            "java", "-cp", System.getProperty("java.class.path"), 
            "com.example.spanner.quicksink.ChangeStreamReader", 
            "--config", configPath.toAbsolutePath().toString()
        );
        pb.redirectErrorStream(true);
        appProcess = pb.start();
        
        // Logger
        Thread logger = new Thread(() -> {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(appProcess.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    System.out.println("[APP] " + line);
                }
            } catch (Exception e) {}
        });
        logger.setDaemon(true);
        logger.start();
        
        System.out.println("Waiting 10s for app to stabilize...");
        Thread.sleep(10000);
        
        if (!appProcess.isAlive()) fail("App failed to start");

        // 4. Inject Data
        injectData();
        
        // 5. Verify
        System.out.println("Waiting 15s for propagation...");
        Thread.sleep(15000);
        
        System.out.println("Verifying BigQuery...");
        String query = String.format("SELECT count(*) as cnt FROM `%s.%s.Users` WHERE _spanner_op != 'DELETE'", projectId, bqDataset);        
        String dedupQuery = String.format("""
            SELECT count(*) as cnt
            FROM (
              SELECT *, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY _spanner_commit_timestamp DESC) as _rn
              FROM `%s.%s.Users`
            )
            WHERE _rn = 1 AND _spanner_op != 'DELETE'
            """, projectId, bqDataset);
            
        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(dedupQuery).build();
        TableResult result = bigQuery.query(queryConfig);
        
        for (FieldValueList row : result.iterateAll()) {
            long count = row.get("cnt").getLongValue();
            System.out.println("Active Users in BQ: " + count);
            assertEquals("Should have 3 active users", 3, count);
            return;
        }
        fail("No result from BQ query");
    }

    private void injectData() {
        // Same logic as SpannerSystemTest
        List<Mutation> mutations = new ArrayList<>();
        mutations.add(Mutation.newInsertBuilder("Users").set("user_id").to("user_001").set("name").to("Alice Chen").build());
        mutations.add(Mutation.newInsertBuilder("Users").set("user_id").to("user_002").set("name").to("Bob Smith").build());
        mutations.add(Mutation.newInsertBuilder("Users").set("user_id").to("user_003").set("name").to("Charlie Davis").build());
        mutations.add(Mutation.newInsertBuilder("Users").set("user_id").to("user_004").set("name").to("Dana Lee").build());
        mutations.add(Mutation.newInsertBuilder("Users").set("user_id").to("user_005").set("name").to("Eliott Vaughn").build());
        sourceClient.write(mutations);
        System.out.println("Inserted 5 users");

        List<Mutation> updates = new ArrayList<>();
        updates.add(Mutation.newUpdateBuilder("Users").set("user_id").to("user_001").set("name").to("Alice Thompson").build());
        updates.add(Mutation.newUpdateBuilder("Users").set("user_id").to("user_002").set("name").to("Robert Smith").build());
        updates.add(Mutation.newUpdateBuilder("Users").set("user_id").to("user_003").set("name").to("Charlie R. Davis").build());
        updates.add(Mutation.newUpdateBuilder("Users").set("user_id").to("user_004").set("name").to("DANA LEE").build());
        updates.add(Mutation.newUpdateBuilder("Users").set("user_id").to("user_005").set("name").to("Eliott Vaughn Jr.").build());
        sourceClient.write(updates);
        System.out.println("Updated 5 users");
        
        sourceClient.readWriteTransaction().run(tx -> {
            tx.executeUpdate(Statement.of("DELETE FROM Users WHERE user_id = 'user_003'")); 
            tx.executeUpdate(Statement.of("DELETE FROM Users WHERE user_id = 'user_005' AND name = 'Eliott Vaughn Jr.'"));
            return null;
        });
        System.out.println("Deleted users");
    }

    private void createDatabase(String dbName, Iterable<String> ddl) throws Exception {
        try {
            dbAdminClient.createDatabase(instanceId, dbName, ddl).get(5, TimeUnit.MINUTES);
            System.out.println("Created database: " + dbName);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create DB " + dbName, e);
        }
    }
    
    private void dropDatabase(String dbName) {
        dbAdminClient.dropDatabase(instanceId, dbName);
        System.out.println("Dropped database: " + dbName);
    }
    
    private Iterable<String> getSourceSchema() {
        return Arrays.asList(
            "CREATE TABLE GroupMgmt (group_id STRING(MAX), grpname STRING(MAX)) PRIMARY KEY(group_id)",
            "CREATE TABLE membership (user_id STRING(MAX), group_id STRING(MAX), enrolled TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true)) PRIMARY KEY(user_id, group_id)",
            "CREATE TABLE Users (user_id STRING(MAX), name STRING(MAX)) PRIMARY KEY(user_id)",
            "CREATE CHANGE STREAM streamall FOR ALL"
        );
    }
}
