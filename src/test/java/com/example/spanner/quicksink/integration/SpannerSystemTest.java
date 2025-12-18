package com.example.spanner.quicksink.integration;

import com.example.spanner.quicksink.ChangeStreamReader;
import com.google.cloud.spanner.*;
import com.google.common.collect.ImmutableList;
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

public class SpannerSystemTest {

    private String projectId;
    private String instanceId;
    private String sourceDbId;
    private String targetDbId;
    
    private Spanner spanner;
    private DatabaseAdminClient dbAdminClient;
    private DatabaseClient sourceClient;
    private DatabaseClient targetClient;
    
    private Process appProcess;
    private Path configPath;
    @Before
    public void setUp() throws Exception {
        projectId = resolveConfig("test.projectId", "SPANNER_PROJECT_ID");
        instanceId = resolveConfig("test.instanceId", "SPANNER_INSTANCE_ID");
        
        if (projectId == null || instanceId == null) {
            throw new IllegalStateException("Test configuration missing. Please provide test.projectId and test.instanceId via System properties (e.g. -Dtest.projectId=...) or Environment variables.");
        }
        
        String uuid = UUID.randomUUID().toString().substring(0, 8);
        sourceDbId = "users_src_" + uuid;
        targetDbId = "users_tgt_" + uuid;

        SpannerOptions options = SpannerOptions.newBuilder().setProjectId(projectId).build();
        spanner = options.getService();
        dbAdminClient = spanner.getDatabaseAdminClient();
        
        System.out.println("=== Test Setup ===");
        System.out.println("Project: " + projectId);
        System.out.println("Instance: " + instanceId);
        System.out.println("Source DB: " + sourceDbId);
        System.out.println("Target DB: " + targetDbId);

        // 1. Create Databases
        createDatabase(sourceDbId, getSourceSchema());
        createDatabase(targetDbId, getTargetSchema());
        
        sourceClient = spanner.getDatabaseClient(DatabaseId.of(projectId, instanceId, sourceDbId));
        targetClient = spanner.getDatabaseClient(DatabaseId.of(projectId, instanceId, targetDbId));
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
            appProcess.destroy(); // SIGTERM
            try {
                if (!appProcess.waitFor(5, TimeUnit.SECONDS)) {
                    appProcess.destroyForcibly();
                }
            } catch (InterruptedException e) {
                appProcess.destroyForcibly();
            }
        }
        
        // Clean up Databases
        try {
            dropDatabase(sourceDbId);
        } catch (Exception e) {
             System.err.println("Failed to drop source db: " + e.getMessage());
        }
        try {
            dropDatabase(targetDbId);
        } catch (Exception e) {
             System.err.println("Failed to drop target db: " + e.getMessage());
        }
        
        if (spanner != null) {
            spanner.close();
        }
        
        // delete config file
        try {
            if (configPath != null) Files.deleteIfExists(configPath);
        } catch (Exception e) { }
    }

    @Test
    public void testEndToEndReplication() throws Exception {
        // 2. Create Config File
        String configContent = String.format("""
            projectId=%s
            instanceId=%s
            databaseId=%s
            changeStream=streamall
            
            startOffsetMinutes=0
            heartbeatMs=1000
            
            sink.type=spanner
            sink.spanner.projectId=%s
            sink.spanner.instanceId=%s
            sink.spanner.databaseId=%s
            sink.spanner.batchSize=50
            sink.spanner.parallelism=4
            sink.spanner.flushTimeoutSeconds=10
            
            buffer.enabled=true
            buffer.windowMs=1000
            buffer.flushIntervalMs=500
            
            console.logLevel=1
            """, projectId, instanceId, sourceDbId, projectId, instanceId, targetDbId);
            
        configPath = Files.createTempFile("quicksink-e2e-", ".properties");
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
        
        // Start a thread to print app output
        Thread logger = new Thread(() -> {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(appProcess.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    System.out.println("[APP] " + line);
                }
            } catch (Exception e) { }
        });
        logger.setDaemon(true);
        logger.start();
        
        // Wait for app to initialize
        System.out.println("Waiting 10s for app to stabilize...");
        Thread.sleep(10000);
        
        if (!appProcess.isAlive()) {
            fail("App failed to start");
        }

        // 4. Inject Data
        System.out.println("Injecting Data...");
        injectData();
        
        // 5. Verify
        System.out.println("Waiting 10s for replication...");
        Thread.sleep(10000);
        
        System.out.println("Verifying Target...");
        try (ResultSet rs = targetClient.singleUse().executeQuery(Statement.of("SELECT COUNT(*) FROM Users"))) {
            if (rs.next()) {
                long count = rs.getLong(0);
                System.out.println("Target Users Count: " + count);
                assertEquals("Should have 3 users remaining", 3, count);
            } else {
                fail("No results from target query");
            }
        }
    }

    private void injectData() {
        List<Mutation> mutations = new ArrayList<>();
        mutations.add(Mutation.newInsertBuilder("Users").set("user_id").to("user_001").set("name").to("Alice Chen").build());
        mutations.add(Mutation.newInsertBuilder("Users").set("user_id").to("user_002").set("name").to("Bob Smith").build());
        mutations.add(Mutation.newInsertBuilder("Users").set("user_id").to("user_003").set("name").to("Charlie Davis").build());
        mutations.add(Mutation.newInsertBuilder("Users").set("user_id").to("user_004").set("name").to("Dana Lee").build());
        mutations.add(Mutation.newInsertBuilder("Users").set("user_id").to("user_005").set("name").to("Eliott Vaughn").build());
        sourceClient.write(mutations);
        System.out.println("Inserted 5 users");

        // Updates
        List<Mutation> updates = new ArrayList<>();
        updates.add(Mutation.newUpdateBuilder("Users").set("user_id").to("user_001").set("name").to("Alice Thompson").build());
        updates.add(Mutation.newUpdateBuilder("Users").set("user_id").to("user_002").set("name").to("Robert Smith").build());
        updates.add(Mutation.newUpdateBuilder("Users").set("user_id").to("user_003").set("name").to("Charlie R. Davis").build());
        updates.add(Mutation.newUpdateBuilder("Users").set("user_id").to("user_004").set("name").to("DANA LEE").build());
        updates.add(Mutation.newUpdateBuilder("Users").set("user_id").to("user_005").set("name").to("Eliott Vaughn Jr.").build());
        sourceClient.write(updates);
        System.out.println("Updated 5 users");
        
        // Deletes
        List<Mutation> deletes = new ArrayList<>();
        deletes.add(Mutation.delete("Users", Key.of("user_003")));
      
        sourceClient.readWriteTransaction().run(tx -> {
            tx.executeUpdate(Statement.of("DELETE FROM Users WHERE user_id = 'user_003'")); // Redundant but testing DML
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
    
    private Iterable<String> getTargetSchema() {
        return Arrays.asList(
            "CREATE TABLE GroupMgmt (group_id STRING(MAX), grpname STRING(MAX)) PRIMARY KEY(group_id)",
            "CREATE TABLE membership (user_id STRING(MAX), group_id STRING(MAX), enrolled TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true)) PRIMARY KEY(user_id, group_id)",
            "CREATE TABLE Users (user_id STRING(MAX), name STRING(MAX)) PRIMARY KEY(user_id)"
        );
    }
}
