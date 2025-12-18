package com.example.spanner.quicksink.sinks;

import com.example.spanner.quicksink.sinks.SinkRecord;
import com.example.spanner.quicksink.sinks.BigQuerySink;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Value;
import com.google.cloud.Timestamp;
import com.google.cloud.bigquery.BigQueryOptions;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;


public class BigQuerySinkTest {

    @Mock
    private BigQuery bigQuery;

    @Mock
    private BigQueryOptions bigQueryOptions;

    private BigQuerySink sink;
    private final String DATASET_NAME = "test_dataset";

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        when(bigQuery.getOptions()).thenReturn(bigQueryOptions);
        when(bigQueryOptions.getProjectId()).thenReturn("test-project");
        when(bigQuery.insertAll(any(InsertAllRequest.class))).thenReturn(mock(InsertAllResponse.class));
        // Use small batch size to trigger flushes easily if needed, or just rely on manual flush()
        sink = new BigQuerySink(bigQuery, DATASET_NAME, new HashMap<>(), 10, 1);
    }

    @Test
    public void testWriteAndFlush() {
        // Mock a Spanner Struct representing a change record
        Struct mod = Struct.newBuilder()
                .set("keys").to(Value.json("{\"id\": 123}"))
                .set("new_values").to(Value.json("{\"name\": \"Alice\"}"))
                .build();

        Timestamp commitTs = Timestamp.parseTimestamp("2024-01-01T12:00:00Z");

        Struct dataChangeRecord = Struct.newBuilder()
                .set("table_name").to("Users")
                .set("mod_type").to("INSERT")
                .set("commit_timestamp").to(commitTs)
                .set("mods").toStructArray(Type.struct(
                        Type.StructField.of("keys", Type.json()),
                        Type.StructField.of("new_values", Type.json())
                ), Collections.singletonList(mod))
                .set("column_types").toStructArray(Type.struct(
                        Type.StructField.of("name", Type.string()),
                        Type.StructField.of("type", Type.json())
                ), Collections.emptyList()) 
                .build();
        
        // Root struct normally has "data_change_record" list
        Struct root = Struct.newBuilder()
                .set("data_change_record").toStructArray(Type.struct(
                        Type.StructField.of("table_name", Type.string()),
                        Type.StructField.of("mod_type", Type.string()),
                        Type.StructField.of("commit_timestamp", Type.timestamp()),
                        Type.StructField.of("mods", Type.array(Type.struct(
                                Type.StructField.of("keys", Type.json()),
                                Type.StructField.of("new_values", Type.json())
                        ))),
                        Type.StructField.of("column_types", Type.array(Type.struct(
                                Type.StructField.of("name", Type.string()),
                                Type.StructField.of("type", Type.json())
                        )))
                ), Collections.singletonList(dataChangeRecord))
                .build();

        SinkRecord record = new SinkRecord(root); 

        sink.write(record);
        sink.flush(); // This should force the async writer to finish

        ArgumentCaptor<InsertAllRequest> captor = ArgumentCaptor.forClass(InsertAllRequest.class);
        verify(bigQuery, times(1)).insertAll(captor.capture());

        InsertAllRequest request = captor.getValue();
        assertEquals(TableId.of("test-project", DATASET_NAME, "Users"), request.getTable());
        
        List<InsertAllRequest.RowToInsert> rows = request.getRows();
        assertEquals(1, rows.size());
        
        Map<String, Object> content = rows.get(0).getContent();
        
        assertEquals("123", content.get("id").toString()); 
        assertEquals("Alice", content.get("name"));
        assertEquals("INSERT", content.get("_spanner_op"));
        assertEquals("2024-01-01T12:00:00Z", content.get("_spanner_commit_timestamp"));
    }

    @Test
    public void testTableMapping() {
        Map<String, String> map = new HashMap<>();
        map.put("Users", "RawUsers");
        // Re-init with mapping
        sink = new BigQuerySink(bigQuery, DATASET_NAME, map, 10, 1);

        Struct mod = Struct.newBuilder()
                .set("keys").to(Value.json("{\"id\": 1}"))
                .set("new_values").to(Value.json("{\"name\": \"Bob\"}"))
                .build();

        Struct dataChangeRecord = Struct.newBuilder()
                .set("table_name").to("Users")
                .set("mod_type").to("INSERT")
                .set("commit_timestamp").to(Timestamp.now())
                .set("mods").toStructArray(Type.struct(
                        Type.StructField.of("keys", Type.json()),
                        Type.StructField.of("new_values", Type.json())
                ), Collections.singletonList(mod))
                .set("column_types").toStructArray(Type.struct(
                        Type.StructField.of("name", Type.string()),
                        Type.StructField.of("type", Type.json())
                ), Collections.emptyList()) 
                .build();
        
        Struct root = Struct.newBuilder()
                .set("data_change_record").toStructArray(Type.struct(
                        Type.StructField.of("table_name", Type.string()),
                        Type.StructField.of("mod_type", Type.string()),
                        Type.StructField.of("commit_timestamp", Type.timestamp()),
                        Type.StructField.of("mods", Type.array(Type.struct(
                                Type.StructField.of("keys", Type.json()),
                                Type.StructField.of("new_values", Type.json())
                        ))),
                        Type.StructField.of("column_types", Type.array(Type.struct(
                                Type.StructField.of("name", Type.string()),
                                Type.StructField.of("type", Type.json())
                        )))
                ), Collections.singletonList(dataChangeRecord))
                .build();

        sink.write(new SinkRecord(root));
        sink.flush();

        ArgumentCaptor<InsertAllRequest> captor = ArgumentCaptor.forClass(InsertAllRequest.class);
        verify(bigQuery, times(1)).insertAll(captor.capture());
        
        // Verify it tries to insert into mapped table "RawUsers"
        assertEquals(TableId.of("test-project", DATASET_NAME, "RawUsers"), captor.getValue().getTable());
    }
}
