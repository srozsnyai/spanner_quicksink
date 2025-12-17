package com.example.spanner.quicksink;

import com.example.spanner.quicksink.sinks.DataSink;
import com.example.spanner.quicksink.sinks.SqlSink;
import com.example.spanner.quicksink.sinks.SinkRecord;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Value;
import com.google.cloud.spanner.Statement;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.math.BigDecimal;
import com.google.cloud.ByteArray;

public class SqlSinkFunctionalTest {

    private static class CapturingSink implements DataSink {
        final List<SinkRecord> captured = new ArrayList<>();

        @Override
        public void write(SinkRecord record) {
            captured.add(record);
        }

        @Override
        public void close() {}

        @Override
        public void flush() {}
    }

    private SqlSink sqlSink;
    private CapturingSink capture;

    @Before
    public void setup() {
        capture = new CapturingSink();
        sqlSink = new SqlSink(capture);
    }

    private Struct createColType(String name, String typeCode) {
        return Struct.newBuilder()
            .set("name").to(name)
            .set("type").to(Struct.newBuilder().set("code").to(typeCode).build())
            .build();
    }

    private Struct createMod(String keysJson, String newValuesJson, String oldValuesJson) {
        return Struct.newBuilder()
            .set("keys").to(Value.json(keysJson))
            .set("new_values").to(Value.json(newValuesJson))
            .set("old_values").to(Value.json(oldValuesJson))
            .build();
    }

    private SinkRecord createRecord(String tableName, String modType, List<Struct> colTypes, List<Struct> mods) {
         Struct dataChange = Struct.newBuilder()
            .set("table_name").to(tableName)
            .set("mod_type").to(modType)
            .set("column_types").toStructArray(Type.struct(
                Type.StructField.of("name", Type.string()),
                Type.StructField.of("type", Type.struct(Type.StructField.of("code", Type.string())))
            ), colTypes)
            .set("mods").toStructArray(Type.struct(
                Type.StructField.of("keys", Type.json()),
                Type.StructField.of("new_values", Type.json()),
                Type.StructField.of("old_values", Type.json())
            ), mods)
            .set("commit_timestamp").to(com.google.cloud.Timestamp.now())
            .set("record_sequence").to("000")
            .set("server_transaction_id").to("tx1")
            .set("is_last_record_in_transaction_in_partition").to(true)
            .build();

         Struct recordStruct = Struct.newBuilder()
            .set("data_change_record").toStructArray(Type.struct(
                Type.StructField.of("table_name", Type.string()),
                Type.StructField.of("mod_type", Type.string()),
                Type.StructField.of("column_types", Type.array(Type.struct(
                    Type.StructField.of("name", Type.string()),
                    Type.StructField.of("type", Type.struct(Type.StructField.of("code", Type.string())))
                ))),
                Type.StructField.of("mods", Type.array(Type.struct(
                    Type.StructField.of("keys", Type.json()),
                    Type.StructField.of("new_values", Type.json()),
                    Type.StructField.of("old_values", Type.json())
                ))),
                Type.StructField.of("commit_timestamp", Type.timestamp()),
                Type.StructField.of("record_sequence", Type.string()),
                Type.StructField.of("server_transaction_id", Type.string()),
                Type.StructField.of("is_last_record_in_transaction_in_partition", Type.bool())
            ), Arrays.asList(dataChange))
            .build();
            
         return new SinkRecord(recordStruct);
    }

    @Test
    public void testAllDataTypesInsert() {
        // Defines types
        List<Struct> colTypes = Arrays.asList(
            createColType("id", "INT64"),
            createColType("active", "BOOL"),
            createColType("score", "FLOAT64"),
            createColType("price", "NUMERIC"),
            createColType("data", "BYTES"),
            createColType("dob", "DATE"),
            createColType("created_at", "TIMESTAMP"),
            createColType("meta", "JSON")
        );

        // Define values
        String keys = "{\"id\": \"123\"}";
        String newValues = "{" +
            "\"active\": true, " +
            "\"score\": 3.14159, " +
            "\"price\": \"99.99\", " +
            "\"data\": \"SGVsbG8=\", " + // Base64 for "Hello"
            "\"dob\": \"2023-01-01\", " +
            "\"created_at\": \"2023-12-15T12:00:00Z\", " +
            "\"meta\": {\"foo\": \"bar\"}" +
            "}";
        
        List<Struct> mods = Collections.singletonList(createMod(keys, newValues, "{}"));
        
        sqlSink.write(createRecord("AllTypes", "INSERT", colTypes, mods));

        assertEquals(1, capture.captured.size());
        Statement stmt = capture.captured.get(0).getStatement();
        assertNotNull(stmt);
        
        // Verify bindings
        assertEquals(123L, stmt.getParameters().get("id").getInt64());
        assertEquals(true, stmt.getParameters().get("active").getBool());
        assertEquals(3.14159, stmt.getParameters().get("score").getFloat64(), 0.0001);
        assertEquals(new BigDecimal("99.99"), stmt.getParameters().get("price").getNumeric());
        assertEquals(ByteArray.copyFrom("Hello".getBytes()), stmt.getParameters().get("data").getBytes());
        assertEquals("2023-01-01", stmt.getParameters().get("dob").getString());
        assertEquals("2023-12-15T12:00:00Z", stmt.getParameters().get("created_at").getString());
        assertEquals("{\"foo\":\"bar\"}", stmt.getParameters().get("meta").getJson()); 
    }

    @Test
    public void testNullValues() {
         List<Struct> colTypes = Arrays.asList(
            createColType("id", "INT64"),
            createColType("nullable_str", "STRING"),
            createColType("nullable_int", "INT64")
        );

        String keys = "{\"id\": \"123\"}";
        String newValues = "{" +
            "\"nullable_str\": null, " +
            "\"nullable_int\": null" +
            "}";
            
        List<Struct> mods = Collections.singletonList(createMod(keys, newValues, "{}"));
        sqlSink.write(createRecord("NullsTable", "INSERT", colTypes, mods));
        
        assertEquals(1, capture.captured.size());
        Statement stmt = capture.captured.get(0).getStatement();
        
        assertTrue(stmt.getParameters().get("nullable_str").isNull());
        assertTrue(stmt.getParameters().get("nullable_int").isNull());
    }

    @Test
    public void testDeleteOperation() {
        List<Struct> colTypes = Arrays.asList(createColType("id", "INT64"));
        String keys = "{\"id\": \"999\"}";
        
        List<Struct> mods = Collections.singletonList(createMod(keys, "{}", "{}"));
        sqlSink.write(createRecord("DeletesTable", "DELETE", colTypes, mods));
        
        assertEquals(1, capture.captured.size());
        Statement stmt = capture.captured.get(0).getStatement();
        assertTrue(stmt.getSql().startsWith("DELETE FROM DeletesTable"));
        assertEquals(999L, stmt.getParameters().get("k_id").getInt64());
    }
    
    @Test
    public void testUpdateOperation() {
         List<Struct> colTypes = Arrays.asList(
            createColType("id", "INT64"),
            createColType("status", "STRING")
        );

        String keys = "{\"id\": \"555\"}";
        String newValues = "{\"status\": \"active\"}";
        
        List<Struct> mods = Collections.singletonList(createMod(keys, newValues, "{}"));
        sqlSink.write(createRecord("UpdatesTable", "UPDATE", colTypes, mods));
        
        assertEquals(1, capture.captured.size());
        Statement stmt = capture.captured.get(0).getStatement();
        assertTrue(stmt.getSql().startsWith("UPDATE UpdatesTable SET"));
        assertEquals("active", stmt.getParameters().get("v_status").getString());
        assertEquals(555L, stmt.getParameters().get("k_id").getInt64());
    }
    
    @Test
    public void testUnknownTypeFallback() {
        Struct customType = Struct.newBuilder()
            .set("name").to("weird_col")
            .set("type").to(Struct.newBuilder().set("code").to("ALIEN_TECH").build())
            .build();
            
        List<Struct> colTypes = Arrays.asList(customType);
        String keys = "{\"weird_col\": \"foo\"}";
        
        List<Struct> mods = Collections.singletonList(createMod(keys, "{}", "{}"));
        sqlSink.write(createRecord("AlienTable", "DELETE", colTypes, mods));
        
        assertEquals(1, capture.captured.size());
        Statement stmt = capture.captured.get(0).getStatement();
        assertEquals("foo", stmt.getParameters().get("k_weird_col").getString());
    }
}
