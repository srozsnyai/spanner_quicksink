import com.example.spanner.quicksink.sinks.DataSink;
import com.example.spanner.quicksink.sinks.SqlSink;
import com.example.spanner.quicksink.sinks.SinkRecord;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Value;
import com.google.cloud.spanner.Statement;
import org.junit.Test;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SqlSinkTest {

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

    @Test
    public void testInsert() {
        CapturingSink capture = new CapturingSink();
        SqlSink sqlSink = new SqlSink(capture);

        Struct colType1 = Struct.newBuilder().set("name").to("user_id").set("type").to(Struct.newBuilder().set("code").to("STRING").build()).build();
        Struct colType2 = Struct.newBuilder().set("name").to("age").set("type").to(Struct.newBuilder().set("code").to("INT64").build()).build();

        Struct mod = Struct.newBuilder()
            .set("keys").to(Value.json("{\"user_id\": \"u1\"}"))
            .set("new_values").to(Value.json("{\"age\": \"30\"}"))
            .set("old_values").to(Value.json("{}"))
            .build();

        Struct dataChange = Struct.newBuilder()
            .set("table_name").to("Users")
            .set("mod_type").to("INSERT")
            .set("column_types").toStructArray(Type.struct(
                Type.StructField.of("name", Type.string()),
                Type.StructField.of("type", Type.struct(Type.StructField.of("code", Type.string())))
            ), Arrays.asList(colType1, colType2))
            .set("mods").toStructArray(Type.struct(
                Type.StructField.of("keys", Type.json()),
                Type.StructField.of("new_values", Type.json()),
                Type.StructField.of("old_values", Type.json())
            ), Arrays.asList(mod))
            
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

        sqlSink.write(new SinkRecord(recordStruct));

        assertEquals(1, capture.captured.size());
        Statement stmt = capture.captured.get(0).getStatement();
        assertNotNull(stmt);
        String sql = stmt.getSql();
       
        assertTrue(sql.contains("INSERT OR UPDATE INTO Users"));
        assertTrue(sql.contains("VALUES"));
   
        assertEquals("u1", stmt.getParameters().get("user_id").getString());
        assertEquals(30L, stmt.getParameters().get("age").getInt64());
    }
    
    @Test
    public void testUpdate() {
        CapturingSink capture = new CapturingSink();
        SqlSink sqlSink = new SqlSink(capture);

        Struct colType1 = Struct.newBuilder().set("name").to("user_id").set("type").to(Struct.newBuilder().set("code").to("STRING").build()).build();
        Struct colType2 = Struct.newBuilder().set("name").to("name").set("type").to(Struct.newBuilder().set("code").to("STRING").build()).build();

        Struct mod = Struct.newBuilder()
            .set("keys").to(Value.json("{\"user_id\": \"u1\"}"))
            .set("new_values").to(Value.json("{\"name\": \"Alice\"}"))
            .set("old_values").to(Value.json("{}"))
            .build();

        Struct dataChange = Struct.newBuilder()
            .set("table_name").to("Users")
            .set("mod_type").to("UPDATE")
            .set("column_types").toStructArray(Type.struct(
                Type.StructField.of("name", Type.string()),
                Type.StructField.of("type", Type.struct(Type.StructField.of("code", Type.string())))
            ), Arrays.asList(colType1, colType2))
            .set("mods").toStructArray(Type.struct(
                Type.StructField.of("keys", Type.json()),
                Type.StructField.of("new_values", Type.json()),
                Type.StructField.of("old_values", Type.json())
            ), Arrays.asList(mod))
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

        sqlSink.write(new SinkRecord(recordStruct));

        assertEquals(1, capture.captured.size());
        Statement stmt = capture.captured.get(0).getStatement();
        // UPDATE Users SET name = @v_name WHERE user_id = @k_user_id
        assertTrue(stmt.getSql().contains("UPDATE Users SET"));
        assertTrue(stmt.getSql().contains("name = @v_name"));
        assertTrue(stmt.getSql().contains("WHERE user_id = @k_user_id"));
        
        assertEquals("Alice", stmt.getParameters().get("v_name").getString());
        assertEquals("u1", stmt.getParameters().get("k_user_id").getString());
    }

    @Test
    public void testDelete() {
        CapturingSink capture = new CapturingSink();
        SqlSink sqlSink = new SqlSink(capture);

        Struct colType1 = Struct.newBuilder().set("name").to("user_id").set("type").to(Struct.newBuilder().set("code").to("STRING").build()).build();

        Struct mod = Struct.newBuilder()
            .set("keys").to(Value.json("{\"user_id\": \"u1\"}"))
            .set("new_values").to(Value.json("{}"))
            .set("old_values").to(Value.json("{}"))
            .build();

        Struct dataChange = Struct.newBuilder()
            .set("table_name").to("Users")
            .set("mod_type").to("DELETE")
            .set("column_types").toStructArray(Type.struct(
                Type.StructField.of("name", Type.string()),
                Type.StructField.of("type", Type.struct(Type.StructField.of("code", Type.string())))
            ), Arrays.asList(colType1))
            .set("mods").toStructArray(Type.struct(
                Type.StructField.of("keys", Type.json()),
                Type.StructField.of("new_values", Type.json()),
                Type.StructField.of("old_values", Type.json())
            ), Arrays.asList(mod))
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

        sqlSink.write(new SinkRecord(recordStruct));

        assertEquals(1, capture.captured.size());
        Statement stmt = capture.captured.get(0).getStatement();
        // DELETE FROM Users WHERE user_id = @k_user_id
        assertTrue(stmt.getSql().contains("DELETE FROM Users WHERE"));
        assertTrue(stmt.getSql().contains("user_id = @k_user_id"));
        
        assertEquals("u1", stmt.getParameters().get("k_user_id").getString());
    }
    @Test
    public void testMembershipCompositeKey() {
        CapturingSink capture = new CapturingSink();
        SqlSink sqlSink = new SqlSink(capture);

        Struct colType1 = Struct.newBuilder().set("name").to("user_id").set("type").to(Struct.newBuilder().set("code").to("STRING").build()).build();
        Struct colType2 = Struct.newBuilder().set("name").to("group_id").set("type").to(Struct.newBuilder().set("code").to("STRING").build()).build();
        Struct colType3 = Struct.newBuilder().set("name").to("enrolled").set("type").to(Struct.newBuilder().set("code").to("TIMESTAMP").build()).build();

        // user_id provided as valid string
        Struct mod1 = Struct.newBuilder()
            .set("keys").to(Value.json("{\"group_id\":\"g1\", \"user_id\":\"u1\"}"))
            .set("new_values").to(Value.json("{\"enrolled\":\"2025-12-15T08:06:11.825Z\"}"))
            .set("old_values").to(Value.json("{}"))
            .build();

        Struct dataChange = Struct.newBuilder()
            .set("table_name").to("membership")
            .set("mod_type").to("INSERT")
            .set("column_types").toStructArray(Type.struct(
                Type.StructField.of("name", Type.string()),
                Type.StructField.of("type", Type.struct(Type.StructField.of("code", Type.string())))
            ), Arrays.asList(colType1, colType2, colType3))
            .set("mods").toStructArray(Type.struct(
                Type.StructField.of("keys", Type.json()),
                Type.StructField.of("new_values", Type.json()),
                Type.StructField.of("old_values", Type.json())
            ), Arrays.asList(mod1))
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

        sqlSink.write(new SinkRecord(recordStruct));

        assertEquals(1, capture.captured.size());
        Statement stmt = capture.captured.get(0).getStatement();
        String sql = stmt.getSql();
        
        assertTrue(sql.contains("INSERT OR UPDATE INTO membership"));
        assertEquals("u1", stmt.getParameters().get("user_id").getString());
        assertEquals("g1", stmt.getParameters().get("group_id").getString());
        assertEquals("2025-12-15T08:06:11.825Z", stmt.getParameters().get("enrolled").getString());
    }
}
