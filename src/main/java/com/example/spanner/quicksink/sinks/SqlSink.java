package com.example.spanner.quicksink.sinks;

import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Statement.Builder;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonElement;
import java.util.Map;
import com.google.cloud.spanner.Value;
import com.google.cloud.spanner.Type;
import java.math.BigDecimal;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SqlSink implements DataSink {

    private final DataSink wrappedSink;
    private final Gson gson;
    private static final Pattern JSON_CODE_PATTERN = Pattern.compile("\"code\"\\s*:\\s*\"([^\"]+)\"");

    public SqlSink(DataSink wrappedSink) {
        this.wrappedSink = wrappedSink;
        this.gson = new Gson();
    }

    @Override
    public void write(SinkRecord record) {
        try {
            com.google.cloud.spanner.Struct struct = record.getStruct();
            if (struct == null || !record.isDataChange()) {
                return;
            }
            
            for (com.google.cloud.spanner.Struct dcr : struct.getStructList("data_change_record")) {
                String tableName = dcr.getString("table_name");
                String modType = dcr.getString("mod_type");
                
                List<com.google.cloud.spanner.Struct> mods = dcr.getStructList("mods");
                List<com.google.cloud.spanner.Struct> columnTypes = dcr.getStructList("column_types");
                
                Map<String, String> typeMap = new HashMap<>();
                for (com.google.cloud.spanner.Struct ct : columnTypes) {
                    String name = ct.getString("name");
                    String code = "STRING"; // default
                    if (!ct.isNull("type")) {
                        Type.Code typeCode = ct.getColumnType("type").getCode();
                        if (typeCode == Type.Code.STRUCT) {
                            com.google.cloud.spanner.Struct typeStruct = ct.getStruct("type");
                            if (!typeStruct.isNull("code")) {
                                 code = typeStruct.getString("code");
                            }
                        } else if (typeCode == Type.Code.JSON) {
                            String json = ct.getJson("type");
                            Matcher m = JSON_CODE_PATTERN.matcher(json);
                            if (m.find()) {
                                code = m.group(1);
                            }
                        }
                    }
                    typeMap.put(name, code);
                }

                for (com.google.cloud.spanner.Struct mod : mods) {
                    Statement statement = generateStatement(tableName, modType, mod, typeMap);
                    String keysJson = mod.getJson("keys");
                    if (statement != null) {
                         // Debug Logging removed
                        wrappedSink.write(new SinkRecord(statement, keysJson));
                    }
                }
            }

        } catch (Exception e) {
            System.err.println("Error processing record in SqlSink: " + e.getMessage());
            // Rethrow to alert caller (e.g. TransactionBufferSink)
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            }
            throw new RuntimeException("Error processing record in SqlSink", e);
        }
    }

    private Statement generateStatement(String tableName, String modType, com.google.cloud.spanner.Struct mod, Map<String, String> typeMap) {
        String keysJson = mod.getJson("keys");
        String newValuesJson = mod.isNull("new_values") ? "{}" : mod.getJson("new_values");
        
        JsonObject keys = gson.fromJson(keysJson, JsonObject.class);
        JsonObject newValues = gson.fromJson(newValuesJson, JsonObject.class);

        switch (modType) {
            case "INSERT":
                return generateInsert(tableName, keys, newValues, typeMap);
            case "UPDATE":
                return generateUpdate(tableName, keys, newValues, typeMap);
            case "DELETE":
                return generateDelete(tableName, keys, typeMap);
            default:
                System.err.println("Unknown mod_type: " + modType);
                return null;
        }
    }

    private Statement generateInsert(String tableName, JsonObject keys, JsonObject newValues, Map<String, String> typeMap) {
        List<String> cols = new ArrayList<>();
        List<String> params = new ArrayList<>();
        
        Map<String, JsonElement> allData = new HashMap<>();
        keys.entrySet().forEach(e -> allData.put(e.getKey(), e.getValue()));
        newValues.entrySet().forEach(e -> allData.put(e.getKey(), e.getValue()));

        StringBuilder sql = new StringBuilder("INSERT OR UPDATE INTO ").append(tableName).append(" (");
        
        boolean first = true;
        for (String col : allData.keySet()) {
            if (!first) sql.append(", ");
            sql.append(col);
            first = false;
        }
        sql.append(") VALUES (");
        
        first = true;
        for (String col : allData.keySet()) {
            if (!first) sql.append(", ");
            sql.append("@").append(col);
            first = false;
        }
        sql.append(")");

        Builder builder = Statement.newBuilder(sql.toString());
        for (Map.Entry<String, JsonElement> entry : allData.entrySet()) {
            bindValue(builder.bind(entry.getKey()), entry.getValue(), typeMap.get(entry.getKey()));
        }
        return builder.build();
    }

    private Statement generateUpdate(String tableName, JsonObject keys, JsonObject newValues, Map<String, String> typeMap) {
        if (newValues.entrySet().isEmpty()) {
            return null;
        }
        
        StringBuilder sql = new StringBuilder("UPDATE ").append(tableName).append(" SET ");
        
        boolean first = true;
        for (String col : newValues.keySet()) {
            if (!first) sql.append(", ");
            sql.append(col).append(" = @v_").append(col);
            first = false;
        }
        
        sql.append(" WHERE ");
        first = true;
        for (String col : keys.keySet()) {
            if (!first) sql.append(" AND ");
            sql.append(col).append(" = @k_").append(col);
            first = false;
        }

        Builder builder = Statement.newBuilder(sql.toString());
        for (Map.Entry<String, JsonElement> entry : newValues.entrySet()) {
            bindValue(builder.bind("v_" + entry.getKey()), entry.getValue(), typeMap.get(entry.getKey()));
        }
        for (Map.Entry<String, JsonElement> entry : keys.entrySet()) {
            bindValue(builder.bind("k_" + entry.getKey()), entry.getValue(), typeMap.get(entry.getKey()));
        }
        return builder.build();
    }

    private Statement generateDelete(String tableName, JsonObject keys, Map<String, String> typeMap) {
        StringBuilder sql = new StringBuilder("DELETE FROM ").append(tableName).append(" WHERE ");
        
        boolean first = true;
        for (String col : keys.keySet()) {
            if (!first) sql.append(" AND ");
            sql.append(col).append(" = @k_").append(col);
            first = false;
        }

        Builder builder = Statement.newBuilder(sql.toString());
        for (Map.Entry<String, JsonElement> entry : keys.entrySet()) {
            bindValue(builder.bind("k_" + entry.getKey()), entry.getValue(), typeMap.get(entry.getKey()));
        }
        return builder.build();
    }

    private void bindValue(com.google.cloud.spanner.ValueBinder binder, JsonElement value, String type) {
        if (value.isJsonNull()) {
            // TODO: check later
        }
        
        if (type == null) type = "STRING"; // default

        switch (type) {
            case "STRING":
            case "JSON": 
                if (value.isJsonNull()) binder.to((String)null);
                else if (type.equals("JSON")) binder.to(Value.json(value.isJsonPrimitive() && value.getAsJsonPrimitive().isString() ? value.getAsString() : value.toString())); 
                else binder.to(value.getAsJsonPrimitive().isString() ? value.getAsString() : value.toString());
                break;
            case "BOOL":
                if (value.isJsonNull()) binder.to((Boolean)null);
                else binder.to(value.getAsBoolean());
                break;
            case "INT64":
                if (value.isJsonNull()) binder.to((Long)null);
                else binder.to(value.getAsLong()); 
                break;
            case "FLOAT64":
                if (value.isJsonNull()) binder.to((Double)null);
                else binder.to(value.getAsDouble());
                break;
            case "NUMERIC":
                 if (value.isJsonNull()) binder.to(Value.numeric(null));
                 else binder.to(Value.numeric(new BigDecimal(value.getAsString())));
                 break;
            case "TIMESTAMP":
            case "DATE":
                if (value.isJsonNull()) binder.to((String)null);
                else binder.to(value.getAsString());
                break;
            case "BYTES":
                 if (value.isJsonNull()) binder.to(Value.bytes(null));
                 else binder.to(Value.bytes(com.google.cloud.ByteArray.fromBase64(value.getAsString())));
                 break;
            default:
                if (value.isJsonNull()) binder.to((String)null);
                else binder.to(value.getAsString());
        }
    }



    @Override
    public void close() {
        wrappedSink.close();
    }

    @Override
    public void flush() {
        wrappedSink.flush();
    }
}
