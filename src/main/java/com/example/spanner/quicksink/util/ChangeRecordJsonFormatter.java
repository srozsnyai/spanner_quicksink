package com.example.spanner.quicksink.util;

import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ChangeRecordJsonFormatter {
    private static final Pattern JSON_CODE_PATTERN = Pattern.compile("\"code\"\\s*:\\s*\"([^\"]+)\"");

    public String format(Struct changeRecord) {
        boolean hasDataChange = hasField(changeRecord, "data_change_record") && 
                               !changeRecord.isNull("data_change_record") &&
                               !changeRecord.getStructList("data_change_record").isEmpty();
        boolean hasHeartbeat = hasField(changeRecord, "heartbeat_record") && 
                             !changeRecord.isNull("heartbeat_record") &&
                             !changeRecord.getStructList("heartbeat_record").isEmpty();
        boolean hasChildPartitions = hasField(changeRecord, "child_partitions_record") && 
                                   !changeRecord.isNull("child_partitions_record") &&
                                   !changeRecord.getStructList("child_partitions_record").isEmpty();

        if (hasDataChange) {
            return formatDataChangeRecord(changeRecord);
        } else if (hasHeartbeat) {
            return formatHeartbeatRecord(changeRecord);
        } else if (hasChildPartitions) {
            return formatChildPartitionsRecord(changeRecord);
        } else {
            return "{\"error\": \"Unknown record type\", \"raw\": \"" + escapeJson(changeRecord.toString()) + "\"}";
        }
    }

    private String formatHeartbeatRecord(Struct changeRecord) {
        List<Struct> heartbeatArray = changeRecord.getStructList("heartbeat_record");
        if (heartbeatArray == null || heartbeatArray.isEmpty()) {
            return "{}";
        }
        Struct heartbeat = heartbeatArray.get(0);

        StringBuilder sb = new StringBuilder();
        sb.append('{');
        appendJsonField(sb, "record_type", "heartbeat", true);
        if (hasField(heartbeat, "partition_token") && !heartbeat.isNull("partition_token")) {
            appendJsonField(sb, "partition_token", heartbeat.getString("partition_token"), false);
        }
        sb.append(", \"heartbeat_record\": {");
        if (hasField(heartbeat, "heartbeat_timestamp") && !heartbeat.isNull("heartbeat_timestamp")) {
            appendJsonField(sb, "heartbeat_timestamp", heartbeat.getTimestamp("heartbeat_timestamp").toString(), true);
        }
        sb.append('}');
        sb.append('}');
        return sb.toString();
    }

    private String formatChildPartitionsRecord(Struct changeRecord) {
        List<Struct> childPartitionsArray = changeRecord.getStructList("child_partitions_record");
        if (childPartitionsArray == null || childPartitionsArray.isEmpty()) {
            return "{}";
        }
        Struct childPartitionsRecord = childPartitionsArray.get(0);

        StringBuilder sb = new StringBuilder();
        sb.append('{');
        appendJsonField(sb, "record_type", "child_partitions", true);
        if (hasField(childPartitionsRecord, "partition_token") && !childPartitionsRecord.isNull("partition_token")) {
            appendJsonField(sb, "partition_token", childPartitionsRecord.getString("partition_token"), false);
        }
        sb.append(", \"child_partitions_record\": {");
        sb.append(" \"child_partitions\": [");
        if (hasField(childPartitionsRecord, "child_partitions") && !childPartitionsRecord.isNull("child_partitions")) {
            List<Struct> children = childPartitionsRecord.getStructList("child_partitions");
            for (int i = 0; i < children.size(); i++) {
                if (i > 0) sb.append(',');
                Struct c = children.get(i);
                sb.append('{');
                appendJsonField(sb, "token", c.getString("token"), true);
                if (hasField(c, "start_timestamp") && !c.isNull("start_timestamp")) {
                    appendJsonField(sb, "start_timestamp", c.getTimestamp("start_timestamp").toString(), false);
                }
                sb.append('}');
            }
        }
        sb.append(']');
        sb.append('}');
        sb.append('}');
        return sb.toString();
    }

    private String formatDataChangeRecord(Struct changeRecord) {
        List<Struct> dataChangeArray = changeRecord.getStructList("data_change_record");
        if (dataChangeArray == null || dataChangeArray.isEmpty()) {
            return "{}";
        }
        Struct dcr = dataChangeArray.get(0);

        StringBuilder sb = new StringBuilder();
        sb.append('{');
        appendJsonField(sb, "record_type", "data_change", true);
        if (hasField(dcr, "partition_token") && !dcr.isNull("partition_token")) {
            appendJsonField(sb, "partition_token", dcr.getString("partition_token"), false);
        }
        if (hasField(dcr, "partition_start_timestamp") && !dcr.isNull("partition_start_timestamp")) {
            appendJsonField(sb, "partition_start_timestamp", dcr.getTimestamp("partition_start_timestamp").toString(), false);
        }
        if (hasField(dcr, "partition_end_timestamp") && !dcr.isNull("partition_end_timestamp")) {
            appendJsonField(sb, "partition_end_timestamp", dcr.getTimestamp("partition_end_timestamp").toString(), false);
        }
        sb.append(", \"data_change_record\": {");

        boolean first = true;
        if (hasField(dcr, "commit_timestamp") && !dcr.isNull("commit_timestamp")) {
            String ts = dcr.getTimestamp("commit_timestamp").toString();
            appendJsonField(sb, "commit_timestamp", ts, first);
            first = false;
        }
        if (hasField(dcr, "record_sequence") && !dcr.isNull("record_sequence")) {
            appendJsonField(sb, "record_sequence", dcr.getString("record_sequence"), first);
            first = false;
        }
        if (hasField(dcr, "server_transaction_id") && !dcr.isNull("server_transaction_id"))
            appendJsonField(sb, "server_transaction_id", dcr.getString("server_transaction_id"), false);
        if (hasField(dcr, "is_last_record_in_transaction_in_partition") && !dcr.isNull("is_last_record_in_transaction_in_partition"))
            appendJsonField(sb, "is_last_record_in_transaction_in_partition", dcr.getBoolean("is_last_record_in_transaction_in_partition"), false);
        if (hasField(dcr, "table_name") && !dcr.isNull("table_name"))
            appendJsonField(sb, "table_name", dcr.getString("table_name"), false);

        if (hasField(dcr, "column_types") && !dcr.isNull("column_types")) {
            sb.append(", \"column_types\": [");
            List<Struct> colTypes = dcr.getStructList("column_types");
            for (int i = 0; i < colTypes.size(); i++) {
                Struct ct = colTypes.get(i);
                if (i > 0) sb.append(',');
                sb.append('{');
                appendJsonField(sb, "name", getNullableString(ct, "name"), true);
                if (hasField(ct, "type") && !ct.isNull("type")) {
                    String code = null;
                    try {
                        Type.Code typeCode = ct.getColumnType("type").getCode();
                        if (typeCode == Type.Code.STRUCT) {
                            Struct typeStruct = ct.getStruct("type");
                            if (hasField(typeStruct, "code") && !typeStruct.isNull("code")) {
                                code = typeStruct.getString("code");
                            }
                        } else if (typeCode == Type.Code.JSON) {
                            String json = ct.getJson("type");
                            Matcher m = JSON_CODE_PATTERN.matcher(json);
                            if (m.find()) {
                                code = m.group(1);
                            }
                        }
                    } catch (Exception e) {
                        // ignore
                    }
                    sb.append(", \"type\": {");
                    if (code != null)
                        appendJsonField(sb, "code", code, true);
                    sb.append('}');
                }
                if (hasField(ct, "is_primary_key") && !ct.isNull("is_primary_key"))
                    appendJsonField(sb, "is_primary_key", ct.getBoolean("is_primary_key"), false);
                if (hasField(ct, "ordinal_position") && !ct.isNull("ordinal_position"))
                    appendJsonField(sb, "ordinal_position", ct.getLong("ordinal_position"), false);
                sb.append('}');
            }
            sb.append(']');
        }

        if (hasField(dcr, "mods") && !dcr.isNull("mods")) {
            sb.append(", \"mods\": [");
            List<Struct> mods = dcr.getStructList("mods");
            for (int i = 0; i < mods.size(); i++) {
                Struct m = mods.get(i);
                if (i > 0) sb.append(',');
                sb.append('{');
                sb.append("\"keys\": ").append(m.isNull("keys") ? "null" : m.getJson("keys"));
                sb.append(", \"new_values\": ").append(m.isNull("new_values") ? "null" : m.getJson("new_values"));
                sb.append(", \"old_values\": ").append(m.isNull("old_values") ? "null" : m.getJson("old_values"));
                sb.append('}');
            }
            sb.append(']');
        }

        if (hasField(dcr, "mod_type") && !dcr.isNull("mod_type"))
            appendJsonField(sb, "mod_type", dcr.getString("mod_type"), false);
        if (hasField(dcr, "value_capture_type") && !dcr.isNull("value_capture_type"))
            appendJsonField(sb, "value_capture_type", dcr.getString("value_capture_type"), false);
        if (hasField(dcr, "number_of_records_in_transaction") && !dcr.isNull("number_of_records_in_transaction"))
            appendJsonField(sb, "number_of_records_in_transaction", dcr.getLong("number_of_records_in_transaction"), false);
        if (hasField(dcr, "number_of_partitions_in_transaction") && !dcr.isNull("number_of_partitions_in_transaction"))
            appendJsonField(sb, "number_of_partitions_in_transaction", dcr.getLong("number_of_partitions_in_transaction"), false);
        if (hasField(dcr, "transaction_tag"))
            appendJsonField(sb, "transaction_tag", dcr.isNull("transaction_tag") ? null : dcr.getString("transaction_tag"), false);
        if (hasField(dcr, "is_system_transaction") && !dcr.isNull("is_system_transaction"))
            appendJsonField(sb, "is_system_transaction", dcr.getBoolean("is_system_transaction"), false);

        sb.append('}');
        sb.append('}');
        return sb.toString();
    }

    private boolean hasField(Struct s, String field) {
        try {
            s.getColumnIndex(field);
            return true;
        } catch (IllegalArgumentException ignore) {
            return false;
        }
    }

    private String getNullableString(Struct s, String field) {
        if (!hasField(s, field) || s.isNull(field)) return null;
        return s.getString(field);
    }

    private void appendJsonField(StringBuilder sb, String name, String value, boolean first) {
        if (!first) sb.append(',');
        sb.append(' ').append('"').append(name).append('"').append(':');
        if (value == null) {
            sb.append(" null");
        } else {
            sb.append(' ').append('"').append(escapeJson(value)).append('"');
        }
    }

    private void appendJsonField(StringBuilder sb, String name, boolean value, boolean first) {
        if (!first) sb.append(',');
        sb.append(' ').append('"').append(name).append('"').append(':').append(' ').append(value);
    }

    private void appendJsonField(StringBuilder sb, String name, long value, boolean first) {
        if (!first) sb.append(',');
        sb.append(' ').append('"').append(name).append('"').append(':').append(' ').append(value);
    }

    private String escapeJson(String s) {
        StringBuilder out = new StringBuilder(s.length() + 16);
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"': out.append("\\\""); break;
                case '\\': out.append("\\\\"); break;
                case '\b': out.append("\\b"); break;
                case '\f': out.append("\\f"); break;
                case '\n': out.append("\\n"); break;
                case '\r': out.append("\\r"); break;
                case '\t': out.append("\\t"); break;
                default:
                    if (c < 0x20) {
                        out.append(String.format("\\u%04x", (int) c));
                    } else {
                        out.append(c);
                    }
            }
        }
        return out.toString();
    }
}
