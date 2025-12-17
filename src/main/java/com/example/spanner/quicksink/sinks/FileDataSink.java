package com.example.spanner.quicksink.sinks;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import com.example.spanner.quicksink.util.Metrics;

public class FileDataSink implements DataSink {
    private final BufferedWriter writer;

    public FileDataSink(String pathStr) {
        try {
            Path path = Path.of(pathStr);
            // Create parent directories if they don't exist
            if (path.getParent() != null) {
                Files.createDirectories(path.getParent());
            }
            this.writer = Files.newBufferedWriter(path, 
                StandardOpenOption.CREATE, 
                StandardOpenOption.APPEND, 
                StandardOpenOption.WRITE);
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize FileDataSink: " + e.getMessage(), e);
        }
    }

    @Override
    public synchronized void write(SinkRecord record) {
        try {
            writer.write(record.toJson());
            writer.newLine();
            writer.flush();
            
            if (record.isDataChange()) {
                long now = System.currentTimeMillis();
                long processLag = now - record.getCaptureTimeMs();
                
                String commitTsStr = record.getCommitTimestamp();
                long dataLag = 0;
                if (commitTsStr != null && !commitTsStr.isEmpty()) {
                     try {
                        long commitTs = com.google.cloud.Timestamp.parseTimestamp(commitTsStr).toDate().getTime();
                        dataLag = now - commitTs;
                     } catch (Exception ignore) {}
                }
                Metrics.markSinked(processLag, dataLag, record.getModCount());
            }
        } catch (IOException e) {
            System.err.println("Failed to write to sink: " + e.getMessage());
        }
    }

    @Override
    public synchronized void close() {
        try {
            if (writer != null) {
                writer.close();
            }
        } catch (IOException e) {
            System.err.println("Error closing file sink: " + e.getMessage());
        }
    }

    @Override
    public synchronized void flush() {
        try {
            if (writer != null) {
                writer.flush();
            }
        } catch (IOException e) {
            System.err.println("Error flushing file sink: " + e.getMessage());
        }
    }
}
