package com.example.spanner.quicksink.sinks;

import com.example.spanner.quicksink.util.Metrics;

public class MonitoredSink implements DataSink {
    
    private final DataSink delegate;
    
    public MonitoredSink(DataSink delegate) {
        this.delegate = delegate;
    }

    @Override
    public void write(SinkRecord record) {
        long start = System.currentTimeMillis();
        delegate.write(record);
        long end = System.currentTimeMillis();
        
        long processLag = end - record.getCaptureTimeMs();
        long dataLag = 0;
        
        if (record.isDataChange()) {
            String commitTsStr = record.getCommitTimestamp();
             if (commitTsStr != null && !commitTsStr.isEmpty()) {
                 try {
                    long commitTs = com.google.cloud.Timestamp.parseTimestamp(commitTsStr).toDate().getTime();
                    dataLag = end - commitTs;
                 } catch (Exception ignore) {}
             }
        }
        
        Metrics.markSinked(processLag, dataLag, record.getModCount());
    }
    
    @Override
    public void flush() {
        delegate.flush();
    }

    @Override
    public void close() {
        delegate.close();
    }
}
