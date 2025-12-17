package com.example.spanner.quicksink.sinks;

public interface DataSink extends AutoCloseable {
    void write(SinkRecord record);
    
    void flush();
    
    @Override
    void close();
}
