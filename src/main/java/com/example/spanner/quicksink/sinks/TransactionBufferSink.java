package com.example.spanner.quicksink.sinks;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.example.spanner.quicksink.util.Log;
import com.example.spanner.quicksink.util.Metrics;

public class TransactionBufferSink implements DataSink {

    private final DataSink wrappedSink;
    private final long windowMs;
    private final ScheduledExecutorService scheduler;
    
    private final ConcurrentHashMap<String, TransactionContext> transactionBuffer;
    
    private volatile boolean closed = false;

    public TransactionBufferSink(DataSink wrappedSink, long windowMs, long flushIntervalMs) {
        this.wrappedSink = wrappedSink;
        this.windowMs = windowMs;
        this.transactionBuffer = new ConcurrentHashMap<>();
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "TransactionBufferFlusher");
            t.setDaemon(true);
            return t;
        });

        // Schedule periodic flush
        this.scheduler.scheduleWithFixedDelay(this::checkBuffer, flushIntervalMs, flushIntervalMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public void write(SinkRecord record) {
        if (closed) return;
        
        BufferedRecord buffered = new BufferedRecord(record);
        
        transactionBuffer.compute(buffered.txId, (k, v) -> {
            if (v == null) {
                v = new TransactionContext(buffered.txId);
            }
            v.addRecord(buffered);
            return v;
        });
        
        Metrics.incrementBuffered();
    }

    @Override
    public void flush() {
        checkBuffer();
    }

    @Override
    public void close() {
        closed = true;
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
        }
        try {
            flushAll();
        } catch (Exception e) {
            System.err.println("Warning: Failed to flush pending transactions during shutdown: " + e.getMessage());
        }
        wrappedSink.close();
    }

    private synchronized void checkBuffer() {
        long now = System.currentTimeMillis();

        List<TransactionContext> allContexts = new ArrayList<>(transactionBuffer.values());
        allContexts.sort(Comparator.comparing(c -> c.commitTimestamp));

        List<TransactionContext> toFlush = new ArrayList<>();
        
        for (TransactionContext ctx : allContexts) {
            if (ctx.commitTimestamp.isEmpty()) {
                continue; 
            }
            
            if (ctx.arrivalTime + windowMs <= now) {
                toFlush.add(ctx);
            } else {
                break;
            }
        }
        
        flushContexts(toFlush);
    }
    
    private synchronized void flushAll() {
        if (transactionBuffer.isEmpty()) return;
        List<TransactionContext> allContexts = new ArrayList<>(transactionBuffer.values());
        allContexts.sort(Comparator.comparing(c -> c.commitTimestamp));
        flushContexts(allContexts);
    }

    private void flushContexts(List<TransactionContext> contexts) {
        if (contexts.isEmpty()) return;

        Log.debug("[Buffer] Flushing " + contexts.size() + " transactions...");

        long now = System.currentTimeMillis();

        for (TransactionContext ctx : contexts) {
            transactionBuffer.remove(ctx.txId);
            
            List<BufferedRecord> records = ctx.getRecords();
            records.sort(Comparator.comparing(r -> r.seq));
            
            for (BufferedRecord r : records) {
                wrappedSink.write(r.record);
                Metrics.decrementBuffered();
                long processLag = now - r.record.getCaptureTimeMs();
                long commitTs = com.google.cloud.Timestamp.parseTimestamp(r.record.getCommitTimestamp()).toDate().getTime();
                long dataLag = now - commitTs;
                Metrics.markSinked(processLag, dataLag, r.record.getModCount());
            }
        }
        wrappedSink.flush();
    }

    private static class TransactionContext {
        final String txId;
        final List<BufferedRecord> records = new ArrayList<>();
        long arrivalTime = 0;
        String commitTimestamp = "";

        TransactionContext(String txId) {
            this.txId = txId;
        }

        synchronized void addRecord(BufferedRecord r) {
            if (records.isEmpty()) {
                arrivalTime = System.currentTimeMillis();
                commitTimestamp = r.commitTs;
            }
            
            records.add(r);
        }

        synchronized List<BufferedRecord> getRecords() {
            return new ArrayList<>(records);
        }


    }

    private static class BufferedRecord {
        final SinkRecord record;
        final String txId;
        final String seq;
        final String commitTs;

        BufferedRecord(SinkRecord record) {
            this.record = record;
            this.txId = record.getTransactionId();
            this.seq = record.getRecordSequence();
            this.commitTs = record.getCommitTimestamp();
        }
    }
}
