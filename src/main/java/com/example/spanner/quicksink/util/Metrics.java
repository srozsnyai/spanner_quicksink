package com.example.spanner.quicksink.util;


import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.Map;

public class Metrics {
    private static final LongAdder receivedCount = new LongAdder();
    private static final LongAdder sinkedCount = new LongAdder();
    private static final AtomicLong bufferedCount = new AtomicLong(0);
    
    // Latency tracking
    private static final LongAdder totalProcessLagMs = new LongAdder();
    private static final LongAdder totalDataLagMs = new LongAdder();
    private static final LongAdder lagSamples = new LongAdder();
    
    // Previous counts for rate calculation
    private static long prevReceived = 0;
    private static long prevSinked = 0;
    private static long lastStatsTime = 0;

    private static ScheduledExecutorService reporter;

    public static void startReporter() {
        if (reporter != null) return;
        reporter = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "MetricsReporter");
            t.setDaemon(true);
            return t;
        });

        lastStatsTime = System.currentTimeMillis();
        reporter.scheduleAtFixedRate(Metrics::printStats, 2, 2, TimeUnit.SECONDS);
    }

    public static void markReceived(int count) {
        receivedCount.add(count);
    }

    public static void markSinked(long processLag, long dataLag, int count) {
        sinkedCount.add(count);
        totalProcessLagMs.add(processLag * count); // Weighted average? Or just sum? Sum for total lag.
        // Actually, simplistic average: just add latency once per batch or N times?
        // Let's add (latency * count) to weight it properly for the average?
        // Existing avg calculation: totalLag / samples. 
        // If we want "Average Lag Per Record", we should add (latency * count) and add (count) to samples.
        totalDataLagMs.add(dataLag * count);
        lagSamples.add(count);
    }
    
    public static void incrementBuffered() {
        bufferedCount.incrementAndGet();
    }
    
    public static void decrementBuffered() {
        bufferedCount.decrementAndGet();
    }

    private static void printStats() {
        long now = System.currentTimeMillis();
        long recv = receivedCount.sum();
        long sink = sinkedCount.sum();
        long buf = bufferedCount.get();
        
        long samples = lagSamples.sumThenReset();
        long processLag = totalProcessLagMs.sumThenReset();
        long dataLag = totalDataLagMs.sumThenReset();
        
        double avgProcess = samples > 0 ? (double) processLag / samples : 0.0;
        double avgData = samples > 0 ? (double) dataLag / samples : 0.0;
        
        // Calculate Rates
        long intervalMs = now - lastStatsTime;
        if (intervalMs <= 0) intervalMs = 2000; // prevent div by zero
        double intervalSec = intervalMs / 1000.0;
        
        double recvRate = (recv - prevReceived) / intervalSec;
        double sinkRate = (sink - prevSinked) / intervalSec;
        
        prevReceived = recv;
        prevSinked = sink;
        lastStatsTime = now;

        Log.info(String.format("[STATS] Recvd: %.1f/s (Tot:%d) | Sinked: %.1f/s (Tot:%d) | Buffered: %d | Lag: Process %.1f ms / Data %.1f ms | Threads: %s",
                recvRate, recv, sinkRate, sink, buf, avgProcess, avgData, getThreadStats()));
    }

    private static String getThreadStats() {
        Map<Thread, StackTraceElement[]> threads = Thread.getAllStackTraces();
        int partition = 0;
        int spanner = 0;
        int buffer = 0;
        int other = 0;
        
        for (Thread t : threads.keySet()) {
            if (!t.isAlive()) continue;
            String name = t.getName();
            if (name.startsWith("Partition-Reader-")) {
                partition++;
            } else if (name.startsWith("SpannerSink-Shard-")) {
                spanner++;
            } else if (name.equals("TransactionBufferFlusher")) {
                buffer++;
            } else if (name.equals("MetricsReporter")) {
                // ignore itself
            } else {
                other++;
            }
        }
        return String.format("Partitions: %d, Spanner: %d, Buffer: %d, Other: %d", partition, spanner, buffer, other);
    }

    public static void stop() {
        if (reporter != null) {
            reporter.shutdownNow();
            reporter = null;
        }
    }
}
