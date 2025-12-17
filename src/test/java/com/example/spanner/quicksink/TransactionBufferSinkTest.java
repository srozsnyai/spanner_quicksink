package com.example.spanner.quicksink;

import com.example.spanner.quicksink.sinks.DataSink;
import com.example.spanner.quicksink.sinks.TransactionBufferSink;
import com.example.spanner.quicksink.sinks.SinkRecord;
import org.junit.Test;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class TransactionBufferSinkTest {

    private static class CapturingSink implements DataSink {
        final List<SinkRecord> captured = new CopyOnWriteArrayList<>();

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
    public void testOrderedExpiration() throws InterruptedException {
        CapturingSink capture = new CapturingSink();
        // Window of 1000ms, Check every 100ms
        TransactionBufferSink bufferSink = new TransactionBufferSink(capture, 1000, 100);

        // tx Early: Timestamp T10 (Older), arrives NOW (Delay simulation: maybe it was stuck in network)
        // tx Late:  Timestamp T20 (Newer), arrives NOW
        
        // Actually, to test blocking:
        // 1. Send txEarly (T10)
        // 2. Send txLate (T20)
        // If we wait 1000ms, both flush. Stable order.
        
        // To test BLOCKING:
        // 1. Send txLate (T20). Arrives at T=0. Expires at T=1000.
        // 2. Send txEarly (T10). Arrives at T=500. Expires at T=1500.
        // At T=1200:
        //    txLate is EXPIRED (1200-0 > 1000).
        //    txEarly is NOT EXPIRED (1200-500 < 1000).
        //    Sorted list: [txEarly, txLate].
        //    Head is txEarly. NOT EXPIRED.
        //    Result: flush() should STOP. txLate should NOT flush yet, even though it IS expired.
        // This ensures txEarly comes out before txLate.
        
        SinkRecord txEarly = new SinkRecord("txEarly", "2023-01-01T00:00:10Z", "000");
        SinkRecord txLate = new SinkRecord("txLate", "2023-01-01T00:00:20Z", "000");
        
        System.out.println("Writing txLate...");
        bufferSink.write(txLate); // T=0
        
        Thread.sleep(500);
        System.out.println("Writing txEarly...");
        bufferSink.write(txEarly); // T=500
        
        // At T=1100 (approx)
        Thread.sleep(600);
        
        // txLate arrival+1000 = 1000. Now is 1100. Late IS expired.
        // txEarly arrival+1000 = 1500. Now is 1100. Early IS NOT expired.
        // Sorted: Early -> Late.
        // Early blocks Late.
        // Expect: 0 captured.
        
        System.out.println("Checking at T=1100...");
        assertEquals("Should be blocked by unexpired earlier transaction", 0, capture.captured.size());
        
        // Wait until T=1600 (Early expires)
        Thread.sleep(600);
        System.out.println("Checking at T=1700...");
        
        // ALL should flush now.
        assertEquals(2, capture.captured.size());
        assertEquals(2, capture.captured.size());
        assertEquals("txEarly", capture.captured.get(0).getTransactionId());
        assertEquals("txLate", capture.captured.get(1).getTransactionId());
        
        bufferSink.close();
    }
}
