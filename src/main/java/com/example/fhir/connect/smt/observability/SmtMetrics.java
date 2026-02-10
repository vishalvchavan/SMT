package com.example.fhir.connect.smt.observability;

import javax.management.*;
import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Thread-safe JMX metrics for the DynamicJsonExtract SMT.
 * Registered as MBean: connect.smt:type=DynamicJsonExtract
 *
 * All counters use LongAdder for high-throughput concurrent writes.
 * Gauges use AtomicLong for point-in-time reads.
 */
public class SmtMetrics implements SmtMetricsMXBean {

    private static final Logger log = LoggerFactory.getLogger(SmtMetrics.class);
    private static final String MBEAN_NAME = "connect.smt:type=DynamicJsonExtract";

    // --- Counters (monotonically increasing) ---
    private final LongAdder recordsProcessed = new LongAdder();
    private final LongAdder recordsFailed = new LongAdder();
    private final LongAdder recordsSkipped = new LongAdder();
    private final LongAdder transformCount = new LongAdder();
    private final LongAdder transformLatencyTotalNanos = new LongAdder();
    private final LongAdder payloadBytesTotal = new LongAdder();
    private final LongAdder mappingLookupMisses = new LongAdder();
    private final LongAdder hotReloadSuccessCount = new LongAdder();
    private final LongAdder hotReloadFailureCount = new LongAdder();
    private final LongAdder encryptCallsTotal = new LongAdder();
    private final LongAdder maskCallsTotal = new LongAdder();

    // --- Gauges (point-in-time values) ---
    private final AtomicLong lastTransformLatencyNanos = new AtomicLong(0);
    private final AtomicLong maxTransformLatencyNanos = new AtomicLong(0);
    private final AtomicLong lastHotReloadTimestamp = new AtomicLong(0);
    private final AtomicLong startTimeMs = new AtomicLong(System.currentTimeMillis());

    // Singleton
    private static volatile SmtMetrics instance;
    private static final Object LOCK = new Object();

    private SmtMetrics() {
    }

    /**
     * Get or create the singleton instance.
     * Thread-safe double-checked locking.
     */
    public static SmtMetrics getInstance() {
        if (instance == null) {
            synchronized (LOCK) {
                if (instance == null) {
                    instance = new SmtMetrics();
                }
            }
        }
        return instance;
    }

    // ========================
    // Recording methods
    // ========================

    public void recordSuccess(long latencyNanos, long payloadBytes) {
        recordsProcessed.increment();
        transformCount.increment();
        transformLatencyTotalNanos.add(latencyNanos);
        lastTransformLatencyNanos.set(latencyNanos);
        payloadBytesTotal.add(payloadBytes);
        // Update max using compare-and-set loop
        long current;
        do {
            current = maxTransformLatencyNanos.get();
            if (latencyNanos <= current)
                break;
        } while (!maxTransformLatencyNanos.compareAndSet(current, latencyNanos));
    }

    public void recordFailure() {
        recordsFailed.increment();
    }

    public void recordSkipped() {
        recordsSkipped.increment();
    }

    public void recordMappingMiss() {
        mappingLookupMisses.increment();
    }

    public void recordHotReloadSuccess() {
        hotReloadSuccessCount.increment();
        lastHotReloadTimestamp.set(System.currentTimeMillis());
    }

    public void recordHotReloadFailure() {
        hotReloadFailureCount.increment();
    }

    public void recordEncryptCall() {
        encryptCallsTotal.increment();
    }

    public void recordMaskCall() {
        maskCallsTotal.increment();
    }

    // ========================
    // MXBean getters (JMX-exposed)
    // ========================

    @Override
    public long getRecordsProcessed() {
        return recordsProcessed.sum();
    }

    @Override
    public long getRecordsFailed() {
        return recordsFailed.sum();
    }

    @Override
    public long getRecordsSkipped() {
        return recordsSkipped.sum();
    }

    @Override
    public long getTransformCount() {
        return transformCount.sum();
    }

    @Override
    public double getAvgTransformLatencyMs() {
        long count = transformCount.sum();
        if (count == 0)
            return 0.0;
        return (transformLatencyTotalNanos.sum() / 1_000_000.0) / count;
    }

    @Override
    public double getLastTransformLatencyMs() {
        return lastTransformLatencyNanos.get() / 1_000_000.0;
    }

    @Override
    public double getMaxTransformLatencyMs() {
        return maxTransformLatencyNanos.get() / 1_000_000.0;
    }

    @Override
    public long getPayloadBytesTotal() {
        return payloadBytesTotal.sum();
    }

    @Override
    public long getMappingLookupMisses() {
        return mappingLookupMisses.sum();
    }

    @Override
    public long getHotReloadSuccessCount() {
        return hotReloadSuccessCount.sum();
    }

    @Override
    public long getHotReloadFailureCount() {
        return hotReloadFailureCount.sum();
    }

    @Override
    public long getLastHotReloadTimestamp() {
        return lastHotReloadTimestamp.get();
    }

    @Override
    public long getEncryptCallsTotal() {
        return encryptCallsTotal.sum();
    }

    @Override
    public long getMaskCallsTotal() {
        return maskCallsTotal.sum();
    }

    @Override
    public long getUptimeSeconds() {
        return (System.currentTimeMillis() - startTimeMs.get()) / 1000;
    }

    @Override
    public void resetMetrics() {
        recordsProcessed.reset();
        recordsFailed.reset();
        recordsSkipped.reset();
        transformCount.reset();
        transformLatencyTotalNanos.reset();
        payloadBytesTotal.reset();
        mappingLookupMisses.reset();
        hotReloadSuccessCount.reset();
        hotReloadFailureCount.reset();
        encryptCallsTotal.reset();
        maskCallsTotal.reset();
        lastTransformLatencyNanos.set(0);
        maxTransformLatencyNanos.set(0);
        lastHotReloadTimestamp.set(0);
        startTimeMs.set(System.currentTimeMillis());
        log.info("SMT metrics reset");
    }

    /**
     * Returns a summary map of all metrics (useful for logging).
     */
    public Map<String, Object> snapshot() {
        return Map.ofEntries(
                Map.entry("recordsProcessed", getRecordsProcessed()),
                Map.entry("recordsFailed", getRecordsFailed()),
                Map.entry("recordsSkipped", getRecordsSkipped()),
                Map.entry("avgTransformLatencyMs", String.format("%.2f", getAvgTransformLatencyMs())),
                Map.entry("lastTransformLatencyMs", String.format("%.2f", getLastTransformLatencyMs())),
                Map.entry("maxTransformLatencyMs", String.format("%.2f", getMaxTransformLatencyMs())),
                Map.entry("payloadBytesTotal", getPayloadBytesTotal()),
                Map.entry("mappingLookupMisses", getMappingLookupMisses()),
                Map.entry("hotReloadSuccessCount", getHotReloadSuccessCount()),
                Map.entry("hotReloadFailureCount", getHotReloadFailureCount()),
                Map.entry("encryptCallsTotal", getEncryptCallsTotal()),
                Map.entry("maskCallsTotal", getMaskCallsTotal()),
                Map.entry("uptimeSeconds", getUptimeSeconds()));
    }

    // ========================
    // JMX Registration
    // ========================

    /**
     * Registers this MBean with the platform MBean server.
     */
    public void register() {
        try {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            ObjectName name = new ObjectName(MBEAN_NAME);
            if (!mbs.isRegistered(name)) {
                mbs.registerMBean(this, name);
                log.info("Registered JMX MBean: {}", MBEAN_NAME);
            }
        } catch (Exception e) {
            log.warn("Failed to register JMX MBean {}: {}", MBEAN_NAME, e.getMessage());
        }
    }

    /**
     * Unregisters this MBean from the platform MBean server.
     */
    public void unregister() {
        try {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            ObjectName name = new ObjectName(MBEAN_NAME);
            if (mbs.isRegistered(name)) {
                mbs.unregisterMBean(name);
                log.info("Unregistered JMX MBean: {}", MBEAN_NAME);
            }
        } catch (Exception e) {
            log.warn("Failed to unregister JMX MBean {}: {}", MBEAN_NAME, e.getMessage());
        }
    }
}
