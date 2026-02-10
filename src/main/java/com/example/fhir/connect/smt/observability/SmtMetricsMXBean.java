package com.example.fhir.connect.smt.observability;

/**
 * MXBean interface for SMT metrics.
 * JMX clients can read all getters and invoke resetMetrics().
 */
public interface SmtMetricsMXBean {

    // Counters
    long getRecordsProcessed();

    long getRecordsFailed();

    long getRecordsSkipped();

    long getTransformCount();

    long getPayloadBytesTotal();

    long getMappingLookupMisses();

    long getHotReloadSuccessCount();

    long getHotReloadFailureCount();

    long getEncryptCallsTotal();

    long getMaskCallsTotal();

    // Gauges
    double getAvgTransformLatencyMs();

    double getLastTransformLatencyMs();

    double getMaxTransformLatencyMs();

    long getLastHotReloadTimestamp();

    long getUptimeSeconds();

    // Operations
    void resetMetrics();
}
