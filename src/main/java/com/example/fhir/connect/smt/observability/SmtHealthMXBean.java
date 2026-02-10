package com.example.fhir.connect.smt.observability;

/**
 * MXBean interface for SMT health status.
 * JMX clients can read all getters and invoke forceReload().
 */
public interface SmtHealthMXBean {

    String getStatus();

    String getMappingSource();

    String getTargetDatabase();

    String getConnectorNames();

    String getLastReloadTime();

    String getLastReloadStatus();

    long getUptimeSeconds();

    double getErrorRate5Min();

    int getMappingsLoadedCount();

    // Operations
    void forceReload();
}
