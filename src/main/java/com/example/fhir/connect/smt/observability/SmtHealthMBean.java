package com.example.fhir.connect.smt.observability;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Health status MBean for the SMT.
 * Registered as: connect.smt:type=SmtHealth
 *
 * Provides a dashboard-friendly snapshot of SMT state.
 */
public class SmtHealthMBean implements SmtHealthMXBean {

    private static final Logger log = LoggerFactory.getLogger(SmtHealthMBean.class);
    private static final String MBEAN_NAME = "connect.smt:type=SmtHealth";
    private static final DateTimeFormatter ISO_FMT = DateTimeFormatter.ISO_INSTANT;

    private final AtomicReference<String> mappingSource = new AtomicReference<>("classpath");
    private final AtomicReference<String> targetDatabase = new AtomicReference<>("mongodb");
    private final AtomicReference<Set<String>> connectorNames = new AtomicReference<>(Set.of());
    private final AtomicReference<String> lastReloadStatus = new AtomicReference<>("N/A");
    private final AtomicLong lastReloadTimeMs = new AtomicLong(0);
    private final AtomicLong startTimeMs = new AtomicLong(System.currentTimeMillis());
    private final AtomicReference<String> status = new AtomicReference<>("HEALTHY");
    private final AtomicReference<Runnable> forceReloadAction = new AtomicReference<>();

    // Sliding window for error rate (last 5 min)
    private final AtomicLong errorWindowStart = new AtomicLong(System.currentTimeMillis());
    private final AtomicLong errorsInWindow = new AtomicLong(0);
    private final AtomicLong totalInWindow = new AtomicLong(0);

    // Singleton
    private static volatile SmtHealthMBean instance;
    private static final Object LOCK = new Object();

    private SmtHealthMBean() {
    }

    public static SmtHealthMBean getInstance() {
        if (instance == null) {
            synchronized (LOCK) {
                if (instance == null) {
                    instance = new SmtHealthMBean();
                }
            }
        }
        return instance;
    }

    // ========================
    // Setters (called from SMT code)
    // ========================

    public void setMappingSource(String source) {
        this.mappingSource.set(source);
    }

    public void setTargetDatabase(String db) {
        this.targetDatabase.set(db);
    }

    public void setConnectorNames(Set<String> names) {
        this.connectorNames.set(names != null ? Set.copyOf(names) : Set.of());
    }

    public void setForceReloadAction(Runnable action) {
        this.forceReloadAction.set(action);
    }

    public void recordReloadSuccess() {
        lastReloadTimeMs.set(System.currentTimeMillis());
        lastReloadStatus.set("SUCCESS");
    }

    public void recordReloadFailure(String error) {
        lastReloadStatus.set("FAILED: " + error);
    }

    /**
     * Records a processing event for error rate calculation.
     * Resets the window every 5 minutes.
     */
    public void recordProcessingEvent(boolean error) {
        long now = System.currentTimeMillis();
        long windowStart = errorWindowStart.get();
        if (now - windowStart > 300_000) { // 5 min window
            errorWindowStart.set(now);
            errorsInWindow.set(0);
            totalInWindow.set(0);
        }
        totalInWindow.incrementAndGet();
        if (error) {
            errorsInWindow.incrementAndGet();
        }
        updateStatus();
    }

    private void updateStatus() {
        double rate = getErrorRate5Min();
        if (rate > 0.5) {
            status.set("UNHEALTHY");
        } else if (rate > 0.1) {
            status.set("DEGRADED");
        } else {
            status.set("HEALTHY");
        }
    }

    // ========================
    // MXBean getters (JMX-exposed)
    // ========================

    @Override
    public String getStatus() {
        return status.get();
    }

    @Override
    public String getMappingSource() {
        return mappingSource.get();
    }

    @Override
    public String getTargetDatabase() {
        return targetDatabase.get();
    }

    @Override
    public String getConnectorNames() {
        return String.join(", ", connectorNames.get());
    }

    @Override
    public String getLastReloadTime() {
        long ts = lastReloadTimeMs.get();
        if (ts == 0)
            return "N/A";
        return ISO_FMT.format(Instant.ofEpochMilli(ts).atZone(ZoneId.of("UTC")));
    }

    @Override
    public String getLastReloadStatus() {
        return lastReloadStatus.get();
    }

    @Override
    public long getUptimeSeconds() {
        return (System.currentTimeMillis() - startTimeMs.get()) / 1000;
    }

    @Override
    public double getErrorRate5Min() {
        long total = totalInWindow.get();
        if (total == 0)
            return 0.0;
        return (double) errorsInWindow.get() / total;
    }

    @Override
    public int getMappingsLoadedCount() {
        return connectorNames.get().size();
    }

    @Override
    public void forceReload() {
        Runnable action = forceReloadAction.get();
        if (action != null) {
            log.info("Force reload triggered via JMX");
            action.run();
        } else {
            log.warn("Force reload requested via JMX but no reload action configured");
        }
    }

    // ========================
    // JMX Registration
    // ========================

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
