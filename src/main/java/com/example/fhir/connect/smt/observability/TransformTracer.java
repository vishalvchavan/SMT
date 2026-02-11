package com.example.fhir.connect.smt.observability;

import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Debug-mode transform tracing logger.
 * Controlled by the {@code app.trace.enabled} config flag.
 *
 * When enabled, logs the full extract→transform→output pipeline
 * at TRACE level for every message. Uses a ThreadLocal StringBuilder
 * to batch trace lines and flush once per record.
 *
 * When disabled (default), all methods are no-ops with zero overhead.
 */
public final class TransformTracer {

    private static final Logger log = LoggerFactory.getLogger(TransformTracer.class);
    private static final int MAX_JSON_LENGTH = 500; // truncate large payloads in trace

    private final boolean enabled;
    private final ThreadLocal<StringBuilder> buffer;

    /**
     * @param enabled if false, all trace methods are no-ops
     */
    public TransformTracer(boolean enabled) {
        this.enabled = enabled;
        this.buffer = enabled ? ThreadLocal.withInitial(() -> new StringBuilder(1024)) : null;
    }

    public boolean isEnabled() {
        return enabled;
    }

    // ========================
    // Trace methods
    // ========================

    /**
     * Start trace for a new record.
     */
    public void traceStart(String topic, int partition, long timestamp, long mappingVersion) {
        if (!enabled)
            return;
        StringBuilder sb = getBuffer();
        sb.setLength(0);
        sb.append("TRACE [topic=").append(topic)
                .append(" partition=").append(partition)
                .append(" timestamp=").append(timestamp)
                .append(" mappingVersion=").append(mappingVersion)
                .append("]\n");
    }

    /**
     * Trace the input JSON payload.
     */
    public void traceInput(JsonNode input) {
        if (!enabled)
            return;
        getBuffer().append("  Input: ").append(truncate(input)).append('\n');
    }

    /**
     * Trace the mapping lookup result.
     */
    public void traceMapping(String connectorOrTopic, String lookupType, boolean found, String root) {
        if (!enabled)
            return;
        StringBuilder sb = getBuffer();
        sb.append("  Mapping lookup: ").append(lookupType).append('=').append(connectorOrTopic);
        if (found) {
            sb.append(" → FOUND (root=").append(root).append(")\n");
        } else {
            sb.append(" → NOT FOUND\n");
        }
    }

    /**
     * Trace a field extraction.
     */
    public void traceField(String fieldName, String path, JsonNode value) {
        if (!enabled)
            return;
        StringBuilder sb = getBuffer();
        sb.append("  Field \"").append(fieldName).append("\" ← ")
                .append(path).append(" = ").append(truncate(value)).append('\n');
    }

    /**
     * Trace a transform step applied to a field.
     */
    public void traceTransformStep(String fieldName, String transformType, JsonNode before, JsonNode after) {
        if (!enabled)
            return;
        StringBuilder sb = getBuffer();
        sb.append("    → ").append(transformType).append(": ")
                .append(truncate(before)).append(" → ").append(truncate(after)).append('\n');
    }

    /**
     * Trace the output JSON and flush the buffer.
     */
    public void traceOutput(JsonNode output, long latencyNanos) {
        if (!enabled)
            return;
        StringBuilder sb = getBuffer();
        sb.append("  Output: ").append(truncate(output)).append('\n');
        sb.append("  Completed in ").append(latencyNanos / 1_000_000.0).append("ms");
        log.trace("{}", sb);
        sb.setLength(0);
    }

    /**
     * Trace a skip (no mapping found).
     */
    public void traceSkip(String reason) {
        if (!enabled)
            return;
        StringBuilder sb = getBuffer();
        sb.append("  SKIPPED: ").append(reason);
        log.trace("{}", sb);
        sb.setLength(0);
    }

    /**
     * Trace an error and flush.
     */
    public void traceError(String message, Throwable error) {
        if (!enabled)
            return;
        StringBuilder sb = getBuffer();
        sb.append("  ERROR: ").append(message);
        if (error != null) {
            sb.append(" (").append(error.getClass().getSimpleName())
                    .append(": ").append(error.getMessage()).append(')');
        }
        log.trace("{}", sb);
        sb.setLength(0);
    }

    // ========================
    // Helpers
    // ========================

    private StringBuilder getBuffer() {
        return buffer.get();
    }

    private static String truncate(JsonNode node) {
        if (node == null || node.isMissingNode())
            return "null";
        String s = node.toString();
        if (s.length() > MAX_JSON_LENGTH) {
            return s.substring(0, MAX_JSON_LENGTH) + "... (" + s.length() + " chars)";
        }
        return s;
    }
}
