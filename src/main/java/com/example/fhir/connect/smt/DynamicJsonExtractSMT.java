package com.example.fhir.connect.smt;

import com.example.fhir.connect.smt.config.HotReloadManager;
import com.example.fhir.connect.smt.config.S3ConfigLoader;
import com.example.fhir.connect.smt.config.SmtConfig;
import com.example.fhir.connect.smt.formatter.AbstractJdbcFormatter;
import com.example.fhir.connect.smt.formatter.DatabaseType;
import com.example.fhir.connect.smt.formatter.OutputFormatter;
import com.example.fhir.connect.smt.formatter.OutputFormatterFactory;
import com.example.fhir.connect.smt.mapping.MappingProvider;
import com.example.fhir.connect.smt.observability.SmtHealthMBean;
import com.example.fhir.connect.smt.observability.SmtMetrics;
import com.example.fhir.connect.smt.observability.TransformTracer;
import com.example.fhir.connect.smt.transform.TransformEngine;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class DynamicJsonExtractSMT<R extends ConnectRecord<R>> implements Transformation<R> {
  private static final Logger log = LoggerFactory.getLogger(DynamicJsonExtractSMT.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private SmtConfig cfg;
  private AtomicReference<MappingProvider> mappingProviderRef;
  private AtomicReference<TransformEngine> engineRef;
  private OutputFormatter formatter;
  private S3ConfigLoader s3ConfigLoader;
  private HotReloadManager hotReloadManager;

  // Observability
  private SmtMetrics metrics;
  private SmtHealthMBean healthMBean;
  private TransformTracer tracer;
  private boolean metricsEnabled;

  @Override
  public void configure(Map<String, ?> configs) {
    try {
      this.cfg = SmtConfig.load(configs);
      this.mappingProviderRef = new AtomicReference<>();
      this.engineRef = new AtomicReference<>();

      // --- Observability init ---
      this.metricsEnabled = cfg.metricsEnabled();
      this.tracer = new TransformTracer(cfg.traceEnabled());

      if (metricsEnabled) {
        this.metrics = SmtMetrics.getInstance();
        metrics.register();

        this.healthMBean = SmtHealthMBean.getInstance();
        healthMBean.register();
      }

      // Load mappings from S3 or classpath
      MappingProvider mappingProvider;
      if (cfg.isS3MappingSource()) {
        log.info("Loading mappings from S3: endpoint={}, bucket={}, key={}",
            cfg.s3Endpoint(), cfg.s3Bucket(), cfg.s3Key());

        this.s3ConfigLoader = new S3ConfigLoader(
            cfg.s3Endpoint(),
            cfg.s3Bucket(),
            cfg.s3Key(),
            cfg.s3AccessKey(),
            cfg.s3SecretKey(),
            cfg.s3Region());

        String jsonConfig = s3ConfigLoader.loadConfig();
        mappingProvider = MappingProvider.fromJsonString(jsonConfig);

        // Enable hot reload if configured
        if (cfg.hotReloadEnabled()) {
          this.hotReloadManager = new HotReloadManager(
              s3ConfigLoader,
              mappingProvider,
              newProvider -> {
                mappingProviderRef.set(newProvider);
                engineRef.set(new TransformEngine(newProvider, tracer));
                log.info("Hot-reloaded mappings from S3");
              },
              cfg.hotReloadIntervalSeconds());
          hotReloadManager.start();
          log.info("Hot-reload enabled with {}s interval", cfg.hotReloadIntervalSeconds());
        }
      } else {
        // Load from classpath (default)
        mappingProvider = new MappingProvider(cfg);
      }

      this.mappingProviderRef.set(mappingProvider);
      this.engineRef.set(new TransformEngine(mappingProvider, tracer));

      // Initialize formatter based on target database
      DatabaseType dbType = cfg.targetDatabase();
      this.formatter = OutputFormatterFactory.createFormatter(dbType);

      // Configure JDBC formatter options if applicable
      if (formatter instanceof AbstractJdbcFormatter jdbcFormatter) {
        jdbcFormatter.setFlattenNested(cfg.jdbcFlattenNested());
        jdbcFormatter.setColumnSeparator(cfg.jdbcColumnSeparator());
        jdbcFormatter.setDefaultArrayMode(cfg.jdbcArrayMode());
      }

      // Update health MBean
      if (healthMBean != null) {
        healthMBean.setMappingSource(cfg.mappingSource());
        healthMBean.setTargetDatabase(dbType.name());
        healthMBean.setConnectorNames(mappingProvider.getConnectorNames());
        if (hotReloadManager != null) {
          healthMBean.setForceReloadAction(() -> hotReloadManager.forceReload());
        }
      }

      log.info("Configured SMT. targetDatabase={}, mappingSource={}, failOnMissingMapping={}, trace={}, metrics={}",
          dbType, cfg.mappingSource(), cfg.failOnMissingMapping(), cfg.traceEnabled(), metricsEnabled);
    } catch (Exception e) {
      throw new DataException("Failed to configure DynamicJsonExtractSMT", e);
    }
  }

  @Override
  public R apply(R record) {
    if (record == null || record.value() == null)
      return record;

    // Set MDC context for structured logging
    MDC.put("connector", cfg.connectorName());
    MDC.put("topic", record.topic());
    if (record.kafkaPartition() != null) {
      MDC.put("partition", String.valueOf(record.kafkaPartition()));
    }

    long startNanos = System.nanoTime();

    try {
      byte[] bytes = toBytes(record.value());
      if (bytes == null)
        return record;

      // Start tracing
      if (tracer.isEnabled()) {
        tracer.traceStart(
            record.topic(),
            record.kafkaPartition() != null ? record.kafkaPartition() : -1,
            record.timestamp() != null ? record.timestamp() : -1L);
      }

      JsonNode input = MAPPER.readTree(bytes);

      if (tracer.isEnabled()) {
        tracer.traceInput(input);
      }

      // Skip root wrapper for JDBC/relational targets (non-MongoDB)
      boolean skipRootWrapper = cfg.targetDatabase() != DatabaseType.MONGODB;
      JsonNode transformedJson = engineRef.get().transform(record.topic(), input, cfg, record, skipRootWrapper);
      if (transformedJson == null) {
        // no mapping => keep or skip? we keep original as per cfg flag; engine returns
        // null for "skip"
        if (metricsEnabled) {
          metrics.recordSkipped();
        }
        if (tracer.isEnabled()) {
          tracer.traceSkip("No mapping found for topic=" + record.topic());
        }
        return record;
      }

      // Use formatter to produce output (with or without schema)
      Object formattedOutput = formatter.format(transformedJson, null);
      Schema outputSchema = formatter.buildSchema(null, transformedJson);

      long latencyNanos = System.nanoTime() - startNanos;

      // Record success metrics
      if (metricsEnabled) {
        metrics.recordSuccess(latencyNanos, bytes.length);
        healthMBean.recordProcessingEvent(false);
      }

      // Trace output
      if (tracer.isEnabled()) {
        tracer.traceOutput(transformedJson, latencyNanos);
      }

      return record.newRecord(
          record.topic(),
          record.kafkaPartition(),
          record.keySchema(),
          record.key(),
          outputSchema,
          formattedOutput,
          record.timestamp());
    } catch (com.fasterxml.jackson.core.JsonProcessingException jpe) {
      // invalid JSON => DataException (so DLQ can handle)
      if (metricsEnabled) {
        metrics.recordFailure();
        healthMBean.recordProcessingEvent(true);
      }
      if (tracer.isEnabled()) {
        tracer.traceError("Invalid JSON payload", jpe);
      }
      throw new DataException("Invalid JSON payload for topic=" + record.topic(), jpe);
    } catch (Exception e) {
      if (metricsEnabled) {
        metrics.recordFailure();
        healthMBean.recordProcessingEvent(true);
      }
      if (tracer.isEnabled()) {
        tracer.traceError("Transform failed", e);
      }
      throw new DataException("Failed to transform record topic=" + record.topic(), e);
    } finally {
      MDC.clear();
    }
  }

  @Override
  public org.apache.kafka.common.config.ConfigDef config() {
    return SmtConfig.configDef();
  }

  @Override
  public void close() {
    if (hotReloadManager != null) {
      hotReloadManager.close();
    }
    if (s3ConfigLoader != null) {
      s3ConfigLoader.close();
    }
    // Unregister JMX MBeans
    if (metrics != null) {
      metrics.unregister();
    }
    if (healthMBean != null) {
      healthMBean.unregister();
    }
  }

  private byte[] toBytes(Object value) {
    try {
      if (value instanceof byte[] b)
        return b;
      if (value instanceof String s)
        return s.getBytes(StandardCharsets.UTF_8);
      if (value instanceof ByteBuffer bb) {
        if (bb.hasArray())
          return bb.array();
        byte[] out = new byte[bb.remaining()];
        bb.get(out);
        return out;
      }
      if (value instanceof Map<?, ?> m)
        return MAPPER.writeValueAsBytes(m);
      if (value instanceof JsonNode j)
        return MAPPER.writeValueAsBytes(j);

      // Struct support without depending on specific schemas: use Jackson conversion
      // via Connect's toString is not safe.
      // We'll attempt reflection-free handling: if it's Struct, we convert by
      // extracting fields.
      if (value instanceof org.apache.kafka.connect.data.Struct s) {
        LinkedHashMap<String, Object> out = new LinkedHashMap<>();
        s.schema().fields().forEach(f -> out.put(f.name(), s.get(f)));
        return MAPPER.writeValueAsBytes(out);
      }

      throw new DataException("Unsupported record value type: " + value.getClass().getName());
    } catch (Exception e) {
      throw new DataException("Failed to convert record value to bytes. Type=" + value.getClass().getName(), e);
    }
  }
}
