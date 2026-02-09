package com.example.fhir.connect.smt;

import com.example.fhir.connect.smt.config.HotReloadManager;
import com.example.fhir.connect.smt.config.S3ConfigLoader;
import com.example.fhir.connect.smt.config.SmtConfig;
import com.example.fhir.connect.smt.formatter.AbstractJdbcFormatter;
import com.example.fhir.connect.smt.formatter.DatabaseType;
import com.example.fhir.connect.smt.formatter.OutputFormatter;
import com.example.fhir.connect.smt.formatter.OutputFormatterFactory;
import com.example.fhir.connect.smt.mapping.MappingProvider;
import com.example.fhir.connect.smt.transform.TransformEngine;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  @Override
  public void configure(Map<String, ?> configs) {
    try {
      this.cfg = SmtConfig.load(configs);
      this.mappingProviderRef = new AtomicReference<>();
      this.engineRef = new AtomicReference<>();

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
                engineRef.set(new TransformEngine(newProvider));
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
      this.engineRef.set(new TransformEngine(mappingProvider));

      // Initialize formatter based on target database
      DatabaseType dbType = cfg.targetDatabase();
      this.formatter = OutputFormatterFactory.createFormatter(dbType);

      // Configure JDBC formatter options if applicable
      if (formatter instanceof AbstractJdbcFormatter jdbcFormatter) {
        jdbcFormatter.setFlattenNested(cfg.jdbcFlattenNested());
        jdbcFormatter.setColumnSeparator(cfg.jdbcColumnSeparator());
        jdbcFormatter.setDefaultArrayMode(cfg.jdbcArrayMode());
      }

      log.info("Configured SMT. targetDatabase={}, mappingSource={}, failOnMissingMapping={}",
          dbType, cfg.mappingSource(), cfg.failOnMissingMapping());
    } catch (Exception e) {
      throw new DataException("Failed to configure DynamicJsonExtractSMT", e);
    }
  }

  @Override
  public R apply(R record) {
    if (record == null || record.value() == null)
      return record;

    try {
      byte[] bytes = toBytes(record.value());
      if (bytes == null)
        return record;

      JsonNode input = MAPPER.readTree(bytes);

      // Skip root wrapper for JDBC/relational targets (non-MongoDB)
      boolean skipRootWrapper = cfg.targetDatabase() != DatabaseType.MONGODB;
      JsonNode transformedJson = engineRef.get().transform(record.topic(), input, cfg, record, skipRootWrapper);
      if (transformedJson == null) {
        // no mapping => keep or skip? we keep original as per cfg flag; engine returns
        // null for "skip"
        return record;
      }

      // Use formatter to produce output (with or without schema)
      Object formattedOutput = formatter.format(transformedJson, null);
      Schema outputSchema = formatter.buildSchema(null, transformedJson);

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
      throw new DataException("Invalid JSON payload for topic=" + record.topic(), jpe);
    } catch (Exception e) {
      throw new DataException("Failed to transform record topic=" + record.topic(), e);
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
