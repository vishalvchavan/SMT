package com.example.fhir.connect.smt.config;

import com.example.fhir.connect.smt.formatter.DatabaseType;
import com.example.fhir.connect.smt.formatter.OutputFormatter.ArrayMode;
import org.apache.kafka.common.config.ConfigDef;

import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

public final class SmtConfig {
  private final Properties defaults;
  private final Map<String, ?> overrides;

  private SmtConfig(Properties defaults, Map<String, ?> overrides) {
    this.defaults = defaults;
    this.overrides = overrides;
  }

  public static SmtConfig load(Map<String, ?> connectorConfigs) throws Exception {
    Properties p = new Properties();
    try (InputStream in = SmtConfig.class.getClassLoader().getResourceAsStream("smt-defaults.properties")) {
      if (in != null)
        p.load(in);
    }
    return new SmtConfig(p, connectorConfigs);
  }

  public static ConfigDef configDef() {
    return new ConfigDef()
        .define("connector.name", ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH,
            "Connector name used to lookup mapping configuration")
        .define("mapping.classpathRoot", ConfigDef.Type.STRING, "mappings/topic-mappings.json",
            ConfigDef.Importance.MEDIUM, "Classpath mapping file inside SMT jar")
        .define("app.failOnMissingMapping", ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.MEDIUM,
            "If true, log error when mapping is missing")
        .define("app.attachKafkaMetadata", ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.LOW,
            "Attach _kafka metadata to output")
        .define("app.storeRawPayload", ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.LOW,
            "Attach _raw input to output")
        // Multi-database support options
        .define("target.database", ConfigDef.Type.STRING, "mongodb", ConfigDef.Importance.HIGH,
            "Target database type: mongodb, mysql, postgresql, sqlserver, oracle, snowflake, delta_lake, neo4j, influxdb, vectordb")
        .define("jdbc.flatten.nested", ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.MEDIUM,
            "Flatten nested objects to columns (for JDBC sinks)")
        .define("jdbc.array.mode", ConfigDef.Type.STRING, "json_string", ConfigDef.Importance.MEDIUM,
            "How to handle arrays: json_string, first_only, native_array, explode")
        .define("jdbc.column.separator", ConfigDef.Type.STRING, "_", ConfigDef.Importance.LOW,
            "Separator for flattened column names (e.g., 'member_name' vs 'member.name')")
        // S3/MinIO external config options
        .define("mapping.source", ConfigDef.Type.STRING, "classpath", ConfigDef.Importance.MEDIUM,
            "Mapping source: classpath or s3")
        .define("mapping.s3.endpoint", ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM,
            "S3/MinIO endpoint URL (e.g., http://minio:9000)")
        .define("mapping.s3.bucket", ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM,
            "S3 bucket name for mapping config")
        .define("mapping.s3.key", ConfigDef.Type.STRING, "mappings/topic-mappings.json", ConfigDef.Importance.MEDIUM,
            "S3 object key (path) for mapping file")
        .define("mapping.s3.accessKey", ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH,
            "S3/MinIO access key")
        .define("mapping.s3.secretKey", ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH,
            "S3/MinIO secret key")
        .define("mapping.s3.region", ConfigDef.Type.STRING, "us-east-1", ConfigDef.Importance.LOW,
            "AWS region (use us-east-1 for MinIO)")
        // Hot reload options
        .define("mapping.hotreload.enabled", ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.MEDIUM,
            "Enable hot-reload of mappings from S3")
        .define("mapping.hotreload.intervalSeconds", ConfigDef.Type.INT, 30, ConfigDef.Importance.LOW,
            "Hot-reload poll interval in seconds")
        // Observability options
        .define("app.trace.enabled", ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.LOW,
            "Enable TRACE-level transform tracing (logs full extract/transform pipeline per record)")
        .define("app.metrics.enabled", ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.LOW,
            "Enable JMX metrics registration for monitoring");
  }

  // Connector config keys are expected as transforms.<name>.<key>. We'll support
  // both direct keys and prefixed ones.
  private String get(String key, String def) {
    Object v = overrides.get(key);
    if (v == null) {
      // try to locate in prefixed keys
      for (Map.Entry<String, ?> e : overrides.entrySet()) {
        String k = e.getKey();
        if (k != null && k.endsWith("." + key)) {
          v = e.getValue();
          break;
        }
      }
    }
    if (v != null)
      return String.valueOf(v).trim();
    String d = defaults.getProperty(key);
    return d != null ? d.trim() : def;
  }

  private boolean getBool(String key, boolean def) {
    String v = get(key, null);
    if (v == null)
      return def;
    return Boolean.parseBoolean(v);
  }

  public String mappingClasspathRoot() {
    return get("mapping.classpathRoot", "mappings/topic-mappings.json");
  }

  public String connectorName() {
    return get("connector.name", "");
  }

  public boolean failOnMissingMapping() {
    return getBool("app.failOnMissingMapping", false);
  }

  public boolean attachKafkaMetadata() {
    return getBool("app.attachKafkaMetadata", true);
  }

  public boolean storeRawPayload() {
    return getBool("app.storeRawPayload", false);
  }

  public boolean traceEnabled() {
    return getBool("app.trace.enabled", false);
  }

  public boolean metricsEnabled() {
    return getBool("app.metrics.enabled", true);
  }

  // Multi-database config getters

  public DatabaseType targetDatabase() {
    String value = get("target.database", "mongodb");
    return DatabaseType.fromConfig(value);
  }

  public boolean jdbcFlattenNested() {
    return getBool("jdbc.flatten.nested", true);
  }

  public ArrayMode jdbcArrayMode() {
    String value = get("jdbc.array.mode", "json_string");
    return switch (value.toLowerCase()) {
      case "first_only" -> ArrayMode.FIRST_ONLY;
      case "native_array" -> ArrayMode.NATIVE_ARRAY;
      case "explode" -> ArrayMode.EXPLODE;
      default -> ArrayMode.JSON_STRING;
    };
  }

  public String jdbcColumnSeparator() {
    return get("jdbc.column.separator", "_");
  }

  // S3/MinIO config getters - support environment variables as fallbacks

  public String mappingSource() {
    String v = get("mapping.source", null);
    if (v != null && !v.isEmpty())
      return v;
    // Check if S3 env vars are set, auto-enable S3
    String endpoint = System.getenv("MAPPING_S3_ENDPOINT");
    if (endpoint != null && !endpoint.isEmpty())
      return "s3";
    return "classpath";
  }

  public boolean isS3MappingSource() {
    return "s3".equalsIgnoreCase(mappingSource());
  }

  public String s3Endpoint() {
    String v = get("mapping.s3.endpoint", null);
    if (v != null && !v.isEmpty())
      return v;
    return System.getenv("MAPPING_S3_ENDPOINT");
  }

  public String s3Bucket() {
    String v = get("mapping.s3.bucket", null);
    if (v != null && !v.isEmpty())
      return v;
    String env = System.getenv("MAPPING_S3_BUCKET");
    return env != null ? env : "";
  }

  public String s3Key() {
    String v = get("mapping.s3.key", null);
    if (v != null && !v.isEmpty())
      return v;
    String env = System.getenv("MAPPING_S3_KEY");
    return env != null ? env : "mappings/topic-mappings.json";
  }

  public String s3AccessKey() {
    String v = get("mapping.s3.accessKey", null);
    if (v != null && !v.isEmpty())
      return v;
    String env = System.getenv("MAPPING_S3_ACCESS_KEY");
    return env != null ? env : "";
  }

  public String s3SecretKey() {
    String v = get("mapping.s3.secretKey", null);
    if (v != null && !v.isEmpty())
      return v;
    String env = System.getenv("MAPPING_S3_SECRET_KEY");
    return env != null ? env : "";
  }

  public String s3Region() {
    String v = get("mapping.s3.region", null);
    if (v != null && !v.isEmpty())
      return v;
    String env = System.getenv("MAPPING_S3_REGION");
    return env != null ? env : "us-east-1";
  }

  // Hot reload config getters

  public boolean hotReloadEnabled() {
    String v = get("mapping.hotreload.enabled", null);
    if (v != null)
      return Boolean.parseBoolean(v);
    String env = System.getenv("MAPPING_HOTRELOAD_ENABLED");
    return env != null && Boolean.parseBoolean(env);
  }

  public int hotReloadIntervalSeconds() {
    String v = get("mapping.hotreload.intervalSeconds", null);
    if (v != null) {
      try {
        return Integer.parseInt(v);
      } catch (NumberFormatException e) {
      }
    }
    String env = System.getenv("MAPPING_HOTRELOAD_INTERVAL_SECONDS");
    if (env != null) {
      try {
        return Integer.parseInt(env);
      } catch (NumberFormatException e) {
      }
    }
    return 30;
  }
}
