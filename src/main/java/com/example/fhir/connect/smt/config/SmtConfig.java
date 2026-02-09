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
            "Target database type: mongodb, mysql, postgresql, sqlserver, oracle, snowflake, delta_lake")
        .define("jdbc.flatten.nested", ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.MEDIUM,
            "Flatten nested objects to columns (for JDBC sinks)")
        .define("jdbc.array.mode", ConfigDef.Type.STRING, "json_string", ConfigDef.Importance.MEDIUM,
            "How to handle arrays: json_string, first_only, native_array, explode")
        .define("jdbc.column.separator", ConfigDef.Type.STRING, "_", ConfigDef.Importance.LOW,
            "Separator for flattened column names (e.g., 'member_name' vs 'member.name')");
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
}
