# App 3 — Kafka Connect SMT (Single Message Transform)

## What it does
- Reads input value as **bytes[]** (supports byte[], String, ByteBuffer, Map, Struct, JsonNode)
- Parses JSON
- Applies **connector-based mapping** from classpath `mappings/topic-mappings.json` (inside JAR)
- Outputs to **multiple database types** with appropriate formatting:
  - **MongoDB**: Schemaless `LinkedHashMap`
  - **MySQL, PostgreSQL, SQL Server, Oracle**: Schema-aware `Struct` with flattened columns
  - **Snowflake**: VARIANT-compatible JSON strings for semi-structured data
  - **Delta Lake**: Spark-compatible nested `Struct` types

## Multi-Database Support

The SMT supports 10 target database types. Configure via `target.database`:

| Database | Config Value | Key Features |
|----------|--------------|--------------|
| MongoDB | `mongodb` (default) | Schemaless, nested JSON preserved |
| MySQL | `mysql` | TINYINT(1) booleans, JSON arrays |
| PostgreSQL | `postgresql` | Native BOOLEAN, JSONB, arrays |
| SQL Server | `sqlserver` | BIT booleans, NVARCHAR(MAX) JSON |
| Oracle | `oracle` | NUMBER(1) booleans, CLOB JSON |
| Snowflake | `snowflake` | VARIANT for nested/arrays |
| Delta Lake | `delta_lake` | Spark StructType, ArrayType |
| Neo4j | `neo4j` | Schemaless JSON maps for graph mapping |
| InfluxDB | `influxdb` | Schemaless JSON maps for time-series field/tag mapping |
| Vector DB | `vectordb` | Schemaless JSON maps preserving embedding arrays |

## Connector-Based Mapping (v2)
Mappings are keyed by **connector name** instead of topic name. This allows:
- Different connectors reading the same topic to apply different transformations
- More flexible deployment scenarios

### Mapping File Structure
```json
{
  "version": 2,
  "connectors": {
    "my-mongo-sink-connector": {
      "root": "documentRoot",
      "output": { ... }
    }
  }
}
```

### Backward Compatibility
You can still use topic-based mappings by using the `topics` key instead of `connectors`. 
If `connector.name` is not configured, the SMT falls back to topic-based lookup.

## Defaults inside JAR
- `src/main/resources/smt-defaults.properties`
- `src/main/resources/mappings/topic-mappings.json`

## Connector override precedence
1) `transforms.<name>.*` from connector config
2) classpath `smt-defaults.properties`
3) hard defaults

## Install
Copy the built JAR to a Connect plugin directory (under `plugin.path`) and restart Connect.

Build:
```bash
mvn clean package
```

Output (shaded):
- `target/connect-smt-1.0.0-shaded.jar`

## Example: MongoDB Sink (default)
```properties
name=mongo-assessment-sink
connector.class=com.mongodb.kafka.connect.MongoSinkConnector
topics=fhir-assessments

transforms=Extract
transforms.Extract.type=com.example.fhir.connect.smt.DynamicJsonExtractSMT
transforms.Extract.connector.name=mongo-assessment-sink
```

## Example: PostgreSQL JDBC Sink
```properties
name=postgres-claims-sink
connector.class=io.confluent.connect.jdbc.JdbcSinkConnector
topics=claims-events

transforms=Extract
transforms.Extract.type=com.example.fhir.connect.smt.DynamicJsonExtractSMT
transforms.Extract.connector.name=postgres-claims-sink
transforms.Extract.target.database=postgresql
transforms.Extract.jdbc.flatten.nested=true
transforms.Extract.jdbc.array.mode=native_array
```

## Example: Snowflake Sink
```properties
name=snowflake-events-sink
connector.class=com.snowflake.kafka.connector.SnowflakeSinkConnector
topics=events

transforms=Extract
transforms.Extract.type=com.example.fhir.connect.smt.DynamicJsonExtractSMT
transforms.Extract.connector.name=snowflake-events-sink
transforms.Extract.target.database=snowflake
```

## Configuration Options

| Property | Default | Description |
|----------|---------|-------------|
| `connector.name` | `""` | **Required**: Connector name for mapping lookup |
| `target.database` | `mongodb` | Target database: mongodb, mysql, postgresql, sqlserver, oracle, snowflake, delta_lake, neo4j, influxdb, vectordb |
| `mapping.classpathRoot` | `mappings/topic-mappings.json` | Path to mapping config inside JAR |
| `app.failOnMissingMapping` | `false` | Log error (vs warn) if no mapping found |
| `app.attachKafkaMetadata` | `true` | Add `_kafka: {topic, partition}` to output |
| `app.storeRawPayload` | `false` | Include original JSON as `_raw` |
| `app.trace.enabled` | `false` | Enable TRACE-level transform tracing per record |
| `app.metrics.enabled` | `true` | Enable JMX metrics registration |
| `jdbc.flatten.nested` | `true` | Flatten nested objects to columns (JDBC only) |
| `jdbc.array.mode` | `json_string` | Array handling: json_string, first_only, native_array, explode |
| `jdbc.column.separator` | `_` | Separator for flattened columns (e.g., `member_name`) |

## Observability

### JMX Metrics

When `app.metrics.enabled=true` (default), the SMT registers two JMX MBeans:

**`connect.smt:type=DynamicJsonExtract`** — Transform metrics:

| Attribute | Type | Description |
|-----------|------|-------------|
| `RecordsProcessed` | Counter | Successful transforms |
| `RecordsFailed` | Counter | Failed transforms |
| `RecordsSkipped` | Counter | Records with no mapping (passthrough) |
| `AvgTransformLatencyMs` | Gauge | Average transform latency |
| `LastTransformLatencyMs` | Gauge | Most recent transform latency |
| `MaxTransformLatencyMs` | Gauge | Worst-case transform latency |
| `PayloadBytesTotal` | Counter | Total input bytes processed |
| `MappingLookupMisses` | Counter | Missing mapping lookups |
| `HotReloadSuccessCount` | Counter | Successful config reloads |
| `HotReloadFailureCount` | Counter | Failed config reloads |
| `EncryptCallsTotal` | Counter | Encrypt transform invocations |
| `MaskCallsTotal` | Counter | Mask transform invocations |
| `UptimeSeconds` | Gauge | Seconds since SMT configured |

JMX Operation: `resetMetrics()` — resets all counters to zero.

**`connect.smt:type=SmtHealth`** — Health status:

| Attribute | Type | Description |
|-----------|------|-------------|
| `Status` | String | `HEALTHY`, `DEGRADED`, or `UNHEALTHY` |
| `MappingSource` | String | `classpath` or `s3` |
| `TargetDatabase` | String | Target database type |
| `ConnectorNames` | String | Comma-separated connector names |
| `LastReloadTime` | String | ISO timestamp of last reload |
| `LastReloadStatus` | String | `SUCCESS`, `FAILED: <reason>`, or `N/A` |
| `ErrorRate5Min` | Double | Error rate in last 5-minute window |
| `UptimeSeconds` | Long | Seconds since startup |

JMX Operation: `forceReload()` — triggers immediate config reload from S3.

### Prometheus JMX Exporter

Example scrape config for `jmx_exporter`:
```yaml
rules:
  - pattern: 'connect.smt<type=DynamicJsonExtract><>(\w+)'
    name: smt_$1
    type: GAUGE
  - pattern: 'connect.smt<type=SmtHealth><>(\w+)'
    name: smt_health_$1
    type: GAUGE
```

### Transform Tracing

Enable per-record trace logging with `app.trace.enabled=true`. Logs at TRACE level:

```
TRACE [topic=fhir-assessments partition=3 offset=14502]
  Input: {"resourceType":"Patient","id":"123"...}
  Mapping lookup: connector=mongo-sink → FOUND (root=documentRoot)
  Field "patient_id" ← $.id = "123"
  Field "birth_date" ← $.birthDate = "1990-01-15"
    → dateFormat: "1990-01-15" → "01/15/1990"
  Field "ssn" ← $.identifier[?(@.system=='ssn')].value = "123-45-6789"
    → mask: "123-45-6789" → "***-**-6789"
  Output: {"patient_id":"123","birth_date":"01/15/1990",...}
  Completed in 3.2ms
```

### Structured Logging (MDC)

Every log line during `apply()` carries MDC context keys:

| MDC Key | Description |
|---------|-------------|
| `connector` | Connector name from config |
| `topic` | Kafka topic name |
| `partition` | Kafka partition number |

Use these in your logging pattern, e.g.:
```
%d [%X{connector}/%X{topic}/%X{partition}] %-5level %logger - %msg%n
```
