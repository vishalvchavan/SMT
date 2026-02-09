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

The SMT supports 7 target database types. Configure via `target.database`:

| Database | Config Value | Key Features |
|----------|--------------|--------------|
| MongoDB | `mongodb` (default) | Schemaless, nested JSON preserved |
| MySQL | `mysql` | TINYINT(1) booleans, JSON arrays |
| PostgreSQL | `postgresql` | Native BOOLEAN, JSONB, arrays |
| SQL Server | `sqlserver` | BIT booleans, NVARCHAR(MAX) JSON |
| Oracle | `oracle` | NUMBER(1) booleans, CLOB JSON |
| Snowflake | `snowflake` | VARIANT for nested/arrays |
| Delta Lake | `delta_lake` | Spark StructType, ArrayType |

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
| `target.database` | `mongodb` | Target database: mongodb, mysql, postgresql, sqlserver, oracle, snowflake, delta_lake |
| `mapping.classpathRoot` | `mappings/topic-mappings.json` | Path to mapping config inside JAR |
| `app.failOnMissingMapping` | `false` | Log error (vs warn) if no mapping found |
| `app.attachKafkaMetadata` | `true` | Add `_kafka: {topic, partition}` to output |
| `app.storeRawPayload` | `false` | Include original JSON as `_raw` |
| `jdbc.flatten.nested` | `true` | Flatten nested objects to columns (JDBC only) |
| `jdbc.array.mode` | `json_string` | Array handling: json_string, first_only, native_array, explode |
| `jdbc.column.separator` | `_` | Separator for flattened columns (e.g., `member_name`) |

