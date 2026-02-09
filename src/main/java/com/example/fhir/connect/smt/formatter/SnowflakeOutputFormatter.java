package com.example.fhir.connect.smt.formatter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Timestamp;

/**
 * Snowflake-specific output formatter.
 * 
 * Type mappings:
 * - Boolean → Native BOOLEAN
 * - Timestamp → Timestamp logical type (TIMESTAMP_NTZ or TIMESTAMP_TZ)
 * - Decimal → Decimal logical type (NUMBER)
 * - Arrays → VARIANT type (supports semi-structured data natively)
 * - Nested objects → VARIANT (no flattening needed)
 * 
 * NOTE: Snowflake Kafka Connector expects raw JSON strings, not Connect
 * Structs.
 * This formatter returns the transformed JSON as a string, not a Struct.
 */
public class SnowflakeOutputFormatter extends AbstractJdbcFormatter {

    public SnowflakeOutputFormatter() {
        // Snowflake VARIANT supports nested structures - no need to flatten
        this.flattenNested = false;
        this.defaultArrayMode = ArrayMode.NATIVE_ARRAY;
    }

    /**
     * For Snowflake, return raw JSON string instead of Struct.
     * The Snowflake connector will parse this into VARIANT type.
     */
    @Override
    public Object format(JsonNode transformedJson, java.util.Map<String, SchemaHint> fieldSchemas) {
        if (transformedJson == null) {
            return null;
        }
        try {
            return MAPPER.writeValueAsString(transformedJson);
        } catch (Exception e) {
            return "{}";
        }
    }

    /**
     * For Snowflake, return STRING schema since we're outputting JSON string.
     */
    @Override
    public Schema buildSchema(java.util.Map<String, SchemaHint> fieldSchemas, JsonNode transformedJson) {
        return Schema.OPTIONAL_STRING_SCHEMA;
    }

    @Override
    public DatabaseType getDatabaseType() {
        return DatabaseType.SNOWFLAKE;
    }

    @Override
    protected Schema getBooleanSchema() {
        // Snowflake has native BOOLEAN
        return Schema.OPTIONAL_BOOLEAN_SCHEMA;
    }

    @Override
    protected Schema getTimestampSchema() {
        // Snowflake TIMESTAMP_NTZ (or TIMESTAMP_TZ)
        return Timestamp.builder().optional().build();
    }

    @Override
    protected Schema getDateSchema() {
        // Snowflake DATE
        return org.apache.kafka.connect.data.Date.builder().optional().build();
    }

    @Override
    protected Schema getDecimalSchema(Integer precision, Integer scale) {
        // Snowflake NUMBER(p,s) - supports up to 38 digits
        int p = precision != null ? precision : 38;
        int s = scale != null ? scale : 10;
        return Decimal.builder(s).optional()
                .parameter("precision", String.valueOf(p))
                .build();
    }

    @Override
    protected Schema getArraySchema(JsonNode array, ArrayMode mode, SchemaHint hint) {
        // Snowflake VARIANT can hold arrays and complex types natively
        // For the connector, we serialize as JSON string which Snowflake parses into
        // VARIANT
        if (mode == ArrayMode.NATIVE_ARRAY) {
            // Use JSON string representation - Snowflake connector can parse this to
            // VARIANT
            return Schema.OPTIONAL_STRING_SCHEMA;
        }
        return super.getArraySchema(array, mode, hint);
    }

    @Override
    protected Object convertArray(ArrayNode array, Schema schema, SchemaHint hint) {
        ArrayMode mode = hint != null ? hint.arrayMode() : defaultArrayMode;

        if (mode == ArrayMode.NATIVE_ARRAY) {
            // Serialize to JSON - Snowflake sink will handle VARIANT conversion
            try {
                return MAPPER.writeValueAsString(array);
            } catch (Exception e) {
                return "[]";
            }
        }

        return super.convertArray(array, schema, hint);
    }

    @Override
    protected void buildSchemaFields(SchemaBuilder builder, JsonNode node,
            java.util.Map<String, SchemaHint> hints, String prefix) {
        if (!node.isObject())
            return;

        var fields = node.fields();
        while (fields.hasNext()) {
            var entry = fields.next();
            String fieldName = entry.getKey();
            JsonNode value = entry.getValue();

            SchemaHint hint = hints.get(fieldName);

            if (value.isObject()) {
                // Snowflake: Keep nested objects as VARIANT (JSON string)
                builder.field(fieldName, Schema.OPTIONAL_STRING_SCHEMA);
            } else if (value.isArray()) {
                builder.field(fieldName, getArraySchema(value,
                        hint != null ? hint.arrayMode() : defaultArrayMode, hint));
            } else {
                builder.field(fieldName, inferSchema(value, hint));
            }
        }
    }

    @Override
    protected Object convertValue(JsonNode value, Schema schema, SchemaHint hint) {
        if (value == null || value.isNull()) {
            return null;
        }

        // Convert nested objects to JSON string for VARIANT
        if (value.isObject()) {
            try {
                return MAPPER.writeValueAsString(value);
            } catch (Exception e) {
                return "{}";
            }
        }

        return super.convertValue(value, schema, hint);
    }

    @Override
    protected String getSchemaName() {
        return "com.example.fhir.connect.smt.snowflake";
    }
}
