package com.example.fhir.connect.smt.formatter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Timestamp;

import java.util.ArrayList;
import java.util.List;

/**
 * PostgreSQL-specific output formatter.
 * 
 * Type mappings:
 * - Boolean → Native BOOLEAN
 * - Timestamp → Timestamp logical type (TIMESTAMPTZ)
 * - Decimal → Decimal logical type (NUMERIC)
 * - Arrays → Native ARRAY type (JSONB as fallback)
 */
public class PostgresOutputFormatter extends AbstractJdbcFormatter {

    public PostgresOutputFormatter() {
        // PostgreSQL supports native arrays
        this.defaultArrayMode = ArrayMode.NATIVE_ARRAY;
    }

    @Override
    public DatabaseType getDatabaseType() {
        return DatabaseType.POSTGRESQL;
    }

    @Override
    protected Schema getBooleanSchema() {
        // PostgreSQL has native BOOLEAN
        return Schema.OPTIONAL_BOOLEAN_SCHEMA;
    }

    @Override
    protected Schema getTimestampSchema() {
        // PostgreSQL TIMESTAMPTZ
        return Timestamp.builder().optional().build();
    }

    @Override
    protected Schema getDateSchema() {
        // PostgreSQL DATE
        return org.apache.kafka.connect.data.Date.builder().optional().build();
    }

    @Override
    protected Schema getDecimalSchema(Integer precision, Integer scale) {
        // PostgreSQL NUMERIC(p,s)
        int p = precision != null ? precision : 38;
        int s = scale != null ? scale : 10;
        return Decimal.builder(s).optional()
                .parameter("precision", String.valueOf(p))
                .build();
    }

    @Override
    protected Schema getArraySchema(JsonNode array, ArrayMode mode, SchemaHint hint) {
        // PostgreSQL supports native arrays - use them when possible
        if (mode == ArrayMode.NATIVE_ARRAY || mode == ArrayMode.JSON_STRING) {
            if (array.isEmpty()) {
                return SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build();
            }

            JsonNode firstElement = array.get(0);
            if (firstElement.isObject()) {
                // Complex objects -> JSONB string
                return Schema.OPTIONAL_STRING_SCHEMA;
            }

            // Simple types -> native array
            Schema elementSchema = inferSchema(firstElement, null);
            return SchemaBuilder.array(elementSchema).optional().build();
        }

        return super.getArraySchema(array, mode, hint);
    }

    @Override
    protected Object convertArray(ArrayNode array, Schema schema, SchemaHint hint) {
        ArrayMode mode = hint != null ? hint.arrayMode() : defaultArrayMode;

        // Check if schema is native array
        if (schema.type() == Schema.Type.ARRAY && mode == ArrayMode.NATIVE_ARRAY) {
            List<Object> list = new ArrayList<>();
            Schema elementSchema = schema.valueSchema();
            for (JsonNode element : array) {
                if (element.isObject()) {
                    // Complex objects serialize to JSON
                    try {
                        list.add(MAPPER.writeValueAsString(element));
                    } catch (Exception e) {
                        list.add("{}");
                    }
                } else {
                    list.add(convertValue(element, elementSchema, null));
                }
            }
            return list;
        }

        return super.convertArray(array, schema, hint);
    }

    @Override
    protected String getSchemaName() {
        return "com.example.fhir.connect.smt.postgresql";
    }
}
