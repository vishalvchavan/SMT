package com.example.fhir.connect.smt.formatter;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.connect.data.Schema;

import java.util.Map;

/**
 * Interface for database-specific output formatters.
 * Each formatter converts the intermediate JSON representation from
 * TransformEngine
 * into a format suitable for the target database sink.
 */
public interface OutputFormatter {

    /**
     * Get the database type this formatter handles.
     */
    DatabaseType getDatabaseType();

    /**
     * Format the transformed JSON into an output suitable for the target sink.
     *
     * @param transformedJson The JSON output from TransformEngine
     * @param fieldSchemas    Optional schema hints from the mapping configuration
     * @return Formatted output (Map for schemaless, Struct for schema-aware sinks)
     */
    Object format(JsonNode transformedJson, Map<String, SchemaHint> fieldSchemas);

    /**
     * Generate a Connect Schema for the output, if required by the sink.
     * Returns null for schemaless sinks (MongoDB).
     *
     * @param fieldSchemas    Optional schema hints from the mapping configuration
     * @param transformedJson Sample JSON to infer schema from (when hints are
     *                        missing)
     * @return Connect Schema or null
     */
    Schema buildSchema(Map<String, SchemaHint> fieldSchemas, JsonNode transformedJson);

    /**
     * Schema hint from the mapping configuration.
     * Used to override auto-inferred types for JDBC sinks.
     */
    record SchemaHint(
            String type, // STRING, INT32, INT64, FLOAT64, BOOLEAN, TIMESTAMP, DECIMAL, BYTES
            String sqlType, // Database-specific type override (e.g., "VARCHAR(50)")
            boolean primaryKey, // Is this a primary key column?
            Integer precision, // For DECIMAL types
            Integer scale, // For DECIMAL types
            String dateFormat, // Custom date format for parsing
            ArrayMode arrayMode // How to handle array fields
    ) {
        public SchemaHint {
            // Defaults
            if (type == null)
                type = "STRING";
            if (arrayMode == null)
                arrayMode = ArrayMode.JSON_STRING;
        }

        public static SchemaHint defaultHint() {
            return new SchemaHint("STRING", null, false, null, null, null, ArrayMode.JSON_STRING);
        }
    }

    /**
     * How to handle array fields in JDBC sinks.
     */
    enum ArrayMode {
        /** Serialize array to JSON string (default) */
        JSON_STRING,
        /** Take only the first element */
        FIRST_ONLY,
        /** Explode array into multiple rows (requires special sink handling) */
        EXPLODE,
        /** Use native array type if supported (PostgreSQL, Snowflake) */
        NATIVE_ARRAY
    }
}
