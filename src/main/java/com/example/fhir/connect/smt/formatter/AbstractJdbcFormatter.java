package com.example.fhir.connect.smt.formatter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.kafka.connect.data.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Abstract base class for JDBC-based database formatters.
 * Provides common functionality for flattening nested JSON, type conversion,
 * and schema generation.
 */
public abstract class AbstractJdbcFormatter implements OutputFormatter {

    private static final Logger log = LoggerFactory.getLogger(AbstractJdbcFormatter.class);
    protected static final ObjectMapper MAPPER = new ObjectMapper();

    // Configurable options
    protected boolean flattenNested = true;
    protected String columnSeparator = "_";
    protected OutputFormatter.ArrayMode defaultArrayMode = ArrayMode.JSON_STRING;

    @Override
    public Object format(JsonNode transformedJson, Map<String, SchemaHint> fieldSchemas) {
        if (transformedJson == null) {
            return null;
        }

        Map<String, SchemaHint> hints = fieldSchemas != null ? fieldSchemas : Map.of();
        Schema schema = buildSchema(hints, transformedJson);

        if (schema == null) {
            // Fallback to schemaless map
            return MAPPER.convertValue(transformedJson, LinkedHashMap.class);
        }

        return buildStruct(schema, transformedJson, hints, "");
    }

    @Override
    public Schema buildSchema(Map<String, SchemaHint> fieldSchemas, JsonNode transformedJson) {
        if (transformedJson == null || !transformedJson.isObject()) {
            return null;
        }

        SchemaBuilder builder = SchemaBuilder.struct().name(getSchemaName());
        Map<String, SchemaHint> hints = fieldSchemas != null ? fieldSchemas : Map.of();

        buildSchemaFields(builder, transformedJson, hints, "");

        return builder.build();
    }

    /**
     * Get the schema name for this database type.
     */
    protected String getSchemaName() {
        return "com.example.fhir.connect.smt." + getDatabaseType().getConfigValue();
    }

    /**
     * Recursively build schema fields from JSON structure.
     */
    protected void buildSchemaFields(SchemaBuilder builder, JsonNode node,
            Map<String, SchemaHint> hints, String prefix) {
        if (!node.isObject())
            return;

        Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();
            String fieldName = entry.getKey();
            JsonNode value = entry.getValue();
            String fullPath = prefix.isEmpty() ? fieldName : prefix + columnSeparator + fieldName;
            String flatName = flattenNested ? fullPath : fieldName;

            SchemaHint hint = hints.get(fullPath);
            if (hint == null)
                hint = hints.get(fieldName);

            if (value.isObject() && flattenNested) {
                // Recurse into nested object
                buildSchemaFields(builder, value, hints, fullPath);
            } else if (value.isArray()) {
                // Handle array based on mode
                ArrayMode mode = hint != null ? hint.arrayMode() : defaultArrayMode;
                builder.field(flatName, getArraySchema(value, mode, hint));
            } else {
                // Scalar value
                builder.field(flatName, inferSchema(value, hint));
            }
        }
    }

    /**
     * Infer a Connect Schema from a JSON value and optional hint.
     */
    protected Schema inferSchema(JsonNode value, SchemaHint hint) {
        if (hint != null && hint.type() != null) {
            return schemaFromHint(hint);
        }

        if (value.isNull()) {
            return Schema.OPTIONAL_STRING_SCHEMA;
        } else if (value.isBoolean()) {
            return getBooleanSchema();
        } else if (value.isInt()) {
            return Schema.OPTIONAL_INT32_SCHEMA;
        } else if (value.isLong()) {
            return Schema.OPTIONAL_INT64_SCHEMA;
        } else if (value.isDouble() || value.isFloat()) {
            return Schema.OPTIONAL_FLOAT64_SCHEMA;
        } else if (value.isBigDecimal()) {
            return getDecimalSchema(null, null);
        } else {
            // Default to string for text and unknown types
            return Schema.OPTIONAL_STRING_SCHEMA;
        }
    }

    /**
     * Create schema from explicit hint.
     */
    protected Schema schemaFromHint(SchemaHint hint) {
        return switch (hint.type().toUpperCase()) {
            case "STRING" -> Schema.OPTIONAL_STRING_SCHEMA;
            case "INT32", "INTEGER", "INT" -> Schema.OPTIONAL_INT32_SCHEMA;
            case "INT64", "LONG", "BIGINT" -> Schema.OPTIONAL_INT64_SCHEMA;
            case "FLOAT32", "FLOAT" -> Schema.OPTIONAL_FLOAT32_SCHEMA;
            case "FLOAT64", "DOUBLE" -> Schema.OPTIONAL_FLOAT64_SCHEMA;
            case "BOOLEAN", "BOOL" -> getBooleanSchema();
            case "TIMESTAMP", "DATETIME" -> getTimestampSchema();
            case "DATE" -> getDateSchema();
            case "DECIMAL", "NUMERIC" -> getDecimalSchema(hint.precision(), hint.scale());
            case "BYTES", "BINARY", "BLOB" -> Schema.OPTIONAL_BYTES_SCHEMA;
            default -> Schema.OPTIONAL_STRING_SCHEMA;
        };
    }

    /**
     * Get the schema for array fields based on mode.
     */
    protected Schema getArraySchema(JsonNode array, ArrayMode mode, SchemaHint hint) {
        return switch (mode) {
            case JSON_STRING -> Schema.OPTIONAL_STRING_SCHEMA; // Serialized as JSON
            case FIRST_ONLY -> {
                // Use the type of the first element
                if (array.isEmpty())
                    yield Schema.OPTIONAL_STRING_SCHEMA;
                yield inferSchema(array.get(0), hint);
            }
            case NATIVE_ARRAY -> {
                // Native array support (subclass override for PostgreSQL, Snowflake)
                if (array.isEmpty())
                    yield SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build();
                Schema elementSchema = inferSchema(array.get(0), null);
                yield SchemaBuilder.array(elementSchema).optional().build();
            }
            case EXPLODE -> Schema.OPTIONAL_STRING_SCHEMA; // Handled at sink level
        };
    }

    /**
     * Build a Struct from JSON using the provided schema.
     */
    protected Struct buildStruct(Schema schema, JsonNode node,
            Map<String, SchemaHint> hints, String prefix) {
        Struct struct = new Struct(schema);

        Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();
            String fieldName = entry.getKey();
            JsonNode value = entry.getValue();
            String fullPath = prefix.isEmpty() ? fieldName : prefix + columnSeparator + fieldName;
            String flatName = flattenNested ? fullPath : fieldName;

            SchemaHint hint = hints.get(fullPath);
            if (hint == null)
                hint = hints.get(fieldName);

            if (value.isObject() && flattenNested) {
                // Flatten nested object - fields are already in parent struct
                populateNestedFields(struct, schema, value, hints, fullPath);
            } else if (schema.field(flatName) != null) {
                Object converted = convertValue(value, schema.field(flatName).schema(), hint);
                struct.put(flatName, converted);
            }
        }

        return struct;
    }

    /**
     * Populate fields from a nested object into the flat struct.
     */
    protected void populateNestedFields(Struct struct, Schema schema, JsonNode node,
            Map<String, SchemaHint> hints, String prefix) {
        Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();
            String fieldName = entry.getKey();
            JsonNode value = entry.getValue();
            String fullPath = prefix + columnSeparator + fieldName;

            if (value.isObject() && flattenNested) {
                populateNestedFields(struct, schema, value, hints, fullPath);
            } else if (schema.field(fullPath) != null) {
                SchemaHint hint = hints.get(fullPath);
                Object converted = convertValue(value, schema.field(fullPath).schema(), hint);
                struct.put(fullPath, converted);
            }
        }
    }

    /**
     * Convert a JSON value to the expected schema type.
     */
    protected Object convertValue(JsonNode value, Schema schema, SchemaHint hint) {
        if (value == null || value.isNull()) {
            return null;
        }

        // Handle arrays
        if (value.isArray()) {
            return convertArray((ArrayNode) value, schema, hint);
        }

        Schema.Type type = schema.type();
        return switch (type) {
            case STRING -> value.asText();
            case INT32 -> value.asInt();
            case INT64 -> value.asLong();
            case FLOAT32 -> (float) value.asDouble();
            case FLOAT64 -> value.asDouble();
            case BOOLEAN -> convertBoolean(value);
            case BYTES -> {
                if (Decimal.LOGICAL_NAME.equals(schema.name())) {
                    yield convertToDecimal(value, schema);
                }
                yield value.asText().getBytes();
            }
            case ARRAY -> convertArray((ArrayNode) value, schema, hint);
            case STRUCT -> {
                if (value.isObject()) {
                    yield buildStruct(schema, value, hint != null ? Map.of() : Map.of(), "");
                }
                yield null;
            }
            default -> value.asText();
        };
    }

    /**
     * Convert array based on array mode.
     */
    protected Object convertArray(ArrayNode array, Schema schema, SchemaHint hint) {
        ArrayMode mode = hint != null ? hint.arrayMode() : defaultArrayMode;

        return switch (mode) {
            case JSON_STRING -> {
                try {
                    yield MAPPER.writeValueAsString(array);
                } catch (JsonProcessingException e) {
                    log.warn("Failed to serialize array to JSON", e);
                    yield "[]";
                }
            }
            case FIRST_ONLY -> {
                if (array.isEmpty())
                    yield null;
                yield convertValue(array.get(0), schema, hint);
            }
            case NATIVE_ARRAY -> {
                List<Object> list = new ArrayList<>();
                Schema elementSchema = schema.valueSchema();
                for (JsonNode element : array) {
                    list.add(convertValue(element, elementSchema, null));
                }
                yield list;
            }
            case EXPLODE -> {
                // Return JSON for explode mode - handled downstream
                try {
                    yield MAPPER.writeValueAsString(array);
                } catch (JsonProcessingException e) {
                    yield "[]";
                }
            }
        };
    }

    /**
     * Convert value to boolean - database specific.
     */
    protected Object convertBoolean(JsonNode value) {
        return value.asBoolean();
    }

    /**
     * Convert to BigDecimal for DECIMAL schema.
     */
    protected BigDecimal convertToDecimal(JsonNode value, Schema schema) {
        if (value.isNumber()) {
            return value.decimalValue();
        }
        try {
            return new BigDecimal(value.asText());
        } catch (NumberFormatException e) {
            log.warn("Failed to parse decimal: {}", value.asText());
            return BigDecimal.ZERO;
        }
    }

    /**
     * Parse timestamp from various formats.
     */
    protected Date parseTimestamp(JsonNode value) {
        if (value.isLong()) {
            return Date.from(Instant.ofEpochMilli(value.asLong()));
        }

        String text = value.asText();

        // Try ISO instant
        try {
            return Date.from(Instant.parse(text));
        } catch (DateTimeParseException ignored) {
        }

        // Try common formats
        for (String pattern : List.of(
                "yyyy-MM-dd'T'HH:mm:ss.SSSX",
                "yyyy-MM-dd'T'HH:mm:ssX",
                "yyyy-MM-dd'T'HH:mm:ss",
                "yyyy-MM-dd HH:mm:ss",
                "yyyy-MM-dd")) {
            try {
                DateTimeFormatter fmt = DateTimeFormatter.ofPattern(pattern)
                        .withZone(ZoneId.of("UTC"));
                if (pattern.equals("yyyy-MM-dd")) {
                    LocalDate date = LocalDate.parse(text, fmt);
                    return Date.from(date.atStartOfDay(ZoneId.of("UTC")).toInstant());
                }
                return Date.from(Instant.from(fmt.parse(text)));
            } catch (DateTimeParseException ignored) {
            }
        }

        log.warn("Failed to parse timestamp: {}", text);
        return null;
    }

    // ===== Abstract methods for database-specific behavior =====

    /**
     * Get the boolean schema for this database type.
     * Override for databases that use INT/NUMBER for booleans.
     */
    protected abstract Schema getBooleanSchema();

    /**
     * Get the timestamp schema for this database type.
     */
    protected abstract Schema getTimestampSchema();

    /**
     * Get the date schema (date only, no time).
     */
    protected abstract Schema getDateSchema();

    /**
     * Get the decimal schema for this database type.
     */
    protected abstract Schema getDecimalSchema(Integer precision, Integer scale);

    // ===== Configuration setters =====

    public void setFlattenNested(boolean flattenNested) {
        this.flattenNested = flattenNested;
    }

    public void setColumnSeparator(String columnSeparator) {
        this.columnSeparator = columnSeparator;
    }

    public void setDefaultArrayMode(ArrayMode defaultArrayMode) {
        this.defaultArrayMode = defaultArrayMode;
    }
}
