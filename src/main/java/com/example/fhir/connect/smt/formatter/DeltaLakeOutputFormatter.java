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
 * Delta Lake (Databricks) output formatter.
 * 
 * Designed for use with Databricks Delta Lake connector.
 * 
 * Type mappings:
 * - Boolean → Native BOOLEAN
 * - Timestamp → Timestamp logical type (mapped to Spark TimestampType)
 * - Decimal → Decimal logical type (mapped to Spark DecimalType)
 * - Arrays → Native ARRAY (Spark ArrayType)
 * - Nested objects → STRUCT (Spark StructType) - preserves structure
 * 
 * Delta Lake excels at:
 * - Schema evolution (adding columns)
 * - ACID transactions
 * - Time travel queries
 * - Efficient Parquet storage
 */
public class DeltaLakeOutputFormatter extends AbstractJdbcFormatter {

    public DeltaLakeOutputFormatter() {
        // Delta Lake supports nested structures natively
        this.flattenNested = false;
        this.defaultArrayMode = ArrayMode.NATIVE_ARRAY;
    }

    @Override
    public DatabaseType getDatabaseType() {
        return DatabaseType.DELTA_LAKE;
    }

    @Override
    protected Schema getBooleanSchema() {
        // Delta Lake / Spark has native BOOLEAN
        return Schema.OPTIONAL_BOOLEAN_SCHEMA;
    }

    @Override
    protected Schema getTimestampSchema() {
        // Delta Lake TimestampType
        return Timestamp.builder().optional().build();
    }

    @Override
    protected Schema getDateSchema() {
        // Delta Lake DateType
        return org.apache.kafka.connect.data.Date.builder().optional().build();
    }

    @Override
    protected Schema getDecimalSchema(Integer precision, Integer scale) {
        // Delta Lake DecimalType(precision, scale)
        int p = precision != null ? precision : 38;
        int s = scale != null ? scale : 10;
        return Decimal.builder(s).optional()
                .parameter("precision", String.valueOf(p))
                .build();
    }

    @Override
    protected Schema getArraySchema(JsonNode array, ArrayMode mode, SchemaHint hint) {
        if (mode == ArrayMode.NATIVE_ARRAY) {
            if (array.isEmpty()) {
                return SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build();
            }

            JsonNode firstElement = array.get(0);
            if (firstElement.isObject()) {
                // Array of structs - build nested schema
                Schema elementSchema = buildNestedStructSchema(firstElement, hint);
                return SchemaBuilder.array(elementSchema).optional().build();
            }

            Schema elementSchema = inferSchema(firstElement, null);
            return SchemaBuilder.array(elementSchema).optional().build();
        }
        return super.getArraySchema(array, mode, hint);
    }

    /**
     * Build a nested struct schema from a JSON object.
     * Delta Lake supports deep nesting via Spark StructType.
     */
    protected Schema buildNestedStructSchema(JsonNode node, SchemaHint hint) {
        if (!node.isObject()) {
            return Schema.OPTIONAL_STRING_SCHEMA;
        }

        SchemaBuilder structBuilder = SchemaBuilder.struct();
        var fields = node.fields();

        while (fields.hasNext()) {
            var entry = fields.next();
            String fieldName = entry.getKey();
            JsonNode value = entry.getValue();

            if (value.isObject()) {
                structBuilder.field(fieldName, buildNestedStructSchema(value, null));
            } else if (value.isArray()) {
                structBuilder.field(fieldName, getArraySchema(value, defaultArrayMode, null));
            } else {
                structBuilder.field(fieldName, inferSchema(value, null));
            }
        }

        return structBuilder.optional().build();
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
                // Delta Lake: Build nested struct schema
                Schema nestedSchema = buildNestedStructSchema(value, hint);
                builder.field(fieldName, nestedSchema);
            } else if (value.isArray()) {
                builder.field(fieldName, getArraySchema(value,
                        hint != null ? hint.arrayMode() : defaultArrayMode, hint));
            } else {
                builder.field(fieldName, inferSchema(value, hint));
            }
        }
    }

    @Override
    protected Object convertArray(ArrayNode array, Schema schema, SchemaHint hint) {
        ArrayMode mode = hint != null ? hint.arrayMode() : defaultArrayMode;

        if (mode == ArrayMode.NATIVE_ARRAY && schema.type() == Schema.Type.ARRAY) {
            List<Object> list = new ArrayList<>();
            Schema elementSchema = schema.valueSchema();

            for (JsonNode element : array) {
                if (element.isObject() && elementSchema.type() == Schema.Type.STRUCT) {
                    // Convert to nested Struct
                    list.add(buildStruct(elementSchema, element, java.util.Map.of(), ""));
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
        return "com.example.fhir.connect.smt.delta_lake";
    }
}
