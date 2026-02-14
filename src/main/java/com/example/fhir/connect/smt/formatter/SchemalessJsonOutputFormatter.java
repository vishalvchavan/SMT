package com.example.fhir.connect.smt.formatter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.data.Schema;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Base formatter for connectors that consume schemaless JSON objects.
 */
abstract class SchemalessJsonOutputFormatter implements OutputFormatter {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public Object format(JsonNode transformedJson, Map<String, SchemaHint> fieldSchemas) {
        if (transformedJson == null) {
            return null;
        }
        return MAPPER.convertValue(transformedJson, LinkedHashMap.class);
    }

    @Override
    public Schema buildSchema(Map<String, SchemaHint> fieldSchemas, JsonNode transformedJson) {
        return null;
    }
}
