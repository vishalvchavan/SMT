package com.example.fhir.connect.smt.formatter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.data.Schema;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * MongoDB output formatter - maintains backward compatibility with the original
 * SMT behavior.
 * Outputs schemaless LinkedHashMap suitable for MongoDB sink connector.
 */
public class MongoOutputFormatter implements OutputFormatter {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public DatabaseType getDatabaseType() {
        return DatabaseType.MONGODB;
    }

    @Override
    public Object format(JsonNode transformedJson, Map<String, SchemaHint> fieldSchemas) {
        if (transformedJson == null) {
            return null;
        }
        // Convert JsonNode to LinkedHashMap - this is the original behavior
        return MAPPER.convertValue(transformedJson, LinkedHashMap.class);
    }

    @Override
    public Schema buildSchema(Map<String, SchemaHint> fieldSchemas, JsonNode transformedJson) {
        // MongoDB sink is schemaless - no schema needed
        return null;
    }
}
