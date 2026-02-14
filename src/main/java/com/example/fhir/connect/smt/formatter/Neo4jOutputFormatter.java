package com.example.fhir.connect.smt.formatter;

/**
 * Neo4j output formatter.
 * Emits schemaless JSON maps for connector-side node/relationship mapping.
 */
public class Neo4jOutputFormatter extends SchemalessJsonOutputFormatter {

    @Override
    public DatabaseType getDatabaseType() {
        return DatabaseType.NEO4J;
    }
}
