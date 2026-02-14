package com.example.fhir.connect.smt.formatter;

/**
 * Vector database output formatter.
 * Emits schemaless JSON maps and preserves embedding arrays for connector-side
 * vector ingestion.
 */
public class VectorDbOutputFormatter extends SchemalessJsonOutputFormatter {

    @Override
    public DatabaseType getDatabaseType() {
        return DatabaseType.VECTORDB;
    }
}
