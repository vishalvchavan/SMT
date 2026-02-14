package com.example.fhir.connect.smt.formatter;

/**
 * RDF/semantic graph output formatter.
 * Emits schemaless JSON maps for connector-side RDF mapping/serialization.
 */
public class RdfOutputFormatter extends SchemalessJsonOutputFormatter {

    @Override
    public DatabaseType getDatabaseType() {
        return DatabaseType.RDF;
    }
}
