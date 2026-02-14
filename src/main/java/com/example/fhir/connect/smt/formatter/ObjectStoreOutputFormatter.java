package com.example.fhir.connect.smt.formatter;

/**
 * Object store output formatter.
 * Emits schemaless JSON maps for object/document persistence connectors.
 */
public class ObjectStoreOutputFormatter extends SchemalessJsonOutputFormatter {

    @Override
    public DatabaseType getDatabaseType() {
        return DatabaseType.OBJECT_STORE;
    }
}
