package com.example.fhir.connect.smt.formatter;

/**
 * Ledger/immutable database output formatter.
 * Emits schemaless JSON maps for connector-side immutable entry mapping.
 */
public class LedgerOutputFormatter extends SchemalessJsonOutputFormatter {

    @Override
    public DatabaseType getDatabaseType() {
        return DatabaseType.LEDGER;
    }
}
