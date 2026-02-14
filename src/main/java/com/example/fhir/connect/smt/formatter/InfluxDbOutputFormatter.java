package com.example.fhir.connect.smt.formatter;

/**
 * InfluxDB output formatter.
 * Emits schemaless JSON maps for connector-side field/tag mapping.
 */
public class InfluxDbOutputFormatter extends SchemalessJsonOutputFormatter {

    @Override
    public DatabaseType getDatabaseType() {
        return DatabaseType.INFLUXDB;
    }
}
