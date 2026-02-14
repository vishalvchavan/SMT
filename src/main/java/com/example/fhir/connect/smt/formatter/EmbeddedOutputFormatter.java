package com.example.fhir.connect.smt.formatter;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Timestamp;

/**
 * Embedded/edge relational output formatter.
 * Intended for JDBC-connectable embedded engines (e.g., SQLite/DuckDB).
 */
public class EmbeddedOutputFormatter extends AbstractJdbcFormatter {

    public EmbeddedOutputFormatter() {
        // Favor wide connector compatibility for embedded JDBC sinks.
        this.flattenNested = true;
        this.defaultArrayMode = ArrayMode.JSON_STRING;
    }

    @Override
    public DatabaseType getDatabaseType() {
        return DatabaseType.EMBEDDED;
    }

    @Override
    protected Schema getBooleanSchema() {
        return Schema.OPTIONAL_BOOLEAN_SCHEMA;
    }

    @Override
    protected Schema getTimestampSchema() {
        return Timestamp.builder().optional().build();
    }

    @Override
    protected Schema getDateSchema() {
        return org.apache.kafka.connect.data.Date.builder().optional().build();
    }

    @Override
    protected Schema getDecimalSchema(Integer precision, Integer scale) {
        int p = precision != null ? precision : 38;
        int s = scale != null ? scale : 10;
        return Decimal.builder(s).optional()
                .parameter("precision", String.valueOf(p))
                .build();
    }

    @Override
    protected String getSchemaName() {
        return "com.example.fhir.connect.smt.embedded";
    }
}
