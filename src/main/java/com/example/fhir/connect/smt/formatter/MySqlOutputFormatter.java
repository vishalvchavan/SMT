package com.example.fhir.connect.smt.formatter;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Timestamp;

/**
 * MySQL-specific output formatter.
 * 
 * Type mappings:
 * - Boolean → INT8 (TINYINT(1) in MySQL)
 * - Timestamp → Timestamp logical type (DATETIME in MySQL)
 * - Decimal → Decimal logical type
 * - Arrays → JSON string (serialized)
 */
public class MySqlOutputFormatter extends AbstractJdbcFormatter {

    @Override
    public DatabaseType getDatabaseType() {
        return DatabaseType.MYSQL;
    }

    @Override
    protected Schema getBooleanSchema() {
        // MySQL uses TINYINT(1) for booleans - map to INT8
        return Schema.OPTIONAL_INT8_SCHEMA;
    }

    @Override
    protected Object convertBoolean(com.fasterxml.jackson.databind.JsonNode value) {
        // Convert boolean to 0/1 for MySQL TINYINT
        return (byte) (value.asBoolean() ? 1 : 0);
    }

    @Override
    protected Schema getTimestampSchema() {
        // MySQL DATETIME/TIMESTAMP
        return Timestamp.builder().optional().build();
    }

    @Override
    protected Schema getDateSchema() {
        // MySQL DATE
        return org.apache.kafka.connect.data.Date.builder().optional().build();
    }

    @Override
    protected Schema getDecimalSchema(Integer precision, Integer scale) {
        // MySQL DECIMAL(p,s)
        int p = precision != null ? precision : 10;
        int s = scale != null ? scale : 2;
        return Decimal.builder(s).optional()
                .parameter("precision", String.valueOf(p))
                .build();
    }

    @Override
    protected String getSchemaName() {
        return "com.example.fhir.connect.smt.mysql";
    }
}
