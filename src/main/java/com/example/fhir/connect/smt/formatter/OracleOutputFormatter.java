package com.example.fhir.connect.smt.formatter;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Timestamp;

/**
 * Oracle Database-specific output formatter.
 * 
 * Type mappings:
 * - Boolean → NUMBER(1) - Oracle has no native boolean type
 * - Timestamp → Timestamp logical type (TIMESTAMP WITH TIME ZONE)
 * - Decimal → Decimal logical type (NUMBER(p,s))
 * - Arrays → CLOB as JSON string
 */
public class OracleOutputFormatter extends AbstractJdbcFormatter {

    @Override
    public DatabaseType getDatabaseType() {
        return DatabaseType.ORACLE;
    }

    @Override
    protected Schema getBooleanSchema() {
        // Oracle has no native BOOLEAN - use NUMBER(1)
        return Schema.OPTIONAL_INT8_SCHEMA;
    }

    @Override
    protected Object convertBoolean(com.fasterxml.jackson.databind.JsonNode value) {
        // Oracle NUMBER(1): 0 or 1
        return (byte) (value.asBoolean() ? 1 : 0);
    }

    @Override
    protected Schema getTimestampSchema() {
        // Oracle TIMESTAMP WITH TIME ZONE
        return Timestamp.builder().optional().build();
    }

    @Override
    protected Schema getDateSchema() {
        // Oracle DATE (includes time in Oracle, but Connect DATE is date-only)
        return org.apache.kafka.connect.data.Date.builder().optional().build();
    }

    @Override
    protected Schema getDecimalSchema(Integer precision, Integer scale) {
        // Oracle NUMBER(p,s) - supports very high precision
        int p = precision != null ? precision : 38;
        int s = scale != null ? scale : 10;
        return Decimal.builder(s).optional()
                .parameter("precision", String.valueOf(p))
                .build();
    }

    @Override
    protected String getSchemaName() {
        return "com.example.fhir.connect.smt.oracle";
    }
}
