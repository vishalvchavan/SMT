package com.example.fhir.connect.smt.formatter;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Timestamp;

/**
 * SQL Server-specific output formatter.
 * 
 * Type mappings:
 * - Boolean → INT8 (BIT in SQL Server)
 * - Timestamp → Timestamp logical type (DATETIME2)
 * - Decimal → Decimal logical type
 * - Arrays → NVARCHAR(MAX) as JSON string
 */
public class SqlServerOutputFormatter extends AbstractJdbcFormatter {

    @Override
    public DatabaseType getDatabaseType() {
        return DatabaseType.SQLSERVER;
    }

    @Override
    protected Schema getBooleanSchema() {
        // SQL Server BIT type - represented as INT8
        return Schema.OPTIONAL_INT8_SCHEMA;
    }

    @Override
    protected Object convertBoolean(com.fasterxml.jackson.databind.JsonNode value) {
        // SQL Server BIT: 0 or 1
        return (byte) (value.asBoolean() ? 1 : 0);
    }

    @Override
    protected Schema getTimestampSchema() {
        // SQL Server DATETIME2
        return Timestamp.builder().optional().build();
    }

    @Override
    protected Schema getDateSchema() {
        // SQL Server DATE
        return org.apache.kafka.connect.data.Date.builder().optional().build();
    }

    @Override
    protected Schema getDecimalSchema(Integer precision, Integer scale) {
        // SQL Server DECIMAL(p,s) - default precision 18
        int p = precision != null ? precision : 18;
        int s = scale != null ? scale : 2;
        return Decimal.builder(s).optional()
                .parameter("precision", String.valueOf(p))
                .build();
    }

    @Override
    protected String getSchemaName() {
        return "com.example.fhir.connect.smt.sqlserver";
    }
}
