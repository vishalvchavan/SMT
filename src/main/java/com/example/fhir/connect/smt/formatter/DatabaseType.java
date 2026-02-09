package com.example.fhir.connect.smt.formatter;

/**
 * Supported database types for the SMT output formatters.
 */
public enum DatabaseType {
    /**
     * MongoDB - Schemaless document output (LinkedHashMap).
     * Default behavior, backward compatible.
     */
    MONGODB("mongodb"),

    /**
     * MySQL - JDBC output with MySQL-specific type handling.
     * Uses TINYINT(1) for booleans, DATETIME for timestamps, JSON for arrays.
     */
    MYSQL("mysql"),

    /**
     * PostgreSQL - JDBC output with PostgreSQL-specific type handling.
     * Uses native BOOLEAN, TIMESTAMPTZ, JSONB for arrays.
     */
    POSTGRESQL("postgresql"),

    /**
     * SQL Server - JDBC output with SQL Server-specific type handling.
     * Uses BIT for booleans, DATETIME2 for timestamps, NVARCHAR(MAX) for JSON.
     */
    SQLSERVER("sqlserver"),

    /**
     * Oracle Database - JDBC output with Oracle-specific type handling.
     * Uses NUMBER(1) for booleans, TIMESTAMP WITH TIME ZONE, CLOB for JSON.
     */
    ORACLE("oracle"),

    /**
     * Snowflake - JDBC output with Snowflake-specific type handling.
     * Uses native BOOLEAN, TIMESTAMP_NTZ/TIMESTAMP_TZ, VARIANT for complex types.
     */
    SNOWFLAKE("snowflake"),

    /**
     * Delta Lake (Databricks) - Parquet-based lakehouse output.
     * Uses Spark/Delta compatible types with struct support.
     */
    DELTA_LAKE("delta_lake");

    private final String configValue;

    DatabaseType(String configValue) {
        this.configValue = configValue;
    }

    public String getConfigValue() {
        return configValue;
    }

    /**
     * Parse a configuration value into a DatabaseType.
     * Case-insensitive matching with underscore/hyphen normalization.
     *
     * @param value Configuration value (e.g., "postgresql", "delta-lake", "ORACLE")
     * @return Matching DatabaseType
     * @throws IllegalArgumentException if no match found
     */
    public static DatabaseType fromConfig(String value) {
        if (value == null || value.isBlank()) {
            return MONGODB; // Default
        }
        String normalized = value.toLowerCase().replace("-", "_").trim();
        for (DatabaseType type : values()) {
            if (type.configValue.equals(normalized)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown database type: " + value +
                ". Supported: mongodb, mysql, postgresql, sqlserver, oracle, snowflake, delta_lake");
    }

    /**
     * Check if this database type requires a Connect schema (JDBC sinks).
     */
    public boolean requiresSchema() {
        return this != MONGODB;
    }

    /**
     * Check if this is a traditional JDBC-based database.
     */
    public boolean isJdbc() {
        return this == MYSQL || this == POSTGRESQL || this == SQLSERVER || this == ORACLE;
    }
}
