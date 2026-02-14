package com.example.fhir.connect.smt.formatter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Factory for creating database-specific output formatters.
 */
public final class OutputFormatterFactory {

    private static final Logger log = LoggerFactory.getLogger(OutputFormatterFactory.class);

    // Cache formatters since they're stateless (with default config)
    private static final Map<DatabaseType, OutputFormatter> FORMATTER_CACHE = new ConcurrentHashMap<>();

    private OutputFormatterFactory() {
        // Utility class
    }

    /**
     * Get a formatter for the specified database type with default configuration.
     */
    public static OutputFormatter getFormatter(DatabaseType databaseType) {
        return FORMATTER_CACHE.computeIfAbsent(databaseType, OutputFormatterFactory::createFormatter);
    }

    /**
     * Get a formatter for the specified database type from config string.
     */
    public static OutputFormatter getFormatter(String databaseConfig) {
        DatabaseType type = DatabaseType.fromConfig(databaseConfig);
        return getFormatter(type);
    }

    /**
     * Create a new formatter instance with custom configuration.
     * Use this when you need specific settings (flatten mode, array handling, etc.)
     */
    public static OutputFormatter createFormatter(DatabaseType databaseType) {
        log.debug("Creating formatter for database type: {}", databaseType);

        return switch (databaseType) {
            case MONGODB -> new MongoOutputFormatter();
            case MYSQL -> new MySqlOutputFormatter();
            case POSTGRESQL -> new PostgresOutputFormatter();
            case SQLSERVER -> new SqlServerOutputFormatter();
            case ORACLE -> new OracleOutputFormatter();
            case SNOWFLAKE -> new SnowflakeOutputFormatter();
            case DELTA_LAKE -> new DeltaLakeOutputFormatter();
            case NEO4J -> new Neo4jOutputFormatter();
            case INFLUXDB -> new InfluxDbOutputFormatter();
            case VECTORDB -> new VectorDbOutputFormatter();
        };
    }

    /**
     * Create a JDBC formatter with custom configuration options.
     */
    public static AbstractJdbcFormatter createJdbcFormatter(
            DatabaseType databaseType,
            boolean flattenNested,
            String columnSeparator,
            OutputFormatter.ArrayMode arrayMode) {

        if (!databaseType.isJdbc() && databaseType != DatabaseType.SNOWFLAKE
                && databaseType != DatabaseType.DELTA_LAKE) {
            throw new IllegalArgumentException(databaseType + " is not a JDBC-based database");
        }

        AbstractJdbcFormatter formatter = (AbstractJdbcFormatter) createFormatter(databaseType);
        formatter.setFlattenNested(flattenNested);
        formatter.setColumnSeparator(columnSeparator);
        formatter.setDefaultArrayMode(arrayMode);

        return formatter;
    }

    /**
     * Clear the formatter cache. Useful for testing or config changes.
     */
    public static void clearCache() {
        FORMATTER_CACHE.clear();
    }
}
