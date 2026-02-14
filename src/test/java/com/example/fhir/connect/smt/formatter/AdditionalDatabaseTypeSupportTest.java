package com.example.fhir.connect.smt.formatter;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;

class AdditionalDatabaseTypeSupportTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  void parses_new_target_database_values() {
    assertEquals(DatabaseType.NEO4J, DatabaseType.fromConfig("neo4j"));
    assertEquals(DatabaseType.INFLUXDB, DatabaseType.fromConfig("influxdb"));
    assertEquals(DatabaseType.VECTORDB, DatabaseType.fromConfig("vectordb"));
  }

  @Test
  void new_schemaless_targets_do_not_require_schema() {
    assertFalse(DatabaseType.NEO4J.requiresSchema());
    assertFalse(DatabaseType.INFLUXDB.requiresSchema());
    assertFalse(DatabaseType.VECTORDB.requiresSchema());
  }

  @Test
  void factory_creates_new_formatters_and_outputs_schemaless_maps() throws Exception {
    var neo4j = OutputFormatterFactory.createFormatter(DatabaseType.NEO4J);
    var influx = OutputFormatterFactory.createFormatter(DatabaseType.INFLUXDB);
    var vector = OutputFormatterFactory.createFormatter(DatabaseType.VECTORDB);

    assertInstanceOf(Neo4jOutputFormatter.class, neo4j);
    assertInstanceOf(InfluxDbOutputFormatter.class, influx);
    assertInstanceOf(VectorDbOutputFormatter.class, vector);

    var json = MAPPER.readTree("{\"id\":\"n1\",\"embedding\":[0.1,0.2],\"score\":42}");
    assertInstanceOf(java.util.Map.class, neo4j.format(json, null));
    assertInstanceOf(java.util.Map.class, influx.format(json, null));
    assertInstanceOf(java.util.Map.class, vector.format(json, null));

    assertNull(neo4j.buildSchema(null, json));
    assertNull(influx.buildSchema(null, json));
    assertNull(vector.buildSchema(null, json));
  }
}
