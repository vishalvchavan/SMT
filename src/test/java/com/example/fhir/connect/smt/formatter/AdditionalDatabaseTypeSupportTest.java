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
    assertEquals(DatabaseType.RDF, DatabaseType.fromConfig("rdf"));
    assertEquals(DatabaseType.LEDGER, DatabaseType.fromConfig("ledger"));
    assertEquals(DatabaseType.EMBEDDED, DatabaseType.fromConfig("embedded"));
    assertEquals(DatabaseType.OBJECT_STORE, DatabaseType.fromConfig("object_store"));
  }

  @Test
  void new_schemaless_targets_do_not_require_schema() {
    assertFalse(DatabaseType.NEO4J.requiresSchema());
    assertFalse(DatabaseType.INFLUXDB.requiresSchema());
    assertFalse(DatabaseType.VECTORDB.requiresSchema());
    assertFalse(DatabaseType.RDF.requiresSchema());
    assertFalse(DatabaseType.LEDGER.requiresSchema());
    assertFalse(DatabaseType.OBJECT_STORE.requiresSchema());
  }

  @Test
  void factory_creates_new_formatters_and_outputs_schemaless_maps() throws Exception {
    var neo4j = OutputFormatterFactory.createFormatter(DatabaseType.NEO4J);
    var influx = OutputFormatterFactory.createFormatter(DatabaseType.INFLUXDB);
    var vector = OutputFormatterFactory.createFormatter(DatabaseType.VECTORDB);
    var rdf = OutputFormatterFactory.createFormatter(DatabaseType.RDF);
    var ledger = OutputFormatterFactory.createFormatter(DatabaseType.LEDGER);
    var objectStore = OutputFormatterFactory.createFormatter(DatabaseType.OBJECT_STORE);
    var embedded = OutputFormatterFactory.createFormatter(DatabaseType.EMBEDDED);

    assertInstanceOf(Neo4jOutputFormatter.class, neo4j);
    assertInstanceOf(InfluxDbOutputFormatter.class, influx);
    assertInstanceOf(VectorDbOutputFormatter.class, vector);
    assertInstanceOf(RdfOutputFormatter.class, rdf);
    assertInstanceOf(LedgerOutputFormatter.class, ledger);
    assertInstanceOf(ObjectStoreOutputFormatter.class, objectStore);
    assertInstanceOf(EmbeddedOutputFormatter.class, embedded);

    var json = MAPPER.readTree("{\"id\":\"n1\",\"embedding\":[0.1,0.2],\"score\":42}");
    assertInstanceOf(java.util.Map.class, neo4j.format(json, null));
    assertInstanceOf(java.util.Map.class, influx.format(json, null));
    assertInstanceOf(java.util.Map.class, vector.format(json, null));
    assertInstanceOf(java.util.Map.class, rdf.format(json, null));
    assertInstanceOf(java.util.Map.class, ledger.format(json, null));
    assertInstanceOf(java.util.Map.class, objectStore.format(json, null));
    assertInstanceOf(org.apache.kafka.connect.data.Struct.class, embedded.format(json, null));

    assertNull(neo4j.buildSchema(null, json));
    assertNull(influx.buildSchema(null, json));
    assertNull(vector.buildSchema(null, json));
    assertNull(rdf.buildSchema(null, json));
    assertNull(ledger.buildSchema(null, json));
    assertNull(objectStore.buildSchema(null, json));
  }
}
