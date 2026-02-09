package com.example.fhir.connect.smt;

import com.example.fhir.connect.smt.config.SmtConfig;
import com.example.fhir.connect.smt.mapping.MappingProvider;
import com.example.fhir.connect.smt.transform.TransformEngine;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class SmtGoldenTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  void transforms_to_schemaless_map_with_connector_name() throws Exception {
    Map<String, Object> connector = new HashMap<>();
    connector.put("transforms.Extract.mapping.classpathRoot", "mappings/topic-mappings.json");
    connector.put("transforms.Extract.connector.name", "mongo-assessment-sink");
    connector.put("transforms.Extract.app.failOnMissingMapping", "false");
    connector.put("transforms.Extract.app.attachKafkaMetadata", "false");
    connector.put("transforms.Extract.app.storeRawPayload", "false");

    SmtConfig cfg = SmtConfig.load(connector);
    MappingProvider mp = new MappingProvider(cfg);
    TransformEngine engine = new TransformEngine(mp);

    JsonNode input = MAPPER.readTree(resourceBytes("vectors/assessment/input1.json"));
    // Topic name is now irrelevant - mapping is looked up by connector name
    JsonNode out = engine.transform("any-topic", input, cfg,
        new SinkRecord("any-topic", 0, null, null, null, null, 10L));

    assertNotNull(out);
    assertTrue(out.has("assessment"));
    assertEquals("12345", out.path("assessment").path("assessmentId").asText());
  }

  private static byte[] resourceBytes(String cp) throws Exception {
    try (var in = SmtGoldenTest.class.getClassLoader().getResourceAsStream(cp)) {
      if (in == null)
        throw new IllegalStateException("Missing test resource: " + cp);
      return in.readAllBytes();
    }
  }
}
