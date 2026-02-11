package com.example.fhir.connect.smt;

import com.example.fhir.connect.smt.config.SmtConfig;
import com.example.fhir.connect.smt.mapping.MappingProvider;
import com.example.fhir.connect.smt.transform.TransformEngine;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TransformEngineDateFormatTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  void dateFormat_supports_local_datetime_without_zone() throws Exception {
    String mapping = """
        {
          "connectors": {
            "test-date-connector": {
              "root": "assessment",
              "output": {
                "eventTime": {
                  "paths": ["$.ts"],
                  "transforms": [
                    {
                      "type": "dateFormat",
                      "inputFormats": ["yyyy-MM-dd'T'HH:mm:ss"],
                      "outputFormat": "yyyy-MM-dd'T'HH:mm:ssXXX",
                      "timezone": "UTC"
                    }
                  ]
                }
              }
            }
          }
        }
        """;

    SmtConfig cfg = SmtConfig.load(Map.of("connector.name", "test-date-connector"));
    MappingProvider provider = MappingProvider.fromJsonString(mapping);
    TransformEngine engine = new TransformEngine(provider);

    JsonNode input = MAPPER.readTree("{\"ts\":\"2026-02-10T12:34:56\"}");
    JsonNode out = engine.transform("unused-topic", input, cfg, new SinkRecord("unused-topic", 0, null, null, null, null, 1L));

    assertEquals("2026-02-10T12:34:56Z", out.path("assessment").path("eventTime").asText());
  }
}
