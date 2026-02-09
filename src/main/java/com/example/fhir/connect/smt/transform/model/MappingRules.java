package com.example.fhir.connect.smt.transform.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class MappingRules {
  public int version;
  public Map<String, TopicMapping> topics;
  public Map<String, TopicMapping> connectors;

  public int getVersion() {
    return version;
  }

  public Map<String, TopicMapping> getTopics() {
    return topics;
  }

  public Map<String, TopicMapping> getConnectors() {
    return connectors;
  }
}
