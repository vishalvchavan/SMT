package com.example.fhir.connect.smt.transform.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class FieldSpec {
  public List<String> paths;
  public Boolean required;
  public String multi;
  public List<Map<String, Object>> transforms;

  public List<String> getPaths() {
    return paths != null ? paths : List.of();
  }

  public boolean isRequired() {
    return required != null && required;
  }

  public String getMulti() {
    return multi == null ? "first" : multi;
  }

  public List<Map<String, Object>> getTransforms() {
    return transforms;
  }
}
