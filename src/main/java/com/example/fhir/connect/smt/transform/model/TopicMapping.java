package com.example.fhir.connect.smt.transform.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TopicMapping {
  public String root;
  public Object output;

  public String getRoot() {
    return root;
  }

  public Object getOutput() {
    return output;
  }
}
