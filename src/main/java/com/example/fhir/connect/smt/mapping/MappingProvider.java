package com.example.fhir.connect.smt.mapping;

import com.example.fhir.connect.smt.config.SmtConfig;
import com.example.fhir.connect.smt.transform.model.MappingRules;
import com.example.fhir.connect.smt.transform.validation.MappingValidator;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.InputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class MappingProvider {
  private static final Logger log = LoggerFactory.getLogger(MappingProvider.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final SmtConfig cfg;
  private final MappingRules rules;

  public MappingProvider(SmtConfig cfg) throws Exception {
    this.cfg = cfg;
    this.rules = loadClasspath(cfg.mappingClasspathRoot());
    MappingValidator.validate(this.rules);
  }

  public MappingRules rules() {
    return rules;
  }

  private MappingRules loadClasspath(String cp) throws Exception {
    log.info("Loading SMT mappings from classpath: {}", cp);
    try (InputStream in = MappingProvider.class.getClassLoader().getResourceAsStream(cp)) {
      if (in == null)
        throw new IllegalStateException("Missing mapping resource on classpath: " + cp);
      return MAPPER.readValue(in, MappingRules.class);
    }
  }
}
