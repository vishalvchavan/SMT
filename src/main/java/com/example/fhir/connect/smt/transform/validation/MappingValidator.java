package com.example.fhir.connect.smt.transform.validation;

import com.example.fhir.connect.smt.transform.model.FieldSpec;
import com.example.fhir.connect.smt.transform.model.MappingRules;
import com.example.fhir.connect.smt.transform.model.TopicMapping;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.*;
import java.util.regex.Pattern;

public final class MappingValidator {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Pattern NUMERIC_INDEX = Pattern.compile("\\[(\\s*\\d+\\s*)\\]");

  private MappingValidator() {
  }

  public static void validate(MappingRules rules) {
    if (rules == null)
      throw new IllegalArgumentException("MappingRules is null");

    boolean hasTopics = rules.topics != null && !rules.topics.isEmpty();
    boolean hasConnectors = rules.connectors != null && !rules.connectors.isEmpty();

    if (!hasTopics && !hasConnectors)
      throw new IllegalArgumentException("MappingRules must have at least 'topics' or 'connectors'");

    // Validate topics if present
    if (hasTopics) {
      for (Map.Entry<String, TopicMapping> e : rules.topics.entrySet()) {
        validateMapping(e.getKey(), e.getValue(), "topic");
      }
    }

    // Validate connectors if present
    if (hasConnectors) {
      for (Map.Entry<String, TopicMapping> e : rules.connectors.entrySet()) {
        validateMapping(e.getKey(), e.getValue(), "connector");
      }
    }
  }

  private static void validateMapping(String key, TopicMapping tm, String type) {
    if (tm == null)
      throw new IllegalArgumentException(type + " mapping is null for: " + key);
    if (tm.root == null || tm.root.isBlank())
      throw new IllegalArgumentException("Missing 'root' for " + type + ": " + key);
    if (tm.output == null)
      throw new IllegalArgumentException("Missing 'output' for " + type + ": " + key);

    validateOutputTree(key, tm.output, "output");
  }

  @SuppressWarnings("unchecked")
  private static void validateOutputTree(String topic, Object node, String path) {
    if (node == null)
      return;

    if (node instanceof Map<?, ?> map) {
      if (map.size() == 1 && map.containsKey("$array")) {
        Map<String, Object> a = (Map<String, Object>) map.get("$array");
        String ap = asString(a.get("path"));
        if (ap == null)
          throw new IllegalArgumentException(topic + ": $array.path missing at " + path);
        validateJsonPathIndexFree(topic, ap, path + ".$array.path");
        Object item = a.get("item");
        if (item == null)
          throw new IllegalArgumentException(topic + ": $array.item missing at " + path);
        validateOutputTree(topic, item, path + ".$array.item");
        return;
      }

      if (map.containsKey("paths")) {
        FieldSpec spec = MAPPER.convertValue(map, FieldSpec.class);
        List<String> paths = spec.getPaths();
        if (paths == null || paths.isEmpty())
          throw new IllegalArgumentException(topic + ": field spec missing 'paths' at " + path);
        for (String p : paths) {
          validateJsonPathIndexFree(topic, p, path + ".paths");
        }
        String multi = spec.getMulti();
        if (!"first".equalsIgnoreCase(multi) && !"array".equalsIgnoreCase(multi))
          throw new IllegalArgumentException(topic + ": invalid multi=" + multi + " at " + path);
        validateTransforms(topic, spec.getTransforms(), path + ".transforms");
        return;
      }

      for (Map.Entry<?, ?> e : map.entrySet()) {
        validateOutputTree(topic, e.getValue(), path + "." + e.getKey());
      }
      return;
    }

    throw new IllegalArgumentException(topic + ": unsupported node type at " + path + " => " + node.getClass());
  }

  private static void validateTransforms(String topic, List<Map<String, Object>> transforms, String path) {
    if (transforms == null)
      return;
    for (int i = 0; i < transforms.size(); i++) {
      Map<String, Object> t = transforms.get(i);
      if (t == null)
        throw new IllegalArgumentException(topic + ": null transform at " + path + "[" + i + "]");
      String type = asString(t.get("type"));
      if (type == null)
        throw new IllegalArgumentException(topic + ": transform.type missing at " + path + "[" + i + "]");
      if ("toString".equals(type)) {
        // ok
      } else if ("dateFormat".equals(type)) {
        Object in = t.get("inputFormats");
        Object out = t.get("outputFormat");
        if (!(in instanceof List) || ((List<?>) in).isEmpty())
          throw new IllegalArgumentException(topic + ": dateFormat.inputFormats required at " + path + "[" + i + "]");
        if (asString(out) == null)
          throw new IllegalArgumentException(topic + ": dateFormat.outputFormat required at " + path + "[" + i + "]");
      } else {
        throw new IllegalArgumentException(
            topic + ": unsupported transform type=" + type + " at " + path + "[" + i + "]");
      }
    }
  }

  private static void validateJsonPathIndexFree(String topic, String jsonPath, String at) {
    if (NUMERIC_INDEX.matcher(jsonPath).find()) {
      throw new IllegalArgumentException(
          topic + ": numeric array index not allowed in JSONPath at " + at + " => " + jsonPath);
    }
  }

  private static String asString(Object o) {
    return o == null ? null : String.valueOf(o);
  }
}
