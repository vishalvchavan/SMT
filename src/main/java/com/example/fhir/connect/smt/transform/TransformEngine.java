package com.example.fhir.connect.smt.transform;

import com.example.fhir.connect.smt.config.SmtConfig;
import com.example.fhir.connect.smt.mapping.MappingProvider;
import com.example.fhir.connect.smt.observability.SmtMetrics;
import com.example.fhir.connect.smt.observability.TransformTracer;
import com.example.fhir.connect.smt.transform.model.FieldSpec;
import com.example.fhir.connect.smt.transform.model.MappingRules;
import com.example.fhir.connect.smt.transform.model.TopicMapping;
import com.example.fhir.connect.smt.util.EncryptionUtil;
import com.example.fhir.connect.smt.util.MaskingUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.*;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * High-performance TransformEngine using pure Jackson tree navigation.
 * NO JSONPath library - all extraction done via native Jackson methods.
 * 
 * Performance optimizations:
 * - Single tree parse per document
 * - Native Jackson tree navigation (no JSONPath overhead)
 * - Custom filter expression parser
 * - Cached DateTimeFormatter instances
 * - Pre-parsed path segments
 * - Recursive projection validation for arrays
 */
public final class TransformEngine {
  private static final Logger log = LoggerFactory.getLogger(TransformEngine.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  // Pattern to parse JSONPath-like expressions into segments
  // Matches: fieldName, [*], [0], [?(@.field=='value')]
  private static final Pattern PATH_SEGMENT_PATTERN = Pattern.compile(
      "\\[\\?\\(@\\.([\\w]+)\\s*==\\s*'?([^'\\]]+)'?(?:\\s*&&\\s*@\\.([\\w]+)\\s*==\\s*'?([^'\\]]+)'?)?\\)]|" +
          "\\[(\\d+|\\*)]|" +
          "([\\w]+)");

  // Cache for parsed path segments
  private static final ConcurrentHashMap<String, PathSegment[]> PATH_CACHE = new ConcurrentHashMap<>();
  // Cache for DateTimeFormatter instances
  private static final ConcurrentHashMap<String, DateTimeFormatter> FORMATTER_CACHE = new ConcurrentHashMap<>();

  private final MappingProvider mappingProvider;
  private final TransformTracer tracer;

  public TransformEngine(MappingProvider mappingProvider) {
    this(mappingProvider, null);
  }

  public TransformEngine(MappingProvider mappingProvider, TransformTracer tracer) {
    this.mappingProvider = mappingProvider;
    this.tracer = tracer;
  }

  /**
   * Represents a single segment in a path expression.
   */
  private static class PathSegment {
    enum Type {
      FIELD, ARRAY_ALL, ARRAY_INDEX, FILTER
    }

    final Type type;
    final String fieldName;
    final int arrayIndex;
    final String filterField1;
    final String filterValue1;
    final String filterField2;
    final String filterValue2;

    PathSegment(Type type, String fieldName, int arrayIndex,
        String filterField1, String filterValue1,
        String filterField2, String filterValue2) {
      this.type = type;
      this.fieldName = fieldName;
      this.arrayIndex = arrayIndex;
      this.filterField1 = filterField1;
      this.filterValue1 = filterValue1;
      this.filterField2 = filterField2;
      this.filterValue2 = filterValue2;
    }

    static PathSegment field(String name) {
      return new PathSegment(Type.FIELD, name, -1, null, null, null, null);
    }

    static PathSegment arrayAll() {
      return new PathSegment(Type.ARRAY_ALL, null, -1, null, null, null, null);
    }

    static PathSegment arrayIndex(int idx) {
      return new PathSegment(Type.ARRAY_INDEX, null, idx, null, null, null, null);
    }

    static PathSegment filter(String field1, String value1, String field2, String value2) {
      return new PathSegment(Type.FILTER, null, -1, field1, value1, field2, value2);
    }
  }

  /**
   * Parse a JSONPath expression into segments for fast navigation.
   */
  private static PathSegment[] parsePath(String jsonPath) {
    return PATH_CACHE.computeIfAbsent(jsonPath, path -> {
      String cleanPath = path.startsWith("$.") ? path.substring(2) : path;

      java.util.List<PathSegment> segments = new java.util.ArrayList<>();
      Matcher matcher = PATH_SEGMENT_PATTERN.matcher(cleanPath);

      while (matcher.find()) {
        if (matcher.group(1) != null) {
          segments.add(PathSegment.filter(
              matcher.group(1), matcher.group(2),
              matcher.group(3), matcher.group(4)));
        } else if (matcher.group(5) != null) {
          String idx = matcher.group(5);
          if ("*".equals(idx)) {
            segments.add(PathSegment.arrayAll());
          } else {
            segments.add(PathSegment.arrayIndex(Integer.parseInt(idx)));
          }
        } else if (matcher.group(6) != null) {
          segments.add(PathSegment.field(matcher.group(6)));
        }
      }

      return segments.toArray(new PathSegment[0]);
    });
  }

  /**
   * Root entry point for recursive navigation.
   */
  private static JsonNode navigatePath(JsonNode root, PathSegment[] segments) {
    return navigateRecursive(root, segments, 0);
  }

  /**
   * Recursive navigation to handle array sorting/broadcasting.
   */
  private static JsonNode navigateRecursive(JsonNode current, PathSegment[] segments, int index) {
    if (current == null || current.isMissingNode()) {
      return MissingNode.getInstance();
    }
    // End of segments? Return current node.
    if (index >= segments.length) {
      return current;
    }

    PathSegment seg = segments[index];

    // IMPLICIT PROJECTION: If attempting field access on an Array, broadcast to
    // children
    // (e.g. $.items.value where items is array)
    if (current.isArray() && seg.type == PathSegment.Type.FIELD) {
      ArrayNode outcome = MAPPER.createArrayNode();
      for (JsonNode child : current) {
        // Apply this SAME segment (FIELD) to the child object
        JsonNode result = navigateRecursive(child, segments, index);
        collectResult(outcome, result);
      }
      return outcome;
    }

    switch (seg.type) {
      case FIELD:
        return navigateRecursive(current.path(seg.fieldName), segments, index + 1);

      case ARRAY_INDEX:
        if (current.isArray() && seg.arrayIndex < current.size()) {
          return navigateRecursive(current.get(seg.arrayIndex), segments, index + 1);
        }
        return MissingNode.getInstance();

      case ARRAY_ALL: // [*]
        if (current.isArray()) {
          ArrayNode outcome = MAPPER.createArrayNode();
          for (JsonNode child : current) {
            JsonNode result = navigateRecursive(child, segments, index + 1);
            collectResult(outcome, result);
          }
          return outcome;
        }
        // If not array, treat [*] as identity or error? JsonPath often treats as error
        // or wildcard on fields.
        // For structured data, assume array.
        return MissingNode.getInstance();

      case FILTER:
        if (current.isArray()) {
          JsonNode filtered = applyFilter(current, seg);
          // Apply REMAINING segments to the filtered array
          // Note: "filtered" is an ArrayNode. The next segment will likely trigger
          // "Implicit Projection" check above if it's a FIELD
          return navigateRecursive(filtered, segments, index + 1);
        }
        return MissingNode.getInstance();
    }

    return MissingNode.getInstance();
  }

  /**
   * Helper to collect results into an array, flattening if the result is already
   * an array.
   */
  private static void collectResult(ArrayNode collector, JsonNode result) {
    if (result != null && !result.isMissingNode() && !result.isNull()) {
      if (result.isArray()) {
        collector.addAll((ArrayNode) result);
      } else {
        collector.add(result);
      }
    }
  }

  /**
   * Apply a filter expression to an array.
   */
  private static JsonNode applyFilter(JsonNode array, PathSegment filter) {
    ArrayNode results = MAPPER.createArrayNode();
    for (JsonNode element : array) {
      boolean match1 = matchesCondition(element, filter.filterField1, filter.filterValue1);
      boolean match2 = filter.filterField2 == null ||
          matchesCondition(element, filter.filterField2, filter.filterValue2);
      if (match1 && match2) {
        results.add(element);
      }
    }
    return results;
  }

  /**
   * Check if an element matches a filter condition.
   */
  private static boolean matchesCondition(JsonNode element, String field, String value) {
    JsonNode fieldNode = element.path(field);
    if (fieldNode.isMissingNode() || fieldNode.isNull()) {
      return false;
    }
    if ("true".equalsIgnoreCase(value)) {
      return fieldNode.isBoolean() && fieldNode.asBoolean();
    }
    if ("false".equalsIgnoreCase(value)) {
      return fieldNode.isBoolean() && !fieldNode.asBoolean();
    }
    String nodeValue = fieldNode.asText();
    return value.equals(nodeValue);
  }

  /**
   * Extract a value using a JSONPath-like expression (pure Jackson).
   */
  private JsonNode extractValue(JsonNode root, String path) {
    PathSegment[] segments = parsePath(path);
    return navigatePath(root, segments);
  }

  /**
   * Get or create a DateTimeFormatter.
   */
  private static DateTimeFormatter getFormatter(String pattern, String timezone) {
    String cacheKey = pattern + "|" + timezone;
    return FORMATTER_CACHE.computeIfAbsent(cacheKey,
        k -> DateTimeFormatter.ofPattern(pattern).withZone(ZoneId.of(timezone)));
  }

  public JsonNode transform(String topic, JsonNode input, SmtConfig cfg, ConnectRecord<?> rec) {
    return transform(topic, input, cfg, rec, false);
  }

  /**
   * Transform with option to skip root wrapper (for JDBC/relational targets).
   */
  public JsonNode transform(String topic, JsonNode input, SmtConfig cfg, ConnectRecord<?> rec,
      boolean skipRootWrapper) {
    MappingRules rules = mappingProvider.rules();
    String connectorName = cfg.connectorName();

    TopicMapping tm = null;
    String lookupKey;

    if (connectorName != null && !connectorName.isBlank() && rules.getConnectors() != null) {
      tm = rules.getConnectors().get(connectorName);
      lookupKey = "connector=" + connectorName;
    } else {
      if (rules.getTopics() != null) {
        tm = rules.getTopics().get(topic);
      }
      lookupKey = "topic=" + topic;
    }

    if (tm == null) {
      if (cfg.failOnMissingMapping())
        log.error("Missing mapping for {}", lookupKey);
      else
        log.warn("No mapping for {} (leaving record unchanged)", lookupKey);

      // Record mapping miss metric
      SmtMetrics metrics = SmtMetrics.getInstance();
      metrics.recordMappingMiss();

      // Trace mapping miss
      if (tracer != null && tracer.isEnabled()) {
        String type = lookupKey.startsWith("connector=") ? "connector" : "topic";
        String name = lookupKey.substring(lookupKey.indexOf('=') + 1);
        tracer.traceMapping(name, type, false, null);
      }

      return null;
    }

    // Trace mapping found
    if (tracer != null && tracer.isEnabled()) {
      String type = lookupKey.startsWith("connector=") ? "connector" : "topic";
      String name = lookupKey.substring(lookupKey.indexOf('=') + 1);
      tracer.traceMapping(name, type, true, tm.getRoot());
    }

    ObjectNode payloadObj = MAPPER.createObjectNode();
    applyOutputTree(topic, input, tm.getOutput(), payloadObj);

    if (cfg.attachKafkaMetadata() && rec != null && !skipRootWrapper) {
      ObjectNode meta = MAPPER.createObjectNode();
      meta.put("topic", rec.topic());
      if (rec.kafkaPartition() != null)
        meta.put("partition", rec.kafkaPartition());
      payloadObj.set("_kafka", meta);
    }

    if (cfg.storeRawPayload() && !skipRootWrapper)
      payloadObj.set("_raw", input);

    // For JDBC/relational targets, return flat payload without root wrapper
    if (skipRootWrapper) {
      return payloadObj;
    }

    ObjectNode rootObj = MAPPER.createObjectNode();
    rootObj.set(tm.getRoot(), payloadObj);
    return rootObj;
  }

  @SuppressWarnings("unchecked")
  private void applyOutputTree(String topic, JsonNode root, Object node, ObjectNode out) {
    if (node == null)
      return;

    if (node instanceof Map<?, ?> map) {
      for (Map.Entry<?, ?> e : map.entrySet()) {
        String key = String.valueOf(e.getKey());
        Object child = e.getValue();

        if (child instanceof Map<?, ?> childMap && childMap.size() == 1 && childMap.containsKey("$array")) {
          Map<String, Object> a = (Map<String, Object>) childMap.get("$array");
          String path = String.valueOf(a.get("path"));
          Object item = a.get("item");
          out.set(key, buildArrayOfObjects(topic, root, path, item));
          continue;
        }

        if (child instanceof Map<?, ?> childAsMap && childAsMap.containsKey("paths")) {
          FieldSpec spec = MAPPER.convertValue(childAsMap, FieldSpec.class);
          out.set(key, extractAndTransform(topic, root, spec, key));
          continue;
        }

        ObjectNode nested = MAPPER.createObjectNode();
        applyOutputTree(topic, root, child, nested);
        out.set(key, nested);
      }
      return;
    }

    throw new IllegalArgumentException("Unsupported output node type for topic=" + topic + " => " + node.getClass());
  }

  private ArrayNode buildArrayOfObjects(String topic, JsonNode root, String arrayPath, Object itemTree) {
    ArrayNode outArr = MAPPER.createArrayNode();

    JsonNode matches = extractValue(root, arrayPath);
    if (matches == null || matches.isMissingNode() || matches.isNull())
      return outArr;

    if (matches.isArray()) {
      for (JsonNode element : matches) {
        ObjectNode obj = MAPPER.createObjectNode();
        applyOutputTree(topic, element, itemTree, obj);
        outArr.add(obj);
      }
    } else {
      ObjectNode obj = MAPPER.createObjectNode();
      applyOutputTree(topic, matches, itemTree, obj);
      outArr.add(obj);
    }
    return outArr;
  }

  private JsonNode extractAndTransform(String topic, JsonNode root, FieldSpec spec, String fieldName) {
    // Navigate using pure Jackson
    JsonNode read = null;
    String matchedPath = null;
    for (String path : spec.getPaths()) {
      read = extractValue(root, path);
      if (read != null && !read.isMissingNode() && !read.isNull()) {
        matchedPath = path;
        break;
      }
    }

    // Trace field extraction
    if (tracer != null && tracer.isEnabled() && matchedPath != null) {
      tracer.traceField(fieldName, matchedPath, read);
    }

    JsonNode normalized;
    String multi = spec.getMulti();

    if (read == null || read.isMissingNode() || read.isNull()) {
      normalized = NullNode.getInstance();
      if (spec.isRequired())
        log.error("Required field missing topic={} field={} paths={}", topic, fieldName, spec.getPaths());
    } else if ("array".equalsIgnoreCase(multi)) {
      if (read.isArray())
        normalized = read;
      else {
        ArrayNode arr = MAPPER.createArrayNode();
        arr.add(read);
        normalized = arr;
      }
    } else {
      // multi = "first" (default)
      if (read.isArray()) {
        normalized = read.size() > 0 ? read.get(0) : NullNode.getInstance();
        if (normalized.isNull() && spec.isRequired())
          log.error("Required field missing (empty array) topic={} field={} paths={}", topic, fieldName,
              spec.getPaths());
      } else
        normalized = read;
    }

    List<Map<String, Object>> transforms = spec.getTransforms();
    if (transforms == null || transforms.isEmpty())
      return normalized;

    JsonNode v = normalized;
    for (Map<String, Object> t : transforms) {
      String type = String.valueOf(t.get("type"));
      JsonNode before = v;

      if ("toString".equals(type))
        v = toStringTransform(v);
      else if ("dateFormat".equals(type))
        v = dateFormatTransform(v, t);
      else if ("encrypt".equals(type))
        v = encryptTransform(v, t);
      else if ("mask".equals(type))
        v = maskTransform(v, t);

      // Trace transform step
      if (tracer != null && tracer.isEnabled()) {
        tracer.traceTransformStep(fieldName, type, before, v);
      }
    }
    return v;
  }

  private JsonNode toStringTransform(JsonNode v) {
    if (v == null || v.isNull())
      return NullNode.getInstance();
    if (v.isArray()) {
      ArrayNode out = MAPPER.createArrayNode();
      for (JsonNode e : v)
        out.add(toStringTransform(e));
      return out;
    }
    if (v.isTextual())
      return v;
    if (v.isNumber() || v.isBoolean())
      return TextNode.valueOf(v.asText());
    return TextNode.valueOf(v.toString());
  }

  @SuppressWarnings("unchecked")
  private JsonNode dateFormatTransform(JsonNode v, Map<String, Object> cfg) {
    if (v == null || v.isNull())
      return NullNode.getInstance();
    if (v.isArray()) {
      ArrayNode out = MAPPER.createArrayNode();
      for (JsonNode e : v)
        out.add(dateFormatTransform(e, cfg));
      return out;
    }
    if (!v.isTextual())
      return NullNode.getInstance();

    List<String> inputFormats = (List<String>) cfg.get("inputFormats");
    String outputFormat = String.valueOf(cfg.get("outputFormat"));
    String tz = cfg.get("timezone") == null ? "UTC" : String.valueOf(cfg.get("timezone"));

    String text = v.asText();
    for (String inFmt : inputFormats) {
      try {
        DateTimeFormatter inFormatter = getFormatter(inFmt, tz);
        Instant inst = Instant.from(inFormatter.parse(text));
        DateTimeFormatter outFormatter = getFormatter(outputFormat, tz);
        return TextNode.valueOf(outFormatter.format(inst));
      } catch (java.time.DateTimeException ex) {
        // Try parsing as LocalDate for date-only formats
        try {
          DateTimeFormatter inFormatter = getFormatter(inFmt, tz);
          LocalDate date = LocalDate.parse(text, inFormatter);
          DateTimeFormatter outFormatter = getFormatter(outputFormat, tz);
          return TextNode.valueOf(date.format(outFormatter));
        } catch (java.time.DateTimeException ex2) {
          // try next format
        }
      }
    }
    return NullNode.getInstance();
  }

  public static void clearCaches() {
    PATH_CACHE.clear();
    FORMATTER_CACHE.clear();
    ENCRYPTION_CACHE.clear();
    log.info("TransformEngine caches cleared");
  }

  public static Map<String, Integer> getCacheStats() {
    return Map.of(
        "parsedPaths", PATH_CACHE.size(),
        "dateFormatters", FORMATTER_CACHE.size(),
        "encryptionUtils", ENCRYPTION_CACHE.size());
  }

  // Cache for EncryptionUtil instances
  private static final ConcurrentHashMap<String, EncryptionUtil> ENCRYPTION_CACHE = new ConcurrentHashMap<>();

  /**
   * Encrypt transform - encrypts the value using AES-256-GCM.
   * Config: { "type": "encrypt", "key": "base64-encoded-32-byte-key" }
   */
  private JsonNode encryptTransform(JsonNode v, Map<String, Object> cfg) {
    if (v == null || v.isNull())
      return NullNode.getInstance();

    String key = String.valueOf(cfg.get("key"));
    if (key == null || key.isEmpty() || "null".equals(key)) {
      log.warn("Encrypt transform missing 'key' config, returning original value");
      return v;
    }

    // Resolve environment variable if key starts with ${
    if (key.startsWith("${") && key.endsWith("}")) {
      String envVar = key.substring(2, key.length() - 1);
      key = System.getenv(envVar);
      if (key == null) {
        log.error("Environment variable {} not set for encryption key", envVar);
        return v;
      }
    }

    final String resolvedKey = key;
    EncryptionUtil encryptionUtil = ENCRYPTION_CACHE.computeIfAbsent(resolvedKey, EncryptionUtil::new);

    // Record encrypt metric
    SmtMetrics.getInstance().recordEncryptCall();

    if (v.isArray()) {
      ArrayNode out = MAPPER.createArrayNode();
      for (JsonNode e : v) {
        out.add(encryptTransform(e, cfg));
      }
      return out;
    }

    if (v.isTextual()) {
      return TextNode.valueOf(encryptionUtil.encrypt(v.asText()));
    }

    // For non-text values, convert to string first
    return TextNode.valueOf(encryptionUtil.encrypt(v.asText()));
  }

  /**
   * Mask transform - masks sensitive data based on pattern.
   * Config: { "type": "mask", "pattern":
   * "ssn|creditcard|email|phone|name|custom", "customMask": "regex|replacement" }
   */
  private JsonNode maskTransform(JsonNode v, Map<String, Object> cfg) {
    if (v == null || v.isNull())
      return NullNode.getInstance();

    String pattern = cfg.get("pattern") != null ? String.valueOf(cfg.get("pattern")) : "partial";
    String customMask = cfg.get("customMask") != null ? String.valueOf(cfg.get("customMask")) : null;

    // Record mask metric
    SmtMetrics.getInstance().recordMaskCall();

    if (v.isArray()) {
      ArrayNode out = MAPPER.createArrayNode();
      for (JsonNode e : v) {
        out.add(maskTransform(e, cfg));
      }
      return out;
    }

    if (v.isTextual()) {
      return TextNode.valueOf(MaskingUtil.mask(v.asText(), pattern, customMask));
    }

    // For non-text values, convert to string first then mask
    return TextNode.valueOf(MaskingUtil.mask(v.asText(), pattern, customMask));
  }
}
