package com.example.fhir.connect.smt.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class S3ConfigLoaderChangeDetectionTest {

  @Test
  void detects_etag_changes_when_both_present() {
    assertTrue(S3ConfigLoader.isEtagChanged("etag-a", "etag-b"));
    assertFalse(S3ConfigLoader.isEtagChanged("etag-a", "etag-a"));
    assertFalse(S3ConfigLoader.isEtagChanged(null, "etag-b"));
  }

  @Test
  void enables_hash_fallback_when_etag_missing_or_unchanged() {
    assertTrue(S3ConfigLoader.shouldCheckHashFallback("same", "same"));
    assertTrue(S3ConfigLoader.shouldCheckHashFallback(null, "new"));
    assertTrue(S3ConfigLoader.shouldCheckHashFallback("old", null));
    assertFalse(S3ConfigLoader.shouldCheckHashFallback("old", "new"));
  }

  @Test
  void detects_hash_changes_when_both_present() {
    assertTrue(S3ConfigLoader.isHashChanged("hash-a", "hash-b"));
    assertFalse(S3ConfigLoader.isHashChanged("hash-a", "hash-a"));
    assertFalse(S3ConfigLoader.isHashChanged(null, "hash-b"));
  }
}
