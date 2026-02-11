package com.example.fhir.connect.smt;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class DynamicJsonExtractSMTByteBufferTest {

  @Test
  void toBytes_respects_position_and_limit_for_array_backed_buffers() throws Exception {
    DynamicJsonExtractSMT<?> smt = new DynamicJsonExtractSMT<>();
    Method toBytes = DynamicJsonExtractSMT.class.getDeclaredMethod("toBytes", Object.class);
    toBytes.setAccessible(true);

    byte[] raw = "xx{\"a\":1}yy".getBytes(StandardCharsets.UTF_8);
    ByteBuffer buffer = ByteBuffer.wrap(raw, 2, 7);
    int originalPosition = buffer.position();

    byte[] out = (byte[]) toBytes.invoke(smt, buffer);

    assertArrayEquals("{\"a\":1}".getBytes(StandardCharsets.UTF_8), out);
    assertEquals(originalPosition, buffer.position());
  }

  @Test
  void toBytes_does_not_advance_position_for_direct_buffers() throws Exception {
    DynamicJsonExtractSMT<?> smt = new DynamicJsonExtractSMT<>();
    Method toBytes = DynamicJsonExtractSMT.class.getDeclaredMethod("toBytes", Object.class);
    toBytes.setAccessible(true);

    ByteBuffer buffer = ByteBuffer.allocateDirect(7);
    buffer.put("{\"b\":2}".getBytes(StandardCharsets.UTF_8));
    buffer.flip();
    int originalPosition = buffer.position();

    byte[] out = (byte[]) toBytes.invoke(smt, buffer);

    assertArrayEquals("{\"b\":2}".getBytes(StandardCharsets.UTF_8), out);
    assertEquals(originalPosition, buffer.position());
  }
}
