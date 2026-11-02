/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.schema.mapping;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.util.Option;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.function.UnaryOperator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestHoodieSchemaHints {

  /**
   * A mapper that stores UUID as string and VECTOR as array of float.
   */
  private static final UnaryOperator<HoodieSchema> IMAGE = s -> {
    switch (s.getType()) {
      case UUID:
        return HoodieSchema.create(HoodieSchemaType.STRING);
      case VECTOR:
        return HoodieSchema.createArray(HoodieSchema.create(HoodieSchemaType.FLOAT));
      default:
        return s;
    }
  };

  private static HoodieSchema restore(HoodieSchema converted, HoodieSchema hint) {
    return HoodieSchemaHints.restore(converted, Option.of(hint),
        t -> t == HoodieSchemaType.UUID || t == HoodieSchemaType.VECTOR, IMAGE, HoodieSchemaEquivalence.Mode.LOGICAL);
  }

  private static HoodieSchema record(HoodieSchemaField... fields) {
    return HoodieSchema.createRecord("r", "ns", null, Arrays.asList(fields));
  }

  @Test
  void restoresLossyTypesFromHint() {
    HoodieSchema hint = record(
        HoodieSchemaField.of("id", HoodieSchema.createUUID()),
        HoodieSchemaField.of("emb", HoodieSchema.createVector(4)),
        HoodieSchemaField.of("n", HoodieSchema.create(HoodieSchemaType.INT)));
    HoodieSchema converted = record(
        HoodieSchemaField.of("id", HoodieSchema.createNullable(HoodieSchemaType.STRING)),
        HoodieSchemaField.of("emb", HoodieSchema.createArray(HoodieSchema.create(HoodieSchemaType.FLOAT))),
        HoodieSchemaField.of("n", HoodieSchema.create(HoodieSchemaType.INT)));
    HoodieSchema restored = restore(converted, hint);
    // Types come from the hint, nullability from the converted schema.
    assertEquals(HoodieSchema.createNullable(HoodieSchema.createUUID()), restored.getField("id").get().schema());
    assertEquals(HoodieSchema.createVector(4), restored.getField("emb").get().schema());
    assertSame(converted.getField("n").get().schema(), restored.getField("n").get().schema());
  }

  @Test
  void keepsGenuineChanges() {
    HoodieSchema hint = record(HoodieSchemaField.of("id", HoodieSchema.createUUID()));
    HoodieSchema converted = record(HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.LONG)));
    assertSame(converted, restore(converted, hint));
  }

  @Test
  void restoresInsideNestedTypesAndIgnoresUnknownFields() {
    HoodieSchema hint = record(HoodieSchemaField.of("ids", HoodieSchema.createMap(HoodieSchema.createUUID())));
    HoodieSchema converted = record(
        HoodieSchemaField.of("ids", HoodieSchema.createMap(HoodieSchema.create(HoodieSchemaType.STRING))),
        HoodieSchemaField.of("extra", HoodieSchema.create(HoodieSchemaType.STRING)));
    HoodieSchema restored = restore(converted, hint);
    assertEquals(HoodieSchemaType.UUID, restored.getField("ids").get().schema().getValueType().getType());
    assertEquals(HoodieSchemaType.STRING, restored.getField("extra").get().schema().getType());
  }

  @Test
  void restoresDecimalBacking() {
    HoodieSchema fixed = HoodieSchema.createDecimal("d", "ns", null, 10, 2, 5);
    HoodieSchema hint = record(HoodieSchemaField.of("d", fixed));
    HoodieSchema converted = record(HoodieSchemaField.of("d", HoodieSchema.createDecimal(10, 2)));
    assertEquals(fixed, restore(converted, hint).getField("d").get().schema());
    HoodieSchema otherScale = record(HoodieSchemaField.of("d", HoodieSchema.createDecimal(10, 3)));
    assertSame(otherScale, restore(otherScale, hint));
  }

  @Test
  void noHintIsANoOp() {
    HoodieSchema converted = record(HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING)));
    assertSame(converted, HoodieSchemaHints.restore(converted, Option.empty(), t -> true, IMAGE,
        HoodieSchemaEquivalence.Mode.LOGICAL));
    assertTrue(HoodieSchemaEquivalence.equivalent(converted, converted, HoodieSchemaEquivalence.Mode.PHYSICAL));
  }
}
