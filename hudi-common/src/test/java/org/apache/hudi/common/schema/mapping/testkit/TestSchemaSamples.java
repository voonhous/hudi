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

package org.apache.hudi.common.schema.mapping.testkit;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaType;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestSchemaSamples {

  @Test
  void everyTypeHasASample() {
    Set<HoodieSchemaType> missing = EnumSet.allOf(HoodieSchemaType.class);
    missing.removeAll(SchemaSamples.coveredTypes());
    assertTrue(missing.isEmpty(), "Add a sample for " + missing);
  }

  @Test
  void idsAreUnique() {
    assertEquals(SchemaSamples.all().size(), SchemaSamples.ids().size());
  }

  @Test
  void casesAreValidSchemas() {
    // Building the case list parses every shape; round-tripping through JSON catches invalid Avro.
    SchemaSamples.allCases().forEach(c -> assertEquals(c.schema(), HoodieSchema.parse(c.schema().toString()), c.id()));
  }

  @Test
  void equivalenceIgnoresNamesButNotParameters() {
    HoodieSchema a = SchemaSamples.record(HoodieSchema.createDecimal("a", "ns.a", null, 10, 2, 5));
    HoodieSchema b = SchemaSamples.record(HoodieSchema.createDecimal("b", "ns.b", null, 10, 2, 5));
    HoodieSchema bytes = SchemaSamples.record(HoodieSchema.createDecimal(10, 2));
    HoodieSchema scale3 = SchemaSamples.record(HoodieSchema.createDecimal(10, 3));
    assertTrue(HoodieSchemaEquivalence.differences(a, b, HoodieSchemaEquivalence.Mode.PHYSICAL).isEmpty());
    assertTrue(HoodieSchemaEquivalence.differences(a, bytes, HoodieSchemaEquivalence.Mode.LOGICAL).isEmpty());
    assertFalse(HoodieSchemaEquivalence.differences(a, bytes, HoodieSchemaEquivalence.Mode.PHYSICAL).isEmpty());
    assertFalse(HoodieSchemaEquivalence.differences(bytes, scale3, HoodieSchemaEquivalence.Mode.LOGICAL).isEmpty());
  }

  @Test
  void equivalenceComparesNullability() {
    HoodieSchema required = SchemaSamples.record(HoodieSchema.create(HoodieSchemaType.INT));
    HoodieSchema nullable = SchemaSamples.record(HoodieSchema.createNullable(HoodieSchemaType.INT));
    assertFalse(HoodieSchemaEquivalence.differences(required, nullable, HoodieSchemaEquivalence.Mode.LOGICAL).isEmpty());
    assertThrows(AssertionError.class,
        () -> HoodieSchemaEquivalence.assertEquivalent(required, nullable, HoodieSchemaEquivalence.Mode.LOGICAL));
  }

  @Test
  void equivalenceComparesTimestampPrecision() {
    HoodieSchema millis = SchemaSamples.record(HoodieSchema.createTimestampMillis());
    HoodieSchema micros = SchemaSamples.record(HoodieSchema.createTimestampMicros());
    HoodieSchema local = SchemaSamples.record(HoodieSchema.createLocalTimestampMillis());
    assertEquals(Collections.singletonList("$.col: expected timestamp precision MILLIS but was MICROS"),
        HoodieSchemaEquivalence.differences(millis, micros, HoodieSchemaEquivalence.Mode.LOGICAL));
    assertFalse(HoodieSchemaEquivalence.differences(millis, local, HoodieSchemaEquivalence.Mode.LOGICAL).isEmpty());
  }
}
