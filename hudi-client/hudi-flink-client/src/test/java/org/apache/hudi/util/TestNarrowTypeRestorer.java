/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.util;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.schema.internal.convert.InternalSchemaConverter;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link NarrowTypeRestorer}.
 */
public class TestNarrowTypeRestorer {

  private static HoodieSchema record(HoodieSchema tiny, HoodieSchema small, HoodieSchema chr) {
    return HoodieSchema.createRecord("r", null, null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT)),
        HoodieSchemaField.of("f_tiny", HoodieSchema.createNullable(tiny), null, HoodieSchema.NULL_VALUE),
        HoodieSchemaField.of("f_small", HoodieSchema.createNullable(small), null, HoodieSchema.NULL_VALUE),
        HoodieSchemaField.of("f_char", HoodieSchema.createNullable(chr), null, HoodieSchema.NULL_VALUE),
        HoodieSchemaField.of("f_array", HoodieSchema.createNullable(HoodieSchema.createArray(HoodieSchema.createNullable(tiny))), null, HoodieSchema.NULL_VALUE)));
  }

  private static HoodieSchema type(HoodieSchema schema, String field) {
    return schema.getField(field).get().schema().getNonNullType();
  }

  @Test
  void testRestoresTheTypesInternalSchemaDrops() {
    HoodieSchema narrow = record(HoodieSchema.createInt8(), HoodieSchema.createInt16(), HoodieSchema.createChar(10));
    HoodieSchema converted = InternalSchemaConverter.convert(InternalSchemaConverter.convert(narrow), "r");
    assertEquals(HoodieSchemaType.INT, type(converted, "f_tiny").getType());
    assertEquals(HoodieSchemaType.STRING, type(converted, "f_char").getType());

    HoodieSchema restored = NarrowTypeRestorer.restore(converted, narrow);
    assertEquals(HoodieSchemaType.INT8, type(restored, "f_tiny").getType());
    assertTrue(restored.getField("f_tiny").get().schema().isNullable());
    assertEquals(HoodieSchemaType.INT16, type(restored, "f_small").getType());
    assertEquals(10, ((HoodieSchema.Char) type(restored, "f_char")).getLength());
    assertEquals(HoodieSchemaType.INT8, type(restored, "f_array").getElementType().getNonNullType().getType());
    assertEquals(HoodieSchemaType.INT, type(restored, "id").getType());
  }

  @Test
  void testSourcesThatDisagreeWiden() {
    HoodieSchema narrow = record(HoodieSchema.createInt8(), HoodieSchema.createInt16(), HoodieSchema.createChar(10));
    HoodieSchema converted = InternalSchemaConverter.convert(InternalSchemaConverter.convert(narrow), "r");
    // INT8 in one source and INT16 in the other: INT16; INT in one: INT; CHAR lengths differ: STRING
    HoodieSchema other = record(HoodieSchema.createInt16(), HoodieSchema.create(HoodieSchemaType.INT), HoodieSchema.createChar(5));
    HoodieSchema restored = NarrowTypeRestorer.restore(converted, Arrays.asList(narrow, other));
    assertEquals(HoodieSchemaType.INT16, type(restored, "f_tiny").getType());
    assertEquals(HoodieSchemaType.INT, type(restored, "f_small").getType());
    assertEquals(HoodieSchemaType.STRING, type(restored, "f_char").getType());
  }

  @Test
  void testNothingToRestore() {
    HoodieSchema legacy = record(HoodieSchema.create(HoodieSchemaType.INT), HoodieSchema.create(HoodieSchemaType.INT),
        HoodieSchema.create(HoodieSchemaType.STRING));
    HoodieSchema converted = InternalSchemaConverter.convert(InternalSchemaConverter.convert(legacy), "r");
    assertSame(converted, NarrowTypeRestorer.restore(converted, legacy));
    assertSame(converted, NarrowTypeRestorer.restore(converted, Collections.singletonList(null)));
  }
}
