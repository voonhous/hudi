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

package org.apache.hudi.metadata;

import org.apache.hudi.common.expression.ArrayData;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.schema.internal.Type;
import org.apache.hudi.common.schema.internal.convert.InternalSchemaConverter;
import org.apache.hudi.common.util.PartitionPathEncodeUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests that {@link AbstractHoodieTableMetadata#extractPartitionValues} parses partition paths against a
 * {@link HoodieSchema} partition record exactly as it did against the equivalent InternalSchema record.
 */
class TestAbstractHoodieTableMetadataPartitionValues {

  private static HoodieSchema partitionRecord(HoodieSchema... fieldSchemas) {
    List<HoodieSchemaField> fields = new ArrayList<>();
    for (int i = 0; i < fieldSchemas.length; i++) {
      fields.add(HoodieSchemaField.of("p" + i, HoodieSchema.createNullable(fieldSchemas[i])));
    }
    return HoodieSchema.createRecord("partition", null, null, fields);
  }

  private static Object oldParse(String value, HoodieSchema schema) {
    Type internalType = InternalSchemaConverter.convertToField(schema);
    return Type.fromPartitionString(value, internalType);
  }

  private static Stream<Arguments> supportedValues() {
    return Stream.of(
        Arguments.of(HoodieSchema.create(HoodieSchemaType.INT), "42"),
        Arguments.of(HoodieSchema.create(HoodieSchemaType.LONG), "4200000000"),
        Arguments.of(HoodieSchema.create(HoodieSchemaType.BOOLEAN), "true"),
        Arguments.of(HoodieSchema.create(HoodieSchemaType.FLOAT), "1.5"),
        Arguments.of(HoodieSchema.create(HoodieSchemaType.DOUBLE), "2.25"),
        Arguments.of(HoodieSchema.createDecimal(10, 2), "12.34"),
        Arguments.of(HoodieSchema.createDecimal("dec", null, null, 10, 2, 5), "12.34"),
        Arguments.of(HoodieSchema.createUUID(), "123e4567-e89b-12d3-a456-426614174000"),
        Arguments.of(HoodieSchema.createDate(), "2023-04-02"),
        Arguments.of(HoodieSchema.create(HoodieSchemaType.STRING), "abc"),
        Arguments.of(HoodieSchema.createEnum("e", null, null, Arrays.asList("abc", "def")), "abc"),
        Arguments.of(HoodieSchema.create(HoodieSchemaType.STRING), PartitionPathEncodeUtils.DEFAULT_PARTITION_PATH),
        Arguments.of(HoodieSchema.create(HoodieSchemaType.INT), PartitionPathEncodeUtils.DEPRECATED_DEFAULT_PARTITION_PATH));
  }

  @ParameterizedTest
  @MethodSource("supportedValues")
  void testSingleFieldValueMatchesInternalSchemaParsing(HoodieSchema type, String value) {
    ArrayData values = AbstractHoodieTableMetadata.extractPartitionValues(partitionRecord(type), "p0=" + value, false);
    assertEquals(1, values.numFields());
    assertEquals(oldParse(value, type), values.get(0, Object.class));
  }

  @Test
  void testMultiFieldValuesMatchInternalSchemaParsing() {
    HoodieSchema[] types = new HoodieSchema[] {
        HoodieSchema.create(HoodieSchemaType.STRING), HoodieSchema.createDate(), HoodieSchema.create(HoodieSchemaType.INT)};
    List<String> rawValues = Arrays.asList("a b", "2023-04-02", "7");
    String path = "p0=" + PartitionPathEncodeUtils.escapePathName(rawValues.get(0)) + "/p1=" + rawValues.get(1) + "/p2=" + rawValues.get(2);

    ArrayData values = AbstractHoodieTableMetadata.extractPartitionValues(partitionRecord(types), path, true);

    List<Object> expected = Arrays.asList(oldParse(rawValues.get(0), types[0]), oldParse(rawValues.get(1), types[1]),
        oldParse(rawValues.get(2), types[2]));
    assertEquals(expected, Stream.of(0, 1, 2).map(i -> values.get(i, Object.class)).collect(Collectors.toList()));
  }

  @Test
  void testSingleFieldValueKeepsSlashes() {
    ArrayData values = AbstractHoodieTableMetadata.extractPartitionValues(
        partitionRecord(HoodieSchema.create(HoodieSchemaType.STRING)), "p0=2023/04/02", false);
    assertEquals("2023/04/02", values.get(0, String.class));
  }

  @Test
  void testNullPartitionValue() {
    ArrayData values = AbstractHoodieTableMetadata.extractPartitionValues(
        partitionRecord(HoodieSchema.create(HoodieSchemaType.LONG)), "p0=" + PartitionPathEncodeUtils.DEFAULT_PARTITION_PATH, false);
    assertNull(values.get(0, Long.class));
  }

  private static Stream<Arguments> unsupportedTypes() {
    return Stream.of(
        Arguments.of(HoodieSchema.createTimestampMicros()),
        Arguments.of(HoodieSchema.createTimeMicros()),
        Arguments.of(HoodieSchema.create(HoodieSchemaType.BYTES)),
        Arguments.of(HoodieSchema.createFixed("f", null, null, 2)));
  }

  @ParameterizedTest
  @MethodSource("unsupportedTypes")
  void testUnsupportedTypesThrowAsBefore(HoodieSchema type) {
    assertThrows(UnsupportedOperationException.class, () -> oldParse("1", type));
    assertThrows(UnsupportedOperationException.class,
        () -> AbstractHoodieTableMetadata.extractPartitionValues(partitionRecord(type), "p0=1", false));
  }

  @Test
  void testPathPartitionLevel() {
    HoodieSchema record = partitionRecord(HoodieSchema.create(HoodieSchemaType.STRING), HoodieSchema.create(HoodieSchemaType.INT));
    assertEquals(0, AbstractHoodieTableMetadata.getPathPartitionLevel(record, ""));
    assertEquals(0, AbstractHoodieTableMetadata.getPathPartitionLevel(null, "p0=a"));
    assertEquals(1, AbstractHoodieTableMetadata.getPathPartitionLevel(record, "p0=a"));
    assertEquals(2, AbstractHoodieTableMetadata.getPathPartitionLevel(record, "p0=a/p1=1"));
    assertEquals(1, AbstractHoodieTableMetadata.getPathPartitionLevel(record, "/p0=a/p1=1"));
    // Pins today's parsing: the separator scan skips the first and last characters and then a leading
    // and a trailing separator are subtracted again, so a path wrapped in separators counts two levels short.
    assertEquals(0, AbstractHoodieTableMetadata.getPathPartitionLevel(record, "/p0=a/p1=1/"));
    assertEquals(1, AbstractHoodieTableMetadata.getPathPartitionLevel(
        HoodieSchema.createRecord("r", null, null, Collections.emptyList()), "p0=a"));
  }
}
