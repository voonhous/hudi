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

package org.apache.hudi.common.expression;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.schema.internal.Type;
import org.apache.hudi.common.schema.internal.Types;
import org.apache.hudi.common.schema.internal.convert.InternalSchemaConverter;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests that typing expressions with {@link HoodieSchema} keeps the behaviour they had when typed with
 * InternalSchema types: the class tags {@link BoundReference} reads values with, the types
 * {@link Comparators} supports, and the types {@link Literal#from} gives.
 */
class TestExpressionTypes {

  /**
   * The InternalSchema types {@code Comparators} had a comparator for before the move to HoodieSchema.
   */
  private static final Set<Type> TYPES_WITH_COMPARATOR = new HashSet<>(Arrays.asList(
      Types.BooleanType.get(),
      Types.IntType.get(),
      Types.LongType.get(),
      Types.FloatType.get(),
      Types.DoubleType.get(),
      Types.DateType.get(),
      Types.TimeType.get(),
      Types.TimestampType.get(),
      Types.TimestampMillisType.get(),
      Types.LocalTimestampMillisType.get(),
      Types.LocalTimestampMicrosType.get(),
      Types.StringType.get(),
      Types.UUIDType.get()));

  private static Stream<Arguments> leafAndNestedSchemas() {
    HoodieSchema record = HoodieSchema.createRecord("rec", null, null,
        Collections.singletonList(HoodieSchemaField.of("a", HoodieSchema.create(HoodieSchemaType.INT))));
    return Stream.of(
        HoodieSchema.create(HoodieSchemaType.BOOLEAN),
        HoodieSchema.create(HoodieSchemaType.INT),
        HoodieSchema.create(HoodieSchemaType.LONG),
        HoodieSchema.create(HoodieSchemaType.FLOAT),
        HoodieSchema.create(HoodieSchemaType.DOUBLE),
        HoodieSchema.create(HoodieSchemaType.STRING),
        HoodieSchema.create(HoodieSchemaType.BYTES),
        HoodieSchema.createEnum("e", null, null, Arrays.asList("A", "B")),
        HoodieSchema.createFixed("f", null, null, 4),
        HoodieSchema.createDecimal(10, 2),
        HoodieSchema.createDecimal("dec_fixed", null, null, 10, 2, 5),
        HoodieSchema.createDate(),
        HoodieSchema.createTimeMillis(),
        HoodieSchema.createTimeMicros(),
        HoodieSchema.createTimestampMillis(),
        HoodieSchema.createTimestampMicros(),
        HoodieSchema.createLocalTimestampMillis(),
        HoodieSchema.createLocalTimestampMicros(),
        HoodieSchema.createUUID(),
        HoodieSchema.createVector(3),
        HoodieSchema.createArray(HoodieSchema.create(HoodieSchemaType.INT)),
        HoodieSchema.createMap(HoodieSchema.create(HoodieSchemaType.INT)),
        record,
        HoodieSchema.createVariant(),
        HoodieSchema.createBlob())
        .map(schema -> Arguments.of(schema.getType() + " " + schema.getName(), schema));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("leafAndNestedSchemas")
  void testClassTagMatchesInternalSchemaClassTag(String name, HoodieSchema schema) {
    Type internalType = InternalSchemaConverter.convertToField(schema);
    assertNotNull(internalType);
    assertSame(internalType.typeId().getClassTag(), ExpressionTypes.classTag(schema));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("leafAndNestedSchemas")
  void testIsNestedMatchesInternalSchema(String name, HoodieSchema schema) {
    Type internalType = InternalSchemaConverter.convertToField(schema);
    assertEquals(internalType.isNestedType(), ExpressionTypes.isNested(schema));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("leafAndNestedSchemas")
  void testComparatorSupportMatchesInternalSchema(String name, HoodieSchema schema) {
    Type internalType = InternalSchemaConverter.convertToField(schema);
    if (TYPES_WITH_COMPARATOR.contains(internalType)) {
      assertNotNull(Comparators.forType(schema));
    } else {
      UnsupportedOperationException e = assertThrows(UnsupportedOperationException.class, () -> Comparators.forType(schema));
      assertTrue(e.getMessage().contains("doesn't support comparator yet"), e::getMessage);
    }
  }

  @Test
  void testComparatorIsNaturalOrder() {
    assertTrue(Comparators.<Integer>forType(HoodieSchema.create(HoodieSchemaType.INT)).compare(1, 2) < 0);
    assertTrue(Comparators.<String>forType(HoodieSchema.create(HoodieSchemaType.STRING)).compare("b", "a") > 0);
    assertEquals(0, Comparators.<Long>forType(HoodieSchema.createTimestampMicros()).compare(5L, 5L));
  }

  @Test
  void testClassTagRejectsNonLeafTypes() {
    assertThrows(IllegalArgumentException.class, () -> ExpressionTypes.classTag(null));
    assertThrows(IllegalArgumentException.class, () -> ExpressionTypes.classTag(HoodieSchema.create(HoodieSchemaType.NULL)));
    assertThrows(IllegalArgumentException.class, () -> ExpressionTypes.classTag(HoodieSchema.createNullable(HoodieSchemaType.INT)));
    assertThrows(UnsupportedOperationException.class, () -> Comparators.forType(HoodieSchema.createNullable(HoodieSchemaType.INT)));
  }

  @Test
  void testBoundReferenceReadsWithClassTag() {
    ArrayData data = new ArrayData(Arrays.asList(7, "x", 9L));
    assertEquals(7, new BoundReference(0, HoodieSchema.createDate()).eval(data));
    assertEquals("x", new BoundReference(1, HoodieSchema.create(HoodieSchemaType.STRING)).eval(data));
    assertEquals(9L, new BoundReference(2, HoodieSchema.createTimestampMillis()).eval(data));
    // The class tag is enforced: a long is not read as an int.
    assertThrows(ClassCastException.class, () -> new BoundReference(2, HoodieSchema.create(HoodieSchemaType.INT)).eval(data));
    assertEquals("boundReference[ordinal: 1, type: string]",
        new BoundReference(1, HoodieSchema.create(HoodieSchemaType.STRING)).toString());
  }

  private static Stream<Arguments> literalValues() {
    byte[] bytes = new byte[] {1, 2, 3};
    UUID uuid = UUID.randomUUID();
    return Stream.of(
        Arguments.of(5, 5, HoodieSchema.create(HoodieSchemaType.INT)),
        Arguments.of((short) 5, (short) 5, HoodieSchema.create(HoodieSchemaType.INT)),
        Arguments.of((byte) 5, 5, HoodieSchema.create(HoodieSchemaType.INT)),
        Arguments.of(5L, 5L, HoodieSchema.create(HoodieSchemaType.LONG)),
        Arguments.of(true, true, HoodieSchema.create(HoodieSchemaType.BOOLEAN)),
        Arguments.of(1.5d, 1.5d, HoodieSchema.create(HoodieSchemaType.DOUBLE)),
        Arguments.of(1.5f, 1.5f, HoodieSchema.create(HoodieSchemaType.FLOAT)),
        Arguments.of(new BigDecimal("123.45"), new BigDecimal("123.45"), HoodieSchema.createDecimal(5, 2)),
        Arguments.of("abc", "abc", HoodieSchema.create(HoodieSchemaType.STRING)),
        Arguments.of(new StringBuilder("abc"), null, HoodieSchema.create(HoodieSchemaType.STRING)),
        Arguments.of(bytes, ByteBuffer.wrap(bytes), HoodieSchema.createFixed("fixed_3", null, null, 3)),
        Arguments.of(ByteBuffer.wrap(bytes), ByteBuffer.wrap(bytes), HoodieSchema.create(HoodieSchemaType.BYTES)),
        Arguments.of(uuid, uuid, HoodieSchema.createUUID()));
  }

  @ParameterizedTest
  @MethodSource("literalValues")
  void testLiteralFromTypes(Object value, Object expectedValue, HoodieSchema expectedType) {
    Literal<?> literal = Literal.from(value);
    assertEquals(expectedType, literal.getDataType());
    if (expectedValue != null) {
      assertEquals(expectedValue, literal.getValue());
    } else {
      assertSame(value, literal.getValue());
    }
  }

  @Test
  void testLiteralFromDecimalOutsideDecimalSchemaBounds() {
    // Precision 1, scale 3: typed decimal(3, 3) rather than failing.
    Literal<?> small = Literal.from(new BigDecimal("0.001"));
    assertEquals(HoodieSchema.createDecimal(3, 3), small.getDataType());
    assertEquals(new BigDecimal("0.001"), small.getValue());
    // Precision 1, scale -3: typed decimal(4, 0) rather than failing.
    Literal<?> large = Literal.from(new BigDecimal("1E+3"));
    assertEquals(HoodieSchema.createDecimal(4, 0), large.getDataType());
    assertEquals(new BigDecimal("1E+3"), large.getValue());
  }

  @Test
  void testLiteralFromUnsupportedValue() {
    assertThrows(IllegalArgumentException.class, () -> Literal.from(new Object()));
  }

  @Test
  void testPredicateTypeIsBoolean() {
    assertEquals(HoodieSchema.create(HoodieSchemaType.BOOLEAN), Predicates.alwaysTrue().getDataType());
    assertEquals(HoodieSchema.create(HoodieSchemaType.BOOLEAN),
        Predicates.eq(Literal.from(1), Literal.from(1)).getDataType());
  }
}
