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

package org.apache.hudi.expression.translate;

import org.apache.hudi.common.expression.Expression;
import org.apache.hudi.common.expression.Literal;
import org.apache.hudi.common.expression.NameReference;
import org.apache.hudi.common.expression.Predicates;
import org.apache.hudi.common.expression.translate.ExpressionTranslator;
import org.apache.hudi.common.expression.translate.testkit.ExpressionEquivalence;
import org.apache.hudi.common.expression.translate.testkit.ExpressionSamples;
import org.apache.hudi.common.expression.translate.testkit.ExpressionTranslatorContractTest;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.util.Option;

import org.apache.spark.sql.sources.AlwaysFalse;
import org.apache.spark.sql.sources.AlwaysTrue;
import org.apache.spark.sql.sources.And;
import org.apache.spark.sql.sources.EqualNullSafe;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.GreaterThanOrEqual;
import org.apache.spark.sql.sources.In;
import org.apache.spark.sql.sources.IsNotNull;
import org.apache.spark.sql.sources.IsNull;
import org.apache.spark.sql.sources.LessThan;
import org.apache.spark.sql.sources.LessThanOrEqual;
import org.apache.spark.sql.sources.Not;
import org.apache.spark.sql.sources.Or;
import org.apache.spark.sql.sources.StringContains;
import org.apache.spark.sql.sources.StringEndsWith;
import org.apache.spark.sql.sources.StringStartsWith;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Contract test of {@link SparkTranslator}, plus the typing rules the contract does not cover.
 */
class TestSparkTranslator extends ExpressionTranslatorContractTest<Filter> {

  private static final long MICROS = 1700000000000000L;

  @Override
  protected ExpressionTranslator<Filter> translator() {
    return new SparkTranslator();
  }

  /**
   * The values Spark puts in data source filters with its default (non-Java 8) datetime API.
   */
  static Object sparkValue(ExpressionSamples.Column column, Object canonical) {
    switch (column.schemaType()) {
      case DATE:
        return Date.valueOf(LocalDate.ofEpochDay((Integer) canonical));
      case TIMESTAMP:
        return Timestamp.from(toInstant((Long) canonical));
      default:
        return canonical;
    }
  }

  static Instant toInstant(long micros) {
    return Instant.ofEpochSecond(Math.floorDiv(micros, 1_000_000L), Math.floorMod(micros, 1_000_000L) * 1000);
  }

  @Override
  protected Option<Filter> engineLeaf(ExpressionSamples.Leaf leaf) {
    ExpressionSamples.Column c = leaf.column();
    String name = c.name();
    Object v = sparkValue(c, c.literal().getValue());
    switch (leaf.op()) {
      case EQ:
        return Option.of(new EqualTo(name, v));
      case LT:
        return Option.of(new LessThan(name, v));
      case LT_EQ:
        return Option.of(new LessThanOrEqual(name, v));
      case GT:
        return Option.of(new GreaterThan(name, v));
      case GT_EQ:
        return Option.of(new GreaterThanOrEqual(name, v));
      case IN:
        return Option.of(new In(name, new Object[] {v, sparkValue(c, c.otherLiteral().getValue())}));
      case IS_NULL:
        return Option.of(new IsNull(name));
      case IS_NOT_NULL:
        return Option.of(new IsNotNull(name));
      case STARTS_WITH:
        return Option.of(new StringStartsWith(name, (String) v));
      case CONTAINS:
        return Option.of(new StringContains(name, (String) v));
      default:
        return Option.empty();
    }
  }

  @Override
  protected Filter engineAnd(Filter left, Filter right) {
    return new And(left, right);
  }

  @Override
  protected Filter engineOr(Filter left, Filter right) {
    return new Or(left, right);
  }

  @Override
  protected Filter engineNot(Filter child) {
    return new Not(child);
  }

  @Override
  protected Filter untranslatable() {
    return new StringEndsWith("c_string", "c");
  }

  private static Option<Expression> translate(Filter filter) {
    return new SparkTranslator().toHudi(filter, ExpressionSamples.SCHEMA);
  }

  private static void assertTranslates(Filter filter, Expression expected) {
    Option<Expression> translated = translate(filter);
    assertTrue(translated.isPresent(), filter + " did not translate");
    ExpressionEquivalence.assertEquivalent(expected, translated.get());
  }

  private static void assertDropped(Filter filter) {
    Option<Expression> translated = translate(filter);
    assertFalse(translated.isPresent(), filter + " must not translate but was " + (translated.isPresent() ? translated.get() : ""));
  }

  private static Expression eq(String column, Object value, HoodieSchema type) {
    return Predicates.eq(new NameReference(column), new Literal<>(value, type));
  }

  @Test
  void testJava8AndInternalValues() {
    // SparkFilterHelper.toLiteral types a LocalDate as a TIMESTAMP holding epoch days
    assertTranslates(new EqualTo("c_date", LocalDate.ofEpochDay(19000)), eq("c_date", 19000, HoodieSchema.createDate()));
    assertTranslates(new EqualTo("c_timestamp", toInstant(MICROS)), eq("c_timestamp", MICROS, HoodieSchema.createTimestampMicros()));
    assertTranslates(new EqualTo("c_string", UTF8String.fromString("abc")), eq("c_string", "abc", HoodieSchema.create(HoodieSchemaType.STRING)));
  }

  @Test
  void testLiteralsAreTypedFromTheColumn() {
    assertTranslates(new EqualTo("c_long", 5), eq("c_long", 5L, HoodieSchema.create(HoodieSchemaType.LONG)));
    assertTranslates(new EqualTo("c_int", 5L), eq("c_int", 5, HoodieSchema.create(HoodieSchemaType.INT)));
    assertTranslates(new EqualTo("c_double", 2), eq("c_double", 2.0d, HoodieSchema.create(HoodieSchemaType.DOUBLE)));
    assertTranslates(new EqualTo("c_decimal", 5), eq("c_decimal", new BigDecimal("5.00"), HoodieSchema.createDecimal(10, 2)));
    assertTranslates(new LessThan("c_decimal", new BigDecimal("1.5")), Predicates.lt(new NameReference("c_decimal"),
        new Literal<>(new BigDecimal("1.50"), HoodieSchema.createDecimal(10, 2))));
  }

  /**
   * Spark reads INT8 and INT16 columns as IntegerType and CHAR as StringType, so their filter values translate as INT
   * and STRING ones, typed as the column; the renderer turns them back into the same Spark values.
   */
  @Test
  void testNarrowIntsAndCharTranslateAsIntAndString() {
    HoodieSchema int8 = HoodieSchema.createInt8();
    HoodieSchema int16 = HoodieSchema.createInt16();
    HoodieSchema char3 = HoodieSchema.createChar(3);
    HoodieSchema schema = HoodieSchema.createRecord("t", "ns", null, Arrays.asList(
        HoodieSchemaField.of("tiny", HoodieSchema.createNullable(int8)),
        HoodieSchemaField.of("small", int16),
        HoodieSchemaField.of("code", char3)));
    SparkTranslator translator = new SparkTranslator();
    ExpressionEquivalence.assertEquivalent(eq("tiny", 5, int8), translator.toHudi(new EqualTo("tiny", (byte) 5), schema).get());
    ExpressionEquivalence.assertEquivalent(eq("small", 300, int16), translator.toHudi(new EqualTo("small", 300L), schema).get());
    assertFalse(translator.toHudi(new EqualTo("small", 1.5d), schema).isPresent());
    ExpressionEquivalence.assertEquivalent(eq("code", "ab ", char3), translator.toHudi(new EqualTo("code", "ab "), schema).get());
    assertFalse(translator.toHudi(new StringStartsWith("code", "a"), schema).isPresent());
    assertEquals(new EqualTo("tiny", 5), new SparkFilterRenderer().render(eq("tiny", 5, int8)).get());
    assertEquals(Integer.class, ((EqualTo) new SparkFilterRenderer().render(eq("small", 300, int16)).get()).value().getClass());
    assertEquals(new EqualTo("code", "ab "), new SparkFilterRenderer().render(eq("code", "ab ", char3)).get());
  }

  @Test
  void testInexactLiteralsAreDropped() {
    assertDropped(new LessThan("c_int", 1.5d));
    assertDropped(new LessThan("c_int", Long.MAX_VALUE));
    assertDropped(new LessThan("c_long", Double.NaN));
    assertDropped(new LessThan("c_decimal", new BigDecimal("1.234")));
    assertDropped(new LessThan("c_decimal", new BigDecimal("123456789.5")));
    assertDropped(new EqualTo("c_int", "5"));
    assertDropped(new EqualTo("c_date", toInstant(MICROS)));
    assertDropped(new StringStartsWith("c_int", "5"));
    // an inexact conjunct is dropped, the rest is kept
    assertTranslates(new And(new EqualTo("c_int", 5), new LessThan("c_int", 1.5d)), eq("c_int", 5, HoodieSchema.create(HoodieSchemaType.INT)));
  }

  @Test
  void testColumns() {
    assertDropped(new EqualTo("missing", 5));
    assertDropped(new IsNull("missing"));
    // nested fields are not supported
    assertDropped(new EqualTo("c_int.x", 5));
    assertDropped(new EqualTo("not a column", 5));
    // a single backtick-quoted part, as Spark writes names that need quoting when nested predicate pushdown is on
    assertTranslates(new EqualTo("`c_int`", 5), eq("c_int", 5, HoodieSchema.create(HoodieSchemaType.INT)));
  }

  @Test
  void testNulls() {
    NameReference ref = new NameReference("c_int");
    assertTranslates(new EqualNullSafe("c_int", null), Predicates.isNull(ref));
    assertTranslates(new Not(new EqualNullSafe("c_int", null)), Predicates.not(Predicates.isNull(ref)));
    Expression eq5 = eq("c_int", 5, HoodieSchema.create(HoodieSchemaType.INT));
    assertTranslates(new EqualNullSafe("c_int", 5), Predicates.and(Predicates.isNotNull(ref), eq5));
    assertDropped(new EqualTo("c_int", null));
    assertDropped(new LessThan("c_int", null));
    assertTranslates(new In("c_int", new Object[] {5, null}), Predicates.in(ref, Arrays.asList(new Literal<>(5, HoodieSchema.create(HoodieSchemaType.INT)))));
    assertDropped(new In("c_int", new Object[] {null}));
  }

  @Test
  void testConstants() {
    assertDropped(new AlwaysTrue());
    assertTranslates(new AlwaysFalse(), Predicates.alwaysFalse());
    assertTranslates(new Not(new AlwaysTrue()), Predicates.alwaysFalse());
  }

  @Test
  void testTimestampMillisColumn() {
    List<HoodieSchemaField> fields = new ArrayList<>();
    fields.add(HoodieSchemaField.of("ts", HoodieSchema.createNullable(HoodieSchema.createTimestampMillis())));
    HoodieSchema schema = HoodieSchema.createRecord("millis", null, null, fields);
    Expression expected = Predicates.gt(new NameReference("ts"), new Literal<>(1700000000123L, HoodieSchema.createTimestampMillis()));
    ExpressionEquivalence.assertEquivalent(expected,
        new SparkTranslator().toHudi(new GreaterThan("ts", Timestamp.from(Instant.ofEpochMilli(1700000000123L))), schema).get());
    // a micros value has no exact millis value
    assertFalse(new SparkTranslator().toHudi(new GreaterThan("ts", toInstant(1700000000123456L)), schema).isPresent());
  }
}
