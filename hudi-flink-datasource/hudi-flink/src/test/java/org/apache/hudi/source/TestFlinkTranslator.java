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

package org.apache.hudi.source;

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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.TypeLiteralExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Contract test of {@link FlinkTranslator}, plus the Flink-specific shapes: a literal on the left, NOT_EQUALS, CAST,
 * literal typing and exactness. Engine expressions are built like in {@code TestExpressionEvaluators}.
 */
public class TestFlinkTranslator extends ExpressionTranslatorContractTest<ResolvedExpression> {

  private static final Map<String, DataType> FLINK_TYPES = new HashMap<>();

  static {
    FLINK_TYPES.put("c_bool", DataTypes.BOOLEAN());
    FLINK_TYPES.put("c_int", DataTypes.INT());
    FLINK_TYPES.put("c_long", DataTypes.BIGINT());
    FLINK_TYPES.put("c_double", DataTypes.DOUBLE());
    FLINK_TYPES.put("c_string", DataTypes.STRING());
    FLINK_TYPES.put("c_decimal", DataTypes.DECIMAL(10, 2));
    FLINK_TYPES.put("c_date", DataTypes.DATE());
    FLINK_TYPES.put("c_timestamp", DataTypes.TIMESTAMP(6));
  }

  private final FlinkTranslator translator = new FlinkTranslator();

  @Override
  protected ExpressionTranslator<ResolvedExpression> translator() {
    return translator;
  }

  @Override
  protected Option<ResolvedExpression> engineLeaf(ExpressionSamples.Leaf leaf) {
    ExpressionSamples.Column c = leaf.column();
    FieldReferenceExpression ref = ref(c.name());
    switch (leaf.op()) {
      case EQ:
        return Option.of(call(BuiltInFunctionDefinitions.EQUALS, ref, literal(c, c.literal())));
      case LT:
        return Option.of(call(BuiltInFunctionDefinitions.LESS_THAN, ref, literal(c, c.literal())));
      case LT_EQ:
        return Option.of(call(BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL, ref, literal(c, c.literal())));
      case GT:
        return Option.of(call(BuiltInFunctionDefinitions.GREATER_THAN, ref, literal(c, c.literal())));
      case GT_EQ:
        return Option.of(call(BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL, ref, literal(c, c.literal())));
      case IN:
        return Option.of(call(BuiltInFunctionDefinitions.IN, ref, literal(c, c.literal()), literal(c, c.otherLiteral())));
      case IS_NULL:
        return Option.of(call(BuiltInFunctionDefinitions.IS_NULL, ref));
      case IS_NOT_NULL:
        return Option.of(call(BuiltInFunctionDefinitions.IS_NOT_NULL, ref));
      case STARTS_WITH:
        return Option.of(call(BuiltInFunctionDefinitions.LIKE, ref, new ValueLiteralExpression(c.literal().getValue() + "%")));
      case CONTAINS:
        return Option.of(call(BuiltInFunctionDefinitions.LIKE, ref, new ValueLiteralExpression("%" + c.literal().getValue() + "%")));
      default:
        return Option.empty();
    }
  }

  @Override
  protected ResolvedExpression engineAnd(ResolvedExpression left, ResolvedExpression right) {
    return call(BuiltInFunctionDefinitions.AND, left, right);
  }

  @Override
  protected ResolvedExpression engineOr(ResolvedExpression left, ResolvedExpression right) {
    return call(BuiltInFunctionDefinitions.OR, left, right);
  }

  @Override
  protected ResolvedExpression engineNot(ResolvedExpression child) {
    return call(BuiltInFunctionDefinitions.NOT, child);
  }

  @Override
  protected ResolvedExpression untranslatable() {
    // a function call neither ExpressionPredicates nor ExpressionEvaluators handles
    return call(BuiltInFunctionDefinitions.LIKE, ref("c_string"), new ValueLiteralExpression("%b%"));
  }

  // -------------------------------------------------------------------------
  //  Flink-specific shapes
  // -------------------------------------------------------------------------

  @Test
  void literalOnTheLeftFlipsTheOperator() {
    assertTranslates(call(BuiltInFunctionDefinitions.LESS_THAN, lit(5), ref("c_int")), Predicates.gt(col("c_int"), intLit(5)));
    assertTranslates(call(BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL, lit(5), ref("c_int")), Predicates.gteq(col("c_int"), intLit(5)));
    assertTranslates(call(BuiltInFunctionDefinitions.GREATER_THAN, lit(5), ref("c_int")), Predicates.lt(col("c_int"), intLit(5)));
    assertTranslates(call(BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL, lit(5), ref("c_int")), Predicates.lteq(col("c_int"), intLit(5)));
    assertTranslates(call(BuiltInFunctionDefinitions.EQUALS, lit(5), ref("c_int")), Predicates.eq(col("c_int"), intLit(5)));
  }

  @Test
  void notEqualsIsNotOfEquals() {
    ResolvedExpression notEquals = call(BuiltInFunctionDefinitions.NOT_EQUALS, ref("c_int"), lit(5));
    assertTranslates(notEquals, Predicates.not(Predicates.eq(col("c_int"), intLit(5))));
    assertTranslates(call(BuiltInFunctionDefinitions.NOT_EQUALS, lit(5), ref("c_int")), Predicates.not(Predicates.eq(col("c_int"), intLit(5))));
    // NOT is pushed down: NOT(a <> v) is a = v
    assertTranslates(call(BuiltInFunctionDefinitions.NOT, notEquals), Predicates.eq(col("c_int"), intLit(5)));
    // a <> v OR untranslatable keeps every row
    assertNothing(call(BuiltInFunctionDefinitions.OR, notEquals, untranslatable()));
  }

  @Test
  void orderPreservingCastIsStripped() {
    // CAST(c_int AS BIGINT) < 5L -> c_int < 5, the literal typed as the INT column
    assertTranslates(call(BuiltInFunctionDefinitions.LESS_THAN, cast(ref("c_int"), DataTypes.BIGINT()), lit(5L)), Predicates.lt(col("c_int"), intLit(5)));
    assertTranslates(call(BuiltInFunctionDefinitions.IS_NULL, cast(ref("c_int"), DataTypes.BIGINT())), Predicates.isNull(col("c_int")));
    assertTranslates(call(BuiltInFunctionDefinitions.EQUALS, cast(ref("c_int"), DataTypes.DOUBLE()), lit(5.0d)), Predicates.eq(col("c_int"), intLit(5)));
    assertTranslates(call(BuiltInFunctionDefinitions.GREATER_THAN, cast(ref("c_decimal"), DataTypes.DECIMAL(12, 3)), lit(new BigDecimal("12.340"), DataTypes.DECIMAL(12, 3))),
        Predicates.gt(col("c_decimal"), new Literal<>(new BigDecimal("12.34"), HoodieSchema.createDecimal(10, 2))));
    assertTranslates(call(BuiltInFunctionDefinitions.IN, cast(ref("c_int"), DataTypes.BIGINT()), lit(5L), lit(7L)),
        Predicates.in(col("c_int"), Arrays.asList(intLit(5), intLit(7))));
  }

  @Test
  void castThatDoesNotPreserveOrderDropsTheLeaf() {
    // narrowing, or to a type with another order
    assertNothing(call(BuiltInFunctionDefinitions.LESS_THAN, cast(ref("c_long"), DataTypes.INT()), lit(5)));
    assertNothing(call(BuiltInFunctionDefinitions.LESS_THAN, cast(ref("c_double"), DataTypes.FLOAT()), lit(1.5f)));
    assertNothing(call(BuiltInFunctionDefinitions.LESS_THAN, cast(ref("c_int"), DataTypes.STRING()), lit("5")));
    assertNothing(call(BuiltInFunctionDefinitions.LESS_THAN, cast(ref("c_long"), DataTypes.DOUBLE()), lit(5.0d)));
    assertNothing(call(BuiltInFunctionDefinitions.LESS_THAN, cast(ref("c_timestamp"), DataTypes.TIMESTAMP(3)), lit(LocalDateTime.of(2024, 1, 1, 0, 0))));
    // in an AND the dropped leaf leaves the other conjunct
    ResolvedExpression p = call(BuiltInFunctionDefinitions.EQUALS, ref("c_int"), lit(5));
    assertTranslates(call(BuiltInFunctionDefinitions.AND, p, call(BuiltInFunctionDefinitions.LESS_THAN, cast(ref("c_long"), DataTypes.INT()), lit(5))),
        Predicates.eq(col("c_int"), intLit(5)));
  }

  @Test
  void literalNotExactInTheColumnTypeDropsTheLeaf() {
    assertNothing(call(BuiltInFunctionDefinitions.EQUALS, cast(ref("c_int"), DataTypes.BIGINT()), lit(5_000_000_000L)));
    assertNothing(call(BuiltInFunctionDefinitions.LESS_THAN, cast(ref("c_int"), DataTypes.DOUBLE()), lit(5.5d)));
    assertNothing(call(BuiltInFunctionDefinitions.EQUALS, ref("c_decimal"), lit(new BigDecimal("12.345"), DataTypes.DECIMAL(5, 3))));
    // sub-microsecond timestamp on a micros column
    assertNothing(call(BuiltInFunctionDefinitions.EQUALS, ref("c_timestamp"), lit(LocalDateTime.of(2024, 1, 1, 0, 0, 0, 1), DataTypes.TIMESTAMP(9))));
    // another type family
    assertNothing(call(BuiltInFunctionDefinitions.EQUALS, ref("c_string"), lit(5)));
    assertNothing(call(BuiltInFunctionDefinitions.EQUALS, ref("c_date"), lit("2024-01-01")));
    // one bad IN value drops the IN
    assertNothing(call(BuiltInFunctionDefinitions.IN, cast(ref("c_int"), DataTypes.BIGINT()), lit(5L), lit(5_000_000_000L)));
  }

  @Test
  void otherShapesAreUntranslatable() {
    // a column missing from the schema
    assertNothing(call(BuiltInFunctionDefinitions.EQUALS, new FieldReferenceExpression("c_missing", DataTypes.INT(), 0, 99), lit(5)));
    // a NULL literal
    assertNothing(call(BuiltInFunctionDefinitions.EQUALS, ref("c_int"), new ValueLiteralExpression(null, DataTypes.INT())));
    assertNothing(call(BuiltInFunctionDefinitions.IN, ref("c_int"), lit(5), new ValueLiteralExpression(null, DataTypes.INT())));
    // field against field, literal against literal, IN with the field last
    assertNothing(call(BuiltInFunctionDefinitions.EQUALS, ref("c_int"), ref("c_long")));
    assertNothing(call(BuiltInFunctionDefinitions.EQUALS, lit(5), lit(5)));
    assertNothing(call(BuiltInFunctionDefinitions.IN, lit(5), ref("c_int")));
    // a function over the field
    assertNothing(call(BuiltInFunctionDefinitions.EQUALS, call(BuiltInFunctionDefinitions.UPPER, ref("c_string")), lit("ABC")));
  }

  @Test
  void literalValuesAreTypedFromTheColumn() {
    HoodieSchema schema = HoodieSchema.createRecord("t", "ns", null, Arrays.asList(
        HoodieSchemaField.of("ts_millis", HoodieSchema.createNullable(HoodieSchema.createTimestampMillis())),
        HoodieSchemaField.of("ts_ltz", HoodieSchema.createNullable(HoodieSchema.createLocalTimestampMicros())),
        HoodieSchemaField.of("s", HoodieSchema.create(HoodieSchemaType.STRING)),
        HoodieSchemaField.of("d", HoodieSchema.createDecimal(10, 2)),
        HoodieSchemaField.of("f", HoodieSchema.create(HoodieSchemaType.FLOAT))));
    LocalDateTime ldt = LocalDateTime.of(2024, 1, 2, 3, 4, 5, 6_000_000);
    long millis = ldt.toInstant(ZoneOffset.UTC).toEpochMilli();
    assertTranslates(schema, call(BuiltInFunctionDefinitions.EQUALS, field("ts_millis", DataTypes.TIMESTAMP(3)), lit(ldt, DataTypes.TIMESTAMP(3))),
        Predicates.eq(col("ts_millis"), new Literal<>(millis, HoodieSchema.createTimestampMillis())));
    ValueLiteralExpression timestampData = lit(TimestampData.fromLocalDateTime(ldt), DataTypes.TIMESTAMP(3).bridgedTo(TimestampData.class));
    assertTranslates(schema, call(BuiltInFunctionDefinitions.EQUALS, field("ts_millis", DataTypes.TIMESTAMP(3)), timestampData),
        Predicates.eq(col("ts_millis"), new Literal<>(millis, HoodieSchema.createTimestampMillis())));
    Instant instant = Instant.ofEpochSecond(1_700_000_000L, 123_000);
    assertTranslates(schema, call(BuiltInFunctionDefinitions.GREATER_THAN, field("ts_ltz", DataTypes.TIMESTAMP_LTZ(6)), lit(instant, DataTypes.TIMESTAMP_LTZ(6))),
        Predicates.gt(col("ts_ltz"), new Literal<>(1_700_000_000_000_123L, HoodieSchema.createLocalTimestampMicros())));
    // a TIMESTAMP (wall clock) literal does not compare with a TIMESTAMP_LTZ column without a cast
    assertNothing(schema, call(BuiltInFunctionDefinitions.GREATER_THAN, field("ts_ltz", DataTypes.TIMESTAMP_LTZ(6)), lit(ldt, DataTypes.TIMESTAMP(6))));
    // (a ValueLiteralExpression cannot hold StringData: Flink rejects BinaryStringData as a STRING literal value)
    assertTranslates(schema, call(BuiltInFunctionDefinitions.EQUALS, field("s", DataTypes.STRING()), lit("x", DataTypes.STRING())),
        Predicates.eq(col("s"), new Literal<>("x", HoodieSchema.create(HoodieSchemaType.STRING))));
    assertTranslates(schema, call(BuiltInFunctionDefinitions.EQUALS, field("d", DataTypes.DECIMAL(10, 2)),
            lit(DecimalData.fromBigDecimal(new BigDecimal("1.50"), 10, 2), DataTypes.DECIMAL(10, 2).bridgedTo(DecimalData.class))),
        Predicates.eq(col("d"), new Literal<>(new BigDecimal("1.50"), HoodieSchema.createDecimal(10, 2))));
    assertTranslates(schema, call(BuiltInFunctionDefinitions.LESS_THAN, cast(field("f", DataTypes.FLOAT()), DataTypes.DOUBLE()), lit(1.5d)),
        Predicates.lt(col("f"), new Literal<>(1.5f, HoodieSchema.create(HoodieSchemaType.FLOAT))));
    assertNothing(schema, call(BuiltInFunctionDefinitions.LESS_THAN, cast(field("f", DataTypes.FLOAT()), DataTypes.DOUBLE()), lit(1.1d)));
  }

  /**
   * INT8 and INT16 columns, which Flink reads as INT, translate as INT columns, and CHAR columns, read as STRING, as
   * STRING ones; the literals take the column's type.
   */
  @Test
  void narrowIntsAndCharTranslateAsIntAndString() {
    HoodieSchema int8 = HoodieSchema.createInt8();
    HoodieSchema int16 = HoodieSchema.createInt16();
    HoodieSchema char3 = HoodieSchema.createChar(3);
    HoodieSchema schema = HoodieSchema.createRecord("t", "ns", null, Arrays.asList(
        HoodieSchemaField.of("tiny", HoodieSchema.createNullable(int8)),
        HoodieSchemaField.of("small", int16),
        HoodieSchemaField.of("code", char3)));
    assertTranslates(schema, call(BuiltInFunctionDefinitions.EQUALS, field("tiny", DataTypes.INT()), lit(5)),
        Predicates.eq(col("tiny"), new Literal<>(5, int8)));
    assertTranslates(schema, call(BuiltInFunctionDefinitions.LESS_THAN, field("small", DataTypes.INT()), lit(300L)),
        Predicates.lt(col("small"), new Literal<>(300, int16)));
    assertNothing(schema, call(BuiltInFunctionDefinitions.EQUALS, field("small", DataTypes.INT()), lit(5_000_000_000L)));
    assertTranslates(schema, call(BuiltInFunctionDefinitions.EQUALS, field("code", DataTypes.STRING()), lit("ab ", DataTypes.STRING())),
        Predicates.eq(col("code"), new Literal<>("ab ", char3)));
    assertNothing(schema, call(BuiltInFunctionDefinitions.EQUALS, field("code", DataTypes.STRING()), lit(5)));
    assertEquals(7, FlinkTranslator.canonicalValue(7, LogicalTypeRoot.TINYINT, int8));
  }

  @Test
  void literalJavaValuesAreCanonical() {
    Expression translated = translator.toHudi(call(BuiltInFunctionDefinitions.EQUALS, ref("c_date"), lit(LocalDate.ofEpochDay(19000))), schema()).get();
    Object value = ((Literal<?>) translated.getChildren().get(1)).getValue();
    assertEquals(Integer.valueOf(19000), value);
    translated = translator.toHudi(call(BuiltInFunctionDefinitions.EQUALS, ref("c_timestamp"), lit(micros(1700000000000000L), DataTypes.TIMESTAMP(6))), schema()).get();
    assertEquals(Long.valueOf(1700000000000000L), ((Literal<?>) translated.getChildren().get(1)).getValue());
  }

  @Test
  void combinatorsTakeEveryOperand() {
    // Flink AND / OR may have more than two operands
    ResolvedExpression a = call(BuiltInFunctionDefinitions.EQUALS, ref("c_int"), lit(1));
    ResolvedExpression b = call(BuiltInFunctionDefinitions.EQUALS, ref("c_int"), lit(2));
    ResolvedExpression c = call(BuiltInFunctionDefinitions.EQUALS, ref("c_int"), lit(3));
    Expression ha = Predicates.eq(col("c_int"), intLit(1));
    Expression hb = Predicates.eq(col("c_int"), intLit(2));
    Expression hc = Predicates.eq(col("c_int"), intLit(3));
    assertTranslates(call(BuiltInFunctionDefinitions.OR, a, b, c), Predicates.or(Predicates.or(ha, hb), hc));
    assertNothing(call(BuiltInFunctionDefinitions.OR, a, b, untranslatable()));
    assertTranslates(call(BuiltInFunctionDefinitions.AND, a, untranslatable(), c), Predicates.and(ha, hc));
    // boolean literals
    assertNothing(new ValueLiteralExpression(true));
    assertTranslates(new ValueLiteralExpression(false), Predicates.alwaysFalse());
  }

  // -------------------------------------------------------------------------
  //  Helpers
  // -------------------------------------------------------------------------

  private void assertTranslates(ResolvedExpression engine, Expression expected) {
    assertTranslates(schema(), engine, expected);
  }

  private void assertTranslates(HoodieSchema schema, ResolvedExpression engine, Expression expected) {
    Option<Expression> translated = translator.toHudi(engine, schema);
    assertTrue(translated.isPresent(), engine + " did not translate; expected " + expected);
    ExpressionEquivalence.assertEquivalent(expected, translated.get());
  }

  private void assertNothing(ResolvedExpression engine) {
    assertNothing(schema(), engine);
  }

  private void assertNothing(HoodieSchema schema, ResolvedExpression engine) {
    Option<Expression> translated = translator.toHudi(engine, schema);
    assertFalse(translated.isPresent(), engine + " must not translate but was " + (translated.isPresent() ? translated.get() : ""));
  }

  private static FieldReferenceExpression ref(String name) {
    int index = ExpressionSamples.COLUMNS.indexOf(ExpressionSamples.column(name));
    return new FieldReferenceExpression(name, FLINK_TYPES.get(name), 0, index);
  }

  private static FieldReferenceExpression field(String name, DataType type) {
    return new FieldReferenceExpression(name, type, 0, 0);
  }

  private static NameReference col(String name) {
    return new NameReference(name);
  }

  private static Literal<Integer> intLit(int value) {
    return new Literal<>(value, HoodieSchema.create(HoodieSchemaType.INT));
  }

  private static ValueLiteralExpression lit(Object value) {
    return new ValueLiteralExpression(value);
  }

  private static ValueLiteralExpression lit(Object value, DataType type) {
    return new ValueLiteralExpression(value, type.notNull());
  }

  /**
   * The Flink literal of a sample value: DATE as {@code LocalDate}, TIMESTAMP as {@code LocalDateTime}.
   */
  private static ValueLiteralExpression literal(ExpressionSamples.Column c, Literal<?> sample) {
    Object value = sample.getValue();
    switch (c.schemaType()) {
      case DATE:
        value = LocalDate.ofEpochDay((Integer) value);
        break;
      case TIMESTAMP:
        value = micros((Long) value);
        break;
      default:
        break;
    }
    return lit(value, FLINK_TYPES.get(c.name()));
  }

  private static LocalDateTime micros(long micros) {
    return LocalDateTime.ofEpochSecond(Math.floorDiv(micros, 1_000_000L), (int) Math.floorMod(micros, 1_000_000L) * 1000, ZoneOffset.UTC);
  }

  private static CallExpression cast(ResolvedExpression operand, DataType type) {
    return CallExpression.permanent(BuiltInFunctionDefinitions.CAST, Arrays.asList(operand, new TypeLiteralExpression(type)), type);
  }

  private static CallExpression call(BuiltInFunctionDefinition definition, ResolvedExpression... args) {
    List<ResolvedExpression> children = Arrays.asList(args);
    DataType output = definition == BuiltInFunctionDefinitions.UPPER ? DataTypes.STRING() : DataTypes.BOOLEAN();
    return CallExpression.permanent(definition, children, output);
  }
}
