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

package org.apache.hudi.hadoop.expression;

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
import org.apache.hudi.hadoop.utils.HiveTypeUtils;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.UDFLike;
import org.apache.hadoop.hive.ql.udf.UDFToDouble;
import org.apache.hadoop.hive.ql.udf.UDFToLong;
import org.apache.hadoop.hive.ql.udf.UDFToString;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBetween;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFIn;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualNS;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNot;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotNull;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNull;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToDecimal;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUpper;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeException;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Contract test of {@link HiveTranslator}, with filters built as Hive builds them: {@link ExprNodeGenericFuncDesc}
 * trees over {@link ExprNodeColumnDesc} and {@link ExprNodeConstantDesc}, columns typed as {@code HiveTypeUtils}
 * maps them (TIMESTAMP(MICROS) is a Hive bigint).
 */
class TestHiveTranslator extends ExpressionTranslatorContractTest<ExprNodeDesc> {

  private static final HiveTranslator TRANSLATOR = new HiveTranslator();

  @Override
  protected ExpressionTranslator<ExprNodeDesc> translator() {
    return TRANSLATOR;
  }

  @Override
  protected Option<ExprNodeDesc> engineLeaf(ExpressionSamples.Leaf leaf) {
    ExpressionSamples.Column c = leaf.column();
    ExprNodeDesc column = column(c);
    switch (leaf.op()) {
      case EQ:
        return Option.of(call(new GenericUDFOPEqual(), column, constant(c, c.literal())));
      case LT:
        return Option.of(call(new GenericUDFOPLessThan(), column, constant(c, c.literal())));
      case LT_EQ:
        return Option.of(call(new GenericUDFOPEqualOrLessThan(), column, constant(c, c.literal())));
      case GT:
        return Option.of(call(new GenericUDFOPGreaterThan(), column, constant(c, c.literal())));
      case GT_EQ:
        return Option.of(call(new GenericUDFOPEqualOrGreaterThan(), column, constant(c, c.literal())));
      case IN:
        return Option.of(call(new GenericUDFIn(), column, constant(c, c.literal()), constant(c, c.otherLiteral())));
      case IS_NULL:
        return Option.of(call(new GenericUDFOPNull(), column));
      case IS_NOT_NULL:
        return Option.of(call(new GenericUDFOPNotNull(), column));
      case STARTS_WITH:
        return Option.of(like(column, c.literal().getValue() + "%"));
      case CONTAINS:
        return Option.of(like(column, "%" + c.literal().getValue() + "%"));
      default:
        return Option.empty();
    }
  }

  @Override
  protected ExprNodeDesc engineAnd(ExprNodeDesc left, ExprNodeDesc right) {
    return call(new GenericUDFOPAnd(), left, right);
  }

  @Override
  protected ExprNodeDesc engineOr(ExprNodeDesc left, ExprNodeDesc right) {
    return call(new GenericUDFOPOr(), left, right);
  }

  @Override
  protected ExprNodeDesc engineNot(ExprNodeDesc child) {
    return call(new GenericUDFOPNot(), child);
  }

  /**
   * {@code upper(c_string) = 'ABC'}: a function call over the column.
   */
  @Override
  protected ExprNodeDesc untranslatable() {
    ExprNodeDesc upper = new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo, new GenericUDFUpper(),
        Collections.singletonList(column("c_string", TypeInfoFactory.stringTypeInfo)));
    return call(new GenericUDFOPEqual(), upper, new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, "ABC"));
  }

  // -------------------------------------------------------------------------
  // Hive-specific shapes
  // -------------------------------------------------------------------------

  @Test
  void constantOnTheLeftFlipsTheComparison() {
    assertTranslates(call(new GenericUDFOPLessThan(), intConstant(5), intColumn()), Predicates.gt(ref("c_int"), intLiteral(5)));
    assertTranslates(call(new GenericUDFOPEqualOrGreaterThan(), intConstant(5), intColumn()), Predicates.lteq(ref("c_int"), intLiteral(5)));
    assertTranslates(call(new GenericUDFOPEqual(), intConstant(5), intColumn()), Predicates.eq(ref("c_int"), intLiteral(5)));
  }

  @Test
  void notEqualBetweenAndBareBooleans() {
    assertTranslates(call(new GenericUDFOPNotEqual(), intColumn(), intConstant(5)), Predicates.not(Predicates.eq(ref("c_int"), intLiteral(5))));
    ExprNodeDesc between = call(new GenericUDFBetween(), new ExprNodeConstantDesc(TypeInfoFactory.booleanTypeInfo, false),
        intColumn(), intConstant(1), intConstant(9));
    Expression range = Predicates.and(Predicates.gteq(ref("c_int"), intLiteral(1)), Predicates.lteq(ref("c_int"), intLiteral(9)));
    assertTranslates(between, range);
    ExprNodeDesc notBetween = call(new GenericUDFBetween(), new ExprNodeConstantDesc(TypeInfoFactory.booleanTypeInfo, true),
        intColumn(), intConstant(1), intConstant(9));
    assertTranslates(notBetween, Predicates.not(range));

    ExprNodeDesc bool = column("c_bool", TypeInfoFactory.booleanTypeInfo);
    Expression isTrue = Predicates.eq(ref("c_bool"), new Literal<>(true, HoodieSchema.create(HoodieSchemaType.BOOLEAN)));
    assertTranslates(call(new GenericUDFOPAnd(), bool, call(new GenericUDFOPNull(), intColumn())), Predicates.and(isTrue, Predicates.isNull(ref("c_int"))));
    assertTranslates(call(new GenericUDFOPNot(), bool), Predicates.not(isTrue));
    // c_bool < true: ordering on booleans is declared unsupported
    assertNothing(call(new GenericUDFOPLessThan(), bool, new ExprNodeConstantDesc(TypeInfoFactory.booleanTypeInfo, true)));
    // <=> has other null semantics
    assertNothing(call(new GenericUDFOPEqualNS(), intColumn(), intConstant(5)));
  }

  @Test
  void booleanConstants() {
    ExprNodeDesc t = new ExprNodeConstantDesc(TypeInfoFactory.booleanTypeInfo, true);
    ExprNodeDesc f = new ExprNodeConstantDesc(TypeInfoFactory.booleanTypeInfo, false);
    assertNothing(t);
    assertTranslates(f, Predicates.alwaysFalse());
    assertTranslates(call(new GenericUDFOPNot(), t), Predicates.alwaysFalse());
    assertTranslates(call(new GenericUDFOPAnd(), t, call(new GenericUDFOPNull(), intColumn())), Predicates.isNull(ref("c_int")));
  }

  @Test
  void castsAreStrippedOnlyWhenOrderPreserving() {
    ExprNodeDesc intAsBigint = bridge(TypeInfoFactory.longTypeInfo, "UDFToLong", UDFToLong.class.getName(), intColumn());
    assertTranslates(call(new GenericUDFOPEqual(), intAsBigint, new ExprNodeConstantDesc(TypeInfoFactory.longTypeInfo, 5L)),
        Predicates.eq(ref("c_int"), intLiteral(5)));
    ExprNodeDesc intAsDouble = bridge(TypeInfoFactory.doubleTypeInfo, "UDFToDouble", UDFToDouble.class.getName(), intColumn());
    assertTranslates(call(new GenericUDFOPLessThan(), intAsDouble, new ExprNodeConstantDesc(TypeInfoFactory.doubleTypeInfo, 5.0d)),
        Predicates.lt(ref("c_int"), intLiteral(5)));
    // 5.5 has no int preimage
    assertNothing(call(new GenericUDFOPLessThan(), intAsDouble, new ExprNodeConstantDesc(TypeInfoFactory.doubleTypeInfo, 5.5d)));
    ExprNodeDesc intAsDecimal = new ExprNodeGenericFuncDesc(TypeInfoFactory.getDecimalTypeInfo(12, 2), new GenericUDFToDecimal(),
        Collections.singletonList(intColumn()));
    assertTranslates(call(new GenericUDFOPGreaterThan(), intAsDecimal, new ExprNodeConstantDesc(TypeInfoFactory.getDecimalTypeInfo(12, 2),
        HiveDecimal.create("7.00"))), Predicates.gt(ref("c_int"), intLiteral(7)));

    // long to double is not injective; int to string and int to decimal(5,2) do not preserve order
    ExprNodeDesc longAsDouble = bridge(TypeInfoFactory.doubleTypeInfo, "UDFToDouble", UDFToDouble.class.getName(),
        column("c_long", TypeInfoFactory.longTypeInfo));
    assertNothing(call(new GenericUDFOPEqual(), longAsDouble, new ExprNodeConstantDesc(TypeInfoFactory.doubleTypeInfo, 5.0d)));
    ExprNodeDesc intAsString = bridge(TypeInfoFactory.stringTypeInfo, "UDFToString", UDFToString.class.getName(), intColumn());
    assertNothing(call(new GenericUDFOPEqual(), intAsString, new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, "5")));
    ExprNodeDesc intAsSmallDecimal = new ExprNodeGenericFuncDesc(TypeInfoFactory.getDecimalTypeInfo(5, 2), new GenericUDFToDecimal(),
        Collections.singletonList(intColumn()));
    assertNothing(call(new GenericUDFOPEqual(), intAsSmallDecimal, new ExprNodeConstantDesc(TypeInfoFactory.getDecimalTypeInfo(5, 2),
        HiveDecimal.create("7"))));
    // a cast under IS NULL follows the same rule
    assertTranslates(call(new GenericUDFOPNull(), intAsBigint), Predicates.isNull(ref("c_int")));
    assertNothing(call(new GenericUDFOPNull(), intAsString));
  }

  /**
   * INT8 and INT16 columns, which HiveTypeUtils reads as Hive int, translate as INT columns, and CHAR columns, read
   * as Hive string, as STRING ones; their literals take the column's type.
   */
  @Test
  void narrowIntsAndCharTranslateAsIntAndString() {
    HoodieSchema int8 = HoodieSchema.createInt8();
    HoodieSchema int16 = HoodieSchema.createInt16();
    HoodieSchema char3 = HoodieSchema.createChar(3);
    HoodieSchema schema = HoodieSchema.createRecord("t", null, null, Arrays.asList(
        HoodieSchemaField.of("tiny", HoodieSchema.createNullable(int8)),
        HoodieSchemaField.of("small", int16),
        HoodieSchemaField.of("code", char3)));
    assertTranslates(schema, call(new GenericUDFOPEqual(), column("tiny", TypeInfoFactory.intTypeInfo), intConstant(5)),
        Predicates.eq(ref("tiny"), new Literal<>(5, int8)));
    // Hive tinyint / smallint columns (e.g. synced by Spark) read the same Integer values
    assertTranslates(schema, call(new GenericUDFOPEqual(), column("small", TypeInfoFactory.shortTypeInfo), intConstant(300)),
        Predicates.eq(ref("small"), new Literal<>(300, int16)));
    assertNothing(schema, call(new GenericUDFOPEqual(), column("small", TypeInfoFactory.intTypeInfo),
        new ExprNodeConstantDesc(TypeInfoFactory.doubleTypeInfo, 1.5d)));
    assertTranslates(schema, call(new GenericUDFOPEqual(), column("code", TypeInfoFactory.stringTypeInfo),
        new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, "ab ")), Predicates.eq(ref("code"), new Literal<>("ab ", char3)));
    // a Hive char column compares without its padding, so it is not translated, as for STRING
    assertNothing(schema, call(new GenericUDFOPEqual(), column("code", TypeInfoFactory.getCharTypeInfo(3)),
        new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, "ab ")));
    assertTrue(TRANSLATOR.support(Expression.Operator.LT, HoodieSchemaType.INT8).isSupported());
    assertTrue(TRANSLATOR.support(Expression.Operator.EQ, HoodieSchemaType.CHAR).isSupported());
    assertFalse(TRANSLATOR.support(Expression.Operator.STARTS_WITH, HoodieSchemaType.CHAR).isSupported());
  }

  @Test
  void literalsMustConvertExactly() {
    // numeric constants of another Hive type
    assertTranslates(call(new GenericUDFOPEqual(), intColumn(), new ExprNodeConstantDesc(TypeInfoFactory.longTypeInfo, 5L)),
        Predicates.eq(ref("c_int"), intLiteral(5)));
    assertTranslates(call(new GenericUDFOPEqual(), intColumn(), new ExprNodeConstantDesc(TypeInfoFactory.doubleTypeInfo, 5.0d)),
        Predicates.eq(ref("c_int"), intLiteral(5)));
    assertNothing(call(new GenericUDFOPEqual(), intColumn(), new ExprNodeConstantDesc(TypeInfoFactory.doubleTypeInfo, 5.5d)));
    assertNothing(call(new GenericUDFOPEqual(), intColumn(), new ExprNodeConstantDesc(TypeInfoFactory.longTypeInfo, 1L << 40)));
    ExprNodeDesc longColumn = column("c_long", TypeInfoFactory.longTypeInfo);
    assertNothing(call(new GenericUDFOPEqual(), longColumn, new ExprNodeConstantDesc(TypeInfoFactory.doubleTypeInfo, (double) (1L << 53))));
    assertTranslates(call(new GenericUDFOPEqual(), column("c_double", TypeInfoFactory.doubleTypeInfo),
        new ExprNodeConstantDesc(TypeInfoFactory.intTypeInfo, 2)), Predicates.eq(ref("c_double"), new Literal<>(2.0d, HoodieSchema.create(HoodieSchemaType.DOUBLE))));

    // decimals: within the column's precision and scale only, never from a floating constant
    ExprNodeDesc decimal = column("c_decimal", TypeInfoFactory.getDecimalTypeInfo(10, 2));
    assertTranslates(call(new GenericUDFOPEqual(), decimal, new ExprNodeConstantDesc(TypeInfoFactory.getDecimalTypeInfo(3, 1), HiveDecimal.create("12.3"))),
        Predicates.eq(ref("c_decimal"), new Literal<>(new BigDecimal("12.30"), HoodieSchema.createDecimal(10, 2))));
    assertNothing(call(new GenericUDFOPEqual(), decimal, new ExprNodeConstantDesc(TypeInfoFactory.getDecimalTypeInfo(5, 3), HiveDecimal.create("12.345"))));
    assertNothing(call(new GenericUDFOPEqual(), decimal, new ExprNodeConstantDesc(TypeInfoFactory.doubleTypeInfo, 12.34d)));

    // strings: not from numbers (Hive compares those as doubles); Text and varchar constants convert
    ExprNodeDesc string = column("c_string", TypeInfoFactory.stringTypeInfo);
    assertNothing(call(new GenericUDFOPEqual(), string, intConstant(5)));
    assertTranslates(call(new GenericUDFOPEqual(), string, new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, new Text("abc"))),
        Predicates.eq(ref("c_string"), new Literal<>("abc", HoodieSchema.create(HoodieSchemaType.STRING))));

    // dates from DateWritable as well as java.sql.Date
    ExprNodeDesc date = column("c_date", TypeInfoFactory.dateTypeInfo);
    assertTranslates(call(new GenericUDFOPEqual(), date, new ExprNodeConstantDesc(TypeInfoFactory.dateTypeInfo, new DateWritable(19000))),
        Predicates.eq(ref("c_date"), new Literal<>(19000, HoodieSchema.createDate())));
    assertNothing(call(new GenericUDFOPEqual(), date, new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, "2022-01-08")));

    // null constants and columns missing from the schema keep every row
    assertNothing(call(new GenericUDFOPEqual(), intColumn(), new ExprNodeConstantDesc(TypeInfoFactory.intTypeInfo, null)));
    assertNothing(call(new GenericUDFOPEqual(), column("missing", TypeInfoFactory.intTypeInfo), intConstant(5)));
    assertNothing(call(new GenericUDFOPNull(), column("missing", TypeInfoFactory.intTypeInfo)));
    // a Hive type the column is not read as
    assertNothing(call(new GenericUDFOPEqual(), column("c_int", TypeInfoFactory.stringTypeInfo), intConstant(5)));
    // Hive lower-cases column names
    assertTranslates(call(new GenericUDFOPEqual(), column("C_INT", TypeInfoFactory.intTypeInfo), intConstant(5)),
        Predicates.eq(ref("c_int"), intLiteral(5)));
  }

  @Test
  void timestamps() {
    HoodieSchema schema = HoodieSchema.createRecord("ts", "hoodie.ts", null, Arrays.asList(
        HoodieSchemaField.of("ts_millis", HoodieSchema.createNullable(HoodieSchema.createTimestampMillis())),
        HoodieSchemaField.of("ts_micros", HoodieSchema.createNullable(HoodieSchema.createTimestampMicros()))));
    ExprNodeDesc millis = column("ts_millis", TypeInfoFactory.timestampTypeInfo);
    Timestamp exact = new Timestamp(1700000000123L);
    assertTranslates(schema, call(new GenericUDFOPGreaterThan(), millis, new ExprNodeConstantDesc(TypeInfoFactory.timestampTypeInfo, exact)),
        Predicates.gt(ref("ts_millis"), new Literal<>(1700000000123L, HoodieSchema.createTimestampMillis())));
    Timestamp beforeEpoch = new Timestamp(-1500L);
    assertTranslates(schema, call(new GenericUDFOPEqual(), millis, new ExprNodeConstantDesc(TypeInfoFactory.timestampTypeInfo, beforeEpoch)),
        Predicates.eq(ref("ts_millis"), new Literal<>(-1500L, HoodieSchema.createTimestampMillis())));
    Timestamp subMillis = new Timestamp(1700000000123L);
    subMillis.setNanos(123_456_000);
    assertNothing(schema, call(new GenericUDFOPGreaterThan(), millis, new ExprNodeConstantDesc(TypeInfoFactory.timestampTypeInfo, subMillis)));

    // MICROS is a Hive bigint of the raw value
    ExprNodeDesc micros = column("ts_micros", TypeInfoFactory.longTypeInfo);
    assertTranslates(schema, call(new GenericUDFOPLessThan(), micros, new ExprNodeConstantDesc(TypeInfoFactory.longTypeInfo, 1700000000000001L)),
        Predicates.lt(ref("ts_micros"), new Literal<>(1700000000000001L, HoodieSchema.createTimestampMicros())));
    // ... unless synced with support_timestamp: the reader then truncates to milliseconds
    ExprNodeDesc microsAsTimestamp = column("ts_micros", TypeInfoFactory.timestampTypeInfo);
    assertNothing(schema, call(new GenericUDFOPLessThan(), microsAsTimestamp, new ExprNodeConstantDesc(TypeInfoFactory.timestampTypeInfo, exact)));
    assertTranslates(schema, call(new GenericUDFOPNotNull(), microsAsTimestamp), Predicates.isNotNull(ref("ts_micros")));
  }

  @Test
  void likePatternsMustBeExact() {
    ExprNodeDesc string = column("c_string", TypeInfoFactory.stringTypeInfo);
    HoodieSchema type = HoodieSchema.create(HoodieSchemaType.STRING);
    assertTranslates(like(string, "ab%"), Predicates.startsWith(ref("c_string"), new Literal<>("ab", type)));
    assertTranslates(like(string, "%b%"), Predicates.contains(ref("c_string"), new Literal<>("b", type)));
    assertNothing(like(string, "a_c%"));
    assertNothing(like(string, "a\\%%"));
    assertNothing(like(string, "a%c%"));
    assertNothing(like(string, "%abc"));
    assertNothing(like(string, "abc"));
    assertNothing(like(intColumn(), "5%"));
  }

  @Test
  void translatesTheSerializedFilter() {
    // the form Hive hands input formats under TableScanDesc.FILTER_EXPR_CONF_STR
    ExprNodeGenericFuncDesc filter = (ExprNodeGenericFuncDesc) call(new GenericUDFOPAnd(),
        call(new GenericUDFOPGreaterThan(), intColumn(), intConstant(3)), untranslatable());
    ExprNodeDesc deserialized = SerializationUtilities.deserializeExpression(SerializationUtilities.serializeExpression(filter));
    assertTranslates(deserialized, Predicates.gt(ref("c_int"), intLiteral(3)));
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private void assertTranslates(ExprNodeDesc hive, Expression expected) {
    assertTranslates(schema(), hive, expected);
  }

  private static void assertTranslates(HoodieSchema schema, ExprNodeDesc hive, Expression expected) {
    Option<Expression> translated = TRANSLATOR.toHudi(hive, schema);
    assertTrue(translated.isPresent(), hive + " did not translate; expected " + expected);
    ExpressionEquivalence.assertEquivalent(expected, translated.get());
  }

  private void assertNothing(ExprNodeDesc hive) {
    assertNothing(schema(), hive);
  }

  private static void assertNothing(HoodieSchema schema, ExprNodeDesc hive) {
    Option<Expression> translated = TRANSLATOR.toHudi(hive, schema);
    assertFalse(translated.isPresent(), hive + " must translate to nothing but was " + (translated.isPresent() ? translated.get() : ""));
  }

  private static ExprNodeDesc call(GenericUDF udf, ExprNodeDesc... children) {
    return new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo, udf, new ArrayList<>(Arrays.asList(children)));
  }

  private static ExprNodeDesc bridge(TypeInfo type, String name, String udfClass, ExprNodeDesc child) {
    return new ExprNodeGenericFuncDesc(type, new GenericUDFBridge(name, false, udfClass), new ArrayList<>(Collections.singletonList(child)));
  }

  private static ExprNodeDesc like(ExprNodeDesc column, String pattern) {
    return call(new GenericUDFBridge("like", true, UDFLike.class.getName()), column,
        new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, pattern));
  }

  private static ExprNodeDesc column(String name, TypeInfo type) {
    return new ExprNodeColumnDesc(type, name, "t", false);
  }

  private static ExprNodeDesc column(ExpressionSamples.Column c) {
    return column(c.name(), hiveType(c));
  }

  private static ExprNodeDesc intColumn() {
    return column("c_int", TypeInfoFactory.intTypeInfo);
  }

  private static ExprNodeDesc intConstant(int value) {
    return new ExprNodeConstantDesc(TypeInfoFactory.intTypeInfo, value);
  }

  private static Literal<?> intLiteral(int value) {
    return new Literal<>(value, HoodieSchema.create(HoodieSchemaType.INT));
  }

  private static NameReference ref(String name) {
    return new NameReference(name);
  }

  /**
   * The Hive type Hudi's Hive sync and readers give the column.
   */
  private static TypeInfo hiveType(ExpressionSamples.Column c) {
    try {
      return HiveTypeUtils.generateTypeInfo(c.type(), null);
    } catch (AvroSerdeException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * The sample literal as the Hive constant Hive would push for it.
   */
  private static ExprNodeDesc constant(ExpressionSamples.Column c, Literal<?> literal) {
    Object value = literal.getValue();
    Object hive;
    switch (c.schemaType()) {
      case DECIMAL:
        hive = HiveDecimal.create((BigDecimal) value);
        break;
      case DATE:
        hive = new Date(DateWritable.daysToMillis((Integer) value));
        break;
      default:
        // TIMESTAMP(MICROS) is a bigint: the raw Long
        hive = value;
    }
    return new ExprNodeConstantDesc(hiveType(c), hive);
  }
}
