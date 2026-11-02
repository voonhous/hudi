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
import org.apache.hudi.common.expression.translate.ExpressionRenderer;
import org.apache.hudi.common.expression.translate.testkit.ExpressionEquivalence;
import org.apache.hudi.common.expression.translate.testkit.ExpressionRendererContractTest;
import org.apache.hudi.common.expression.translate.testkit.ExpressionSamples;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.util.Option;

import org.apache.spark.sql.sources.AlwaysFalse;
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
import org.apache.spark.sql.sources.StringStartsWith;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Contract test of {@link SparkFilterRenderer}, with the expected {@code Filter} of every sample leaf.
 */
class TestSparkFilterRenderer extends ExpressionRendererContractTest<Filter> {

  @Override
  protected ExpressionRenderer<Filter> renderer() {
    return new SparkFilterRenderer();
  }

  /**
   * The value Spark 3.5's {@code ParquetFilters} pushes down for a sample column.
   */
  private static Object parquetValue(ExpressionSamples.Column column, Object canonical) {
    switch (column.schemaType()) {
      case DATE:
        return LocalDate.ofEpochDay((Integer) canonical);
      case TIMESTAMP:
        return TestSparkTranslator.toInstant((Long) canonical);
      default:
        return canonical;
    }
  }

  @Override
  protected Map<String, Filter> expectedRenderings() {
    Map<String, Filter> expected = new HashMap<>();
    for (ExpressionSamples.Leaf leaf : ExpressionSamples.leaves()) {
      ExpressionSamples.Column c = leaf.column();
      String name = c.name();
      Object v = parquetValue(c, c.literal().getValue());
      Filter filter;
      switch (leaf.op()) {
        case EQ:
          filter = new EqualTo(name, v);
          break;
        case LT:
          filter = new LessThan(name, v);
          break;
        case LT_EQ:
          filter = new LessThanOrEqual(name, v);
          break;
        case GT:
          filter = new GreaterThan(name, v);
          break;
        case GT_EQ:
          filter = new GreaterThanOrEqual(name, v);
          break;
        case IN:
          filter = new In(name, new Object[] {v, parquetValue(c, c.otherLiteral().getValue())});
          break;
        case IS_NULL:
          filter = new IsNull(name);
          break;
        case IS_NOT_NULL:
          filter = new IsNotNull(name);
          break;
        case STARTS_WITH:
          filter = new StringStartsWith(name, (String) v);
          break;
        case CONTAINS:
          filter = new StringContains(name, (String) v);
          break;
        default:
          throw new IllegalStateException("No expected rendering for " + leaf);
      }
      expected.put(leaf.id(), filter);
    }
    return expected;
  }

  @Override
  protected Option<Expression> unrenderable() {
    // every sample leaf renders; TIME has no Spark filter value
    return Option.of(Predicates.eq(new NameReference("c_time"), new Literal<>(1000L, HoodieSchema.createTimeMicros())));
  }

  private static Option<Filter> render(Expression expr) {
    return new SparkFilterRenderer().render(expr);
  }

  @Test
  void testEverySampleLeafHasAnExpectedRendering() {
    assertEquals(ExpressionSamples.leaves().size(), expectedRenderings().size());
  }

  /**
   * {@code Filter.equals} compares values with Scala's {@code ==}, for which {@code 5 == 5L}; ParquetFilters
   * checks the Java class, so check it separately.
   */
  @Test
  void testRenderedValueClasses() {
    for (ExpressionSamples.Leaf leaf : ExpressionSamples.leaves()) {
      if (leaf.op() != Expression.Operator.EQ) {
        continue;
      }
      Object expected = ((EqualTo) expectedRenderings().get(leaf.id())).value();
      Object actual = ((EqualTo) render(leaf.expression()).get()).value();
      assertEquals(expected.getClass(), actual.getClass(), leaf.id());
    }
  }

  @Test
  void testRoundTripThroughTheTranslator() {
    for (ExpressionSamples.Leaf leaf : ExpressionSamples.leaves()) {
      Option<Expression> back = new SparkTranslator().toHudi(render(leaf.expression()).get(), ExpressionSamples.SCHEMA);
      assertTrue(back.isPresent(), leaf + " did not translate back");
      ExpressionEquivalence.assertEquivalent(leaf.expression(), back.get());
    }
  }

  @Test
  void testValues() {
    NameReference ts = new NameReference("ts");
    assertEquals(new GreaterThan("ts", Instant.ofEpochMilli(1700000000123L)),
        render(Predicates.gt(ts, new Literal<>(1700000000123L, HoodieSchema.createTimestampMillis()))).get());
    assertEquals(new GreaterThan("ts", LocalDateTime.of(2023, 11, 14, 22, 13, 20)),
        render(Predicates.gt(ts, new Literal<>(1700000000000000L, HoodieSchema.createLocalTimestampMicros()))).get());
    // ParquetFilters needs the decimal at the column's scale
    assertEquals(new EqualTo("d", new BigDecimal("5.00")),
        render(Predicates.eq(new NameReference("d"), new Literal<>(new BigDecimal("5"), HoodieSchema.createDecimal(10, 2)))).get());
    assertFalse(render(Predicates.eq(new NameReference("d"), new Literal<>(new BigDecimal("5.001"), HoodieSchema.createDecimal(10, 2)))).isPresent());
    assertFalse(render(Predicates.eq(new NameReference("c_int"), new Literal<>(null, HoodieSchema.create(HoodieSchemaType.INT)))).isPresent());
    assertFalse(render(Predicates.eq(new NameReference("b"), new Literal<>(new byte[] {1}, HoodieSchema.create(HoodieSchemaType.BYTES)))).isPresent());
  }

  @Test
  void testShapes() {
    Literal<Integer> five = new Literal<>(5, HoodieSchema.create(HoodieSchemaType.INT));
    assertEquals(new LessThan("c_int", 5), render(Predicates.gt(five, new NameReference("c_int"))).get());
    assertEquals(new EqualTo("`a.b`", 5), render(Predicates.eq(new NameReference("a.b"), five)).get());
    assertEquals(new EqualTo("`1`", 5), render(Predicates.eq(new NameReference("1"), five)).get());
    assertEquals(new Not(new EqualTo("c_int", 5)), render(Predicates.not(Predicates.eq(new NameReference("c_int"), five))).get());
    assertEquals(new AlwaysFalse(), render(Predicates.alwaysFalse()).get());
    assertFalse(render(Predicates.alwaysTrue()).isPresent());
    HoodieSchema string = HoodieSchema.create(HoodieSchemaType.STRING);
    assertEquals(new Or(new StringStartsWith("s", "a"), new StringStartsWith("s", "b")),
        render(Predicates.startsWithAny(new NameReference("s"), Arrays.asList(new Literal<>("a", string), new Literal<>("b", string)))).get());
  }
}
