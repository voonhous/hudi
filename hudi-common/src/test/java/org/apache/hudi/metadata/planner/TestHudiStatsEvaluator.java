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

package org.apache.hudi.metadata.planner;

import org.apache.hudi.common.expression.Expression;
import org.apache.hudi.common.expression.Literal;
import org.apache.hudi.common.expression.NameReference;
import org.apache.hudi.common.expression.Predicates;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaType;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static org.apache.hudi.common.expression.Predicates.and;
import static org.apache.hudi.common.expression.Predicates.eq;
import static org.apache.hudi.common.expression.Predicates.gt;
import static org.apache.hudi.common.expression.Predicates.gteq;
import static org.apache.hudi.common.expression.Predicates.lt;
import static org.apache.hudi.common.expression.Predicates.lteq;
import static org.apache.hudi.common.expression.Predicates.not;
import static org.apache.hudi.common.expression.Predicates.or;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests of {@link HudiStatsEvaluator}: every operator and type, nulls, NOT push-down and unknown parts.
 */
public class TestHudiStatsEvaluator {

  /**
   * A string of the given code points (written so to keep the sources ASCII).
   */
  private static String cp(int... codePoints) {
    StringBuilder sb = new StringBuilder();
    for (int codePoint : codePoints) {
      sb.appendCodePoint(codePoint);
    }
    return sb.toString();
  }

  private static final HoodieSchema INT = HoodieSchema.create(HoodieSchemaType.INT);
  private static final HoodieSchema LONG = HoodieSchema.create(HoodieSchemaType.LONG);
  private static final HoodieSchema FLOAT = HoodieSchema.create(HoodieSchemaType.FLOAT);
  private static final HoodieSchema DOUBLE = HoodieSchema.create(HoodieSchemaType.DOUBLE);
  private static final HoodieSchema BOOLEAN = HoodieSchema.create(HoodieSchemaType.BOOLEAN);
  private static final HoodieSchema STRING = HoodieSchema.create(HoodieSchemaType.STRING);
  private static final HoodieSchema DECIMAL = HoodieSchema.createDecimal(10, 2);
  private static final HoodieSchema DATE = HoodieSchema.createDate();
  private static final HoodieSchema TS_MICROS = HoodieSchema.createTimestampMicros();
  private static final HoodieSchema TS_MILLIS = HoodieSchema.createTimestampMillis();
  private static final HoodieSchema LOCAL_TS_MICROS = HoodieSchema.createLocalTimestampMicros();
  private static final HoodieSchema BYTES = HoodieSchema.create(HoodieSchemaType.BYTES);

  private static final HudiStatsEvaluator EVALUATOR = HudiStatsEvaluator.getInstance();

  private static NameReference col(String name) {
    return new NameReference(name);
  }

  private static <T> Literal<T> lit(T value, HoodieSchema type) {
    return new Literal<>(value, type);
  }

  private static Map<String, ColumnRange> stats(String name, ColumnRange range) {
    Map<String, ColumnRange> stats = new HashMap<>();
    stats.put(name, range);
    return stats;
  }

  /**
   * a: INT in [10, 20], 2 nulls out of 10.
   */
  private static Map<String, ColumnRange> intStats() {
    return stats("a", ColumnRange.of(INT, 10, 20, 2L, 10L));
  }

  private static boolean might(Expression filter, Map<String, ColumnRange> stats) {
    return EVALUATOR.mightMatch(filter, stats);
  }

  @Test
  void comparisons() {
    Map<String, ColumnRange> s = intStats();
    assertTrue(might(eq(col("a"), lit(10, INT)), s));
    assertTrue(might(eq(col("a"), lit(15, INT)), s));
    assertTrue(might(eq(col("a"), lit(20, INT)), s));
    assertFalse(might(eq(col("a"), lit(9, INT)), s));
    assertFalse(might(eq(col("a"), lit(21, INT)), s));

    assertFalse(might(lt(col("a"), lit(10, INT)), s));
    assertTrue(might(lt(col("a"), lit(11, INT)), s));
    assertFalse(might(lteq(col("a"), lit(9, INT)), s));
    assertTrue(might(lteq(col("a"), lit(10, INT)), s));
    assertFalse(might(gt(col("a"), lit(20, INT)), s));
    assertTrue(might(gt(col("a"), lit(19, INT)), s));
    assertFalse(might(gteq(col("a"), lit(21, INT)), s));
    assertTrue(might(gteq(col("a"), lit(20, INT)), s));
  }

  @Test
  void literalOnTheLeft() {
    Map<String, ColumnRange> s = intStats();
    // 25 < a  is  a > 25
    assertFalse(might(lt(lit(25, INT), col("a")), s));
    assertTrue(might(lt(lit(15, INT), col("a")), s));
    // 5 >= a  is  a <= 5
    assertFalse(might(gteq(lit(5, INT), col("a")), s));
    assertTrue(might(gteq(lit(10, INT), col("a")), s));
    assertFalse(might(eq(lit(5, INT), col("a")), s));
  }

  @Test
  void negatedComparisons() {
    Map<String, ColumnRange> s = intStats();
    // NOT(a < 25) is a >= 25 over the non-null values
    assertFalse(might(not(lt(col("a"), lit(25, INT))), s));
    assertTrue(might(not(lt(col("a"), lit(20, INT))), s));
    assertFalse(might(not(lteq(col("a"), lit(20, INT))), s));
    assertFalse(might(not(gt(col("a"), lit(5, INT))), s));
    assertTrue(might(not(gt(col("a"), lit(10, INT))), s));
    assertTrue(might(not(gteq(col("a"), lit(11, INT))), s));
    assertFalse(might(not(gteq(col("a"), lit(10, INT))), s));
    // double negation
    assertFalse(might(not(not(eq(col("a"), lit(9, INT)))), s));
  }

  @Test
  void notEqualPrunesOnlyASingleValue() {
    // every non-null value is 7; its nulls do not match a != 7 either
    Map<String, ColumnRange> single = stats("a", ColumnRange.of(INT, 7, 7, 3L, 10L));
    assertFalse(might(not(eq(col("a"), lit(7, INT))), single));
    assertTrue(might(not(eq(col("a"), lit(8, INT))), single));
    assertTrue(might(eq(col("a"), lit(7, INT)), single));
    // unknown null count: still only the value 7
    assertFalse(might(not(eq(col("a"), lit(7, INT))), stats("a", ColumnRange.of(INT, 7, 7, null, null))));
    assertTrue(might(not(eq(col("a"), lit(7, INT))), intStats()));
  }

  @Test
  void deMorgan() {
    Map<String, ColumnRange> s = intStats();
    Expression outside = lt(col("a"), lit(5, INT));
    Expression inside = eq(col("a"), lit(15, INT));
    assertFalse(might(and(outside, inside), s));
    assertTrue(might(or(outside, inside), s));
    assertFalse(might(or(outside, gt(col("a"), lit(30, INT))), s));
    // NOT(a >= 5 AND a <= 25) = a < 5 OR a > 25: no
    assertFalse(might(not(and(gteq(col("a"), lit(5, INT)), lteq(col("a"), lit(25, INT)))), s));
    // NOT(a < 5 OR a > 25) = a >= 5 AND a <= 25: yes
    assertTrue(might(not(or(lt(col("a"), lit(5, INT)), gt(col("a"), lit(25, INT)))), s));
    // NOT(a < 15 OR a > 12) = a >= 15 AND a <= 12: the evaluator cannot see the contradiction, but must not prune
    assertTrue(might(not(or(lt(col("a"), lit(15, INT)), gt(col("a"), lit(12, INT)))), s));
  }

  @Test
  void constants() {
    Map<String, ColumnRange> s = intStats();
    assertTrue(might(Predicates.alwaysTrue(), s));
    assertFalse(might(Predicates.alwaysFalse(), s));
    assertFalse(might(not(Predicates.alwaysTrue()), s));
    assertTrue(might(not(Predicates.alwaysFalse()), s));
    assertFalse(might(and(Predicates.alwaysTrue(), Predicates.alwaysFalse()), s));
    assertTrue(might(or(Predicates.alwaysFalse(), eq(col("a"), lit(15, INT))), s));
  }

  @Test
  void in() {
    Map<String, ColumnRange> s = intStats();
    assertTrue(might(Predicates.in(col("a"), Arrays.asList(lit(1, INT), lit(12, INT))), s));
    assertFalse(might(Predicates.in(col("a"), Arrays.asList(lit(1, INT), lit(30, INT))), s));
    assertFalse(might(Predicates.in(col("a"), Collections.emptyList()), s));
    // a null in the list never makes IN true
    assertFalse(might(Predicates.in(col("a"), Arrays.asList(lit(1, INT), lit(null, INT))), s));
    // a non-literal in the list: unknown
    assertTrue(might(Predicates.in(col("a"), Arrays.asList(lit(1, INT), col("b"))), s));

    Map<String, ColumnRange> single = stats("a", ColumnRange.of(INT, 7, 7, 0L, 10L));
    assertFalse(might(not(Predicates.in(col("a"), Arrays.asList(lit(1, INT), lit(7, INT)))), single));
    assertTrue(might(not(Predicates.in(col("a"), Arrays.asList(lit(1, INT), lit(8, INT)))), single));
    assertTrue(might(not(Predicates.in(col("a"), Arrays.asList(lit(10, INT), lit(20, INT)))), s));
  }

  @Test
  void nullChecks() {
    Map<String, ColumnRange> someNulls = intStats();
    Map<String, ColumnRange> noNulls = stats("a", ColumnRange.of(INT, 10, 20, 0L, 10L));
    Map<String, ColumnRange> allNulls = stats("a", ColumnRange.of(INT, null, null, 10L, 10L));
    Map<String, ColumnRange> unknownCounts = stats("a", ColumnRange.of(INT, 10, 20, null, null));

    assertTrue(might(Predicates.isNull(col("a")), someNulls));
    assertFalse(might(Predicates.isNull(col("a")), noNulls));
    assertTrue(might(Predicates.isNull(col("a")), allNulls));
    assertTrue(might(Predicates.isNull(col("a")), unknownCounts));

    assertTrue(might(Predicates.isNotNull(col("a")), someNulls));
    assertTrue(might(Predicates.isNotNull(col("a")), noNulls));
    assertFalse(might(Predicates.isNotNull(col("a")), allNulls));
    assertTrue(might(Predicates.isNotNull(col("a")), unknownCounts));

    // NOT(IS_NULL) is IS_NOT_NULL and the other way round
    assertFalse(might(not(Predicates.isNull(col("a"))), allNulls));
    assertTrue(might(not(Predicates.isNull(col("a"))), noNulls));
    assertFalse(might(not(Predicates.isNotNull(col("a"))), noNulls));
    assertTrue(might(not(Predicates.isNotNull(col("a"))), allNulls));
  }

  @Test
  void comparisonsNeverMatchNulls() {
    Map<String, ColumnRange> allNulls = stats("a", ColumnRange.of(INT, null, null, 10L, 10L));
    assertFalse(might(eq(col("a"), lit(1, INT)), allNulls));
    assertFalse(might(not(eq(col("a"), lit(1, INT))), allNulls));
    assertFalse(might(lt(col("a"), lit(1, INT)), allNulls));
    assertFalse(might(not(lt(col("a"), lit(1, INT))), allNulls));
    assertFalse(might(Predicates.in(col("a"), Collections.singletonList(lit(1, INT))), allNulls));
    assertFalse(might(not(Predicates.in(col("a"), Collections.singletonList(lit(1, INT)))), allNulls));
    // an empty file
    assertFalse(might(eq(col("a"), lit(1, INT)), stats("a", ColumnRange.of(INT, null, null, 0L, 0L))));
  }

  @Test
  void unknownParts() {
    // no stats for the column
    assertTrue(might(eq(col("b"), lit(1, INT)), intStats()));
    assertTrue(might(not(eq(col("b"), lit(1, INT))), intStats()));
    // no min
    Map<String, ColumnRange> noMin = stats("a", ColumnRange.of(INT, null, 20, 0L, 10L));
    assertTrue(might(lt(col("a"), lit(-100, INT)), noMin));
    assertFalse(might(gt(col("a"), lit(100, INT)), noMin));
    assertTrue(might(eq(col("a"), lit(-100, INT)), noMin));
    assertFalse(might(eq(col("a"), lit(100, INT)), noMin));
    // no max
    Map<String, ColumnRange> noMax = stats("a", ColumnRange.of(INT, 10, null, 0L, 10L));
    assertTrue(might(gt(col("a"), lit(100, INT)), noMax));
    assertFalse(might(lt(col("a"), lit(-100, INT)), noMax));
    // no min and max, but the counts do not say all null
    Map<String, ColumnRange> noBounds = stats("a", ColumnRange.of(INT, null, null, 1L, 10L));
    assertTrue(might(eq(col("a"), lit(1, INT)), noBounds));
    assertTrue(might(not(eq(col("a"), lit(1, INT))), noBounds));
    Map<String, ColumnRange> nothing = stats("a", ColumnRange.of(INT, null, null, null, null));
    assertTrue(might(eq(col("a"), lit(1, INT)), nothing));
    assertTrue(might(Predicates.isNull(col("a")), nothing));
    assertTrue(might(Predicates.isNotNull(col("a")), nothing));
  }

  @Test
  void unknownLeaves() {
    Map<String, ColumnRange> s = stats("s", ColumnRange.of(STRING, "b", "c", 0L, 10L));
    assertTrue(might(Predicates.contains(col("s"), lit("zzz", STRING)), s));
    assertTrue(might(not(Predicates.contains(col("s"), lit("b", STRING))), s));
    // two columns
    assertTrue(might(eq(col("s"), col("t")), s));
    // a null literal
    assertTrue(might(eq(col("s"), lit(null, STRING)), s));
    assertTrue(might(not(eq(col("s"), lit(null, STRING))), s));
    // under AND, the unknown leaf does not stop the others from pruning
    assertFalse(might(and(Predicates.contains(col("s"), lit("x", STRING)), eq(col("s"), lit("a", STRING))), s));
  }

  @Test
  void typeMismatchMightMatch() {
    Map<String, ColumnRange> s = intStats();
    // LONG literal against an INT range
    assertTrue(might(eq(col("a"), lit(100L, LONG)), s));
    // INT type but a Long value
    assertTrue(might(eq(col("a"), lit(100L, INT)), s));
    // a range whose values are not of the column's class
    assertTrue(might(eq(col("x"), lit(100, INT)), stats("x", ColumnRange.of(INT, 1L, 2L, 0L, 10L))));
    // timestamps of another unit or time zone adjustment
    Map<String, ColumnRange> ts = stats("t", ColumnRange.of(TS_MICROS, 1_000_000L, 2_000_000L, 0L, 10L));
    assertFalse(might(eq(col("t"), lit(5_000_000L, TS_MICROS)), ts));
    assertTrue(might(eq(col("t"), lit(5_000_000L, TS_MILLIS)), ts));
    assertTrue(might(eq(col("t"), lit(5_000_000L, LOCAL_TS_MICROS)), ts));
    // bytes are not compared
    assertTrue(might(eq(col("b"), lit(java.nio.ByteBuffer.wrap(new byte[] {9}), BYTES)),
        stats("b", ColumnRange.of(BYTES, null, null, 0L, 1L))));
  }

  @Test
  void otherTypes() {
    Map<String, ColumnRange> longs = stats("l", ColumnRange.of(LONG, -5L, 5L, 0L, 10L));
    assertFalse(might(gt(col("l"), lit(5L, LONG)), longs));
    assertTrue(might(gteq(col("l"), lit(5L, LONG)), longs));

    Map<String, ColumnRange> bools = stats("b", ColumnRange.of(BOOLEAN, false, false, 0L, 10L));
    assertFalse(might(eq(col("b"), lit(true, BOOLEAN)), bools));
    assertTrue(might(eq(col("b"), lit(false, BOOLEAN)), bools));
    assertFalse(might(not(eq(col("b"), lit(false, BOOLEAN))), bools));

    Map<String, ColumnRange> decimals = stats("d", ColumnRange.of(DECIMAL, new BigDecimal("1.50"), new BigDecimal("2.00"), 0L, 10L));
    // numeric comparison, whatever the scale
    assertTrue(might(eq(col("d"), lit(new BigDecimal("1.5"), HoodieSchema.createDecimal(2, 1))), decimals));
    assertTrue(might(eq(col("d"), lit(new BigDecimal("2"), HoodieSchema.createDecimal(1, 0))), decimals));
    assertFalse(might(eq(col("d"), lit(new BigDecimal("2.01"), DECIMAL)), decimals));
    assertFalse(might(lt(col("d"), lit(new BigDecimal("1.5"), DECIMAL)), decimals));
    Map<String, ColumnRange> oneDecimal = stats("d", ColumnRange.of(DECIMAL, new BigDecimal("2.00"), new BigDecimal("2.00"), 0L, 10L));
    assertFalse(might(not(eq(col("d"), lit(new BigDecimal("2"), HoodieSchema.createDecimal(1, 0)))), oneDecimal));

    Map<String, ColumnRange> dates = stats("dt", ColumnRange.of(DATE, 18000, 18010, 0L, 10L));
    assertFalse(might(lt(col("dt"), lit(18000, DATE)), dates));
    assertTrue(might(lteq(col("dt"), lit(18000, DATE)), dates));
    // an INT literal is not a DATE
    assertTrue(might(lt(col("dt"), lit(1, INT)), dates));

    Map<String, ColumnRange> ts = stats("t", ColumnRange.of(TS_MICROS, 100L, 200L, 0L, 10L));
    assertFalse(might(gt(col("t"), lit(200L, TS_MICROS)), ts));
    assertTrue(might(gt(col("t"), lit(199L, TS_MICROS)), ts));

    Map<String, ColumnRange> floats = stats("f", ColumnRange.of(FLOAT, 1.5f, 2.5f, 0L, 10L));
    assertFalse(might(gt(col("f"), lit(2.5f, FLOAT)), floats));
    assertTrue(might(eq(col("f"), lit(2.0f, FLOAT)), floats));
  }

  @Test
  void nanAndSignedZeroMightMatch() {
    Map<String, ColumnRange> d = stats("d", ColumnRange.of(DOUBLE, 1.0, 2.0, 0L, 10L));
    assertFalse(might(gt(col("d"), lit(3.0, DOUBLE)), d));
    assertTrue(might(gt(col("d"), lit(Double.NaN, DOUBLE)), d));
    assertTrue(might(eq(col("d"), lit(Double.NaN, DOUBLE)), d));
    Map<String, ColumnRange> nanMax = stats("d", ColumnRange.of(DOUBLE, 1.0, Double.NaN, 0L, 10L));
    assertTrue(might(gt(col("d"), lit(3.0, DOUBLE)), nanMax));
    // the min still bounds the values from below
    assertFalse(might(lt(col("d"), lit(0.5, DOUBLE)), nanMax));

    // -0.0 = 0.0 in SQL
    Map<String, ColumnRange> negativeZero = stats("d", ColumnRange.of(DOUBLE, -0.0, -0.0, 0L, 10L));
    assertTrue(might(eq(col("d"), lit(0.0, DOUBLE)), negativeZero));
    assertTrue(might(not(eq(col("d"), lit(0.0, DOUBLE))), negativeZero));
    assertFalse(might(not(eq(col("d"), lit(-0.0, DOUBLE))), negativeZero));
    assertTrue(might(gteq(col("d"), lit(0.0, DOUBLE)), negativeZero));
    assertFalse(might(gt(col("d"), lit(1.0, DOUBLE)), negativeZero));
  }

  @Test
  void startsWith() {
    Map<String, ColumnRange> s = stats("s", ColumnRange.of(STRING, "abc1", "abc9", 0L, 10L));
    // prefix bounds: every value starts with "abc" although min > "abc"
    assertTrue(might(Predicates.startsWith(col("s"), lit("abc", STRING)), s));
    assertTrue(might(Predicates.startsWith(col("s"), lit("abc5", STRING)), s));
    assertTrue(might(Predicates.startsWith(col("s"), lit("", STRING)), s));
    assertFalse(might(Predicates.startsWith(col("s"), lit("abd", STRING)), s));
    assertFalse(might(Predicates.startsWith(col("s"), lit("abb", STRING)), s));
    assertFalse(might(Predicates.startsWith(col("s"), lit("b", STRING)), s));
    assertFalse(might(Predicates.startsWith(col("s"), lit("abc95", STRING)), s));
    // NOT startsWith prunes only when min and max both start with the prefix
    assertFalse(might(not(Predicates.startsWith(col("s"), lit("abc", STRING))), s));
    assertTrue(might(not(Predicates.startsWith(col("s"), lit("abc1", STRING))), s));

    Map<String, ColumnRange> wide = stats("s", ColumnRange.of(STRING, "aba", "adf", 1L, 10L));
    assertTrue(might(Predicates.startsWith(col("s"), lit("abc", STRING)), wide));
    assertTrue(might(not(Predicates.startsWith(col("s"), lit("abc", STRING))), wide));

    assertTrue(might(Predicates.startsWithAny(col("s"), Arrays.asList(lit("x", STRING), lit("abc", STRING))), s));
    assertFalse(might(Predicates.startsWithAny(col("s"), Arrays.asList(lit("x", STRING), lit("y", STRING))), s));
    assertFalse(might(not(Predicates.startsWithAny(col("s"), Arrays.asList(lit("x", STRING), lit("ab", STRING)))), s));
    assertTrue(might(not(Predicates.startsWithAny(col("s"), Arrays.asList(lit("x", STRING), lit("y", STRING)))), s));

    // not a string column
    assertTrue(might(Predicates.startsWith(col("a"), lit("1", STRING)), intStats()));
    // all null
    assertFalse(might(Predicates.startsWith(col("s"), lit("a", STRING)), stats("s", ColumnRange.of(STRING, null, null, 3L, 3L))));
  }

  @Test
  void stringOrders() {
    Map<String, ColumnRange> s = stats("s", ColumnRange.of(STRING, "b", "d", 0L, 10L));
    assertFalse(might(lt(col("s"), lit("b", STRING)), s));
    assertFalse(might(gt(col("s"), lit("d", STRING)), s));
    assertTrue(might(gt(col("s"), lit("c" + cp(0xFFFF), STRING)), s));
    // the decision is made before any character at or above U+D800
    assertFalse(might(gt(col("s"), lit("e" + cp(0x1F600), STRING)), s));
    // U+E000 sorts after a surrogate pair in UTF-16 and before it in UTF-8: unknown
    Map<String, ColumnRange> high = stats("s", ColumnRange.of(STRING, "x" + cp(0xE000), "x" + cp(0xE000), 0L, 10L));
    assertTrue(might(gt(col("s"), lit("x" + cp(0x1F600), STRING)), high));
    assertTrue(might(lt(col("s"), lit("x" + cp(0x1F600), STRING)), high));
    // equality does not depend on the order
    assertFalse(might(not(eq(col("s"), lit("x" + cp(0xE000), STRING))), high));
    // max = U+FFFF in UTF-16 order, but a smiley sorts after it in UTF-8 order
    Map<String, ColumnRange> utf16 = stats("s", ColumnRange.of(STRING, cp(0x1F601), cp(0xFFFF), 0L, 3L));
    assertTrue(might(gt(col("s"), lit(cp(0xFFFF), STRING)), utf16));

    assertEquals(0, HudiStatsEvaluator.compareStrings("abc", "abc"));
    // equal, but a bound equal to "a" + U+FFFF bounds strings that sort differently in the two orders
    assertNull(HudiStatsEvaluator.compareStrings("a" + cp(0xFFFF), "a" + cp(0xFFFF)));
    assertNull(HudiStatsEvaluator.compareStrings("a" + cp(0xD83D), "a" + cp(0xD83D)));
    assertEquals(-1, HudiStatsEvaluator.compareStrings("ab", "abc"));
    assertEquals(1, HudiStatsEvaluator.compareStrings("b" + cp(0xE000), "a" + cp(0xE000)));
    assertNull(HudiStatsEvaluator.compareStrings("a" + cp(0xE000), "a" + cp(0x1F600)));
    assertNull(HudiStatsEvaluator.compareStrings(cp(0xE000) + "b", cp(0xE000) + "c"));
  }

  @Test
  void pruneManyFiles() {
    Map<String, Map<String, ColumnRange>> files = new HashMap<>();
    files.put("f1", stats("a", ColumnRange.of(INT, 1, 5, 0L, 10L)));
    files.put("f2", stats("a", ColumnRange.of(INT, 6, 10, 0L, 10L)));
    files.put("f3", new HashMap<>());
    assertEquals(new HashSet<>(Arrays.asList("f2", "f3")), EVALUATOR.prune(gt(col("a"), lit(5, INT)), files));
  }
}
