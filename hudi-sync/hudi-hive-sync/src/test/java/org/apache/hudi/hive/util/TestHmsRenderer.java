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

package org.apache.hudi.hive.util;

import org.apache.hudi.common.expression.Expression;
import org.apache.hudi.common.expression.Literal;
import org.apache.hudi.common.expression.NameReference;
import org.apache.hudi.common.expression.Predicates;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.util.Option;

import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * {@link HmsRenderer} against {@link FilterGenVisitor} on the expressions {@link PartitionFilterGenerator} builds,
 * plus what only the renderer handles.
 */
class TestHmsRenderer {

  private static final HmsRenderer RENDERER = new HmsRenderer();

  /**
   * Builds an expression with DATE literals either as {@link PartitionFilterGenerator} types them today (the
   * partition value string) or canonically (epoch days).
   */
  private static void assertSameAsVisitor(String expected, Function<Boolean, Expression> build) {
    assertEquals(expected, build.apply(false).accept(new FilterGenVisitor()));
    assertEquals(expected, render(build.apply(true)));
  }

  @Test
  void matchesFilterGenVisitor() {
    // buildPartitionExpression: OR of ANDs of equalities
    assertSameAsVisitor("((((dt = 2022-09-01 AND year = \"2022\") AND month = 9) AND day = 1) OR (((dt = 2022-09-02 AND year = \"2022\") AND month = 9) AND day = 2))",
        canonical -> Predicates.or(partition(canonical, "2022-09-01", 1L), partition(canonical, "2022-09-02", 2L)));
    // buildMinMaxPartitionExpression: ranges
    assertSameAsVisitor("((dt >= 2022-09-01 AND dt <= 2022-09-04) AND (day >= -9223372036854775808 AND day <= 2))",
        canonical -> Predicates.and(
            Predicates.and(Predicates.gteq(ref("dt"), date("2022-09-01", canonical)), Predicates.lteq(ref("dt"), date("2022-09-04", canonical))),
            Predicates.and(Predicates.gteq(ref("day"), Literal.from(Long.MIN_VALUE)), Predicates.lteq(ref("day"), Literal.from(2L)))));
    // unsupported types are dropped from an AND and drop an OR
    assertSameAsVisitor("dt = 2022-09-01",
        canonical -> Predicates.and(Predicates.eq(ref("dt"), date("2022-09-01", canonical)), Predicates.eq(ref("finished"), Literal.from(true))));
    assertSameAsVisitor("", canonical -> Predicates.or(Predicates.eq(ref("dt"), date("2022-09-01", canonical)), Predicates.eq(ref("finished"), Literal.from(true))));
    assertSameAsVisitor("", canonical -> Predicates.eq(ref("x"), Literal.from(1.5d)));
    // quoting
    assertSameAsVisitor("s = 'a\"b'", canonical -> Predicates.eq(ref("s"), Literal.from("a\"b")));
    assertSameAsVisitor("s < \"a'b\"", canonical -> Predicates.lt(ref("s"), Literal.from("a'b")));
  }

  @Test
  void differsFromFilterGenVisitor() {
    // canonical DATE literals: the visitor prints the epoch days
    Expression canonicalDate = Predicates.eq(ref("dt"), date("2022-09-01", true));
    assertEquals("dt = 19236", canonicalDate.accept(new FilterGenVisitor()));
    assertEquals("dt = 2022-09-01", render(canonicalDate));
    // NOT and IN are dropped by the visitor
    Expression notIn = Predicates.not(Predicates.in(ref("p"), Arrays.asList(Literal.from(1), Literal.from(2))));
    assertEquals("", notIn.accept(new FilterGenVisitor()));
    assertEquals("(p <> 1 AND p <> 2)", render(notIn));
    assertEquals("(p >= 3 OR s <> \"x\")", render(Predicates.not(Predicates.and(Predicates.lt(ref("p"), Literal.from(3)), Predicates.eq(ref("s"), Literal.from("x"))))));
    // literal on the left is written column first
    assertEquals("3 < p", Predicates.lt(Literal.from(3), ref("p")).accept(new FilterGenVisitor()));
    assertEquals("p > 3", render(Predicates.lt(Literal.from(3), ref("p"))));
    // a string with both quotes is unknown instead of throwing
    Expression bothQuotes = Predicates.eq(ref("s"), Literal.from("a'\"b"));
    assertThrows(UnsupportedOperationException.class, () -> bothQuotes.accept(new FilterGenVisitor()));
    assertEquals("p = 1", render(Predicates.and(Predicates.eq(ref("p"), Literal.from(1)), bothQuotes)));
    // two columns are not a metastore comparison
    assertEquals("p = q", Predicates.eq(ref("p"), ref("q")).accept(new FilterGenVisitor()));
    assertEquals("", render(Predicates.eq(ref("p"), ref("q"))));
  }

  @Test
  void placesOnlyPartitionColumns() {
    HoodieSchema partitions = HoodieSchema.createRecord("partitions", null, null, Arrays.asList(
        HoodieSchemaField.of("dt", HoodieSchema.createDate()),
        HoodieSchemaField.of("hr", HoodieSchema.createNullable(HoodieSchema.create(HoodieSchemaType.INT)))));
    HmsRenderer renderer = new HmsRenderer(partitions);
    Expression onData = Predicates.eq(ref("amount"), Literal.from(5));
    Expression onPartition = Predicates.eq(ref("hr"), Literal.from(5));
    assertEquals("hr = 5", renderer.render(Predicates.and(onPartition, onData)).get());
    assertFalse(renderer.render(Predicates.or(onPartition, onData)).isPresent());
    // a literal of another type than the partition column is not placed
    assertFalse(renderer.render(Predicates.eq(ref("hr"), Literal.from(5L))).isPresent());
    // names the filter grammar cannot hold
    assertFalse(RENDERER.render(Predicates.eq(ref("date"), Literal.from(5))).isPresent());
    assertFalse(RENDERER.render(Predicates.eq(ref("a.b"), Literal.from(5))).isPresent());
    assertFalse(RENDERER.render(Predicates.eq(ref("_c"), Literal.from(5))).isPresent());
  }

  private static String render(Expression expr) {
    Option<String> rendered = RENDERER.render(expr);
    return rendered.isPresent() ? rendered.get() : "";
  }

  private static Expression partition(boolean canonical, String dt, long day) {
    Expression root = Predicates.eq(ref("dt"), date(dt, canonical));
    root = Predicates.and(root, Predicates.eq(ref("year"), Literal.from("2022")));
    root = Predicates.and(root, Predicates.eq(ref("month"), Literal.from(9)));
    return Predicates.and(root, Predicates.eq(ref("day"), Literal.from(day)));
  }

  private static Literal<?> date(String iso, boolean canonical) {
    return canonical
        ? new Literal<>((int) LocalDate.parse(iso).toEpochDay(), HoodieSchema.createDate())
        : new Literal<>(iso, HoodieSchema.createDate());
  }

  private static NameReference ref(String name) {
    return new NameReference(name);
  }
}
