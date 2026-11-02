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

package org.apache.hudi.aws.sync.util;

import org.apache.hudi.common.expression.Expression;
import org.apache.hudi.common.expression.Literal;
import org.apache.hudi.common.expression.NameReference;
import org.apache.hudi.common.expression.Predicates;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.Option;

import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * {@link GlueRenderer} against {@link GlueFilterGenVisitor} on the expressions {@link GluePartitionFilterGenerator}
 * builds, plus what only the renderer handles.
 */
class TestGlueRenderer {

  private static final GlueRenderer RENDERER = new GlueRenderer();

  private static void assertSameAsVisitor(String expected, Function<Boolean, Expression> build) {
    assertEquals(expected, build.apply(false).accept(new GlueFilterGenVisitor()));
    assertEquals(expected, render(build.apply(true)));
  }

  @Test
  void matchesGlueFilterGenVisitor() {
    assertSameAsVisitor("((((date = 2022-09-01 AND year = '2022') AND month = 9) AND day = 1) OR (((date = 2022-09-02 AND year = '2022') AND month = 9) AND day = 2))",
        canonical -> Predicates.or(partition(canonical, "2022-09-01", 1L), partition(canonical, "2022-09-02", 2L)));
    assertSameAsVisitor("((date >= 2022-09-01 AND date <= 2022-09-04) AND (day >= 1 AND day <= 2))",
        canonical -> Predicates.and(
            Predicates.and(Predicates.gteq(ref("date"), date("2022-09-01", canonical)), Predicates.lteq(ref("date"), date("2022-09-04", canonical))),
            Predicates.and(Predicates.gteq(ref("day"), Literal.from(1L)), Predicates.lteq(ref("day"), Literal.from(2L)))));
    assertSameAsVisitor("date = 2022-09-01",
        canonical -> Predicates.and(Predicates.eq(ref("date"), date("2022-09-01", canonical)), Predicates.eq(ref("finished"), Literal.from(true))));
    assertSameAsVisitor("", canonical -> Predicates.or(Predicates.eq(ref("date"), date("2022-09-01", canonical)), Predicates.eq(ref("finished"), Literal.from(true))));
    assertSameAsVisitor("status = 'to_be_''escaped'", canonical -> Predicates.eq(ref("status"), Literal.from("to_be_'escaped")));
    assertSameAsVisitor("status = 'a\"''b'", canonical -> Predicates.eq(ref("status"), Literal.from("a\"'b")));
  }

  @Test
  void differsFromGlueFilterGenVisitor() {
    Expression canonicalDate = Predicates.eq(ref("date"), date("2022-09-01", true));
    assertEquals("date = 19236", canonicalDate.accept(new GlueFilterGenVisitor()));
    assertEquals("date = 2022-09-01", render(canonicalDate));
    Expression notIn = Predicates.not(Predicates.in(ref("p"), Arrays.asList(Literal.from("x"), Literal.from("y"))));
    assertEquals("", notIn.accept(new GlueFilterGenVisitor()));
    assertEquals("(p <> 'x' AND p <> 'y')", render(notIn));
    assertEquals("", render(Predicates.eq(ref("not"), Literal.from(1))));
  }

  private static String render(Expression expr) {
    Option<String> rendered = RENDERER.render(expr);
    return rendered.isPresent() ? rendered.get() : "";
  }

  private static Expression partition(boolean canonical, String dt, long day) {
    Expression root = Predicates.eq(ref("date"), date(dt, canonical));
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
