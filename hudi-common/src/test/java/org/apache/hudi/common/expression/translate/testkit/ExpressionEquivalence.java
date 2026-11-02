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

package org.apache.hudi.common.expression.translate.testkit;

import org.apache.hudi.common.expression.BoundReference;
import org.apache.hudi.common.expression.Expression;
import org.apache.hudi.common.expression.Literal;
import org.apache.hudi.common.expression.NameReference;
import org.apache.hudi.common.expression.Predicate;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Structural equality of Hudi expressions: the same tree of operators over the same column names and literal
 * values. Literals are equal when their {@code HoodieSchemaType} and normalized values are equal (any
 * {@code CharSequence} as a string, decimals by value), so translators may build values with other Java classes.
 */
public final class ExpressionEquivalence {

  private ExpressionEquivalence() {
  }

  public static List<String> differences(Expression expected, Expression actual) {
    List<String> diffs = new ArrayList<>();
    compare("$", expected, actual, diffs);
    return diffs;
  }

  public static void assertEquivalent(Expression expected, Expression actual) {
    List<String> diffs = differences(expected, actual);
    if (!diffs.isEmpty()) {
      throw new AssertionError("Expressions differ:\n  " + String.join("\n  ", diffs)
          + "\nexpected: " + expected + "\nactual:   " + actual);
    }
  }

  private static void compare(String path, Expression e, Expression a, List<String> diffs) {
    if (a == null) {
      diffs.add(path + ": expected " + e + " but was null");
      return;
    }
    if (e.getClass() != a.getClass()) {
      diffs.add(path + ": expected " + e.getClass().getSimpleName() + " " + e + " but was " + a.getClass().getSimpleName() + " " + a);
      return;
    }
    if (e instanceof NameReference) {
      check(path, "column", ((NameReference) e).getName(), ((NameReference) a).getName(), diffs);
      return;
    }
    if (e instanceof BoundReference) {
      check(path, "bound reference", e.toString(), a.toString(), diffs);
      return;
    }
    if (e instanceof Literal) {
      Literal<?> el = (Literal<?>) e;
      Literal<?> al = (Literal<?>) a;
      check(path, "literal type", el.getDataType().getType(), al.getDataType().getType(), diffs);
      check(path, "literal value", normalize(el.getValue()), normalize(al.getValue()), diffs);
      return;
    }
    if (e instanceof Predicate) {
      check(path, "operator", ((Predicate) e).getOperator(), ((Predicate) a).getOperator(), diffs);
    }
    List<Expression> ec = e.getChildren();
    List<Expression> ac = a.getChildren();
    int ecSize = ec == null ? 0 : ec.size();
    int acSize = ac == null ? 0 : ac.size();
    if (ecSize != acSize) {
      diffs.add(path + ": expected " + ecSize + " operands but was " + acSize + " (" + e + " vs " + a + ")");
      return;
    }
    for (int i = 0; i < ecSize; i++) {
      compare(path + "/" + i, ec.get(i), ac.get(i), diffs);
    }
  }

  private static Object normalize(Object value) {
    if (value instanceof CharSequence) {
      return value.toString();
    }
    if (value instanceof BigDecimal) {
      return ((BigDecimal) value).stripTrailingZeros();
    }
    return value;
  }

  private static void check(String path, String what, Object expected, Object actual, List<String> diffs) {
    if (!Objects.equals(expected, actual)) {
      diffs.add(path + ": expected " + what + " " + expected + " but was " + actual);
    }
  }
}
