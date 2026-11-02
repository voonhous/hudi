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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * The expression oracle of RFC-88's test plan for {@link HudiStatsEvaluator}: over random files and random filters,
 * a file holding a row that matches the filter must never be pruned, and the evaluator must still prune.
 *
 * <p>Rows are evaluated with SQL (three-valued) semantics. The order an engine compares strings and doubles with,
 * and the order the stats were computed with, are drawn independently per round: strings by code point (UTF-8 bytes)
 * or UTF-16 ({@link String#compareTo}), doubles by SQL ({@code -0.0 = 0.0}) or Java ({@link Double#compare}).
 */
public class TestHudiStatsEvaluatorOracle {

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

  private static final long SEED = 20261102L;
  private static final int ROUNDS = 1000;
  private static final int FILES_PER_ROUND = 40;
  private static final int FILTERS_PER_ROUND = 60;

  private static final Map<String, HoodieSchema> COLUMNS = new LinkedHashMap<>();

  static {
    COLUMNS.put("b", HoodieSchema.create(HoodieSchemaType.BOOLEAN));
    COLUMNS.put("i", HoodieSchema.create(HoodieSchemaType.INT));
    COLUMNS.put("l", HoodieSchema.create(HoodieSchemaType.LONG));
    COLUMNS.put("d", HoodieSchema.create(HoodieSchemaType.DOUBLE));
    COLUMNS.put("f", HoodieSchema.create(HoodieSchemaType.FLOAT));
    COLUMNS.put("s", HoodieSchema.create(HoodieSchemaType.STRING));
    COLUMNS.put("dec", HoodieSchema.createDecimal(10, 2));
    COLUMNS.put("dt", HoodieSchema.createDate());
    COLUMNS.put("ts", HoodieSchema.createTimestampMicros());
  }

  private static final String[] STRINGS = {
      "", "a", "ab", "abc", "abd", "abc1", "b", "ba", "bb", "c", "z", cp(0x00E9), cp(0x07FF), cp(0xE000), cp(0xE000) + "x", cp(0xFFFF),
      cp(0x1F600), cp(0x1F601), "a" + cp(0xE000), "a" + cp(0x1F600), "x" + cp(0xFFFF), "x" + cp(0x10000)};

  private static final double[] DOUBLES = {-2.5, -1.0, -0.0, 0.0, 0.5, 1.0, 2.0, 3.25, Double.NaN};

  private final Random random = new Random(SEED);

  /**
   * The engine's (and stats') orders for one round.
   */
  private static final class Orders {
    final Comparator<String> statsStrings;
    final Comparator<String> engineStrings;
    final boolean engineJavaDoubles;

    Orders(Comparator<String> statsStrings, Comparator<String> engineStrings, boolean engineJavaDoubles) {
      this.statsStrings = statsStrings;
      this.engineStrings = engineStrings;
      this.engineJavaDoubles = engineJavaDoubles;
    }
  }

  private static final Comparator<String> UTF16 = String::compareTo;

  private static final Comparator<String> UTF8 = (a, b) -> {
    byte[] x = a.getBytes(StandardCharsets.UTF_8);
    byte[] y = b.getBytes(StandardCharsets.UTF_8);
    int n = Math.min(x.length, y.length);
    for (int k = 0; k < n; k++) {
      int c = Integer.compare(x[k] & 0xff, y[k] & 0xff);
      if (c != 0) {
        return c;
      }
    }
    return Integer.compare(x.length, y.length);
  };

  @Test
  void neverPrunesAFileWithAMatchingRow() {
    long checks = 0;
    long matching = 0;
    long pruned = 0;
    long notMatching = 0;
    for (int round = 0; round < ROUNDS; round++) {
      Orders orders = new Orders(random.nextBoolean() ? UTF8 : UTF16, random.nextBoolean() ? UTF8 : UTF16, random.nextBoolean());
      List<List<Map<String, Object>>> files = new ArrayList<>();
      List<Map<String, ColumnRange>> stats = new ArrayList<>();
      for (int f = 0; f < FILES_PER_ROUND; f++) {
        List<Map<String, Object>> rows = randomRows();
        files.add(rows);
        stats.add(rangesOf(rows, orders));
      }
      for (int q = 0; q < FILTERS_PER_ROUND; q++) {
        Expression filter = randomFilter(random.nextInt(4));
        for (int f = 0; f < files.size(); f++) {
          boolean anyRowMatches = false;
          for (Map<String, Object> row : files.get(f)) {
            if (Boolean.TRUE.equals(eval(filter, row, orders))) {
              anyRowMatches = true;
              break;
            }
          }
          boolean might = HudiStatsEvaluator.getInstance().mightMatch(filter, stats.get(f));
          checks++;
          if (anyRowMatches) {
            matching++;
            if (!might) {
              fail(ascii("Pruned a file with a matching row. Filter: " + filter + "\nrows: " + files.get(f) + "\nstats: " + stats.get(f))
                  + "\nstats strings " + (orders.statsStrings == UTF8 ? "UTF-8" : "UTF-16") + ", engine strings "
                  + (orders.engineStrings == UTF8 ? "UTF-8" : "UTF-16") + ", engine doubles " + (orders.engineJavaDoubles ? "Java" : "SQL"));
            }
          } else {
            notMatching++;
            if (!might) {
              pruned++;
            }
          }
        }
      }
    }
    System.out.println("Expression oracle: " + checks + " (file, filter) checks, " + matching + " with a matching row, "
        + pruned + " of " + notMatching + " files without a matching row pruned");
    assertTrue(matching > checks / 10, "too few matching files: " + matching + " of " + checks);
    // the evaluator is not trivially "might match": it prunes a good share of the files without a matching row
    assertTrue(pruned > notMatching / 4, "pruned only " + pruned + " of " + notMatching + " files without a matching row");
  }

  /**
   * Escapes non-ASCII characters, for readable failure messages.
   */
  private static String ascii(String text) {
    StringBuilder sb = new StringBuilder();
    for (char c : text.toCharArray()) {
      if (c < 128) {
        sb.append(c);
      } else {
        sb.append(String.format("<U+%04X>", (int) c));
      }
    }
    return sb.toString();
  }

  // -------------------------------------------------------------------------
  //  Data
  // -------------------------------------------------------------------------

  private List<Map<String, Object>> randomRows() {
    int count = random.nextInt(8);
    // per file: some columns all null, some with many nulls, values from a narrow window for pruning
    Map<String, Double> nullRate = new HashMap<>();
    Map<String, Integer> offset = new HashMap<>();
    for (String column : COLUMNS.keySet()) {
      int kind = random.nextInt(10);
      nullRate.put(column, kind == 0 ? 1.0 : kind < 3 ? 0.4 : 0.05);
      offset.put(column, random.nextInt(6));
    }
    List<Map<String, Object>> rows = new ArrayList<>();
    for (int r = 0; r < count; r++) {
      Map<String, Object> row = new HashMap<>();
      for (String column : COLUMNS.keySet()) {
        if (random.nextDouble() < nullRate.get(column)) {
          row.put(column, null);
        } else {
          row.put(column, randomValue(column, offset.get(column)));
        }
      }
      rows.add(row);
    }
    return rows;
  }

  private Object randomValue(String column, int offset) {
    int window = random.nextInt(4);
    switch (column) {
      case "b":
        return random.nextBoolean();
      case "i":
        return offset * 3 + window - 6;
      case "l":
        return (offset * 3L + window - 6) * 1000L;
      case "d":
        return random.nextInt(30) == 0 ? Double.NaN : DOUBLES[(offset + window) % (DOUBLES.length - 1)];
      case "f":
        return (float) DOUBLES[(offset + window) % (DOUBLES.length - 1)];
      case "s":
        return STRINGS[(offset * 3 + window) % STRINGS.length];
      case "dec":
        BigDecimal value = BigDecimal.valueOf((offset * 3 + window - 6) * 25L, 2);
        return random.nextBoolean() ? value : value.stripTrailingZeros();
      case "dt":
        return 18000 + offset * 3 + window;
      case "ts":
        return 1_600_000_000_000_000L + (offset * 3L + window) * 1_000_000L;
      default:
        throw new IllegalArgumentException(column);
    }
  }

  private Object randomLiteralValue(String column) {
    switch (column) {
      case "d":
        return random.nextInt(10) == 0 ? Double.NaN : DOUBLES[random.nextInt(DOUBLES.length - 1)];
      case "s":
        return STRINGS[random.nextInt(STRINGS.length)];
      default:
        return randomValue(column, random.nextInt(7));
    }
  }

  /**
   * The stats of a file as a writer computes them: min and max in the stats order, null and value counts; parts are
   * dropped at random (unknown).
   */
  private Map<String, ColumnRange> rangesOf(List<Map<String, Object>> rows, Orders orders) {
    Map<String, ColumnRange> ranges = new HashMap<>();
    for (Map.Entry<String, HoodieSchema> column : COLUMNS.entrySet()) {
      if (random.nextInt(15) == 0) {
        continue;
      }
      Comparable<?> min = null;
      Comparable<?> max = null;
      long nulls = 0;
      for (Map<String, Object> row : rows) {
        Object value = row.get(column.getKey());
        if (value == null) {
          nulls++;
          continue;
        }
        if (min == null || statsCompare(value, min, orders) < 0) {
          min = (Comparable<?>) value;
        }
        if (max == null || statsCompare(value, max, orders) > 0) {
          max = (Comparable<?>) value;
        }
      }
      Long nullCount = random.nextInt(10) == 0 ? null : nulls;
      Long valueCount = random.nextInt(10) == 0 ? null : (long) rows.size();
      if (random.nextInt(20) == 0) {
        min = null;
      }
      if (random.nextInt(20) == 0) {
        max = null;
      }
      ranges.put(column.getKey(), ColumnRange.of(column.getValue(), min, max, nullCount, valueCount));
    }
    return ranges;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static int statsCompare(Object a, Object b, Orders orders) {
    if (a instanceof String) {
      return orders.statsStrings.compare((String) a, (String) b);
    }
    // writers order doubles and floats as Java does: -0.0 before 0.0, NaN last
    return ((Comparable) a).compareTo(b);
  }

  // -------------------------------------------------------------------------
  //  Filters
  // -------------------------------------------------------------------------

  private Expression randomFilter(int depth) {
    if (depth == 0 || random.nextInt(4) == 0) {
      return randomLeaf();
    }
    switch (random.nextInt(3)) {
      case 0:
        return Predicates.and(randomFilter(depth - 1), randomFilter(depth - 1));
      case 1:
        return Predicates.or(randomFilter(depth - 1), randomFilter(depth - 1));
      default:
        return Predicates.not(randomFilter(depth - 1));
    }
  }

  private Expression randomLeaf() {
    List<String> names = new ArrayList<>(COLUMNS.keySet());
    String column = names.get(random.nextInt(names.size()));
    HoodieSchema type = COLUMNS.get(column);
    NameReference ref = new NameReference(column);
    int kind = random.nextInt(20);
    if (kind == 0) {
      return Predicates.isNull(ref);
    } else if (kind == 1) {
      return Predicates.isNotNull(ref);
    } else if (kind == 2) {
      return random.nextBoolean() ? Predicates.alwaysTrue() : Predicates.alwaysFalse();
    } else if (kind <= 4) {
      List<Expression> values = new ArrayList<>();
      int n = random.nextInt(4);
      for (int k = 0; k < n; k++) {
        values.add(random.nextInt(8) == 0 ? new Literal<>(null, type) : literal(column, type));
      }
      return Predicates.in(ref, values);
    } else if (kind <= 7) {
      // string predicates on the string column
      NameReference s = new NameReference("s");
      HoodieSchema string = COLUMNS.get("s");
      String prefix = STRINGS[random.nextInt(STRINGS.length)];
      if (random.nextBoolean() && prefix.length() > 1) {
        prefix = prefix.substring(0, prefix.length() - 1);
      }
      switch (random.nextInt(3)) {
        case 0:
          return Predicates.startsWith(s, new Literal<>(prefix, string));
        case 1:
          List<Expression> prefixes = new ArrayList<>();
          prefixes.add(new Literal<>(prefix, string));
          prefixes.add(new Literal<>(STRINGS[random.nextInt(STRINGS.length)], string));
          return Predicates.startsWithAny(s, prefixes);
        default:
          return Predicates.contains(s, new Literal<>(prefix, string));
      }
    }
    Expression lit = random.nextInt(25) == 0 ? new Literal<>(null, type) : literal(column, type);
    boolean flipped = random.nextBoolean();
    Expression left = flipped ? lit : ref;
    Expression right = flipped ? ref : lit;
    switch (random.nextInt(5)) {
      case 0:
        return Predicates.eq(left, right);
      case 1:
        return Predicates.lt(left, right);
      case 2:
        return Predicates.lteq(left, right);
      case 3:
        return Predicates.gt(left, right);
      default:
        return Predicates.gteq(left, right);
    }
  }

  private Literal<?> literal(String column, HoodieSchema type) {
    Object value = randomLiteralValue(column);
    if (value instanceof BigDecimal) {
      // decimal literals carry their own precision and scale
      BigDecimal decimal = (BigDecimal) value;
      return new Literal<>(decimal, HoodieSchema.createDecimal(Math.max(decimal.precision(), Math.max(decimal.scale(), 1)), Math.max(decimal.scale(), 0)));
    }
    return new Literal<>(value, type);
  }

  // -------------------------------------------------------------------------
  //  SQL evaluation of a row
  // -------------------------------------------------------------------------

  /**
   * TRUE, FALSE or null (UNKNOWN).
   */
  private static Boolean eval(Expression expr, Map<String, Object> row, Orders orders) {
    if (expr instanceof Predicates.TrueExpression) {
      return true;
    }
    if (expr instanceof Predicates.FalseExpression) {
      return false;
    }
    if (expr instanceof Predicates.Not) {
      Boolean child = eval(expr.getChildren().get(0), row, orders);
      return child == null ? null : !child;
    }
    if (expr instanceof Predicates.And) {
      Boolean left = eval(((Predicates.And) expr).getLeft(), row, orders);
      Boolean right = eval(((Predicates.And) expr).getRight(), row, orders);
      if (Boolean.FALSE.equals(left) || Boolean.FALSE.equals(right)) {
        return false;
      }
      return left == null || right == null ? null : true;
    }
    if (expr instanceof Predicates.Or) {
      Boolean left = eval(((Predicates.Or) expr).getLeft(), row, orders);
      Boolean right = eval(((Predicates.Or) expr).getRight(), row, orders);
      if (Boolean.TRUE.equals(left) || Boolean.TRUE.equals(right)) {
        return true;
      }
      return left == null || right == null ? null : false;
    }
    if (expr instanceof Predicates.IsNull) {
      return value(expr.getChildren().get(0), row) == null;
    }
    if (expr instanceof Predicates.IsNotNull) {
      return value(expr.getChildren().get(0), row) != null;
    }
    if (expr instanceof Predicates.In) {
      List<Expression> children = expr.getChildren();
      Object value = value(children.get(0), row);
      if (value == null) {
        return null;
      }
      boolean sawNull = false;
      for (int k = 1; k < children.size(); k++) {
        Object candidate = value(children.get(k), row);
        if (candidate == null) {
          sawNull = true;
        } else if (Integer.valueOf(0).equals(engineCompare(value, candidate, orders))) {
          return true;
        }
      }
      return sawNull ? null : false;
    }
    if (expr instanceof Predicates.StringStartsWith) {
      Object value = value(((Predicates.StringStartsWith) expr).getLeft(), row);
      Object prefix = value(((Predicates.StringStartsWith) expr).getRight(), row);
      return value == null || prefix == null ? null : value.toString().startsWith(prefix.toString());
    }
    if (expr instanceof Predicates.StringStartsWithAny) {
      List<Expression> children = expr.getChildren();
      Object value = value(children.get(0), row);
      if (value == null) {
        return null;
      }
      for (int k = 1; k < children.size(); k++) {
        if (value.toString().startsWith(value(children.get(k), row).toString())) {
          return true;
        }
      }
      return false;
    }
    if (expr instanceof Predicates.StringContains) {
      Object value = value(((Predicates.StringContains) expr).getLeft(), row);
      Object part = value(((Predicates.StringContains) expr).getRight(), row);
      return value == null || part == null ? null : value.toString().contains(part.toString());
    }
    if (expr instanceof Predicates.BinaryComparison) {
      Predicates.BinaryComparison cmp = (Predicates.BinaryComparison) expr;
      Object left = value(cmp.getLeft(), row);
      Object right = value(cmp.getRight(), row);
      if (left == null || right == null) {
        return null;
      }
      Integer c = engineCompare(left, right, orders);
      if (c == null) {
        // NaN in SQL (IEEE) comparisons
        return false;
      }
      switch (cmp.getOperator()) {
        case EQ:
          return c == 0;
        case LT:
          return c < 0;
        case LT_EQ:
          return c <= 0;
        case GT:
          return c > 0;
        case GT_EQ:
          return c >= 0;
        default:
          throw new IllegalArgumentException(cmp.toString());
      }
    }
    throw new IllegalArgumentException("Unexpected expression " + expr);
  }

  private static Object value(Expression expr, Map<String, Object> row) {
    if (expr instanceof NameReference) {
      return row.get(((NameReference) expr).getName());
    }
    return ((Literal<?>) expr).getValue();
  }

  /**
   * The engine's comparison; null when a SQL comparison with NaN is false whatever the operator.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  private static Integer engineCompare(Object a, Object b, Orders orders) {
    if (a instanceof String) {
      return Integer.signum(orders.engineStrings.compare((String) a, (String) b));
    }
    if (a instanceof Double || a instanceof Float) {
      double x = ((Number) a).doubleValue();
      double y = ((Number) b).doubleValue();
      if (orders.engineJavaDoubles) {
        return Integer.signum(Double.compare(x, y));
      }
      if (Double.isNaN(x) || Double.isNaN(y)) {
        return null;
      }
      return x < y ? -1 : x > y ? 1 : 0;
    }
    if (a instanceof BigDecimal) {
      return Integer.signum(((BigDecimal) a).compareTo((BigDecimal) b));
    }
    return Integer.signum(((Comparable) a).compareTo(b));
  }
}
