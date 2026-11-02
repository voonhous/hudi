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

import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.IntPredicate;

/**
 * The {@link StatsEvaluator} of RFC-88 section 4.3 that evaluates in {@code hudi-common}: a conservative,
 * three-valued evaluation of a Hudi {@link Expression} over {@link ColumnRange}s.
 *
 * <p>A row matches when the filter is TRUE for it, as in SQL: a comparison with a null value is NULL, never TRUE, and
 * so is its negation. NOT is pushed down to the leaves: {@code NOT(a AND b)} is {@code NOT a OR NOT b},
 * {@code NOT(a OR b)} is {@code NOT a AND NOT b}, and a negated leaf asks whether the leaf may be FALSE for some row,
 * i.e. hold the opposite way for a non-null value. For example {@code NOT(a = v)} rules a file out only if
 * {@code min = max = v} (its nulls do not match either), {@code NOT(a < v)} is {@code a >= v} over the non-null values,
 * and {@code NOT(a IS NULL)} is {@code a IS NOT NULL}. AND rules a file out if any operand does, OR if every operand
 * does.
 *
 * <p>Every answer is conservative: {@code false} only when no row can match. The following mean "might match":
 * <ul>
 *   <li>a column without a range, and any unknown part of a range;</li>
 *   <li>a leaf the evaluator does not understand (e.g. CONTAINS, a comparison of two columns, a null literal);</li>
 *   <li>a literal whose type or Java class differs from the range's (no coercion is attempted);</li>
 *   <li>NaN in a range or a literal, and a comparison of {@code -0.0} with {@code 0.0}, which SQL engines treat as
 *       equal and Java orders. Writers order NaN last, so a floating-point column whose max is unknown or NaN may hold
 *       NaN, which satisfies every negated comparison under IEEE semantics: such a negated comparison might match;</li>
 *   <li>a string comparison that the code-point (UTF-8 byte) order and the UTF-16 ({@link String#compareTo}) order
 *       could decide differently. Column stats read from Parquet footers and computed from Avro records use the
 *       unsigned UTF-8 byte order, while merged stats (e.g. partition stats) take the minimum and maximum with
 *       {@link String#compareTo}; the two orders agree unless the strings differ at, or after, a character at or above
 *       U+D800 (see {@link #compareStrings}).</li>
 * </ul>
 */
public class HudiStatsEvaluator implements StatsEvaluator {

  private static final long serialVersionUID = 1L;

  private static final HudiStatsEvaluator INSTANCE = new HudiStatsEvaluator();

  public static HudiStatsEvaluator getInstance() {
    return INSTANCE;
  }

  @Override
  public boolean mightMatch(Expression filter, Map<String, ColumnRange> stats) {
    return mightMatch(filter, false, stats);
  }

  /**
   * @param negated whether the expression is under an odd number of NOTs: then the question is whether it may be
   *                FALSE (not NULL) for some row
   */
  private static boolean mightMatch(Expression expr, boolean negated, Map<String, ColumnRange> stats) {
    if (expr instanceof Predicates.TrueExpression) {
      return !negated;
    }
    if (expr instanceof Predicates.FalseExpression) {
      return negated;
    }
    if (expr instanceof Predicates.Not) {
      return mightMatch(expr.getChildren().get(0), !negated, stats);
    }
    if (expr instanceof Predicates.And) {
      Predicates.And and = (Predicates.And) expr;
      return negated
          ? mightMatch(and.getLeft(), true, stats) || mightMatch(and.getRight(), true, stats)
          : mightMatch(and.getLeft(), false, stats) && mightMatch(and.getRight(), false, stats);
    }
    if (expr instanceof Predicates.Or) {
      Predicates.Or or = (Predicates.Or) expr;
      return negated
          ? mightMatch(or.getLeft(), true, stats) && mightMatch(or.getRight(), true, stats)
          : mightMatch(or.getLeft(), false, stats) || mightMatch(or.getRight(), false, stats);
    }
    try {
      return leaf(expr, negated, stats);
    } catch (RuntimeException e) {
      // a malformed leaf, e.g. missing children: it might match
      return true;
    }
  }

  private static boolean leaf(Expression expr, boolean negated, Map<String, ColumnRange> stats) {
    if (expr instanceof Predicates.BinaryComparison) {
      Predicates.BinaryComparison cmp = (Predicates.BinaryComparison) expr;
      if (cmp.getLeft() instanceof NameReference && cmp.getRight() instanceof Literal) {
        return comparison(Cmp.of(cmp.getOperator()), (NameReference) cmp.getLeft(), (Literal<?>) cmp.getRight(), negated, stats);
      }
      if (cmp.getLeft() instanceof Literal && cmp.getRight() instanceof NameReference) {
        Cmp op = Cmp.of(cmp.getOperator());
        return comparison(op == null ? null : op.flip(), (NameReference) cmp.getRight(), (Literal<?>) cmp.getLeft(), negated, stats);
      }
      return true;
    }
    if (expr instanceof Predicates.In) {
      return in((Predicates.In) expr, negated, stats);
    }
    if (expr instanceof Predicates.IsNull) {
      return nullCheck(expr.getChildren().get(0), !negated, stats);
    }
    if (expr instanceof Predicates.IsNotNull) {
      return nullCheck(expr.getChildren().get(0), negated, stats);
    }
    if (expr instanceof Predicates.StringStartsWith) {
      Predicates.StringStartsWith startsWith = (Predicates.StringStartsWith) expr;
      return startsWithAny(startsWith.getLeft(), Collections.singletonList(startsWith.getRight()), negated, stats);
    }
    if (expr instanceof Predicates.StringStartsWithAny) {
      List<Expression> children = expr.getChildren();
      return startsWithAny(children.get(0), ((Predicates.StringStartsWithAny) expr).getRightChildren(), negated, stats);
    }
    // CONTAINS and anything else
    return true;
  }

  // -------------------------------------------------------------------------
  //  Leaves
  // -------------------------------------------------------------------------

  /**
   * {@code column op literal}: may it be TRUE ({@code negated = false}) or FALSE ({@code negated = true}) for a row?
   */
  private static boolean comparison(Cmp op, NameReference column, Literal<?> literal, boolean negated,
                                    Map<String, ColumnRange> stats) {
    ColumnRange range = stats.get(column.getName());
    if (op == null || range == null) {
      return true;
    }
    if (isAllNull(range)) {
      // a comparison is NULL for every row, neither TRUE nor FALSE
      return false;
    }
    Object value = literalValue(literal, range);
    if (value == null) {
      return true;
    }
    Cmp effective = negated ? op.negate() : op;
    Object min = range.min().orElse(null);
    Object max = range.max().orElse(null);
    if (negated && mayHoldNaN(value, max)) {
      // with IEEE comparisons (e.g. Trino) every comparison with NaN is FALSE, so NaN satisfies the negation of any
      // comparison; NaN sorts last in the stats, so it may be present unless the max is a number
      return true;
    }
    switch (effective) {
      case EQ:
        return !isTrue(compare(min, value), c -> c > 0) && !isTrue(compare(max, value), c -> c < 0);
      case LT:
        return !isTrue(compare(min, value), c -> c >= 0);
      case LT_EQ:
        return !isTrue(compare(min, value), c -> c > 0);
      case GT:
        return !isTrue(compare(max, value), c -> c <= 0);
      case GT_EQ:
        return !isTrue(compare(max, value), c -> c < 0);
      case NOT_EQ:
        // some non-null value differs from v unless every one of them equals v
        return !(isEqual(min, value) && isEqual(max, value));
      default:
        return true;
    }
  }

  /**
   * Whether a floating-point column may hold NaN: its max is unknown or NaN.
   */
  private static boolean mayHoldNaN(Object value, Object max) {
    if (!(value instanceof Double) && !(value instanceof Float)) {
      return false;
    }
    return max == null || (max instanceof Double && ((Double) max).isNaN()) || (max instanceof Float && ((Float) max).isNaN());
  }

  private static boolean in(Predicates.In in, boolean negated, Map<String, ColumnRange> stats) {
    List<Expression> children = in.getChildren();
    if (!(children.get(0) instanceof NameReference)) {
      return true;
    }
    ColumnRange range = stats.get(((NameReference) children.get(0)).getName());
    if (range == null) {
      return true;
    }
    if (isAllNull(range)) {
      return false;
    }
    Object min = range.min().orElse(null);
    Object max = range.max().orElse(null);
    List<Expression> values = in.getRightChildren();
    if (!negated) {
      // TRUE when the value equals one of the list: some v_i must fall in [min, max]
      for (Expression valueExpr : values) {
        if (!(valueExpr instanceof Literal)) {
          return true;
        }
        Literal<?> literal = (Literal<?>) valueExpr;
        if (literal.getValue() == null) {
          // a null in the list never makes IN TRUE
          continue;
        }
        Object value = literalValue(literal, range);
        if (value == null) {
          return true;
        }
        if (!isTrue(compare(min, value), c -> c > 0) && !isTrue(compare(max, value), c -> c < 0)) {
          return true;
        }
      }
      return false;
    }
    // FALSE when the non-null value equals none of the list: impossible only if min = max = some v_i. A null in the
    // list makes NOT IN never TRUE in SQL; it is ignored here, which keeps more files.
    for (Expression valueExpr : values) {
      if (valueExpr instanceof Literal && ((Literal<?>) valueExpr).getValue() != null) {
        Object value = literalValue((Literal<?>) valueExpr, range);
        if (value != null && isEqual(min, value) && isEqual(max, value)) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   * @param wantNull true: may some row be null? false: may some row be non-null?
   */
  private static boolean nullCheck(Expression child, boolean wantNull, Map<String, ColumnRange> stats) {
    if (!(child instanceof NameReference)) {
      return true;
    }
    ColumnRange range = stats.get(((NameReference) child).getName());
    if (range == null) {
      return true;
    }
    if (wantNull) {
      return !range.nullCount().isPresent() || range.nullCount().get() > 0;
    }
    return !isAllNull(range);
  }

  private static boolean startsWithAny(Expression left, List<Expression> prefixes, boolean negated, Map<String, ColumnRange> stats) {
    if (!(left instanceof NameReference)) {
      return true;
    }
    ColumnRange range = stats.get(((NameReference) left).getName());
    if (range == null || range.type() == null || nonNull(range.type()).getType() != HoodieSchemaType.STRING) {
      return true;
    }
    if (isAllNull(range)) {
      return false;
    }
    Object min = range.min().orElse(null);
    Object max = range.max().orElse(null);
    if (!negated) {
      // TRUE when the value starts with one of the prefixes
      for (Expression prefixExpr : prefixes) {
        String prefix = stringLiteral(prefixExpr);
        if (prefix == null || mightStartWith(min, max, prefix)) {
          return true;
        }
      }
      return false;
    }
    // FALSE when the non-null value starts with none of them: impossible only if every value between min and max
    // starts with one prefix, i.e. both min and max do (in any lexicographic order)
    for (Expression prefixExpr : prefixes) {
      String prefix = stringLiteral(prefixExpr);
      if (prefix != null && min instanceof String && max instanceof String
          && ((String) min).startsWith(prefix) && ((String) max).startsWith(prefix)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Whether a string in {@code [min, max]} might start with {@code prefix}: every such string is at least
   * {@code prefix}, and below any string that is greater than {@code prefix} without starting with it.
   */
  private static boolean mightStartWith(Object min, Object max, String prefix) {
    if (max instanceof String && isTrue(compareStrings((String) max, prefix), c -> c < 0)) {
      return false;
    }
    if (min instanceof String && !((String) min).startsWith(prefix) && isTrue(compareStrings((String) min, prefix), c -> c > 0)) {
      return false;
    }
    return true;
  }

  private static String stringLiteral(Expression expr) {
    if (!(expr instanceof Literal)) {
      return null;
    }
    Literal<?> literal = (Literal<?>) expr;
    if (literal.getValue() == null || literal.getDataType() == null || nonNull(literal.getDataType()).getType() != HoodieSchemaType.STRING
        || !(literal.getValue() instanceof CharSequence)) {
      return null;
    }
    return literal.getValue().toString();
  }

  // -------------------------------------------------------------------------
  //  Values and comparisons
  // -------------------------------------------------------------------------

  /**
   * Provably no non-null value: the counts say every value is null and there is no min or max.
   */
  private static boolean isAllNull(ColumnRange range) {
    return !range.min().isPresent() && !range.max().isPresent()
        && range.nullCount().isPresent() && range.valueCount().isPresent()
        && range.nullCount().get() >= range.valueCount().get();
  }

  /**
   * The literal's value if it can be compared with the range, otherwise null (unknown).
   */
  private static Object literalValue(Literal<?> literal, ColumnRange range) {
    Object value = literal.getValue();
    if (value == null || literal.getDataType() == null || range.type() == null) {
      return null;
    }
    HoodieSchema literalType = nonNull(literal.getDataType());
    HoodieSchema columnType = nonNull(range.type());
    if (!sameComparableType(literalType, columnType)) {
      return null;
    }
    if (value instanceof CharSequence && !(value instanceof String)) {
      value = value.toString();
    }
    Class<?> expected = valueClass(columnType);
    if (expected == null || !expected.isInstance(value)) {
      return null;
    }
    if (range.min().isPresent() && !expected.isInstance(range.min().get())) {
      return null;
    }
    if (range.max().isPresent() && !expected.isInstance(range.max().get())) {
      return null;
    }
    return value;
  }

  /**
   * Whether literals of one type compare with ranges of the other: the same type, and for timestamps and times the
   * same precision and time zone adjustment. Decimals compare numerically whatever their precision and scale. INT8,
   * INT16 and INT compare with each other: their values are all Integers. CHAR compares only with CHAR of the same
   * length, never with STRING: engines compare CHAR ignoring trailing spaces, so the order of the stored, padded
   * values is not the order of a STRING literal.
   */
  static boolean sameComparableType(HoodieSchema a, HoodieSchema b) {
    if (a.getType().isIntLike() && b.getType().isIntLike()) {
      return true;
    }
    if (a.getType() != b.getType() || valueClass(a) == null) {
      return false;
    }
    if (a instanceof HoodieSchema.Char && b instanceof HoodieSchema.Char) {
      return ((HoodieSchema.Char) a).getLength() == ((HoodieSchema.Char) b).getLength();
    }
    if (a instanceof HoodieSchema.Timestamp && b instanceof HoodieSchema.Timestamp) {
      HoodieSchema.Timestamp ta = (HoodieSchema.Timestamp) a;
      HoodieSchema.Timestamp tb = (HoodieSchema.Timestamp) b;
      return ta.getPrecision() == tb.getPrecision() && ta.isUtcAdjusted() == tb.isUtcAdjusted();
    }
    if (a instanceof HoodieSchema.Time && b instanceof HoodieSchema.Time) {
      return ((HoodieSchema.Time) a).getPrecision() == ((HoodieSchema.Time) b).getPrecision();
    }
    return a.getType() != HoodieSchemaType.TIMESTAMP && a.getType() != HoodieSchemaType.TIME;
  }

  /**
   * The canonical Java class of the values of the types this evaluator compares; null for the others (bytes, fixed,
   * enums, whose index order is not their Java order, UUIDs, nested types).
   */
  static Class<?> valueClass(HoodieSchema type) {
    switch (type.getType()) {
      case BOOLEAN:
        return Boolean.class;
      case INT:
      case INT8:
      case INT16:
      case DATE:
        return Integer.class;
      case LONG:
      case TIMESTAMP:
        return Long.class;
      case FLOAT:
        return Float.class;
      case DOUBLE:
        return Double.class;
      case STRING:
      case CHAR:
        return String.class;
      case DECIMAL:
        return BigDecimal.class;
      case TIME:
        return ((HoodieSchema.Time) type).getPrecision() == HoodieSchema.TimePrecision.MILLIS ? Integer.class : Long.class;
      default:
        return null;
    }
  }

  private static HoodieSchema nonNull(HoodieSchema schema) {
    return schema.getType() == HoodieSchemaType.UNION ? schema.getNonNullType() : schema;
  }

  /**
   * Compares two canonical values of the same class.
   *
   * @return the sign of the comparison, or null if either is absent or the order is not reliable
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  static Integer compare(Object a, Object b) {
    if (a == null || b == null || a.getClass() != b.getClass()) {
      return null;
    }
    if (a instanceof String) {
      return compareStrings((String) a, (String) b);
    }
    if (a instanceof Double) {
      return compareDoubles((Double) a, (Double) b);
    }
    if (a instanceof Float) {
      return compareDoubles(((Float) a).doubleValue(), ((Float) b).doubleValue());
    }
    if (a instanceof BigDecimal) {
      return Integer.signum(((BigDecimal) a).compareTo((BigDecimal) b));
    }
    return Integer.signum(((Comparable) a).compareTo(b));
  }

  /**
   * Whether two canonical values are equal, in any order: with {@code min = max = v} every non-null value is v.
   */
  private static boolean isEqual(Object a, Object b) {
    if (a == null || b == null || a.getClass() != b.getClass()) {
      return false;
    }
    if (a instanceof String) {
      return a.equals(b);
    }
    Integer comparison = compare(a, b);
    return comparison != null && comparison == 0;
  }

  /**
   * NaN has no reliable place in stats (Parquet drops min/max with NaN, Java orders it last, SQL engines differ), and
   * {@code -0.0} equals {@code 0.0} in SQL but not in Java's order, so both are unknown.
   */
  private static Integer compareDoubles(double a, double b) {
    if (Double.isNaN(a) || Double.isNaN(b)) {
      return null;
    }
    if (a == b) {
      return Double.doubleToRawLongBits(a) == Double.doubleToRawLongBits(b) ? 0 : null;
    }
    return a < b ? -1 : 1;
  }

  /**
   * Compares strings where the code-point order (that of unsigned UTF-8 bytes, used by Parquet and Avro stats and by
   * Spark and Flink) and the UTF-16 order of {@link String#compareTo} (used when stats are merged) agree.
   *
   * <p>The orders can only disagree at a position where both strings hold a character at or above U+D800 (a
   * surrogate against a character in U+E000 to U+FFFF). So the comparison is decided only if no character up to and
   * including the first difference (all of them, for equal strings) is at or above U+D800; a bound compared this
   * way also bounds every string on its side in both orders.
   *
   * @return the sign of the comparison, or null if the orders could disagree
   */
  static Integer compareStrings(String a, String b) {
    if (a.equals(b)) {
      // a bound equal to the literal bounds the values on its side in the stats order only, unless no character
      // of it can sort differently
      for (int i = 0; i < a.length(); i++) {
        if (a.charAt(i) >= Character.MIN_SURROGATE) {
          return null;
        }
      }
      return 0;
    }
    int n = Math.min(a.length(), b.length());
    for (int i = 0; i < n; i++) {
      char ca = a.charAt(i);
      char cb = b.charAt(i);
      if (ca >= Character.MIN_SURROGATE || cb >= Character.MIN_SURROGATE) {
        return null;
      }
      if (ca != cb) {
        return ca < cb ? -1 : 1;
      }
    }
    return a.length() < b.length() ? -1 : 1;
  }

  private static boolean isTrue(Integer comparison, IntPredicate predicate) {
    return comparison != null && predicate.test(comparison);
  }

  // -------------------------------------------------------------------------
  //  Operators
  // -------------------------------------------------------------------------

  /**
   * A comparison of a column with a value, closed under negation over non-null values.
   */
  private enum Cmp {
    EQ, NOT_EQ, LT, LT_EQ, GT, GT_EQ;

    static Cmp of(Expression.Operator op) {
      switch (op) {
        case EQ:
          return EQ;
        case LT:
          return LT;
        case LT_EQ:
          return LT_EQ;
        case GT:
          return GT;
        case GT_EQ:
          return GT_EQ;
        default:
          return null;
      }
    }

    /**
     * {@code v op column} as {@code column op' v}.
     */
    Cmp flip() {
      switch (this) {
        case LT:
          return GT;
        case LT_EQ:
          return GT_EQ;
        case GT:
          return LT;
        case GT_EQ:
          return LT_EQ;
        default:
          return this;
      }
    }

    /**
     * The comparison that holds for a non-null value exactly when this one does not.
     */
    Cmp negate() {
      switch (this) {
        case EQ:
          return NOT_EQ;
        case NOT_EQ:
          return EQ;
        case LT:
          return GT_EQ;
        case LT_EQ:
          return GT;
        case GT:
          return LT_EQ;
        default:
          return LT;
      }
    }
  }
}
