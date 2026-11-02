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

import org.apache.hudi.common.expression.BinaryExpression;
import org.apache.hudi.common.expression.Expression;
import org.apache.hudi.common.expression.Literal;
import org.apache.hudi.common.expression.NameReference;
import org.apache.hudi.common.expression.Predicates;
import org.apache.hudi.common.expression.translate.ConservativeTranslation;
import org.apache.hudi.common.expression.translate.ExpressionMappers;
import org.apache.hudi.common.expression.translate.ExpressionRenderer;
import org.apache.hudi.common.expression.translate.ExpressionSupport;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.schema.mapping.MappingOptions;
import org.apache.hudi.common.util.Option;

import org.apache.spark.sql.sources.AlwaysFalse;
import org.apache.spark.sql.sources.And;
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

import java.util.List;

/**
 * Renders Hudi {@link Expression}s as Spark data source {@link Filter}s (RFC-88 section 4.2), registered as
 * {@link ExpressionMappers#SPARK_FILTER}, to feed Spark's Parquet reader.
 *
 * <p>Literal values are converted from Hudi's canonical values to the Java types Spark's {@code ParquetFilters}
 * (Spark 3.5) pushes down: {@code Integer}, {@code Long}, {@code Float}, {@code Double}, {@code Boolean},
 * {@code String}, {@code BigDecimal} at the literal type's scale, {@code LocalDate} for DATE and {@code Instant}
 * for TIMESTAMP (millis or micros). Local timestamps render with {@code LocalDateTime}, Spark's value for
 * {@code TIMESTAMP_NTZ}, which {@code ParquetFilters} does not push down. Column names are quoted the way
 * {@code ParquetFilters} keys its fields ({@code `a.b`} for a top-level column named {@code a.b}).
 *
 * <p>AND, OR and NOT follow {@link ConservativeTranslation}; FALSE renders as {@code AlwaysFalse}. Leaves are
 * comparisons, IN, IS NULL, IS NOT NULL, STARTS_WITH (also with several prefixes) and CONTAINS of a column and
 * literals; anything else, including a comparison with a null literal, renders to nothing.
 */
public class SparkFilterRenderer implements ExpressionRenderer<Filter> {

  private static final long serialVersionUID = 1L;

  private static final ConservativeTranslation.Target<Filter> TARGET = new ConservativeTranslation.Target<Filter>() {
    @Override
    public Filter and(Filter left, Filter right) {
      return new And(left, right);
    }

    @Override
    public Filter or(Filter left, Filter right) {
      return new Or(left, right);
    }

    @Override
    public Option<Filter> not(Filter leaf) {
      return Option.of(new Not(leaf));
    }

    @Override
    public Option<Filter> alwaysFalse() {
      return Option.of(new AlwaysFalse());
    }
  };

  @Override
  public String target() {
    return ExpressionMappers.SPARK_FILTER;
  }

  @Override
  public Option<Filter> render(Expression expr, MappingOptions opts) {
    return ConservativeTranslation.translate(expr, ConservativeTranslation.HUDI_SOURCE, SparkFilterRenderer::renderLeaf, TARGET);
  }

  @Override
  public ExpressionSupport support(Expression.Operator op, HoodieSchemaType type) {
    return SparkFilterValues.support(op, type);
  }

  private static Option<Filter> renderLeaf(Expression leaf) {
    if (leaf instanceof Predicates.IsNull || leaf instanceof Predicates.IsNotNull) {
      Expression child = leaf.getChildren().get(0);
      if (!(child instanceof NameReference)) {
        return Option.empty();
      }
      String name = quote(((NameReference) child).getName());
      return Option.of(leaf instanceof Predicates.IsNull ? new IsNull(name) : new IsNotNull(name));
    } else if (leaf instanceof Predicates.BinaryComparison) {
      return comparison((Predicates.BinaryComparison) leaf);
    } else if (leaf instanceof Predicates.In) {
      return in((Predicates.In) leaf);
    } else if (leaf instanceof Predicates.StringStartsWith || leaf instanceof Predicates.StringContains) {
      BinaryExpression binary = (BinaryExpression) leaf;
      boolean startsWith = leaf instanceof Predicates.StringStartsWith;
      return stringPattern(binary.getLeft(), binary.getRight())
          .map(p -> startsWith ? (Filter) new StringStartsWith(p[0], p[1]) : (Filter) new StringContains(p[0], p[1]));
    } else if (leaf instanceof Predicates.StringStartsWithAny) {
      List<Expression> children = leaf.getChildren();
      Filter result = null;
      for (Expression prefix : children.subList(1, children.size())) {
        Option<String[]> pattern = stringPattern(children.get(0), prefix);
        if (!pattern.isPresent()) {
          return Option.empty();
        }
        Filter one = new StringStartsWith(pattern.get()[0], pattern.get()[1]);
        result = result == null ? one : new Or(result, one);
      }
      return Option.ofNullable(result);
    }
    return Option.empty();
  }

  private static Option<Filter> comparison(Predicates.BinaryComparison leaf) {
    Expression.Operator op = leaf.getOperator();
    Expression column = leaf.getLeft();
    Expression value = leaf.getRight();
    if (column instanceof Literal && value instanceof NameReference) {
      // literal op column: flip to column op' literal
      column = leaf.getRight();
      value = leaf.getLeft();
      op = flip(op);
    }
    if (!(column instanceof NameReference) || !(value instanceof Literal)) {
      return Option.empty();
    }
    String name = quote(((NameReference) column).getName());
    Option<Object> sparkValue = sparkValue((Literal<?>) value);
    if (!sparkValue.isPresent()) {
      return Option.empty();
    }
    Object v = sparkValue.get();
    switch (op) {
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
      default:
        return Option.empty();
    }
  }

  private static Option<Filter> in(Predicates.In leaf) {
    List<Expression> children = leaf.getChildren();
    if (!(children.get(0) instanceof NameReference) || children.size() < 2) {
      return Option.empty();
    }
    Object[] values = new Object[children.size() - 1];
    for (int i = 1; i < children.size(); i++) {
      Expression child = children.get(i);
      Option<Object> v = child instanceof Literal ? sparkValue((Literal<?>) child) : Option.empty();
      if (!v.isPresent()) {
        return Option.empty();
      }
      values[i - 1] = v.get();
    }
    return Option.of(new In(quote(((NameReference) children.get(0)).getName()), values));
  }

  /**
   * The quoted column name and the pattern of a string predicate, or empty if it is not column-and-string.
   */
  private static Option<String[]> stringPattern(Expression column, Expression pattern) {
    if (!(column instanceof NameReference) || !(pattern instanceof Literal)) {
      return Option.empty();
    }
    Literal<?> literal = (Literal<?>) pattern;
    if (literal.getDataType().getType() != HoodieSchemaType.STRING || !(literal.getValue() instanceof CharSequence)) {
      return Option.empty();
    }
    return Option.of(new String[] {quote(((NameReference) column).getName()), literal.getValue().toString()});
  }

  private static Option<Object> sparkValue(Literal<?> literal) {
    if (literal.getDataType() == null || !SparkFilterValues.VALUE_TYPES.contains(literal.getDataType().getType())) {
      return Option.empty();
    }
    return SparkFilterValues.toSpark(literal.getValue(), literal.getDataType());
  }

  private static Expression.Operator flip(Expression.Operator op) {
    switch (op) {
      case LT:
        return Expression.Operator.GT;
      case LT_EQ:
        return Expression.Operator.GT_EQ;
      case GT:
        return Expression.Operator.LT;
      case GT_EQ:
        return Expression.Operator.LT_EQ;
      default:
        return op;
    }
  }

  /**
   * Quotes a top-level column name as Spark's {@code quoteIfNeeded} does, which is how {@code ParquetFilters}
   * names its fields.
   */
  static String quote(String name) {
    if (name.matches("[a-zA-Z0-9_]+") && !name.matches("\\d+")) {
      return name;
    }
    return "`" + name.replace("`", "``") + "`";
  }
}
