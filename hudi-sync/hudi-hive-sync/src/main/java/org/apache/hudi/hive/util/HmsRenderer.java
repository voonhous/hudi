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
import org.apache.hudi.common.expression.translate.ConservativeTranslation;
import org.apache.hudi.common.expression.translate.ExpressionMappers;
import org.apache.hudi.common.expression.translate.ExpressionRenderer;
import org.apache.hudi.common.expression.translate.ExpressionSupport;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.schema.mapping.MappingOptions;
import org.apache.hudi.common.util.Option;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * {@link ExpressionRenderer} to a Hive Metastore partition filter string, the {@code filter} argument of
 * {@code listPartitionsByFilter} (RFC-88 section 4.2). Replaces later {@link FilterGenVisitor} as used by
 * {@link PartitionFilterGenerator}.
 *
 * <p>Leaves, literal formatting and quoting mirror {@link FilterGenVisitor}, so an expression of the shape
 * {@link PartitionFilterGenerator} builds renders to the same string: {@code col op literal}, AND and OR as
 * {@code (l AND r)} / {@code (l OR r)}, STRING literals in double quotes (single quotes when the value holds a
 * double quote; a value with both is not rendered), INT and LONG as digits, DATE unquoted as {@code yyyy-MM-dd}
 * (the metastore's {@code DateLiteral}). The metastore filter grammar has no literal of any other type and no
 * {@code IS NULL}; {@code LIKE} is not used because its pattern semantics differ between the JDO and direct SQL
 * paths. IN is rendered as an OR of equalities. NOT is pushed to the leaves by {@link ConservativeTranslation} and
 * rendered by inverting the comparison ({@code =} becomes {@code <>}, {@code <} becomes {@code >=}, ...), which keeps
 * every partition the negation keeps.
 *
 * <p>Differences from {@link FilterGenVisitor}: DATE literals are the canonical epoch days and are printed as dates
 * (the visitor prints the raw value, so epoch days come out as a number); NOT and IN are rendered instead of
 * dropped; a comparison whose literal is on the left is written column first; a comparison between two columns, a
 * literal of the wrong Java class, or a string with both quote characters makes the leaf unknown instead of
 * producing an invalid filter or throwing.
 *
 * <p>Partition filters only see partition columns. A renderer built with {@link #HmsRenderer(HoodieSchema)} places
 * a column only if it is a top-level field of that partition schema and the literal has the field's type; the
 * registry instance has no schema and places any column by name. Either way the name must be a metastore filter
 * identifier ({@code [A-Za-z0-9][A-Za-z0-9_]*}, not a filter keyword). A leaf on a column that cannot be placed is
 * unknown, so it keeps every partition.
 */
public class HmsRenderer implements ExpressionRenderer<String> {

  private static final long serialVersionUID = 1L;

  private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z0-9][A-Za-z0-9_]*");
  /**
   * Keywords of the metastore's {@code Filter.g}, whose lexer is case-insensitive.
   */
  private static final Set<String> KEYWORDS = new HashSet<>(Arrays.asList("not", "and", "or", "like", "date", "const", "struct", "in", "between"));

  private final HoodieSchema partitionSchema;

  /**
   * The registry instance: no partition schema, columns are placed by name and typed by their literal.
   */
  public HmsRenderer() {
    this(null);
  }

  /**
   * @param partitionSchema the partition columns as a record; {@code null} places any column by name
   */
  public HmsRenderer(HoodieSchema partitionSchema) {
    this.partitionSchema = partitionSchema;
  }

  @Override
  public String target() {
    return ExpressionMappers.HMS;
  }

  @Override
  public Option<String> render(Expression expr, MappingOptions opts) {
    return ConservativeTranslation.translate(expr, ConservativeTranslation.HUDI_SOURCE, this::renderLeaf, FilterNode.TARGET)
        .map(FilterNode::toString);
  }

  @Override
  public ExpressionSupport support(Expression.Operator op, HoodieSchemaType type) {
    switch (op) {
      case EQ:
      case LT:
      case LT_EQ:
      case GT:
      case GT_EQ:
      case IN:
        return supportType(type);
      case IS_NULL:
      case IS_NOT_NULL:
        return ExpressionSupport.unsupported("the metastore filter grammar has no IS NULL; null partition values are stored as __HIVE_DEFAULT_PARTITION__");
      case STARTS_WITH:
      case CONTAINS:
        return ExpressionSupport.unsupported("metastore LIKE patterns are regular expressions on the JDO path and SQL patterns on the direct SQL path");
      default:
        return ExpressionSupport.unsupported("not a leaf operator of the metastore filter grammar");
    }
  }

  protected ExpressionSupport supportType(HoodieSchemaType type) {
    switch (type) {
      case INT:
      case LONG:
      case STRING:
      case DATE:
        return ExpressionSupport.supported();
      default:
        return ExpressionSupport.unsupported("the metastore filter grammar has string, integral and date literals only, so FilterGenVisitor pushes INT, LONG, DATE and STRING");
    }
  }

  /**
   * Quotes a STRING literal, or returns empty when the target cannot quote it.
   */
  protected Option<String> quoteString(String value) {
    // FilterGenVisitor.quoteStringLiteral; the grammar has no escapes
    if (!value.contains("\"")) {
      return Option.of("\"" + value + "\"");
    } else if (!value.contains("'")) {
      return Option.of("'" + value + "'");
    }
    return Option.empty();
  }

  /**
   * Whether a column name can be written into the filter as is.
   */
  protected boolean isIdentifier(String name) {
    return IDENTIFIER.matcher(name).matches() && !KEYWORDS.contains(name.toLowerCase(Locale.ROOT));
  }

  private Option<FilterNode> renderLeaf(Expression leaf) {
    if (leaf instanceof Predicates.BinaryComparison) {
      Predicates.BinaryComparison comparison = (Predicates.BinaryComparison) leaf;
      Expression.Operator op = comparison.getOperator();
      if (comparison.getLeft() instanceof NameReference && comparison.getRight() instanceof Literal) {
        return comparison(((NameReference) comparison.getLeft()).getName(), CompareOp.of(op), (Literal<?>) comparison.getRight());
      }
      if (comparison.getLeft() instanceof Literal && comparison.getRight() instanceof NameReference) {
        return comparison(((NameReference) comparison.getRight()).getName(), CompareOp.of(op).flip(), (Literal<?>) comparison.getLeft());
      }
      return Option.empty();
    }
    if (leaf instanceof Predicates.In) {
      Predicates.In in = (Predicates.In) leaf;
      List<Expression> children = in.getChildren();
      if (!(children.get(0) instanceof NameReference) || children.size() < 2) {
        return Option.empty();
      }
      String column = ((NameReference) children.get(0)).getName();
      FilterNode result = null;
      for (Expression value : in.getRightChildren()) {
        if (!(value instanceof Literal)) {
          return Option.empty();
        }
        Option<FilterNode> eq = comparison(column, CompareOp.EQ, (Literal<?>) value);
        if (!eq.isPresent()) {
          return Option.empty();
        }
        result = result == null ? eq.get() : new FilterNode.Junction(false, result, eq.get());
      }
      return Option.of(result);
    }
    return Option.empty();
  }

  private Option<FilterNode> comparison(String column, CompareOp op, Literal<?> literal) {
    if (op == null || literal.getDataType() == null || literal.getValue() == null || !isPlaceable(column, literal.getDataType())) {
      return Option.empty();
    }
    return renderLiteral(literal).map(text -> new FilterNode.Comparison(column, op, text));
  }

  private boolean isPlaceable(String column, HoodieSchema literalType) {
    if (!isIdentifier(column)) {
      return false;
    }
    if (partitionSchema == null) {
      return true;
    }
    Option<HoodieSchemaField> field = partitionSchema.getField(column);
    return field.isPresent() && field.get().schema().getNonNullType().getType() == literalType.getType();
  }

  private Option<String> renderLiteral(Literal<?> literal) {
    HoodieSchemaType type = literal.getDataType().getType();
    Object value = literal.getValue();
    if (!supportType(type).isSupported()) {
      return Option.empty();
    }
    switch (type) {
      case STRING:
        return value instanceof CharSequence ? quoteString(value.toString()) : Option.empty();
      case INT:
        return value instanceof Integer ? Option.of(value.toString()) : Option.empty();
      case LONG:
        return value instanceof Long || value instanceof Integer ? Option.of(value.toString()) : Option.empty();
      case DATE:
        if (!(value instanceof Integer)) {
          return Option.empty();
        }
        LocalDate date = LocalDate.ofEpochDay((Integer) value);
        // the grammar's DateLiteral has a four-digit year
        return date.getYear() >= 0 && date.getYear() <= 9999 ? Option.of(date.toString()) : Option.empty();
      default:
        return Option.empty();
    }
  }

  /**
   * The comparison operators of the filter grammar.
   */
  enum CompareOp {
    EQ("="), NOT_EQ("<>"), LT("<"), LT_EQ("<="), GT(">"), GT_EQ(">=");

    private final String sql;

    CompareOp(String sql) {
      this.sql = sql;
    }

    static CompareOp of(Expression.Operator op) {
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
     * The operator with its operands swapped: {@code 5 < c} is {@code c > 5}.
     */
    CompareOp flip() {
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

    CompareOp negate() {
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

  /**
   * A rendered filter, kept as a tree until the end so negated leaves can be inverted.
   */
  abstract static class FilterNode {

    static final ConservativeTranslation.Target<FilterNode> TARGET = new ConservativeTranslation.Target<FilterNode>() {
      @Override
      public FilterNode and(FilterNode left, FilterNode right) {
        return new Junction(true, left, right);
      }

      @Override
      public FilterNode or(FilterNode left, FilterNode right) {
        return new Junction(false, left, right);
      }

      @Override
      public Option<FilterNode> not(FilterNode leaf) {
        return Option.of(leaf.negate());
      }

      @Override
      public Option<FilterNode> alwaysFalse() {
        // the grammar has no constant; keeping every partition is conservative
        return Option.empty();
      }
    };

    abstract FilterNode negate();

    static final class Comparison extends FilterNode {
      private final String column;
      private final CompareOp op;
      private final String literal;

      Comparison(String column, CompareOp op, String literal) {
        this.column = column;
        this.op = op;
        this.literal = literal;
      }

      @Override
      FilterNode negate() {
        return new Comparison(column, op.negate(), literal);
      }

      @Override
      public String toString() {
        return column + " " + op.sql + " " + literal;
      }
    }

    static final class Junction extends FilterNode {
      private final boolean conjunction;
      private final FilterNode left;
      private final FilterNode right;

      Junction(boolean conjunction, FilterNode left, FilterNode right) {
        this.conjunction = conjunction;
        this.left = left;
        this.right = right;
      }

      @Override
      FilterNode negate() {
        return new Junction(!conjunction, left.negate(), right.negate());
      }

      @Override
      public String toString() {
        return "(" + left + (conjunction ? " AND " : " OR ") + right + ")";
      }
    }
  }
}
