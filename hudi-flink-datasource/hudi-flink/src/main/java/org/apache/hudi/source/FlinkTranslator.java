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

package org.apache.hudi.source;

import org.apache.hudi.common.expression.Expression;
import org.apache.hudi.common.expression.Literal;
import org.apache.hudi.common.expression.NameReference;
import org.apache.hudi.common.expression.Predicates;
import org.apache.hudi.common.expression.translate.ConservativeTranslation;
import org.apache.hudi.common.expression.translate.ExpressionSupport;
import org.apache.hudi.common.expression.translate.ExpressionTranslator;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.schema.mapping.TypeSystems;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.util.ExpressionUtils;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Translates a Flink filter ({@link ResolvedExpression}) into a Hudi {@link Expression} (RFC-88 section 4.2).
 *
 * <p>It accepts the shapes that {@link ExpressionUtils#isSimpleCallExpression}, {@link ExpressionEvaluators} and
 * {@link ExpressionPredicates} accept: AND, OR, NOT, and calls of EQUALS, NOT_EQUALS, LESS_THAN, LESS_THAN_OR_EQUAL,
 * GREATER_THAN, GREATER_THAN_OR_EQUAL between a field reference and a literal in either order (a literal on the left
 * flips the operator), IN with the field first, and IS_NULL / IS_NOT_NULL of a field. Hudi has no not-equals
 * operator, so {@code a <> v} is translated as {@code NOT(a = v)}. The field may be wrapped in a CAST that preserves
 * order (RFC-88 rule 2: a widening of integers, FLOAT to DOUBLE, a widening DECIMAL, VARCHAR or TIMESTAMP); any other
 * cast makes the leaf untranslatable. Combinators follow {@link ConservativeTranslation}.
 *
 * <p>Literals are typed from the referenced column's {@link HoodieSchema} and converted to Hudi's canonical values
 * (DATE as epoch days, TIMESTAMP as epoch micros or millis, DECIMAL as {@link BigDecimal}, STRING as {@link String}).
 * A column missing from the schema, a NULL literal, or a literal that is not exact in the column type makes the leaf
 * untranslatable.
 *
 * <p>{@link IndexPlannerSupport} uses it for the RFC-88 index planner. It later replaces {@link ExpressionUtils} and
 * the evaluator building in {@link ExpressionEvaluators#fromExpression}.
 */
public class FlinkTranslator implements ExpressionTranslator<ResolvedExpression> {

  private static final long serialVersionUID = 1L;

  private static final ConservativeTranslation.Source<ResolvedExpression> SOURCE = new FlinkSource();

  @Override
  public String system() {
    return TypeSystems.FLINK;
  }

  @Override
  public Option<Expression> toHudi(ResolvedExpression expr, HoodieSchema schema) {
    return ConservativeTranslation.translate(expr, SOURCE, leaf -> translateLeaf(leaf, schema), ConservativeTranslation.HUDI_TARGET);
  }

  @Override
  public ExpressionSupport support(Expression.Operator op, HoodieSchemaType type) {
    switch (op) {
      case AND:
      case OR:
      case NOT:
      case TRUE:
      case FALSE:
      case IS_NULL:
      case IS_NOT_NULL:
        return ExpressionSupport.supported();
      case EQ:
      case LT:
      case LT_EQ:
      case GT:
      case GT_EQ:
      case IN:
        return comparisonSupport(type);
      case STARTS_WITH:
      case CONTAINS:
        return ExpressionSupport.unsupported("Flink states it as LIKE, which ExpressionPredicates and ExpressionEvaluators do not handle");
      default:
        return ExpressionSupport.unsupported("no Flink filter translates to " + op);
    }
  }

  private static ExpressionSupport comparisonSupport(HoodieSchemaType type) {
    switch (type) {
      case BOOLEAN:
      case INT:
      // Flink reads INT8 and INT16 as TINYINT and SMALLINT, whose literals compare as Integers (the column stats are
      // INT), and CHAR(n) as CHAR(n), which Flink compares with its padding, as STRING values compare
      case INT8:
      case INT16:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case DECIMAL:
      case STRING:
      case CHAR:
      case DATE:
      case TIMESTAMP:
        return ExpressionSupport.supported();
      case TIME:
        return ExpressionSupport.unsupported("no canonical TIME literal value is defined for Hudi expressions");
      case BYTES:
      case FIXED:
        return ExpressionSupport.unsupported("no canonical binary literal value is defined for Hudi expressions");
      case ENUM:
      case UUID:
        return ExpressionSupport.unsupported("Flink reads the column as STRING, so a Flink literal cannot be typed as " + type);
      default:
        return ExpressionSupport.unsupported(type + " is not a type whose values Flink filters compare");
    }
  }

  // -------------------------------------------------------------------------
  //  Leaves
  // -------------------------------------------------------------------------

  private Option<Expression> translateLeaf(ResolvedExpression leaf, HoodieSchema schema) {
    if (!(leaf instanceof CallExpression)) {
      return Option.empty();
    }
    CallExpression call = (CallExpression) leaf;
    FunctionDefinition def = call.getFunctionDefinition();
    List<ResolvedExpression> args = call.getResolvedChildren();

    if (BuiltInFunctionDefinitions.IS_NULL.equals(def) || BuiltInFunctionDefinitions.IS_NOT_NULL.equals(def)) {
      Expression.Operator op = BuiltInFunctionDefinitions.IS_NULL.equals(def) ? Expression.Operator.IS_NULL : Expression.Operator.IS_NOT_NULL;
      Option<Column> column = args.size() == 1 ? column(args.get(0), schema) : Option.empty();
      if (!column.isPresent() || !support(op, column.get().type.getType()).isSupported()) {
        return Option.empty();
      }
      NameReference ref = column.get().ref();
      return Option.of(op == Expression.Operator.IS_NULL ? Predicates.isNull(ref) : Predicates.isNotNull(ref));
    }

    if (BuiltInFunctionDefinitions.IN.equals(def)) {
      // like ExpressionEvaluators, IN expects the field first and literals after it
      Option<Column> column = args.size() >= 2 ? column(args.get(0), schema) : Option.empty();
      if (!column.isPresent() || !support(Expression.Operator.IN, column.get().type.getType()).isSupported()) {
        return Option.empty();
      }
      List<Expression> values = new ArrayList<>();
      for (int i = 1; i < args.size(); i++) {
        Option<Literal<?>> value = literal(args.get(i), column.get().type);
        if (!value.isPresent()) {
          return Option.empty();
        }
        values.add(value.get());
      }
      return Option.of(Predicates.in(column.get().ref(), values));
    }

    Expression.Operator op = comparison(def);
    if (op == null || args.size() != 2) {
      return Option.empty();
    }
    boolean literalOnRight = args.get(1) instanceof ValueLiteralExpression;
    if (!literalOnRight && !(args.get(0) instanceof ValueLiteralExpression)) {
      return Option.empty();
    }
    Option<Column> column = column(args.get(literalOnRight ? 0 : 1), schema);
    if (!literalOnRight) {
      op = flip(op);
    }
    if (!column.isPresent() || !support(op, column.get().type.getType()).isSupported()) {
      return Option.empty();
    }
    Option<Literal<?>> value = literal(args.get(literalOnRight ? 1 : 0), column.get().type);
    if (!value.isPresent()) {
      return Option.empty();
    }
    return Option.of(comparison(op, column.get().ref(), value.get()));
  }

  private static Expression.Operator comparison(FunctionDefinition def) {
    if (BuiltInFunctionDefinitions.EQUALS.equals(def)) {
      return Expression.Operator.EQ;
    } else if (BuiltInFunctionDefinitions.LESS_THAN.equals(def)) {
      return Expression.Operator.LT;
    } else if (BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL.equals(def)) {
      return Expression.Operator.LT_EQ;
    } else if (BuiltInFunctionDefinitions.GREATER_THAN.equals(def)) {
      return Expression.Operator.GT;
    } else if (BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL.equals(def)) {
      return Expression.Operator.GT_EQ;
    }
    return null;
  }

  /**
   * The operator with its operands swapped: {@code v < a} is {@code a > v}.
   */
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

  private static Expression comparison(Expression.Operator op, Expression ref, Expression value) {
    switch (op) {
      case EQ:
        return Predicates.eq(ref, value);
      case LT:
        return Predicates.lt(ref, value);
      case LT_EQ:
        return Predicates.lteq(ref, value);
      case GT:
        return Predicates.gt(ref, value);
      case GT_EQ:
        return Predicates.gteq(ref, value);
      default:
        throw new IllegalStateException("Not a comparison: " + op);
    }
  }

  // -------------------------------------------------------------------------
  //  Columns and casts
  // -------------------------------------------------------------------------

  /**
   * A referenced top-level column and its non-null type in the table schema.
   */
  private static final class Column {
    private final String name;
    private final HoodieSchema type;

    private Column(String name, HoodieSchema type) {
      this.name = name;
      this.type = type;
    }

    private NameReference ref() {
      return new NameReference(name);
    }
  }

  /**
   * The column a field reference, optionally under an order-preserving CAST, refers to; empty if the operand is
   * something else or the column is not in the schema.
   */
  private static Option<Column> column(ResolvedExpression operand, HoodieSchema schema) {
    FieldReferenceExpression field;
    if (operand instanceof FieldReferenceExpression) {
      field = (FieldReferenceExpression) operand;
    } else if (isOrderPreservingCastOfField(operand)) {
      field = (FieldReferenceExpression) operand.getResolvedChildren().get(0);
    } else {
      return Option.empty();
    }
    if (schema == null || !schema.hasFields()) {
      return Option.empty();
    }
    Option<HoodieSchemaField> schemaField = schema.getField(field.getName());
    if (!schemaField.isPresent() || schemaField.get().schema().isComplexUnion()) {
      return Option.empty();
    }
    return Option.of(new Column(field.getName(), schemaField.get().schema().getNonNullType()));
  }

  private static boolean isOrderPreservingCastOfField(ResolvedExpression operand) {
    if (!(operand instanceof CallExpression)
        || !BuiltInFunctionDefinitions.CAST.equals(((CallExpression) operand).getFunctionDefinition())) {
      return false;
    }
    List<ResolvedExpression> args = operand.getResolvedChildren();
    if (args.isEmpty() || !(args.get(0) instanceof FieldReferenceExpression)) {
      return false;
    }
    return preservesOrder(args.get(0).getOutputDataType().getLogicalType(), operand.getOutputDataType().getLogicalType());
  }

  /**
   * Whether the cast is strictly monotonic, so that {@code CAST(a) op v} equals {@code a op v'} for the exact
   * preimage {@code v'} of {@code v}; a literal with no exact preimage is then untranslatable.
   */
  static boolean preservesOrder(LogicalType from, LogicalType to) {
    if (from.copy(true).equals(to.copy(true))) {
      return true;
    }
    LogicalTypeRoot source = from.getTypeRoot();
    LogicalTypeRoot target = to.getTypeRoot();
    int fromDigits = integerDigits(source);
    if (fromDigits > 0) {
      int toDigits = integerDigits(target);
      if (toDigits > 0) {
        return toDigits >= fromDigits;
      }
      switch (target) {
        case FLOAT:
          // every value of 16 bits or less is exact in a float's 24-bit significand
          return fromDigits <= 5;
        case DOUBLE:
          return fromDigits <= 10;
        case DECIMAL:
          DecimalType decimal = (DecimalType) to;
          return decimal.getPrecision() - decimal.getScale() >= fromDigits;
        default:
          return false;
      }
    }
    switch (source) {
      case FLOAT:
        return target == LogicalTypeRoot.DOUBLE;
      case DECIMAL:
        if (target != LogicalTypeRoot.DECIMAL) {
          return false;
        }
        DecimalType fromDecimal = (DecimalType) from;
        DecimalType toDecimal = (DecimalType) to;
        return toDecimal.getScale() >= fromDecimal.getScale()
            && toDecimal.getPrecision() - toDecimal.getScale() >= fromDecimal.getPrecision() - fromDecimal.getScale();
      case VARCHAR:
        return target == LogicalTypeRoot.VARCHAR && ((VarCharType) to).getLength() >= ((VarCharType) from).getLength();
      case TIMESTAMP_WITHOUT_TIME_ZONE:
        return target == source && ((TimestampType) to).getPrecision() >= ((TimestampType) from).getPrecision();
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return target == source && ((LocalZonedTimestampType) to).getPrecision() >= ((LocalZonedTimestampType) from).getPrecision();
      default:
        return false;
    }
  }

  /**
   * The decimal digits an integer type holds, or 0 for a non-integer type.
   */
  private static int integerDigits(LogicalTypeRoot root) {
    switch (root) {
      case TINYINT:
        return 3;
      case SMALLINT:
        return 5;
      case INTEGER:
        return 10;
      case BIGINT:
        return 19;
      default:
        return 0;
    }
  }

  // -------------------------------------------------------------------------
  //  Literals
  // -------------------------------------------------------------------------

  /**
   * The literal typed as the column, or empty if the operand is not a non-NULL literal of the column's type family
   * whose value is exact in the column type.
   */
  private static Option<Literal<?>> literal(ResolvedExpression operand, HoodieSchema columnType) {
    if (!(operand instanceof ValueLiteralExpression) || ((ValueLiteralExpression) operand).isNull()) {
      return Option.empty();
    }
    ValueLiteralExpression literal = (ValueLiteralExpression) operand;
    LogicalTypeRoot root = literal.getOutputDataType().getLogicalType().getTypeRoot();
    Object value = javaValue(literal, root);
    if (value == null) {
      return Option.empty();
    }
    Object canonical = canonicalValue(value, root, columnType);
    return canonical == null ? Option.empty() : Option.of(new Literal<>(canonical, columnType));
  }

  /**
   * The literal's value as a Java object: {@link ExpressionUtils#getValueFromLiteral} for the types it extracts
   * exactly, the java.time classes for DATE and TIMESTAMP (it reads timestamps as millis), and the stored value
   * (e.g. {@link StringData}, {@link DecimalData}, {@link TimestampData}) when no conversion applies.
   */
  private static Object javaValue(ValueLiteralExpression literal, LogicalTypeRoot root) {
    Object value;
    switch (root) {
      case DATE:
        value = literal.getValueAs(LocalDate.class).orElse(null);
        break;
      case TIMESTAMP_WITHOUT_TIME_ZONE:
        value = literal.getValueAs(LocalDateTime.class).orElse(null);
        break;
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        value = literal.getValueAs(Instant.class).orElse(null);
        break;
      case BOOLEAN:
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
      case FLOAT:
      case DOUBLE:
      case DECIMAL:
      case CHAR:
      case VARCHAR:
        value = ExpressionUtils.getValueFromLiteral(literal);
        break;
      default:
        return null;
    }
    return value != null ? value : literal.getValueAs(Object.class).orElse(null);
  }

  /**
   * Converts a literal value to the canonical value of the column type; null if the literal is of another type
   * family or not exact in the column type.
   */
  static Object canonicalValue(Object value, LogicalTypeRoot literalType, HoodieSchema columnType) {
    try {
      switch (columnType.getType()) {
        case BOOLEAN:
          return literalType == LogicalTypeRoot.BOOLEAN && value instanceof Boolean ? value : null;
        case INT:
        case INT8:
        case INT16: {
          BigDecimal number = number(value, literalType);
          return number == null ? null : number.intValueExact();
        }
        case LONG: {
          BigDecimal number = number(value, literalType);
          return number == null ? null : number.longValueExact();
        }
        case FLOAT: {
          BigDecimal number = number(value, literalType);
          float f = number == null ? Float.NaN : number.floatValue();
          return Float.isFinite(f) && new BigDecimal(f).compareTo(number) == 0 ? f : null;
        }
        case DOUBLE: {
          BigDecimal number = number(value, literalType);
          double d = number == null ? Double.NaN : number.doubleValue();
          return Double.isFinite(d) && new BigDecimal(d).compareTo(number) == 0 ? d : null;
        }
        case DECIMAL: {
          BigDecimal number = number(value, literalType);
          if (number == null || !(columnType instanceof HoodieSchema.Decimal)) {
            return null;
          }
          HoodieSchema.Decimal decimal = (HoodieSchema.Decimal) columnType;
          BigDecimal scaled = number.setScale(decimal.getScale(), RoundingMode.UNNECESSARY);
          return scaled.precision() <= decimal.getPrecision() ? scaled : null;
        }
        case STRING:
        case CHAR:
          if (literalType != LogicalTypeRoot.CHAR && literalType != LogicalTypeRoot.VARCHAR) {
            return null;
          }
          return value instanceof CharSequence || value instanceof StringData ? value.toString() : null;
        case DATE:
          if (literalType != LogicalTypeRoot.DATE) {
            return null;
          }
          if (value instanceof LocalDate) {
            return Math.toIntExact(((LocalDate) value).toEpochDay());
          }
          // DATE's internal representation: epoch days
          return value instanceof Integer ? value : null;
        case TIMESTAMP:
          return timestamp(value, literalType, columnType);
        default:
          return null;
      }
    } catch (ArithmeticException | NumberFormatException e) {
      // not exact in the column type (overflow, a fraction, NaN or infinity)
      return null;
    }
  }

  /**
   * A numeric literal as an exact {@link BigDecimal}, or null if the literal is not numeric.
   */
  private static BigDecimal number(Object value, LogicalTypeRoot literalType) {
    if (integerDigits(literalType) == 0 && literalType != LogicalTypeRoot.FLOAT && literalType != LogicalTypeRoot.DOUBLE
        && literalType != LogicalTypeRoot.DECIMAL) {
      return null;
    }
    if (value instanceof BigDecimal) {
      return (BigDecimal) value;
    } else if (value instanceof DecimalData) {
      return ((DecimalData) value).toBigDecimal();
    } else if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long) {
      return BigDecimal.valueOf(((Number) value).longValue());
    } else if (value instanceof Float || value instanceof Double) {
      // the exact binary value; NaN and infinity throw NumberFormatException
      return new BigDecimal(((Number) value).doubleValue());
    }
    return null;
  }

  /**
   * Epoch micros (or millis for a millis column) of a timestamp literal. Flink TIMESTAMP is Hudi's UTC-adjusted
   * timestamp and TIMESTAMP_LTZ the local one (see {@code HoodieSchemaConverter}); both store the epoch value of the
   * literal ({@code LocalDateTime} at UTC, or the {@code Instant}).
   */
  private static Object timestamp(Object value, LogicalTypeRoot literalType, HoodieSchema columnType) {
    if (!(columnType instanceof HoodieSchema.Timestamp)) {
      return null;
    }
    HoodieSchema.Timestamp timestamp = (HoodieSchema.Timestamp) columnType;
    LogicalTypeRoot expected = timestamp.isUtcAdjusted() ? LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE : LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
    if (literalType != expected) {
      return null;
    }
    Instant instant;
    if (value instanceof LocalDateTime) {
      instant = ((LocalDateTime) value).toInstant(ZoneOffset.UTC);
    } else if (value instanceof Instant) {
      instant = (Instant) value;
    } else if (value instanceof TimestampData) {
      instant = ((TimestampData) value).toInstant();
    } else {
      return null;
    }
    boolean millis = timestamp.getPrecision() == HoodieSchema.TimePrecision.MILLIS;
    int nanosPerUnit = millis ? 1_000_000 : 1_000;
    if (instant.getNano() % nanosPerUnit != 0) {
      return null;
    }
    return Math.addExact(Math.multiplyExact(instant.getEpochSecond(), millis ? 1_000L : 1_000_000L), instant.getNano() / nanosPerUnit);
  }

  // -------------------------------------------------------------------------
  //  Source
  // -------------------------------------------------------------------------

  /**
   * Reads Flink filter trees for {@link ConservativeTranslation}. NOT_EQUALS reads as NOT over EQUALS.
   */
  private static final class FlinkSource implements ConservativeTranslation.Source<ResolvedExpression> {

    @Override
    public ConservativeTranslation.Kind kind(ResolvedExpression node) {
      if (node instanceof ValueLiteralExpression) {
        ValueLiteralExpression literal = (ValueLiteralExpression) node;
        if (literal.getOutputDataType().getLogicalType().getTypeRoot() == LogicalTypeRoot.BOOLEAN) {
          Boolean value = literal.getValueAs(Boolean.class).orElse(null);
          if (value != null) {
            return value ? ConservativeTranslation.Kind.TRUE : ConservativeTranslation.Kind.FALSE;
          }
        }
        return ConservativeTranslation.Kind.LEAF;
      }
      if (!(node instanceof CallExpression)) {
        return ConservativeTranslation.Kind.LEAF;
      }
      FunctionDefinition def = ((CallExpression) node).getFunctionDefinition();
      int arity = node.getResolvedChildren().size();
      if (BuiltInFunctionDefinitions.AND.equals(def) && arity >= 2) {
        return ConservativeTranslation.Kind.AND;
      } else if (BuiltInFunctionDefinitions.OR.equals(def) && arity >= 2) {
        return ConservativeTranslation.Kind.OR;
      } else if (BuiltInFunctionDefinitions.NOT.equals(def) && arity == 1) {
        return ConservativeTranslation.Kind.NOT;
      } else if (BuiltInFunctionDefinitions.NOT_EQUALS.equals(def) && arity == 2) {
        return ConservativeTranslation.Kind.NOT;
      }
      return ConservativeTranslation.Kind.LEAF;
    }

    @Override
    public List<ResolvedExpression> children(ResolvedExpression node) {
      CallExpression call = (CallExpression) node;
      if (BuiltInFunctionDefinitions.NOT_EQUALS.equals(call.getFunctionDefinition())) {
        // a <> v is NOT(a = v); Hudi has no not-equals operator
        return Collections.singletonList(
            CallExpression.permanent(BuiltInFunctionDefinitions.EQUALS, call.getResolvedChildren(), call.getOutputDataType()));
      }
      return call.getResolvedChildren();
    }
  }
}
