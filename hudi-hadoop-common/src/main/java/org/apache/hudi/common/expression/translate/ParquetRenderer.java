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

package org.apache.hudi.common.expression.translate;

import org.apache.hudi.common.expression.Expression;
import org.apache.hudi.common.expression.Literal;
import org.apache.hudi.common.expression.NameReference;
import org.apache.hudi.common.expression.Predicates;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.schema.mapping.MappingOptions;
import org.apache.hudi.common.schema.mapping.ParquetSchemaMapper;
import org.apache.hudi.common.schema.mapping.PhysicalOptions;
import org.apache.hudi.common.util.Option;

import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

/**
 * {@link ExpressionRenderer} to a parquet-mr {@link FilterPredicate} (RFC-88 section 4.2), usable by any parquet-mr
 * reader for row group, page and record filtering. Replaces later Flink's {@code ExpressionPredicates}, whose leaf
 * mapping it follows: comparisons with the literal on the right, IN as an OR of equalities (at most
 * {@value #IN_LIMIT} values, like {@code ExpressionPredicates.In}; {@code FilterApi.in} is not used because the
 * parquet 1.12 profiles lack it), NOT as {@link FilterApi#not}.
 *
 * <p>Literals are converted to the Parquet physical type {@link ParquetSchemaMapper} writes for the column type:
 * BOOLEAN, INT and DATE (epoch days) as INT32, LONG and every TIMESTAMP (epoch micros or millis, as the column's
 * precision) as INT64, FLOAT, DOUBLE, STRING as UTF-8 {@link Binary}, DECIMAL as the unscaled value in the
 * writer's layout (INT32 / INT64, minimal two's complement BINARY, or sign-extended FIXED_LEN_BYTE_ARRAY). The
 * filter is meant for files written in that layout: a file whose column has another physical type is rejected
 * by parquet-mr's schema check.
 *
 * <p>A leaf is unknown (keeps every row) when it cannot be rendered exactly: a column that is nested (a dotted
 * name), absent from the table schema, or of another type than the literal; a null or NaN literal; a decimal
 * literal that needs rounding or exceeds the column's precision; a string literal with an unpaired surrogate,
 * or, for a range comparison, with a character at or above U+D800, where Parquet's UTF-8 byte order differs
 * from the Java string order Hudi's {@code Comparators} use. IS NULL needs the column's type, so only a renderer
 * built with {@link #ParquetRenderer(HoodieSchema)} renders it; the registry instance types columns by their
 * literal.
 *
 * <p>Differences from {@code ExpressionPredicates}: IS NULL / IS NOT NULL and DECIMAL are rendered (it drops
 * them); an AND keeps its renderable side (it drops the whole AND); TIMESTAMP literals are the canonical epoch
 * value of the column's precision.
 */
public class ParquetRenderer implements ExpressionRenderer<FilterPredicate> {

  private static final long serialVersionUID = 1L;

  static final int IN_LIMIT = 200;

  private static final ParquetSchemaMapper PARQUET = new ParquetSchemaMapper();

  private static final ConservativeTranslation.Target<FilterPredicate> TARGET = new ConservativeTranslation.Target<FilterPredicate>() {
    @Override
    public FilterPredicate and(FilterPredicate left, FilterPredicate right) {
      return FilterApi.and(left, right);
    }

    @Override
    public FilterPredicate or(FilterPredicate left, FilterPredicate right) {
      return FilterApi.or(left, right);
    }

    @Override
    public Option<FilterPredicate> not(FilterPredicate leaf) {
      return Option.of(FilterApi.not(leaf));
    }

    @Override
    public Option<FilterPredicate> alwaysFalse() {
      // FilterApi has no constant; keeping every row is conservative
      return Option.empty();
    }
  };

  private final HoodieSchema tableSchema;

  /**
   * The registry instance: no table schema, columns are typed by their literal.
   */
  public ParquetRenderer() {
    this(null);
  }

  /**
   * @param tableSchema the record schema of the file's columns; {@code null} types columns by their literal
   */
  public ParquetRenderer(HoodieSchema tableSchema) {
    this.tableSchema = tableSchema;
  }

  @Override
  public String target() {
    return ExpressionMappers.PARQUET;
  }

  @Override
  public Option<FilterPredicate> render(Expression expr, MappingOptions opts) {
    return ConservativeTranslation.translate(expr, ConservativeTranslation.HUDI_SOURCE, this::renderLeaf, TARGET);
  }

  @Override
  public ExpressionSupport support(Expression.Operator op, HoodieSchemaType type) {
    ExpressionSupport typeSupport = supportType(type);
    if (!typeSupport.isSupported()) {
      return typeSupport;
    }
    switch (op) {
      case EQ:
      case IN:
      case IS_NULL:
      case IS_NOT_NULL:
        return ExpressionSupport.supported();
      case LT:
      case LT_EQ:
      case GT:
      case GT_EQ:
        return type == HoodieSchemaType.BOOLEAN
            ? ExpressionSupport.unsupported("FilterApi boolean columns support only eq and notEq")
            : ExpressionSupport.supported();
      case STARTS_WITH:
      case CONTAINS:
        return ExpressionSupport.unsupported("FilterApi has no prefix or substring predicate (no UserDefinedPredicate is added)");
      default:
        return ExpressionSupport.unsupported("not a leaf operator");
    }
  }

  private static ExpressionSupport supportType(HoodieSchemaType type) {
    switch (type) {
      case BOOLEAN:
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case STRING:
      case DECIMAL:
      case DATE:
      case TIMESTAMP:
        return ExpressionSupport.supported();
      case BYTES:
      case FIXED:
      case UUID:
      case ENUM:
      case TIME:
        return ExpressionSupport.unsupported("no canonical literal value or comparator for " + type + " in Hudi expressions");
      default:
        return ExpressionSupport.unsupported(type + " is not a primitive Parquet column; nested columns are out of scope");
    }
  }

  private Option<FilterPredicate> renderLeaf(Expression leaf) {
    if (leaf instanceof Predicates.BinaryComparison) {
      Predicates.BinaryComparison comparison = (Predicates.BinaryComparison) leaf;
      if (comparison.getLeft() instanceof NameReference && comparison.getRight() instanceof Literal) {
        return leaf(((NameReference) comparison.getLeft()).getName(), comparison.getOperator(), Collections.singletonList(comparison.getRight()));
      }
      if (comparison.getLeft() instanceof Literal && comparison.getRight() instanceof NameReference) {
        return leaf(((NameReference) comparison.getRight()).getName(), flip(comparison.getOperator()), Collections.singletonList(comparison.getLeft()));
      }
      return Option.empty();
    }
    if (leaf instanceof Predicates.In) {
      Predicates.In in = (Predicates.In) leaf;
      List<Expression> values = in.getRightChildren();
      if (!(in.getChildren().get(0) instanceof NameReference) || values.isEmpty() || values.size() > IN_LIMIT) {
        return Option.empty();
      }
      return leaf(((NameReference) in.getChildren().get(0)).getName(), Expression.Operator.IN, values);
    }
    if (leaf instanceof Predicates.IsNull || leaf instanceof Predicates.IsNotNull) {
      Expression child = leaf.getChildren().get(0);
      if (!(child instanceof NameReference)) {
        return Option.empty();
      }
      Expression.Operator op = leaf instanceof Predicates.IsNull ? Expression.Operator.IS_NULL : Expression.Operator.IS_NOT_NULL;
      return leaf(((NameReference) child).getName(), op, Collections.emptyList());
    }
    return Option.empty();
  }

  private Option<FilterPredicate> leaf(String name, Expression.Operator op, List<Expression> literals) {
    if (name.contains(".")) {
      // FilterApi reads a dotted name as a nested path
      return Option.empty();
    }
    Option<HoodieSchema> columnType = columnType(name, literals);
    if (!columnType.isPresent() || !support(op, columnType.get().getType()).isSupported()) {
      return Option.empty();
    }
    Option<PrimitiveType> physical = physicalType(columnType.get());
    if (!physical.isPresent()) {
      return Option.empty();
    }
    if (literals.isEmpty()) {
      return compare(name, physical.get(), op, null);
    }
    FilterPredicate result = null;
    for (Expression expression : literals) {
      if (!(expression instanceof Literal)) {
        return Option.empty();
      }
      Literal<?> literal = (Literal<?>) expression;
      if (literal.getDataType() == null || !sameDomain(columnType.get(), literal.getDataType())) {
        return Option.empty();
      }
      Option<Object> value = physicalValue(columnType.get(), physical.get(), op, literal.getValue());
      if (!value.isPresent()) {
        return Option.empty();
      }
      Option<FilterPredicate> predicate = compare(name, physical.get(), op == Expression.Operator.IN ? Expression.Operator.EQ : op, value.get());
      if (!predicate.isPresent()) {
        return Option.empty();
      }
      result = result == null ? predicate.get() : FilterApi.or(result, predicate.get());
    }
    return Option.of(result);
  }

  /**
   * The column's type: from the table schema when there is one, otherwise from the (first) literal.
   */
  private Option<HoodieSchema> columnType(String name, List<Expression> literals) {
    if (tableSchema != null) {
      Option<HoodieSchemaField> field = tableSchema.getField(name);
      return field.isPresent() ? Option.of(field.get().schema().getNonNullType()) : Option.empty();
    }
    if (literals.isEmpty() || !(literals.get(0) instanceof Literal)) {
      return Option.empty();
    }
    return Option.ofNullable(((Literal<?>) literals.get(0)).getDataType());
  }

  /**
   * The physical column {@link ParquetSchemaMapper} writes for a column of this type.
   */
  private static Option<PrimitiveType> physicalType(HoodieSchema columnType) {
    HoodieSchema record = HoodieSchema.createRecord("parquet_renderer", null, null,
        Collections.singletonList(HoodieSchemaField.of("c", HoodieSchema.createNullable(columnType))));
    MessageType message = PARQUET.toPhysicalSchema(record, PhysicalOptions.DEFAULT);
    if (message.getFieldCount() != 1) {
      return Option.empty();
    }
    Type type = message.getType(0);
    return type.isPrimitive() ? Option.of(type.asPrimitiveType()) : Option.empty();
  }

  /**
   * Whether a literal of {@code literalType} has values of the column's type. Decimals of any precision and scale
   * qualify; they are rescaled exactly or not rendered.
   */
  private static boolean sameDomain(HoodieSchema columnType, HoodieSchema literalType) {
    if (columnType.getType() != literalType.getType()) {
      return false;
    }
    if (columnType.getType() == HoodieSchemaType.TIMESTAMP) {
      HoodieSchema.Timestamp column = (HoodieSchema.Timestamp) columnType;
      HoodieSchema.Timestamp literal = (HoodieSchema.Timestamp) literalType;
      return column.getPrecision() == literal.getPrecision() && column.isUtcAdjusted() == literal.isUtcAdjusted();
    }
    return true;
  }

  /**
   * The literal as the Java value of the physical column, or empty when that is not exact.
   */
  private static Option<Object> physicalValue(HoodieSchema columnType, PrimitiveType physical, Expression.Operator op, Object value) {
    if (value == null) {
      return Option.empty();
    }
    switch (columnType.getType()) {
      case BOOLEAN:
        return value instanceof Boolean ? Option.of(value) : Option.empty();
      case INT:
      case DATE:
        return value instanceof Integer ? Option.of(value) : Option.empty();
      case LONG:
      case TIMESTAMP:
        return value instanceof Long ? Option.of(value) : Option.empty();
      case FLOAT:
        return value instanceof Float && !((Float) value).isNaN() ? Option.of(value) : Option.empty();
      case DOUBLE:
        return value instanceof Double && !((Double) value).isNaN() ? Option.of(value) : Option.empty();
      case STRING:
        return value instanceof CharSequence ? stringValue(value.toString(), op) : Option.empty();
      case DECIMAL:
        return value instanceof BigDecimal ? decimalValue((HoodieSchema.Decimal) columnType, physical, (BigDecimal) value) : Option.empty();
      default:
        return Option.empty();
    }
  }

  private static Option<Object> stringValue(String value, Expression.Operator op) {
    byte[] utf8 = value.getBytes(StandardCharsets.UTF_8);
    if (!new String(utf8, StandardCharsets.UTF_8).equals(value)) {
      // an unpaired surrogate does not survive UTF-8
      return Option.empty();
    }
    boolean range = op == Expression.Operator.LT || op == Expression.Operator.LT_EQ || op == Expression.Operator.GT || op == Expression.Operator.GT_EQ;
    if (range && value.chars().anyMatch(c -> c >= 0xD800)) {
      return Option.empty();
    }
    return Option.of(Binary.fromConstantByteArray(utf8));
  }

  private static Option<Object> decimalValue(HoodieSchema.Decimal columnType, PrimitiveType physical, BigDecimal value) {
    BigDecimal scaled;
    try {
      scaled = value.setScale(columnType.getScale());
    } catch (ArithmeticException e) {
      return Option.empty();
    }
    if (scaled.precision() > columnType.getPrecision()) {
      return Option.empty();
    }
    BigInteger unscaled = scaled.unscaledValue();
    switch (physical.getPrimitiveTypeName()) {
      case INT32:
        return Option.of(unscaled.intValueExact());
      case INT64:
        return Option.of(unscaled.longValueExact());
      case BINARY:
        return Option.of(Binary.fromConstantByteArray(unscaled.toByteArray()));
      case FIXED_LEN_BYTE_ARRAY:
        byte[] minimal = unscaled.toByteArray();
        int length = physical.getTypeLength();
        if (minimal.length > length) {
          return Option.empty();
        }
        byte[] fixed = new byte[length];
        byte pad = (byte) (unscaled.signum() < 0 ? 0xFF : 0x00);
        for (int i = 0; i < length - minimal.length; i++) {
          fixed[i] = pad;
        }
        System.arraycopy(minimal, 0, fixed, length - minimal.length, minimal.length);
        return Option.of(Binary.fromConstantByteArray(fixed));
      default:
        return Option.empty();
    }
  }

  /**
   * One FilterApi predicate; {@code value} is null for IS NULL / IS NOT NULL.
   */
  private static Option<FilterPredicate> compare(String name, PrimitiveType physical, Expression.Operator op, Object value) {
    switch (physical.getPrimitiveTypeName()) {
      case BOOLEAN:
        Operators.BooleanColumn column = FilterApi.booleanColumn(name);
        switch (op) {
          case EQ:
            return Option.of(FilterApi.eq(column, (Boolean) value));
          case IS_NULL:
            return Option.of(FilterApi.eq(column, null));
          case IS_NOT_NULL:
            return Option.of(FilterApi.notEq(column, null));
          default:
            return Option.empty();
        }
      case INT32:
        return compare(FilterApi.intColumn(name), op, (Integer) value);
      case INT64:
        return compare(FilterApi.longColumn(name), op, (Long) value);
      case FLOAT:
        return compare(FilterApi.floatColumn(name), op, (Float) value);
      case DOUBLE:
        return compare(FilterApi.doubleColumn(name), op, (Double) value);
      case BINARY:
      case FIXED_LEN_BYTE_ARRAY:
        return compare(FilterApi.binaryColumn(name), op, (Binary) value);
      default:
        return Option.empty();
    }
  }

  private static <T extends Comparable<T>, C extends Operators.Column<T> & Operators.SupportsLtGt> Option<FilterPredicate> compare(C column, Expression.Operator op, T value) {
    switch (op) {
      case EQ:
        return Option.of(FilterApi.eq(column, value));
      case LT:
        return Option.of(FilterApi.lt(column, value));
      case LT_EQ:
        return Option.of(FilterApi.ltEq(column, value));
      case GT:
        return Option.of(FilterApi.gt(column, value));
      case GT_EQ:
        return Option.of(FilterApi.gtEq(column, value));
      case IS_NULL:
        return Option.of(FilterApi.eq(column, null));
      case IS_NOT_NULL:
        return Option.of(FilterApi.notEq(column, null));
      default:
        return Option.empty();
    }
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
}
