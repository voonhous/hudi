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

package org.apache.hudi.hadoop.expression;

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

import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.UDFLike;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBetween;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFIn;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNot;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotNull;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNull;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * {@link ExpressionTranslator} for the filter Hive pushes into input formats (RFC-88 section 4.2), keyed
 * {@link TypeSystems#HIVE}. It is new: Hive has no metadata-table data skipping today.
 *
 * <p>The engine form is Hive's {@link ExprNodeDesc} tree, as serialized under
 * {@code TableScanDesc.FILTER_EXPR_CONF_STR} ({@code hive.io.filter.expr.serialized}) and read back with
 * {@code SerializationUtilities.deserializeExpression}. The tree is used rather than the {@code SearchArgument}
 * Hive derives from it: the sarg is lossy (no LIKE or casts, integer and floating types collapsed) and is built
 * only when {@code hive.optimize.index.filter} is on.
 *
 * <p>AND, OR, NOT and boolean constants go through {@link ConservativeTranslation}. Leaves:
 * <ul>
 *   <li>{@code =, <>, <, <=, >, >=} with a column and a constant in either order, {@code IN},
 *   {@code [NOT] BETWEEN}, {@code IS [NOT] NULL}, and a bare boolean column;</li>
 *   <li>{@code LIKE 'abc%'} as STARTS_WITH and {@code LIKE '%abc%'} as CONTAINS, only when the pattern has no
 *   other wildcard and no escape, so the translation is exact;</li>
 *   <li>a cast over the column is stripped only when it is injective and order-preserving (integral and decimal
 *   widening, int to double, float to double, varchar to string); any other cast makes the leaf unknown.</li>
 * </ul>
 *
 * <p>Literals are typed from the column's {@link HoodieSchema} and must convert exactly: a constant outside the
 * column's type (e.g. {@code c_int = 5.5}, a decimal with too many digits, a timestamp with sub-millisecond digits
 * on a millisecond column), a null constant, or a column missing from the schema makes the leaf unknown. A column's
 * Hive type must also be the one Hudi's Hive readers produce for it ({@code HiveTypeUtils}); TIMESTAMP(MICROS) is a
 * Hive {@code bigint} holding the raw microseconds, so only integral constants are accepted on it.
 */
public final class HiveTranslator implements ExpressionTranslator<ExprNodeDesc> {

  private static final long serialVersionUID = 1L;

  private static final long MAX_EXACT_DOUBLE_LONG = 1L << 53;

  private static final String HIVE3_DATE = "org.apache.hadoop.hive.common.type.Date";
  private static final String HIVE3_TIMESTAMP = "org.apache.hadoop.hive.common.type.Timestamp";
  private static final String HIVE3_DATE_WRITABLE = "org.apache.hadoop.hive.serde2.io.DateWritableV2";
  private static final String HIVE3_TIMESTAMP_WRITABLE = "org.apache.hadoop.hive.serde2.io.TimestampWritableV2";

  private static final Set<String> BRIDGE_CASTS = new HashSet<>(Arrays.asList(
      "org.apache.hadoop.hive.ql.udf.UDFToBoolean", "org.apache.hadoop.hive.ql.udf.UDFToByte",
      "org.apache.hadoop.hive.ql.udf.UDFToShort", "org.apache.hadoop.hive.ql.udf.UDFToInteger",
      "org.apache.hadoop.hive.ql.udf.UDFToLong", "org.apache.hadoop.hive.ql.udf.UDFToFloat",
      "org.apache.hadoop.hive.ql.udf.UDFToDouble", "org.apache.hadoop.hive.ql.udf.UDFToString"));

  /**
   * Generic cast UDFs by simple name, so that the ones of later Hive versions (e.g. {@code GenericUDFToString})
   * are recognized too; the cast's result type comes from the function node.
   */
  private static final Set<String> GENERIC_CASTS = new HashSet<>(Arrays.asList(
      "GenericUDFToDecimal", "GenericUDFToVarchar", "GenericUDFToChar", "GenericUDFToString", "GenericUDFToDate",
      "GenericUDFTimestamp", "GenericUDFToTimestampLocalTZ", "GenericUDFToBinary"));

  /**
   * Hive filter trees for {@link ConservativeTranslation}.
   */
  private static final ConservativeTranslation.Source<ExprNodeDesc> SOURCE = new ConservativeTranslation.Source<ExprNodeDesc>() {
    @Override
    public ConservativeTranslation.Kind kind(ExprNodeDesc node) {
      if (node instanceof ExprNodeGenericFuncDesc) {
        GenericUDF udf = ((ExprNodeGenericFuncDesc) node).getGenericUDF();
        if (udf instanceof GenericUDFOPAnd) {
          return ConservativeTranslation.Kind.AND;
        } else if (udf instanceof GenericUDFOPOr) {
          return ConservativeTranslation.Kind.OR;
        } else if (udf instanceof GenericUDFOPNot && node.getChildren().size() == 1) {
          return ConservativeTranslation.Kind.NOT;
        }
      } else if (node instanceof ExprNodeConstantDesc) {
        Object value = unwrap(((ExprNodeConstantDesc) node).getValue());
        if (Boolean.TRUE.equals(value)) {
          return ConservativeTranslation.Kind.TRUE;
        } else if (Boolean.FALSE.equals(value)) {
          return ConservativeTranslation.Kind.FALSE;
        }
      }
      return ConservativeTranslation.Kind.LEAF;
    }

    @Override
    public List<ExprNodeDesc> children(ExprNodeDesc node) {
      return node.getChildren();
    }
  };

  @Override
  public String system() {
    return TypeSystems.HIVE;
  }

  @Override
  public Option<Expression> toHudi(ExprNodeDesc expr, HoodieSchema schema) {
    if (expr == null || schema == null) {
      return Option.empty();
    }
    return ConservativeTranslation.translate(expr, SOURCE, leaf -> translateLeaf(leaf, schema), ConservativeTranslation.HUDI_TARGET);
  }

  @Override
  public ExpressionSupport support(Expression.Operator op, HoodieSchemaType type) {
    switch (op) {
      case TRUE:
      case FALSE:
      case AND:
      case OR:
      case NOT:
      case IS_NULL:
      case IS_NOT_NULL:
        return ExpressionSupport.supported();
      case EQ:
      case IN:
        return isValueType(type) ? ExpressionSupport.supported() : unsupportedType(type);
      case LT:
      case LT_EQ:
      case GT:
      case GT_EQ:
        if (type == HoodieSchemaType.BOOLEAN) {
          return ExpressionSupport.unsupported("ordering comparisons on BOOLEAN are not translated");
        }
        return isValueType(type) ? ExpressionSupport.supported() : unsupportedType(type);
      case STARTS_WITH:
      case CONTAINS:
        return type == HoodieSchemaType.STRING ? ExpressionSupport.supported()
            : ExpressionSupport.unsupported("LIKE is translated on STRING columns only");
      default:
        return ExpressionSupport.unsupported("operator " + op + " is not translated from Hive");
    }
  }

  private static ExpressionSupport unsupportedType(HoodieSchemaType type) {
    return ExpressionSupport.unsupported("constants of Hive columns of type " + type + " are not translated");
  }

  private static boolean isValueType(HoodieSchemaType type) {
    switch (type) {
      case BOOLEAN:
      case INT:
      case INT8:
      case INT16:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case STRING:
      case CHAR:
      case DECIMAL:
      case DATE:
      case TIMESTAMP:
        return true;
      default:
        return false;
    }
  }

  // -------------------------------------------------------------------------
  // Leaves
  // -------------------------------------------------------------------------

  private static Option<Expression> translateLeaf(ExprNodeDesc node, HoodieSchema schema) {
    if (node instanceof ExprNodeColumnDesc) {
      // a bare boolean column used as a predicate
      Option<Column> column = column(node, schema);
      if (column.isPresent() && column.get().valueComparable && column.get().type.getType() == HoodieSchemaType.BOOLEAN) {
        return Option.of(Predicates.eq(column.get().ref, new Literal<>(true, column.get().type)));
      }
      return Option.empty();
    }
    if (!(node instanceof ExprNodeGenericFuncDesc)) {
      return Option.empty();
    }
    GenericUDF udf = ((ExprNodeGenericFuncDesc) node).getGenericUDF();
    List<ExprNodeDesc> args = node.getChildren() == null ? Collections.emptyList() : node.getChildren();
    // exact classes: GenericUDFOPEqualNS extends GenericUDFOPEqual and has other null semantics
    Class<?> udfClass = udf.getClass();
    if (udfClass == GenericUDFOPEqual.class) {
      return comparison(Expression.Operator.EQ, args, schema);
    } else if (udfClass == GenericUDFOPNotEqual.class) {
      return comparison(Expression.Operator.EQ, args, schema).map(Predicates::not);
    } else if (udfClass == GenericUDFOPLessThan.class) {
      return comparison(Expression.Operator.LT, args, schema);
    } else if (udfClass == GenericUDFOPEqualOrLessThan.class) {
      return comparison(Expression.Operator.LT_EQ, args, schema);
    } else if (udfClass == GenericUDFOPGreaterThan.class) {
      return comparison(Expression.Operator.GT, args, schema);
    } else if (udfClass == GenericUDFOPEqualOrGreaterThan.class) {
      return comparison(Expression.Operator.GT_EQ, args, schema);
    } else if (udfClass == GenericUDFOPNull.class && args.size() == 1) {
      return column(args.get(0), schema).map(c -> Predicates.isNull(c.ref));
    } else if (udfClass == GenericUDFOPNotNull.class && args.size() == 1) {
      return column(args.get(0), schema).map(c -> Predicates.isNotNull(c.ref));
    } else if (udfClass == GenericUDFIn.class) {
      return in(args, schema);
    } else if (udfClass == GenericUDFBetween.class) {
      return between(args, schema);
    } else if (udf instanceof GenericUDFBridge && UDFLike.class.getName().equals(((GenericUDFBridge) udf).getUdfClassName())) {
      return like(args, schema);
    }
    return Option.empty();
  }

  private static Option<Expression> comparison(Expression.Operator op, List<ExprNodeDesc> args, HoodieSchema schema) {
    if (args.size() != 2) {
      return Option.empty();
    }
    if (args.get(1) instanceof ExprNodeConstantDesc) {
      return compare(op, args.get(0), (ExprNodeConstantDesc) args.get(1), schema);
    } else if (args.get(0) instanceof ExprNodeConstantDesc) {
      return compare(flip(op), args.get(1), (ExprNodeConstantDesc) args.get(0), schema);
    }
    return Option.empty();
  }

  private static Option<Expression> compare(Expression.Operator op, ExprNodeDesc columnNode, ExprNodeConstantDesc constant, HoodieSchema schema) {
    Option<Column> column = column(columnNode, schema);
    if (!column.isPresent() || (op != Expression.Operator.EQ && column.get().type.getType() == HoodieSchemaType.BOOLEAN)) {
      return Option.empty();
    }
    Column c = column.get();
    return literal(c, constant).map(l -> comparison(op, c.ref, l));
  }

  private static Expression comparison(Expression.Operator op, Expression left, Expression right) {
    switch (op) {
      case EQ:
        return Predicates.eq(left, right);
      case LT:
        return Predicates.lt(left, right);
      case LT_EQ:
        return Predicates.lteq(left, right);
      case GT:
        return Predicates.gt(left, right);
      case GT_EQ:
        return Predicates.gteq(left, right);
      default:
        throw new IllegalArgumentException("Not a comparison: " + op);
    }
  }

  /**
   * The operator with its operands swapped: {@code 5 < c} is {@code c > 5}.
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

  private static Option<Expression> in(List<ExprNodeDesc> args, HoodieSchema schema) {
    if (args.size() < 2) {
      return Option.empty();
    }
    Option<Column> column = column(args.get(0), schema);
    if (!column.isPresent()) {
      return Option.empty();
    }
    List<Expression> values = new ArrayList<>();
    for (ExprNodeDesc arg : args.subList(1, args.size())) {
      Option<Literal<?>> value = arg instanceof ExprNodeConstantDesc ? literal(column.get(), (ExprNodeConstantDesc) arg) : Option.empty();
      if (!value.isPresent()) {
        return Option.empty();
      }
      values.add(value.get());
    }
    return Option.of(Predicates.in(column.get().ref, values));
  }

  /**
   * {@code BETWEEN(invert, c, low, high)}: {@code c >= low AND c <= high}, negated when {@code invert}.
   */
  private static Option<Expression> between(List<ExprNodeDesc> args, HoodieSchema schema) {
    if (args.size() != 4 || !(args.get(0) instanceof ExprNodeConstantDesc)
        || !(args.get(2) instanceof ExprNodeConstantDesc) || !(args.get(3) instanceof ExprNodeConstantDesc)) {
      return Option.empty();
    }
    Object invert = unwrap(((ExprNodeConstantDesc) args.get(0)).getValue());
    Option<Expression> low = compare(Expression.Operator.GT_EQ, args.get(1), (ExprNodeConstantDesc) args.get(2), schema);
    Option<Expression> high = compare(Expression.Operator.LT_EQ, args.get(1), (ExprNodeConstantDesc) args.get(3), schema);
    if (!(invert instanceof Boolean) || !low.isPresent() || !high.isPresent()) {
      return Option.empty();
    }
    Expression range = Predicates.and(low.get(), high.get());
    return Option.of((Boolean) invert ? Predicates.not(range) : range);
  }

  /**
   * {@code c LIKE 'abc%'} is STARTS_WITH and {@code c LIKE '%abc%'} is CONTAINS when {@code abc} has no wildcard
   * and no escape; any other pattern is unknown.
   */
  private static Option<Expression> like(List<ExprNodeDesc> args, HoodieSchema schema) {
    if (args.size() != 2 || !(args.get(1) instanceof ExprNodeConstantDesc)) {
      return Option.empty();
    }
    Option<Column> column = column(args.get(0), schema);
    Object pattern = unwrap(((ExprNodeConstantDesc) args.get(1)).getValue());
    if (!column.isPresent() || !column.get().valueComparable || column.get().type.getType() != HoodieSchemaType.STRING
        || !(pattern instanceof String)) {
      return Option.empty();
    }
    String p = (String) pattern;
    if (p.indexOf('_') >= 0 || p.indexOf('\\') >= 0 || !p.endsWith("%")) {
      return Option.empty();
    }
    Column c = column.get();
    if (p.length() >= 2 && p.startsWith("%")) {
      String infix = p.substring(1, p.length() - 1);
      return infix.indexOf('%') >= 0 ? Option.empty() : Option.of(Predicates.contains(c.ref, new Literal<>(infix, c.type)));
    }
    String prefix = p.substring(0, p.length() - 1);
    return prefix.indexOf('%') >= 0 ? Option.empty() : Option.of(Predicates.startsWith(c.ref, new Literal<>(prefix, c.type)));
  }

  // -------------------------------------------------------------------------
  // Columns and casts
  // -------------------------------------------------------------------------

  /**
   * A table column a leaf refers to, possibly under order-preserving casts.
   */
  private static final class Column {
    private final NameReference ref;
    /**
     * The column's non-null type; literals take it.
     */
    private final HoodieSchema type;
    /**
     * The Hive type of the expression (the column, or the outermost stripped cast).
     */
    private final TypeInfo hiveType;
    /**
     * Whether the column's Hive type is the one Hudi's Hive readers produce for its HoodieSchema type, so its
     * values compare with constants as the HoodieSchema values do.
     */
    private final boolean valueComparable;

    private Column(NameReference ref, HoodieSchema type, TypeInfo hiveType, boolean valueComparable) {
      this.ref = ref;
      this.type = type;
      this.hiveType = hiveType;
      this.valueComparable = valueComparable;
    }
  }

  private static Option<Column> column(ExprNodeDesc node, HoodieSchema schema) {
    if (node instanceof ExprNodeColumnDesc) {
      ExprNodeColumnDesc columnDesc = (ExprNodeColumnDesc) node;
      Option<HoodieSchemaField> field = field(schema, columnDesc.getColumn());
      if (!field.isPresent()) {
        return Option.empty();
      }
      HoodieSchema type = field.get().schema().getNonNullType();
      TypeInfo hiveType = columnDesc.getTypeInfo();
      return Option.of(new Column(new NameReference(field.get().name()), type, hiveType, readsAs(type, hiveType)));
    }
    if (node instanceof ExprNodeGenericFuncDesc && isCast(((ExprNodeGenericFuncDesc) node).getGenericUDF())
        && node.getChildren() != null && node.getChildren().size() == 1) {
      Option<Column> inner = column(node.getChildren().get(0), schema);
      if (inner.isPresent() && preservesOrder(inner.get().hiveType, node.getTypeInfo())) {
        Column c = inner.get();
        return Option.of(new Column(c.ref, c.type, node.getTypeInfo(), c.valueComparable));
      }
    }
    return Option.empty();
  }

  /**
   * The field by name; Hive lower-cases column names, so a unique case-insensitive match is used otherwise.
   */
  private static Option<HoodieSchemaField> field(HoodieSchema schema, String name) {
    HoodieSchema record = schema.getNonNullType();
    if (name == null || record.getType() != HoodieSchemaType.RECORD) {
      return Option.empty();
    }
    Option<HoodieSchemaField> exact = record.getField(name);
    if (exact.isPresent()) {
      return exact;
    }
    HoodieSchemaField match = null;
    for (HoodieSchemaField f : record.getFields()) {
      if (f.name().toLowerCase(Locale.ROOT).equals(name.toLowerCase(Locale.ROOT))) {
        if (match != null) {
          return Option.empty();
        }
        match = f;
      }
    }
    return Option.ofNullable(match);
  }

  private static boolean isCast(GenericUDF udf) {
    if (udf instanceof GenericUDFBridge) {
      return BRIDGE_CASTS.contains(((GenericUDFBridge) udf).getUdfClassName());
    }
    return GENERIC_CASTS.contains(udf.getClass().getSimpleName());
  }

  /**
   * Whether the Hive type is the one {@code HiveTypeUtils} maps the HoodieSchema type to, i.e. what Hudi's Hive
   * readers hand Hive for the column.
   */
  private static boolean readsAs(HoodieSchema type, TypeInfo hiveType) {
    if (!(hiveType instanceof PrimitiveTypeInfo)) {
      return false;
    }
    PrimitiveCategory category = ((PrimitiveTypeInfo) hiveType).getPrimitiveCategory();
    switch (type.getType()) {
      case BOOLEAN:
        return category == PrimitiveCategory.BOOLEAN;
      case INT:
      case INT8:
      case INT16:
        // HiveTypeUtils reads INT8 and INT16 as Hive tinyint and smallint, whose values are the same Integers
        return category == PrimitiveCategory.INT || category == PrimitiveCategory.SHORT || category == PrimitiveCategory.BYTE;
      case LONG:
        return category == PrimitiveCategory.LONG;
      case FLOAT:
        return category == PrimitiveCategory.FLOAT;
      case DOUBLE:
        return category == PrimitiveCategory.DOUBLE;
      case STRING:
      case CHAR:
        // not a Hive CHAR: Hive compares chars without their padding. HiveTypeUtils reads a HoodieSchema CHAR(n) as Hive
        // char(n), which is therefore not translated, or as string beyond Hive's char length, whose values compare as
        // stored.
        return category == PrimitiveCategory.STRING || category == PrimitiveCategory.VARCHAR;
      case DECIMAL:
        return category == PrimitiveCategory.DECIMAL;
      case DATE:
        return category == PrimitiveCategory.DATE;
      case TIMESTAMP:
        // MILLIS is a Hive timestamp; MICROS is a Hive bigint of the raw value, unless the table is synced with
        // support_timestamp, where the reader truncates it to milliseconds and constants are not exact
        boolean millis = ((HoodieSchema.Timestamp) type).getPrecision() == HoodieSchema.TimePrecision.MILLIS;
        return millis ? category == PrimitiveCategory.TIMESTAMP : category == PrimitiveCategory.LONG;
      default:
        return false;
    }
  }

  /**
   * Rule 2: a cast is stripped only if it is injective and preserves order on every value of its input, so that
   * {@code cast(c) op k} holds exactly when {@code c op k'} for the {@code k'} with {@code cast(k') = k}.
   */
  static boolean preservesOrder(TypeInfo from, TypeInfo to) {
    if (!(from instanceof PrimitiveTypeInfo) || !(to instanceof PrimitiveTypeInfo)) {
      return false;
    }
    if (from.equals(to)) {
      return true;
    }
    PrimitiveCategory source = ((PrimitiveTypeInfo) from).getPrimitiveCategory();
    PrimitiveCategory target = ((PrimitiveTypeInfo) to).getPrimitiveCategory();
    int digits = integralDigits(source);
    if (digits > 0) {
      switch (target) {
        case BYTE:
        case SHORT:
        case INT:
        case LONG:
          return integralDigits(target) >= digits;
        case FLOAT:
          // exact up to 2^24
          return source == PrimitiveCategory.BYTE || source == PrimitiveCategory.SHORT;
        case DOUBLE:
          // exact up to 2^53
          return source != PrimitiveCategory.LONG;
        case DECIMAL:
          DecimalTypeInfo decimal = (DecimalTypeInfo) to;
          return decimal.getPrecision() - decimal.getScale() >= digits;
        default:
          return false;
      }
    }
    switch (source) {
      case FLOAT:
        return target == PrimitiveCategory.DOUBLE;
      case DECIMAL:
        if (target != PrimitiveCategory.DECIMAL) {
          return false;
        }
        DecimalTypeInfo in = (DecimalTypeInfo) from;
        DecimalTypeInfo out = (DecimalTypeInfo) to;
        return out.getScale() >= in.getScale() && out.getPrecision() - out.getScale() >= in.getPrecision() - in.getScale();
      case VARCHAR:
        return target == PrimitiveCategory.STRING
            || (target == PrimitiveCategory.VARCHAR && ((VarcharTypeInfo) to).getLength() >= ((VarcharTypeInfo) from).getLength());
      case STRING:
        return target == PrimitiveCategory.STRING;
      default:
        return false;
    }
  }

  /**
   * The decimal digits an integral type needs, or 0 if it is not integral.
   */
  private static int integralDigits(PrimitiveCategory category) {
    switch (category) {
      case BYTE:
        return 3;
      case SHORT:
        return 5;
      case INT:
        return 10;
      case LONG:
        return 19;
      default:
        return 0;
    }
  }

  // -------------------------------------------------------------------------
  // Literals
  // -------------------------------------------------------------------------

  /**
   * The constant as a literal of the column's type, or empty if the conversion is not exact.
   */
  private static Option<Literal<?>> literal(Column column, ExprNodeConstantDesc constant) {
    if (!column.valueComparable) {
      return Option.empty();
    }
    Object value = canonical(column.type, unwrap(constant.getValue()));
    return value == null ? Option.empty() : Option.of(new Literal<>(value, column.type));
  }

  /**
   * Converts an unwrapped Hive constant to the canonical value of the column type, or null if not exact.
   */
  private static Object canonical(HoodieSchema type, Object value) {
    if (value == null) {
      return null;
    }
    switch (type.getType()) {
      case BOOLEAN:
        return value instanceof Boolean ? value : null;
      case INT:
      case INT8:
      case INT16:
        BigDecimal i = exactNumber(value);
        return i != null && isIntegral(i) && i.compareTo(BigDecimal.valueOf(Integer.MIN_VALUE)) >= 0
            && i.compareTo(BigDecimal.valueOf(Integer.MAX_VALUE)) <= 0 ? i.intValue() : null;
      case LONG:
        if (value instanceof Double && Math.abs((Double) value) >= MAX_EXACT_DOUBLE_LONG) {
          // Hive compares a bigint with a double as doubles, which is not injective beyond 2^53
          return null;
        }
        BigDecimal l = exactNumber(value);
        return l != null && isIntegral(l) && l.compareTo(BigDecimal.valueOf(Long.MIN_VALUE)) >= 0
            && l.compareTo(BigDecimal.valueOf(Long.MAX_VALUE)) <= 0 ? l.longValue() : null;
      case FLOAT:
        BigDecimal f = floatingNumber(value);
        return f != null && Float.isFinite(f.floatValue()) && new BigDecimal(f.floatValue()).compareTo(f) == 0 ? f.floatValue() : null;
      case DOUBLE:
        BigDecimal d = floatingNumber(value);
        return d != null && Double.isFinite(d.doubleValue()) && new BigDecimal(d.doubleValue()).compareTo(d) == 0 ? d.doubleValue() : null;
      case DECIMAL:
        // Hive may compare a decimal with a floating constant as doubles; that is not translated
        return value instanceof Long || value instanceof BigDecimal ? decimal((HoodieSchema.Decimal) type, exactNumber(value)) : null;
      case STRING:
      case CHAR:
        return value instanceof String ? value : null;
      case DATE:
        return value instanceof LocalDate ? Math.toIntExact(((LocalDate) value).toEpochDay()) : null;
      case TIMESTAMP:
        return timestamp((HoodieSchema.Timestamp) type, value);
      default:
        return null;
    }
  }

  private static boolean isIntegral(BigDecimal value) {
    return value.signum() == 0 || value.stripTrailingZeros().scale() <= 0;
  }

  /**
   * The exact value of a numeric constant; floating values only when finite.
   */
  private static BigDecimal exactNumber(Object value) {
    if (value instanceof Long) {
      return BigDecimal.valueOf((Long) value);
    } else if (value instanceof BigDecimal) {
      return (BigDecimal) value;
    } else if (value instanceof Double && Double.isFinite((Double) value)) {
      return new BigDecimal((Double) value);
    }
    return null;
  }

  /**
   * The exact value of a constant compared with a floating column. A fractional decimal constant is not
   * translated: Hive may compare it in decimal space, where the column's doubles are converted through their
   * string form rather than exactly.
   */
  private static BigDecimal floatingNumber(Object value) {
    BigDecimal number = exactNumber(value);
    return number != null && value instanceof BigDecimal && !isIntegral(number) ? null : number;
  }

  private static BigDecimal decimal(HoodieSchema.Decimal type, BigDecimal value) {
    try {
      BigDecimal scaled = value.setScale(type.getScale(), RoundingMode.UNNECESSARY);
      return scaled.precision() <= type.getPrecision() ? scaled : null;
    } catch (ArithmeticException e) {
      return null;
    }
  }

  /**
   * MILLIS columns take Hive timestamps without sub-millisecond digits; MICROS columns are Hive bigints and take
   * integral constants as the raw value.
   */
  private static Object timestamp(HoodieSchema.Timestamp type, Object value) {
    if (type.getPrecision() == HoodieSchema.TimePrecision.MICROS) {
      return value instanceof Long ? value : null;
    }
    if (!(value instanceof Instant)) {
      return null;
    }
    Instant instant = (Instant) value;
    if (instant.getNano() % 1_000_000 != 0) {
      return null;
    }
    try {
      return Math.addExact(Math.multiplyExact(instant.getEpochSecond(), 1000L), instant.getNano() / 1_000_000);
    } catch (ArithmeticException e) {
      return null;
    }
  }

  /**
   * Normalizes a Hive constant: integral numbers to {@code Long}, floating to {@code Double}, decimals to
   * {@code BigDecimal}, strings to {@code String}, dates to {@code LocalDate} and timestamps to the {@code Instant}
   * with the same epoch seconds and nanos as Hive's value; anything else (e.g. {@code HiveChar}) is returned as is
   * and does not convert.
   *
   * <p>Hive 2 dates and timestamps ({@code java.sql}) convert as Hive's own {@code DateWritable} does, and agree
   * with the values {@code Hive2Shim} gives Hive for the column. Hive 3 types are not on the compile classpath and
   * are read by reflection.
   */
  static Object unwrap(Object value) {
    if (value == null) {
      return null;
    } else if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long) {
      return ((Number) value).longValue();
    } else if (value instanceof Float || value instanceof Double) {
      return ((Number) value).doubleValue();
    } else if (value instanceof BigDecimal) {
      return value;
    } else if (value instanceof HiveDecimal) {
      return ((HiveDecimal) value).bigDecimalValue();
    } else if (value instanceof String || value instanceof Boolean) {
      return value;
    } else if (value instanceof Text) {
      return value.toString();
    } else if (value instanceof HiveVarchar) {
      return ((HiveVarchar) value).getValue();
    } else if (value instanceof HiveChar) {
      return value;
    } else if (value instanceof java.sql.Date) {
      return LocalDate.ofEpochDay(DateWritable.dateToDays((java.sql.Date) value));
    } else if (value instanceof Timestamp) {
      Timestamp timestamp = (Timestamp) value;
      return Instant.ofEpochSecond(Math.floorDiv(timestamp.getTime(), 1000L), timestamp.getNanos());
    } else if (value instanceof DateWritable) {
      return LocalDate.ofEpochDay(((DateWritable) value).getDays());
    } else if (value instanceof TimestampWritable) {
      return unwrap(((TimestampWritable) value).getTimestamp());
    } else if (value instanceof HiveDecimalWritable) {
      return unwrap(((HiveDecimalWritable) value).getHiveDecimal());
    } else if (value instanceof HiveVarcharWritable) {
      return unwrap(((HiveVarcharWritable) value).getHiveVarchar());
    } else if (value instanceof BooleanWritable) {
      return ((BooleanWritable) value).get();
    } else if (value instanceof ByteWritable) {
      return (long) ((ByteWritable) value).get();
    } else if (value instanceof ShortWritable) {
      return (long) ((ShortWritable) value).get();
    } else if (value instanceof IntWritable) {
      return (long) ((IntWritable) value).get();
    } else if (value instanceof LongWritable) {
      return ((LongWritable) value).get();
    } else if (value instanceof FloatWritable) {
      return (double) ((FloatWritable) value).get();
    } else if (value instanceof DoubleWritable) {
      // also Hive's serde2.io.DoubleWritable, which extends it
      return ((DoubleWritable) value).get();
    }
    return unwrapHive3(value);
  }

  private static Object unwrapHive3(Object value) {
    try {
      switch (value.getClass().getName()) {
        case HIVE3_DATE:
          return LocalDate.ofEpochDay(((Number) value.getClass().getMethod("toEpochDay").invoke(value)).longValue());
        case HIVE3_TIMESTAMP:
          // a LocalDateTime at UTC, as Hive3Shim builds it for the column
          long seconds = ((Number) value.getClass().getMethod("toEpochSecond").invoke(value)).longValue();
          int nanos = ((Number) value.getClass().getMethod("getNanos").invoke(value)).intValue();
          return Instant.ofEpochSecond(seconds, nanos);
        case HIVE3_DATE_WRITABLE:
          return LocalDate.ofEpochDay(((Number) value.getClass().getMethod("getDays").invoke(value)).longValue());
        case HIVE3_TIMESTAMP_WRITABLE:
          return unwrap(value.getClass().getMethod("getTimestamp").invoke(value));
        default:
          return value;
      }
    } catch (ReflectiveOperationException | RuntimeException e) {
      // an unknown shape does not convert, which keeps every row
      return value;
    }
  }
}
