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

import org.apache.hudi.common.expression.Expression;
import org.apache.hudi.common.expression.translate.ExpressionSupport;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.util.Option;

import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.UTF8String;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.EnumSet;
import java.util.Set;

/**
 * Value conversions and the support table shared by {@link SparkTranslator} and {@link SparkFilterRenderer}:
 * Spark data source filter values to Hudi's canonical literal values for a column type, and back.
 *
 * <p>Every conversion is exact: a value that the target type cannot hold exactly converts to nothing, and the
 * predicate holding it is dropped.
 */
final class SparkFilterValues {

  /**
   * The column types whose values convert in both directions; the types {@code SparkFilterHelper.convertDataType}
   * handles today.
   */
  static final Set<HoodieSchemaType> VALUE_TYPES = EnumSet.of(HoodieSchemaType.BOOLEAN, HoodieSchemaType.INT,
      HoodieSchemaType.LONG, HoodieSchemaType.FLOAT, HoodieSchemaType.DOUBLE, HoodieSchemaType.STRING,
      HoodieSchemaType.DECIMAL, HoodieSchemaType.DATE, HoodieSchemaType.TIMESTAMP);

  private static final long MICROS_PER_SECOND = 1_000_000L;
  private static final long MICROS_PER_MILLI = 1_000L;

  private SparkFilterValues() {
  }

  /**
   * What Spark data source filters can state, in both directions.
   */
  static ExpressionSupport support(Expression.Operator op, HoodieSchemaType type) {
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
      case LT:
      case LT_EQ:
      case GT:
      case GT_EQ:
      case IN:
        return VALUE_TYPES.contains(type) ? ExpressionSupport.supported()
            : ExpressionSupport.unsupported("no exact conversion between Spark filter values and " + type + " literals");
      case STARTS_WITH:
      case CONTAINS:
        return type == HoodieSchemaType.STRING ? ExpressionSupport.supported()
            : ExpressionSupport.unsupported("Spark string filters apply to STRING columns only");
      default:
        return ExpressionSupport.unsupported("Spark data source filters have no " + op);
    }
  }

  /**
   * Converts a Spark filter value to the canonical literal value of a column type (DATE as epoch days, TIMESTAMP
   * as epoch micros or millis by the column's precision, DECIMAL with the column's scale).
   *
   * @param value a value as Spark puts it in a data source filter
   * @param type  the column's non-null schema
   */
  static Option<Object> toCanonical(Object value, HoodieSchema type) {
    if (value == null) {
      return Option.empty();
    }
    try {
      switch (type.getType()) {
        case BOOLEAN:
          return value instanceof Boolean ? Option.of(value) : Option.empty();
        case INT:
          return exactNumber(value).map(BigDecimal::intValueExact);
        case LONG:
          return exactNumber(value).map(BigDecimal::longValueExact);
        case FLOAT:
          return exactNumber(value).flatMap(d -> {
            float f = d.floatValue();
            return Float.isFinite(f) && new BigDecimal(f).compareTo(d) == 0 ? Option.of(f) : Option.empty();
          });
        case DOUBLE:
          return exactNumber(value).flatMap(d -> {
            double v = d.doubleValue();
            return Double.isFinite(v) && new BigDecimal(v).compareTo(d) == 0 ? Option.of(v) : Option.empty();
          });
        case DECIMAL:
          return exactNumber(value).flatMap(d -> decimal(d, (HoodieSchema.Decimal) type));
        case STRING:
          return value instanceof CharSequence || value instanceof UTF8String ? Option.of(value.toString()) : Option.empty();
        case DATE:
          if (value instanceof Date) {
            return Option.of(DateTimeUtils.fromJavaDate((Date) value));
          }
          return value instanceof LocalDate ? Option.of(Math.toIntExact(((LocalDate) value).toEpochDay())) : Option.empty();
        case TIMESTAMP:
          return timestampMicros(value, (HoodieSchema.Timestamp) type).flatMap(micros -> timestampValue(micros, (HoodieSchema.Timestamp) type));
        default:
          return Option.empty();
      }
    } catch (ArithmeticException e) {
      // out of range for the column type, or not exact
      return Option.empty();
    }
  }

  /**
   * Converts a canonical literal value to the Java value Spark's Parquet filter pushdown ({@code ParquetFilters}
   * in Spark 3.5) accepts for the type: {@code Integer}, {@code Long}, {@code Float}, {@code Double},
   * {@code Boolean}, {@code String}, {@code BigDecimal} with the column's scale, {@code LocalDate} for DATE and
   * {@code Instant} for TIMESTAMP ({@code LocalDateTime} for local timestamps, which it does not push down).
   *
   * @param value the canonical value
   * @param type  the literal's schema
   */
  static Option<Object> toSpark(Object value, HoodieSchema type) {
    if (value == null) {
      return Option.empty();
    }
    try {
      switch (type.getType()) {
        case BOOLEAN:
        case INT:
        case LONG:
        case FLOAT:
        case DOUBLE:
        case DECIMAL:
        case STRING:
          // the canonical values of these types are the Spark values; still check they are exact
          return toCanonical(value, type);
        case DATE:
          return value instanceof Integer ? Option.of(LocalDate.ofEpochDay((Integer) value)) : Option.empty();
        case TIMESTAMP:
          if (!(value instanceof Long)) {
            return Option.empty();
          }
          HoodieSchema.Timestamp ts = (HoodieSchema.Timestamp) type;
          long micros = ts.getPrecision() == HoodieSchema.TimePrecision.MILLIS
              ? Math.multiplyExact((Long) value, MICROS_PER_MILLI) : (Long) value;
          long seconds = Math.floorDiv(micros, MICROS_PER_SECOND);
          int nanos = (int) (Math.floorMod(micros, MICROS_PER_SECOND) * 1000);
          return ts.isUtcAdjusted() ? Option.of(Instant.ofEpochSecond(seconds, nanos))
              : Option.of(LocalDateTime.ofEpochSecond(seconds, nanos, ZoneOffset.UTC));
        default:
          return Option.empty();
      }
    } catch (ArithmeticException | java.time.DateTimeException e) {
      return Option.empty();
    }
  }

  /**
   * Any numeric value Spark or a caller may use, as an exact {@code BigDecimal}; non-finite floating point values
   * have none.
   */
  private static Option<BigDecimal> exactNumber(Object value) {
    if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long) {
      return Option.of(BigDecimal.valueOf(((Number) value).longValue()));
    } else if (value instanceof Float || value instanceof Double) {
      double d = ((Number) value).doubleValue();
      return Double.isFinite(d) ? Option.of(new BigDecimal(d)) : Option.empty();
    } else if (value instanceof BigDecimal) {
      return Option.of((BigDecimal) value);
    } else if (value instanceof BigInteger) {
      return Option.of(new BigDecimal((BigInteger) value));
    } else if (value instanceof Decimal) {
      return Option.of(((Decimal) value).toJavaBigDecimal());
    } else if (value instanceof scala.math.BigDecimal) {
      return Option.of(((scala.math.BigDecimal) value).bigDecimal());
    }
    return Option.empty();
  }

  private static Option<Object> decimal(BigDecimal value, HoodieSchema.Decimal type) {
    // setScale without a rounding mode throws ArithmeticException unless the value is exact at the column's scale
    BigDecimal scaled = value.setScale(type.getScale());
    return scaled.precision() <= type.getPrecision() ? Option.of(scaled) : Option.empty();
  }

  private static Option<Long> timestampMicros(Object value, HoodieSchema.Timestamp type) {
    if (type.isUtcAdjusted()) {
      if (value instanceof Timestamp) {
        return Option.of(DateTimeUtils.fromJavaTimestamp((Timestamp) value));
      } else if (value instanceof Instant) {
        return Option.of(DateTimeUtils.instantToMicros((Instant) value));
      }
    } else if (value instanceof LocalDateTime) {
      LocalDateTime local = (LocalDateTime) value;
      return Option.of(Math.addExact(Math.multiplyExact(local.toEpochSecond(ZoneOffset.UTC), MICROS_PER_SECOND), local.getNano() / 1000));
    }
    return Option.empty();
  }

  private static Option<Object> timestampValue(long micros, HoodieSchema.Timestamp type) {
    if (type.getPrecision() == HoodieSchema.TimePrecision.MICROS) {
      return Option.of(micros);
    }
    return micros % MICROS_PER_MILLI == 0 ? Option.of(micros / MICROS_PER_MILLI) : Option.empty();
  }
}
