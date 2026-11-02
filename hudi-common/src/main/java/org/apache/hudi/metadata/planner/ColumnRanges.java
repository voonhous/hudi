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

import org.apache.hudi.avro.model.HoodieMetadataColumnStats;
import org.apache.hudi.common.avro.HoodieAvroWrapperUtils;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.util.DateTimeUtils;
import org.apache.hudi.metadata.stats.ValueMetadata;
import org.apache.hudi.metadata.stats.ValueType;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;

/**
 * Reads metadata-table statistics into {@link ColumnRange}s, for every stats-based lookup.
 *
 * <p>Values are converted to the canonical representation of the column's current type (see {@link ColumnRange}).
 * Whatever cannot be converted safely becomes unknown: a deleted (tombstone) entry, a missing value, a value of a type
 * other than the column's (e.g. written before a schema change), timestamp-millis stats of a V1 index (whose unit
 * is not reliable, see {@code HoodieTableMetadataUtil.getValidIndexedColumns}), and types the evaluator does not
 * compare. The only type changes read through are widenings that keep the order: int to long, float or double, long
 * to float or double, float to double.
 */
public final class ColumnRanges {

  private ColumnRanges() {
  }

  /**
   * A range of which nothing is known.
   */
  public static ColumnRange unknown(HoodieSchema columnType) {
    return ColumnRange.of(columnType, null, null, null, null);
  }

  /**
   * @param stats      a column stats or partition stats entry
   * @param columnType the column's current type (nullable or not)
   */
  public static ColumnRange fromColumnStats(HoodieMetadataColumnStats stats, HoodieSchema columnType) {
    return fromColumnStats(stats, columnType, true);
  }

  /**
   * @param withCounts whether to keep the null and value counts; partition stats drop them, as Spark does not prune
   *                   partitions on them (they are not exact after updates and deletes in MOR tables)
   */
  public static ColumnRange fromColumnStats(HoodieMetadataColumnStats stats, HoodieSchema columnType, boolean withCounts) {
    HoodieSchema type = nonNull(columnType);
    if (stats == null || Boolean.TRUE.equals(stats.getIsDeleted())) {
      return unknown(type);
    }
    Long nullCount = withCounts ? stats.getNullCount() : null;
    Long valueCount = withCounts ? stats.getValueCount() : null;
    Comparable<?> min = null;
    Comparable<?> max = null;
    try {
      ValueMetadata valueMetadata = ValueMetadata.getValueMetadata(stats.getValueType());
      min = toCanonical(stats.getMinValue(), valueMetadata, type);
      max = toCanonical(stats.getMaxValue(), valueMetadata, type);
    } catch (RuntimeException e) {
      // unreadable value type or wrapper: the bounds are unknown
      min = null;
      max = null;
    }
    return ColumnRange.of(type, min, max, nullCount, valueCount);
  }

  private static Comparable<?> toCanonical(Object wrapper, ValueMetadata valueMetadata, HoodieSchema type) {
    if (wrapper == null) {
      return null;
    }
    if (valueMetadata.isV1()) {
      return fromV1(HoodieAvroWrapperUtils.unwrapAvroValueWrapper(wrapper), type);
    }
    ValueType stored = valueMetadata.getValueType();
    if (stored == ValueType.NULL || !isReadableAs(stored, type)) {
      return null;
    }
    return fromV2(valueMetadata.unwrapValue(wrapper), type);
  }

  /**
   * Whether V2 stats of the stored value type can be read as the column's current type.
   */
  private static boolean isReadableAs(ValueType stored, HoodieSchema type) {
    ValueType current;
    try {
      current = ValueType.fromSchema(type);
    } catch (RuntimeException e) {
      return false;
    }
    if (stored == current) {
      return true;
    }
    switch (current) {
      case LONG:
        return stored == ValueType.INT;
      case FLOAT:
        return stored == ValueType.INT || stored == ValueType.LONG;
      case DOUBLE:
        return stored == ValueType.INT || stored == ValueType.LONG || stored == ValueType.FLOAT;
      default:
        return false;
    }
  }

  /**
   * V2 values come in the stored value type's in-memory class: {@code LocalDate}, {@code Instant},
   * {@code LocalDateTime}, {@code LocalTime}, {@code BigDecimal}, {@code String} or a boxed primitive.
   */
  private static Comparable<?> fromV2(Comparable<?> value, HoodieSchema type) {
    if (value == null) {
      return null;
    }
    switch (type.getType()) {
      case DATE:
        return value instanceof LocalDate ? (Comparable<?>) Math.toIntExact(((LocalDate) value).toEpochDay()) : null;
      case TIMESTAMP:
        return timestamp(value, (HoodieSchema.Timestamp) type, false);
      case TIME:
        if (!(value instanceof LocalTime)) {
          return null;
        }
        long nanos = ((LocalTime) value).toNanoOfDay();
        return ((HoodieSchema.Time) type).getPrecision() == HoodieSchema.TimePrecision.MILLIS
            ? (Comparable<?>) (int) (nanos / 1_000_000L) : (Comparable<?>) (nanos / 1_000L);
      default:
        return primitive(value, type);
    }
  }

  /**
   * V1 values come unwrapped by {@link HoodieAvroWrapperUtils#unwrapAvroValueWrapper}: {@code java.sql.Date} or
   * {@code LocalDate} for dates, {@code Instant} for timestamp-micros wrappers, {@code BigDecimal} for decimals, a
   * boxed primitive or {@code String} otherwise.
   */
  private static Comparable<?> fromV1(Comparable<?> value, HoodieSchema type) {
    if (value == null) {
      return null;
    }
    switch (type.getType()) {
      case DATE:
        if (value instanceof java.sql.Date) {
          return Math.toIntExact(((java.sql.Date) value).toLocalDate().toEpochDay());
        } else if (value instanceof LocalDate) {
          return Math.toIntExact(((LocalDate) value).toEpochDay());
        }
        return value instanceof Integer ? value : null;
      case TIMESTAMP:
        return timestamp(value, (HoodieSchema.Timestamp) type, true);
      case TIME:
        // the unit of V1 time stats is not recorded
        return null;
      default:
        return primitive(value, type);
    }
  }

  private static Comparable<?> timestamp(Comparable<?> value, HoodieSchema.Timestamp type, boolean v1) {
    boolean micros = type.getPrecision() == HoodieSchema.TimePrecision.MICROS;
    if (v1 && !micros) {
      return null;
    }
    Instant instant;
    if (value instanceof Instant) {
      instant = (Instant) value;
    } else if (value instanceof LocalDateTime) {
      instant = ((LocalDateTime) value).toInstant(ZoneOffset.UTC);
    } else if (value instanceof java.sql.Timestamp) {
      instant = ((java.sql.Timestamp) value).toInstant();
    } else if (v1 && value instanceof Long) {
      // V1 stores timestamps as epoch micros
      return value;
    } else {
      return null;
    }
    return micros ? DateTimeUtils.instantToMicros(instant) : instant.toEpochMilli();
  }

  private static Comparable<?> primitive(Comparable<?> value, HoodieSchema type) {
    switch (type.getType()) {
      case BOOLEAN:
        return value instanceof Boolean ? value : null;
      case INT:
      case INT8:
      case INT16:
        return value instanceof Integer ? value : null;
      case LONG:
        if (value instanceof Long) {
          return value;
        }
        return value instanceof Integer ? (Comparable<?>) ((Integer) value).longValue() : null;
      case FLOAT:
        if (value instanceof Float) {
          return value;
        } else if (value instanceof Integer || value instanceof Long) {
          return ((Number) value).floatValue();
        }
        return null;
      case DOUBLE:
        if (value instanceof Double) {
          return value;
        } else if (value instanceof Float) {
          // exact, unlike Double.valueOf(value.toString())
          return (double) (Float) value;
        } else if (value instanceof Integer || value instanceof Long) {
          return ((Number) value).doubleValue();
        }
        return null;
      case STRING:
      case CHAR:
        return value instanceof CharSequence ? value.toString() : null;
      case DECIMAL:
        return value instanceof BigDecimal ? value : null;
      default:
        return null;
    }
  }

  private static HoodieSchema nonNull(HoodieSchema schema) {
    return schema.getType() == HoodieSchemaType.UNION ? schema.getNonNullType() : schema;
  }
}
