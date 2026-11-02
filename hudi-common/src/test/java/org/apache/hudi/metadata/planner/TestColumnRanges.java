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
import org.apache.hudi.metadata.HoodieIndexVersion;
import org.apache.hudi.metadata.stats.ValueMetadata;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Tests {@link ColumnRanges}: metadata-table column stats to canonical {@link ColumnRange}s.
 */
public class TestColumnRanges {

  private static final HoodieSchema INT = HoodieSchema.create(HoodieSchemaType.INT);
  private static final HoodieSchema LONG = HoodieSchema.create(HoodieSchemaType.LONG);
  private static final HoodieSchema DOUBLE = HoodieSchema.create(HoodieSchemaType.DOUBLE);
  private static final HoodieSchema STRING = HoodieSchema.create(HoodieSchemaType.STRING);

  private static HoodieMetadataColumnStats v2(HoodieSchema writtenType, Comparable<?> min, Comparable<?> max, long nulls, long values) {
    ValueMetadata meta = ValueMetadata.getValueMetadata(writtenType, HoodieIndexVersion.V2);
    return stats(meta.wrapValue(min), meta.wrapValue(max), nulls, values, false).setValueType(meta.getValueTypeInfo()).build();
  }

  private static HoodieMetadataColumnStats v1(Comparable<?> min, Comparable<?> max, long nulls, long values) {
    return stats(HoodieAvroWrapperUtils.wrapValueIntoAvro(min), HoodieAvroWrapperUtils.wrapValueIntoAvro(max), nulls, values, false).build();
  }

  private static HoodieMetadataColumnStats.Builder stats(Object min, Object max, long nulls, long values, boolean deleted) {
    return HoodieMetadataColumnStats.newBuilder()
        .setFileName("f1.parquet")
        .setColumnName("c")
        .setMinValue(min)
        .setMaxValue(max)
        .setNullCount(nulls)
        .setValueCount(values)
        .setTotalSize(1L)
        .setTotalUncompressedSize(1L)
        .setIsDeleted(deleted);
  }

  @Test
  void v2Values() {
    assertEquals(ColumnRange.of(INT, 1, 9, 2L, 10L), ColumnRanges.fromColumnStats(v2(INT, 1, 9, 2, 10), HoodieSchema.createNullable(INT)));
    assertEquals(ColumnRange.of(STRING, "a", "z", 0L, 3L), ColumnRanges.fromColumnStats(v2(STRING, "a", "z", 0, 3), STRING));
    HoodieSchema date = HoodieSchema.createDate();
    assertEquals(ColumnRange.of(date, 18000, 18010, 0L, 3L),
        ColumnRanges.fromColumnStats(v2(date, LocalDate.ofEpochDay(18000), LocalDate.ofEpochDay(18010), 0, 3), date));
    HoodieSchema micros = HoodieSchema.createTimestampMicros();
    Instant instant = Instant.ofEpochSecond(1_600_000_000L, 123_456_000L);
    assertEquals(ColumnRange.of(micros, 1_600_000_000_123_456L, 1_600_000_000_123_456L, 0L, 1L),
        ColumnRanges.fromColumnStats(v2(micros, instant, instant, 0, 1), micros));
    HoodieSchema millis = HoodieSchema.createTimestampMillis();
    assertEquals(ColumnRange.of(millis, 1_600_000_000_123L, 1_600_000_000_123L, 0L, 1L),
        ColumnRanges.fromColumnStats(v2(millis, Instant.ofEpochMilli(1_600_000_000_123L), Instant.ofEpochMilli(1_600_000_000_123L), 0, 1), millis));
    HoodieSchema localMicros = HoodieSchema.createLocalTimestampMicros();
    LocalDateTime local = LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
    assertEquals(ColumnRange.of(localMicros, 1_600_000_000_123_456L, 1_600_000_000_123_456L, 0L, 1L),
        ColumnRanges.fromColumnStats(v2(localMicros, local, local, 0, 1), localMicros));
    HoodieSchema decimal = HoodieSchema.createDecimal(10, 2);
    ColumnRange decimals = ColumnRanges.fromColumnStats(v2(decimal, new BigDecimal("-1.25"), new BigDecimal("3.50"), 0, 2), decimal);
    assertEquals(0, new BigDecimal("-1.25").compareTo((BigDecimal) decimals.min().get()));
    assertEquals(0, new BigDecimal("3.5").compareTo((BigDecimal) decimals.max().get()));
  }

  @Test
  void v2Widening() {
    assertEquals(ColumnRange.of(LONG, 1L, 9L, 0L, 3L), ColumnRanges.fromColumnStats(v2(INT, 1, 9, 0, 3), LONG));
    assertEquals(ColumnRange.of(DOUBLE, 1.0, 9.0, 0L, 3L), ColumnRanges.fromColumnStats(v2(INT, 1, 9, 0, 3), DOUBLE));
    HoodieSchema floatType = HoodieSchema.create(HoodieSchemaType.FLOAT);
    // exact: (double) 0.1f, not 0.1
    assertEquals(ColumnRange.of(DOUBLE, (double) 0.1f, (double) 0.2f, 0L, 3L), ColumnRanges.fromColumnStats(v2(floatType, 0.1f, 0.2f, 0, 3), DOUBLE));
  }

  @Test
  void otherTypeChangesAreUnknown() {
    // a column that changed from string to int, and from long to int
    assertEquals(ColumnRange.of(INT, null, null, 0L, 3L), ColumnRanges.fromColumnStats(v2(STRING, "1", "9", 0, 3), INT));
    assertEquals(ColumnRange.of(INT, null, null, 0L, 3L), ColumnRanges.fromColumnStats(v2(LONG, 1L, 9L, 0, 3), INT));
    // millis stats for a micros column
    HoodieSchema micros = HoodieSchema.createTimestampMicros();
    ColumnRange range = ColumnRanges.fromColumnStats(v2(HoodieSchema.createTimestampMillis(), Instant.ofEpochMilli(1), Instant.ofEpochMilli(2), 0, 3), micros);
    assertFalse(range.min().isPresent());
    assertFalse(range.max().isPresent());
    // a type the evaluator does not compare
    HoodieSchema bytes = HoodieSchema.create(HoodieSchemaType.BYTES);
    range = ColumnRanges.fromColumnStats(v2(bytes, java.nio.ByteBuffer.wrap(new byte[] {1}), java.nio.ByteBuffer.wrap(new byte[] {2}), 0, 3), bytes);
    assertFalse(range.min().isPresent());
  }

  @Test
  void deletedAndMissing() {
    HoodieMetadataColumnStats deleted = stats(null, null, 0, 3, true).build();
    assertEquals(ColumnRanges.unknown(INT), ColumnRanges.fromColumnStats(deleted, INT));
    // no min/max: counts only
    HoodieMetadataColumnStats allNull = stats(null, null, 3, 3, false).build();
    assertEquals(ColumnRange.of(INT, null, null, 3L, 3L), ColumnRanges.fromColumnStats(allNull, INT));
    // partition stats drop the counts
    assertEquals(ColumnRange.of(INT, 1, 9, null, null), ColumnRanges.fromColumnStats(v2(INT, 1, 9, 2, 10), INT, false));
  }

  @Test
  void v1Values() {
    assertEquals(ColumnRange.of(INT, 1, 9, 0L, 3L), ColumnRanges.fromColumnStats(v1(1, 9, 0, 3), INT));
    assertEquals(ColumnRange.of(LONG, 1L, 9L, 0L, 3L), ColumnRanges.fromColumnStats(v1(1, 9, 0, 3), LONG));
    assertEquals(ColumnRange.of(STRING, "a", "b", 0L, 3L), ColumnRanges.fromColumnStats(v1("a", "b", 0, 3), STRING));
    HoodieSchema date = HoodieSchema.createDate();
    assertEquals(ColumnRange.of(date, 18000, 18001, 0L, 3L),
        ColumnRanges.fromColumnStats(v1(Date.valueOf(LocalDate.ofEpochDay(18000)), Date.valueOf(LocalDate.ofEpochDay(18001)), 0, 3), date));
    assertEquals(ColumnRange.of(date, 18000, 18001, 0L, 3L), ColumnRanges.fromColumnStats(v1(18000, 18001, 0, 3), date));
    HoodieSchema micros = HoodieSchema.createTimestampMicros();
    Timestamp ts = Timestamp.from(Instant.ofEpochSecond(1_600_000_000L, 5_000L));
    assertEquals(ColumnRange.of(micros, 1_600_000_000_000_005L, 1_600_000_000_000_005L, 0L, 1L),
        ColumnRanges.fromColumnStats(v1(ts, ts, 0, 1), micros));
    assertEquals(ColumnRange.of(micros, 7L, 8L, 0L, 1L), ColumnRanges.fromColumnStats(v1(7L, 8L, 0, 1), micros));
    // the unit of V1 timestamp-millis stats is not reliable
    HoodieSchema millis = HoodieSchema.createTimestampMillis();
    assertEquals(ColumnRange.of(millis, null, null, 0L, 1L), ColumnRanges.fromColumnStats(v1(7L, 8L, 0, 1), millis));
    HoodieSchema decimal = HoodieSchema.createDecimal(10, 2);
    ColumnRange decimals = ColumnRanges.fromColumnStats(v1(new BigDecimal("1.50"), new BigDecimal("2.25"), 0, 1), decimal);
    assertEquals(0, new BigDecimal("1.5").compareTo((BigDecimal) decimals.min().get()));
    assertEquals(0, new BigDecimal("2.25").compareTo((BigDecimal) decimals.max().get()));
    // a V1 int range for a string column
    assertEquals(ColumnRange.of(STRING, null, null, 0L, 3L), ColumnRanges.fromColumnStats(v1(1, 9, 0, 3), STRING));
  }
}
