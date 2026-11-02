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

package org.apache.hudi.common.schema.mapping;

import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.schema.mapping.testkit.FormatSchemaMapperContractTest;
import org.apache.hudi.common.schema.mapping.testkit.SchemaSamples;

import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

import java.util.HashMap;
import java.util.Map;

import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MICROS;
import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MILLIS;

/**
 * Contract test for {@link ParquetSchemaMapper} with today's defaults (2-level lists, no UUID annotation).
 *
 * <p>Expected support: RFC-88 section 1 (Parquet: VECTOR as FIXED_LEN_BYTE_ARRAY, BLOB as a plain group, UUID as
 * string while the UUID annotation is off by default). ENUM and UNION are LOSSY because Parquet has no type that keeps
 * enum symbols or union branches, and CHAR because Parquet has no CHAR (its values are kept as STRING); NULL is
 * UNSUPPORTED because Parquet has no null type. INT8 and INT16 are NATIVE: INT32 with the INTEGER(8 / 16, signed)
 * annotation. Expected column types: RFC-99's "Apache Parquet Type" column.
 */
class TestParquetSchemaMapper extends FormatSchemaMapperContractTest<MessageType> {

  @Override
  protected FormatSchemaMapper<MessageType> mapper() {
    return new ParquetSchemaMapper();
  }

  @Override
  protected Map<HoodieSchemaType, TypeSupport.Level> expectedSupport() {
    return nativeExcept(
        HoodieSchemaType.UUID, uuidLevel(),
        HoodieSchemaType.ENUM, TypeSupport.Level.LOSSY,
        HoodieSchemaType.UNION, TypeSupport.Level.LOSSY,
        HoodieSchemaType.CHAR, TypeSupport.Level.LOSSY,
        HoodieSchemaType.BLOB, TypeSupport.Level.LOSSY,
        HoodieSchemaType.VECTOR, TypeSupport.Level.LOSSY,
        HoodieSchemaType.NULL, TypeSupport.Level.UNSUPPORTED);
  }

  protected TypeSupport.Level uuidLevel() {
    return TypeSupport.Level.LOSSY;
  }

  @Override
  protected Map<String, MessageType> expectedColumnTypes() {
    Map<String, Type> types = new HashMap<>();
    types.put("boolean", Types.required(PrimitiveTypeName.BOOLEAN).named(SchemaSamples.COLUMN));
    types.put("int", Types.required(PrimitiveTypeName.INT32).named(SchemaSamples.COLUMN));
    // RFC-99: TINYINT / SMALLINT are INT32 + INTEGER(8 / 16, signed=true).
    types.put("int8", Types.required(PrimitiveTypeName.INT32).as(LogicalTypeAnnotation.intType(8, true)).named(SchemaSamples.COLUMN));
    types.put("int16", Types.required(PrimitiveTypeName.INT32).as(LogicalTypeAnnotation.intType(16, true)).named(SchemaSamples.COLUMN));
    types.put("long", Types.required(PrimitiveTypeName.INT64).named(SchemaSamples.COLUMN));
    types.put("float", Types.required(PrimitiveTypeName.FLOAT).named(SchemaSamples.COLUMN));
    types.put("double", Types.required(PrimitiveTypeName.DOUBLE).named(SchemaSamples.COLUMN));
    types.put("string", Types.required(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named(SchemaSamples.COLUMN));
    types.put("char_10", Types.required(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named(SchemaSamples.COLUMN));
    types.put("bytes", Types.required(PrimitiveTypeName.BINARY).named(SchemaSamples.COLUMN));
    // RFC-99: DECIMAL is FIXED_LEN_BYTE_ARRAY + DECIMAL; a bytes-backed decimal gets the minimal width for its precision.
    types.put("decimal_10_2_bytes", decimal(10, 2, 5));
    types.put("decimal_10_2_fixed_5", decimal(10, 2, 5));
    types.put("decimal_38_10_fixed_16", decimal(38, 10, 16));
    types.put("date", Types.required(PrimitiveTypeName.INT32).as(LogicalTypeAnnotation.dateType()).named(SchemaSamples.COLUMN));
    types.put("time_millis", Types.required(PrimitiveTypeName.INT32).as(LogicalTypeAnnotation.timeType(false, MILLIS)).named(SchemaSamples.COLUMN));
    types.put("time_micros", Types.required(PrimitiveTypeName.INT64).as(LogicalTypeAnnotation.timeType(false, MICROS)).named(SchemaSamples.COLUMN));
    types.put("timestamp_millis", timestamp(true, MILLIS));
    types.put("timestamp_micros", timestamp(true, MICROS));
    types.put("local_timestamp_millis", timestamp(false, MILLIS));
    types.put("local_timestamp_micros", timestamp(false, MICROS));
    // RFC-99: LIST is Group + LIST, in the standard 3-level layout.
    types.put("array_int", Types.requiredGroup().as(LogicalTypeAnnotation.listType())
        .addField(Types.repeatedGroup().addField(Types.required(PrimitiveTypeName.INT32).named("element")).named("list"))
        .named(SchemaSamples.COLUMN));
    // RFC-99: MAP is Group + MAP, in the standard key_value layout.
    types.put("map_long", Types.requiredGroup().as(LogicalTypeAnnotation.mapType())
        .addField(Types.repeatedGroup()
            .addField(Types.required(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("key"))
            .addField(Types.required(PrimitiveTypeName.INT64).named("value"))
            .named("key_value"))
        .named(SchemaSamples.COLUMN));
    types.put("struct", Types.requiredGroup()
        .required(PrimitiveTypeName.INT32).named("a")
        .optional(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("b")
        .named(SchemaSamples.COLUMN));
    // RFC-88 section 1: VECTOR as FIXED_LEN_BYTE_ARRAY(dimension * element size).
    types.put("vector_float_128", fixed(512));
    types.put("vector_double_16", fixed(128));
    types.put("vector_int8_32", fixed(32));
    Map<String, MessageType> expected = new HashMap<>();
    types.forEach((id, type) -> expected.put(id, new MessageType(SchemaSamples.RECORD_NAME, type)));
    return expected;
  }

  @Override
  protected MessageType columnType(MessageType record, String column) {
    return new MessageType(SchemaSamples.RECORD_NAME, record.getType(column));
  }

  @Override
  protected Map<String, String> knownGaps() {
    Map<String, String> gaps = new HashMap<>();
    gaps.put(SUPPORT_EXPECTED + "/VARIANT", "AvroSchemaConverterWithTimestampNTZ reads the variant group back as RECORD: its group visitor has no "
        + "VARIANT case, and parquet-java below 1.16 writes no VARIANT annotation to read, so only the hint restores VARIANT");
    for (String type : new String[] {"INT8", "INT16"}) {
      String bits = type.substring(3);
      // RFC-88 step 2c: reading INTEGER(8 / 16) as INT8 / INT16 without a hint changes the read of the INT(16) columns
      // Spark writes for ShortType today, so it waits for the compatibility work
      gaps.put(SUPPORT_EXPECTED + "/" + type, "AvroSchemaConverterWithTimestampNTZ writes " + type + " as INT32 + INTEGER(" + bits
          + ", signed) but reads INTEGER(" + bits + ") back as INT without a hint (the Spark row writer writes INT(16) for ShortType today)");
    }
    gaps.put(ROUND_TRIP + "/null", "AvroSchemaConverterWithTimestampNTZ.convertFields silently drops NULL fields instead of failing fast, "
        + "so the column disappears from the MessageType");
    gaps.put(EXPECTED_OUTPUT + "/time_millis", "AvroSchemaConverterWithTimestampNTZ writes TIME with isAdjustedToUTC=true; RFC-99 asks for "
        + "TIME(isAdjustedToUTC=false, MILLIS)");
    gaps.put(EXPECTED_OUTPUT + "/time_micros", "AvroSchemaConverterWithTimestampNTZ writes TIME with isAdjustedToUTC=true; RFC-99 asks for "
        + "TIME(isAdjustedToUTC=false)");
    gaps.put(EXPECTED_OUTPUT + "/decimal_10_2_bytes", "A bytes-backed DECIMAL is written as BINARY + DECIMAL; RFC-99 asks for "
        + "FIXED_LEN_BYTE_ARRAY + DECIMAL, which the Spark and Flink row writers use (minimal width for the precision)");
    gaps.put(EXPECTED_OUTPUT + "/map_long", "ConversionPatterns.stringKeyMapType annotates the repeated key_value group with the legacy "
        + "MAP_KEY_VALUE annotation");
    if (options().getBoolean(ParquetSchemaMapper.WRITE_OLD_LIST_STRUCTURE, ParquetSchemaMapper.WRITE_OLD_LIST_STRUCTURE_DEFAULT)) {
      gaps.put(EXPECTED_OUTPUT + "/array_int", "parquet.avro.write-old-list-structure defaults to true, so the Avro writer path writes "
          + "2-level lists (repeated int32 array) instead of the 3-level list / element layout");
    }
    return gaps;
  }

  private static Type decimal(int precision, int scale, int bytes) {
    return Types.required(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY).length(bytes)
        .as(LogicalTypeAnnotation.decimalType(scale, precision)).named(SchemaSamples.COLUMN);
  }

  private static Type timestamp(boolean utcAdjusted, LogicalTypeAnnotation.TimeUnit unit) {
    return Types.required(PrimitiveTypeName.INT64).as(LogicalTypeAnnotation.timestampType(utcAdjusted, unit)).named(SchemaSamples.COLUMN);
  }

  private static Type fixed(int bytes) {
    return Types.required(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY).length(bytes).named(SchemaSamples.COLUMN);
  }
}
