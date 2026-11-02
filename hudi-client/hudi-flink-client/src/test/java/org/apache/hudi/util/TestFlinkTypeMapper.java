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

package org.apache.hudi.util;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.schema.mapping.TypeSupport;
import org.apache.hudi.common.schema.mapping.TypeSystemMapper;
import org.apache.hudi.common.schema.mapping.testkit.TypeSystemMapperContractTest;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.schema.HoodieSchemaType.BLOB;
import static org.apache.hudi.common.schema.HoodieSchemaType.ENUM;
import static org.apache.hudi.common.schema.HoodieSchemaType.FIXED;
import static org.apache.hudi.common.schema.HoodieSchemaType.UNION;
import static org.apache.hudi.common.schema.HoodieSchemaType.UUID;
import static org.apache.hudi.common.schema.HoodieSchemaType.VARIANT;
import static org.apache.hudi.common.schema.HoodieSchemaType.VECTOR;
import static org.apache.hudi.common.schema.mapping.TypeSupport.Level.LOSSY;
import static org.apache.hudi.common.schema.mapping.TypeSupport.Level.UNSUPPORTED;

/**
 * Shared mapper contract for {@link FlinkTypeMapper}.
 *
 * <p>Expected support follows RFC-88 section 1, arrow (4): Flink loses the same identity as Spark (UUID and ENUM as
 * string, FIXED as binary, VECTOR as array, BLOB as a row), except that timestamps and TIME keep their precision.
 * Flink has no field metadata, so every LOSSY type comes back from the hint only. Expected column types are RFC-99's
 * "Apache Flink Type" column.
 */
class TestFlinkTypeMapper extends TypeSystemMapperContractTest<DataType> {

  @Override
  protected TypeSystemMapper<DataType> mapper() {
    return new FlinkTypeMapper();
  }

  @Override
  protected Map<HoodieSchemaType, TypeSupport.Level> expectedSupport() {
    // RFC-99 has no union row; Flink has no union type, so the spec cannot ask for more than failing fast.
    return nativeExcept(ENUM, LOSSY, FIXED, LOSSY, UUID, LOSSY, VECTOR, LOSSY, BLOB, LOSSY, UNION, UNSUPPORTED);
  }

  @Override
  protected Map<String, DataType> expectedColumnTypes() {
    Map<String, DataType> types = new HashMap<>();
    types.put("boolean", DataTypes.BOOLEAN().notNull());
    types.put("int", DataTypes.INT().notNull());
    types.put("long", DataTypes.BIGINT().notNull());
    types.put("float", DataTypes.FLOAT().notNull());
    types.put("double", DataTypes.DOUBLE().notNull());
    types.put("string", DataTypes.STRING().notNull());
    types.put("bytes", DataTypes.BYTES().notNull());
    types.put("decimal_10_2_bytes", DataTypes.DECIMAL(10, 2).notNull());
    types.put("decimal_10_2_fixed_5", DataTypes.DECIMAL(10, 2).notNull());
    types.put("decimal_38_10_fixed_16", DataTypes.DECIMAL(38, 10).notNull());
    types.put("date", DataTypes.DATE().notNull());
    types.put("time_millis", DataTypes.TIME(3).notNull());
    // RFC-99: TIMESTAMP(us) (no time zone, Hudi local-timestamp-micros) is TIMESTAMP(6);
    // TIMESTAMPTZ(us) (UTC-adjusted, Hudi timestamp-micros) is TIMESTAMP(6) WITH LOCAL TIME ZONE.
    types.put("local_timestamp_micros", DataTypes.TIMESTAMP(6).notNull());
    types.put("timestamp_micros", DataTypes.TIMESTAMP_LTZ(6).notNull());
    types.put("array_int", DataTypes.ARRAY(DataTypes.INT().notNull()).notNull());
    types.put("map_long", DataTypes.MAP(DataTypes.STRING().notNull(), DataTypes.BIGINT().notNull()).notNull());
    types.put("struct", DataTypes.ROW(
        DataTypes.FIELD("a", DataTypes.INT().notNull()),
        DataTypes.FIELD("b", DataTypes.STRING())).notNull());
    types.put("vector_float_128", DataTypes.ARRAY(DataTypes.FLOAT().notNull()).notNull());
    types.put("blob", DataTypes.ROW(
        DataTypes.FIELD(HoodieSchema.Blob.TYPE, DataTypes.STRING().notNull()),
        DataTypes.FIELD(HoodieSchema.Blob.INLINE_DATA_FIELD, DataTypes.BYTES()),
        DataTypes.FIELD(HoodieSchema.Blob.EXTERNAL_REFERENCE, DataTypes.ROW(
            DataTypes.FIELD(HoodieSchema.Blob.EXTERNAL_REFERENCE_PATH, DataTypes.STRING().notNull()),
            DataTypes.FIELD(HoodieSchema.Blob.EXTERNAL_REFERENCE_OFFSET, DataTypes.BIGINT()),
            DataTypes.FIELD(HoodieSchema.Blob.EXTERNAL_REFERENCE_LENGTH, DataTypes.BIGINT()),
            DataTypes.FIELD(HoodieSchema.Blob.EXTERNAL_REFERENCE_IS_MANAGED, DataTypes.BOOLEAN().notNull())))).notNull());
    return types;
  }

  @Override
  protected DataType columnType(DataType record, String column) {
    List<String> names = DataType.getFieldNames(record);
    return DataType.getFieldDataTypes(record).get(names.indexOf(column));
  }

  @Override
  protected Map<String, String> knownGaps() {
    Map<String, String> gaps = new HashMap<>();
    gaps.put("expected_output/timestamp_micros",
        "HoodieSchemaConverter.convertTimestamp maps UTC-adjusted timestamp-micros to TIMESTAMP(6), not TIMESTAMP_LTZ(6) (time zone semantics swapped)");
    gaps.put("expected_output/local_timestamp_micros",
        "HoodieSchemaConverter.convertTimestamp maps local-timestamp-micros to TIMESTAMP_LTZ(6), not TIMESTAMP(6) (time zone semantics swapped)");
    gaps.put("round_trip/union_int_string.*",
        "HoodieSchemaConverter.convertUnion writes a multi-branch union as RAW(Object) instead of failing fast; convertToSchema then throws on RAW");
    if (mapper().support(VARIANT).isUnsupported()) {
      gaps.put("support_expected/VARIANT", "Flink before 2.1 has no VARIANT type (DataTypeAdapter.createVariantType throws)");
    }
    return gaps;
  }
}
