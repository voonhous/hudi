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

package org.apache.hudi.schema.mapping;

import org.apache.hudi.HoodieSparkUtils;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.schema.mapping.TypeSupport;
import org.apache.hudi.common.schema.mapping.TypeSystemMapper;
import org.apache.hudi.common.schema.mapping.testkit.TypeSystemMapperContractTest;

import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.HashMap;
import java.util.Map;

import static org.apache.hudi.common.schema.HoodieSchemaType.BLOB;
import static org.apache.hudi.common.schema.HoodieSchemaType.ENUM;
import static org.apache.hudi.common.schema.HoodieSchemaType.FIXED;
import static org.apache.hudi.common.schema.HoodieSchemaType.TIME;
import static org.apache.hudi.common.schema.HoodieSchemaType.TIMESTAMP;
import static org.apache.hudi.common.schema.HoodieSchemaType.UNION;
import static org.apache.hudi.common.schema.HoodieSchemaType.UUID;
import static org.apache.hudi.common.schema.HoodieSchemaType.VARIANT;
import static org.apache.hudi.common.schema.HoodieSchemaType.VECTOR;
import static org.apache.hudi.common.schema.mapping.TypeSupport.Level.LOSSY;
import static org.apache.hudi.common.schema.mapping.TypeSupport.Level.NATIVE;

/**
 * Runs the RFC-88 mapper contract against {@link SparkTypeMapper}. Expected support and column types follow RFC-99's
 * interoperability table ("Apache Spark Type") and RFC-88 section 1, arrow (4): TIMESTAMP(3) read back as micros,
 * TIME as long, UUID and ENUM as string, FIXED as binary, VECTOR as array and BLOB as struct are LOSSY.
 */
class TestSparkTypeMapper extends TypeSystemMapperContractTest<DataType> {

  private static final boolean SPARK_4 = HoodieSparkUtils.gteqSpark4_0();

  @Override
  protected TypeSystemMapper<DataType> mapper() {
    return new SparkTypeMapper();
  }

  @Override
  protected Map<HoodieSchemaType, TypeSupport.Level> expectedSupport() {
    return nativeExcept(
        ENUM, LOSSY,
        FIXED, LOSSY,
        UUID, LOSSY,
        TIME, LOSSY,
        TIMESTAMP, LOSSY,
        // Spark has no union type; the values are kept in a struct of member fields.
        UNION, LOSSY,
        VECTOR, LOSSY,
        BLOB, LOSSY,
        // RFC-99 maps VARIANT to VariantType (Spark 4). Spark 3 has none, but keeps the values in
        // struct<metadata binary, value binary> with a hudi_type marker, which toHoodieType already reads.
        VARIANT, SPARK_4 ? NATIVE : LOSSY);
  }

  @Override
  protected Map<String, DataType> expectedColumnTypes() {
    Map<String, DataType> types = new HashMap<>();
    types.put("boolean", DataTypes.BooleanType);
    types.put("int", DataTypes.IntegerType);
    types.put("long", DataTypes.LongType);
    types.put("float", DataTypes.FloatType);
    types.put("double", DataTypes.DoubleType);
    types.put("string", DataTypes.StringType);
    types.put("bytes", DataTypes.BinaryType);
    types.put("fixed_16", DataTypes.BinaryType);
    types.put("enum", DataTypes.StringType);
    types.put("uuid", DataTypes.StringType);
    types.put("decimal_10_2_bytes", new DecimalType(10, 2));
    types.put("decimal_10_2_fixed_5", new DecimalType(10, 2));
    types.put("decimal_38_10_fixed_16", new DecimalType(38, 10));
    types.put("date", DataTypes.DateType);
    // RFC-99: TIME(ms) as LongType (micros); RFC-88: TIME read back as long.
    types.put("time_millis", DataTypes.LongType);
    types.put("time_micros", DataTypes.LongType);
    types.put("timestamp_millis", DataTypes.TimestampType);
    types.put("timestamp_micros", DataTypes.TimestampType);
    types.put("local_timestamp_millis", DataTypes.TimestampNTZType);
    types.put("local_timestamp_micros", DataTypes.TimestampNTZType);
    types.put("array_int", new ArrayType(DataTypes.IntegerType, false));
    types.put("map_long", new MapType(DataTypes.StringType, DataTypes.LongType, false));
    types.put("struct", new StructType(new StructField[] {
        field("a", DataTypes.IntegerType, false),
        field("b", DataTypes.StringType, true)}));
    types.put("vector_float_128", new ArrayType(DataTypes.FloatType, false));
    types.put("vector_double_16", new ArrayType(DataTypes.DoubleType, false));
    types.put("vector_int8_32", new ArrayType(DataTypes.ByteType, false));
    // RFC-99: StructType (w/ BLOB metadata); the metadata is on the enclosing field, the struct is nullable everywhere.
    types.put("blob", new StructType(new StructField[] {
        field(HoodieSchema.Blob.TYPE, DataTypes.StringType, true),
        field(HoodieSchema.Blob.INLINE_DATA_FIELD, DataTypes.BinaryType, true),
        field(HoodieSchema.Blob.EXTERNAL_REFERENCE, new StructType(new StructField[] {
            field(HoodieSchema.Blob.EXTERNAL_REFERENCE_PATH, DataTypes.StringType, true),
            field(HoodieSchema.Blob.EXTERNAL_REFERENCE_OFFSET, DataTypes.LongType, true),
            field(HoodieSchema.Blob.EXTERNAL_REFERENCE_LENGTH, DataTypes.LongType, true),
            field(HoodieSchema.Blob.EXTERNAL_REFERENCE_IS_MANAGED, DataTypes.BooleanType, true)}), true)}));
    if (SPARK_4) {
      // VariantType does not exist in Spark 3, so it is looked up by name.
      types.put("variant", DataType.fromDDL("variant"));
    }
    return types;
  }

  @Override
  protected DataType columnType(DataType record, String column) {
    return ((StructType) record).apply(column).dataType();
  }

  @Override
  protected Map<String, String> knownGaps() {
    Map<String, String> gaps = new HashMap<>();
    gaps.put("expected_output/time_millis",
        "HoodieSparkSchemaConverters.toSqlType maps TIME(millis) to IntegerType (millis), RFC-99 asks LongType (micros)");
    if (!SPARK_4) {
      gaps.put("support_expected/VARIANT",
          "HoodieSparkSchemaConverters.toSqlType fails for VARIANT on Spark 3 instead of writing struct<metadata, value> with the"
              + " hudi_type=VARIANT marker that toHoodieType already reads");
    }
    return gaps;
  }

  private static StructField field(String name, DataType type, boolean nullable) {
    return new StructField(name, type, nullable, Metadata.empty());
  }
}
