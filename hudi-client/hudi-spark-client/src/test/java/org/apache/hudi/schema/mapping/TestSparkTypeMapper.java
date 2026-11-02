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

import org.apache.hudi.HoodieSchemaConversionUtils;
import org.apache.hudi.HoodieSparkUtils;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.schema.mapping.TypeSupport;
import org.apache.hudi.common.schema.mapping.TypeSystemMapper;
import org.apache.hudi.common.schema.mapping.testkit.TypeSystemMapperContractTest;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.util.Option;

import org.apache.spark.sql.catalyst.util.CharVarcharUtils;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.CharType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.VarcharType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Runs the RFC-88 mapper contract against {@link SparkTypeMapper}. Expected support and column types follow RFC-99's
 * interoperability table ("Apache Spark Type") and RFC-88 section 1, arrow (4): TIMESTAMP(3) read back as micros,
 * TIME as long, UUID and ENUM as string, FIXED as binary, VECTOR as array and BLOB as struct are LOSSY. INT8, INT16
 * and CHAR(n) are NATIVE: ByteType, ShortType (RFC-99 TINYINT / SMALLINT) and CharType(n), which Spark keeps in a
 * StructType as StringType with the raw type in the field metadata.
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
    types.put("int8", DataTypes.ByteType);
    types.put("int16", DataTypes.ShortType);
    types.put("char_10", new CharType(10));
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

  /**
   * The column's type as Spark sees it: a CHAR(n) column is StringType with its raw type in the field metadata.
   */
  @Override
  protected DataType columnType(DataType record, String column) {
    StructField field = ((StructType) record).apply(column);
    scala.Option<DataType> raw = CharVarcharUtils.getRawType(field.metadata());
    return raw.isDefined() ? raw.get() : field.dataType();
  }

  /**
   * Spark keeps per-column markers in StructField metadata.
   */
  @Override
  protected boolean targetHasMetadata() {
    return true;
  }

  @Override
  protected Map<String, String> knownGaps() {
    Map<String, String> gaps = new HashMap<>();
    String noMarker = "HoodieSparkSchemaConverters writes no hudi_type marker for %s, so without a hint it reads back as %s";
    gaps.put("marker_declared/ENUM", String.format(noMarker, "ENUM", "STRING"));
    gaps.put("marker_declared/FIXED", String.format(noMarker, "FIXED", "BYTES"));
    gaps.put("marker_declared/UUID", String.format(noMarker, "UUID", "STRING"));
    gaps.put("marker_declared/TIME", String.format(noMarker, "TIME", "INT / LONG"));
    gaps.put("marker_declared/TIMESTAMP", String.format(noMarker, "TIMESTAMP(millis)", "TIMESTAMP(micros)"));
    gaps.put("expected_output/time_millis",
        "HoodieSparkSchemaConverters.toSqlType maps TIME(millis) to IntegerType (millis), RFC-99 asks LongType (micros)");
    if (!SPARK_4) {
      gaps.put("support_expected/VARIANT",
          "HoodieSparkSchemaConverters.toSqlType fails for VARIANT on Spark 3 instead of writing struct<metadata, value> with the"
              + " hudi_type=VARIANT marker that toHoodieType already reads");
    }
    return gaps;
  }

  /**
   * INT8, INT16 and CHAR(n) map to ByteType, ShortType and Spark's CHAR(n) (StringType with the raw type in the field
   * metadata), and record fields carry the hudi_type marker that restores them. A CHAR outside a record has no field
   * for the metadata and maps to StringType.
   */
  @Test
  void narrowIntsAndCharMapToSparkTypesWithMarkers() {
    assertEquals(DataTypes.ByteType, HoodieSchemaConversionUtils.convertHoodieSchemaToDataType(HoodieSchema.createInt8()));
    assertEquals(DataTypes.ShortType, HoodieSchemaConversionUtils.convertHoodieSchemaToDataType(HoodieSchema.createInt16()));
    assertEquals(DataTypes.StringType, HoodieSchemaConversionUtils.convertHoodieSchemaToDataType(HoodieSchema.createChar(10)));
    assertEquals(DataTypes.LongType, HoodieSchemaConversionUtils.convertHoodieSchemaToDataType(
        HoodieSchema.createUnion(HoodieSchema.createInt16(), HoodieSchema.create(HoodieSchemaType.LONG))));

    HoodieSchema nested = HoodieSchema.createRecord("nested", "ns", null, Collections.singletonList(
        HoodieSchemaField.of("code", HoodieSchema.createChar(2))));
    HoodieSchema record = HoodieSchema.createRecord("r", "ns", null, Arrays.asList(
        HoodieSchemaField.of("b", HoodieSchema.createNullable(HoodieSchema.createInt8())),
        HoodieSchemaField.of("s", HoodieSchema.createInt16()),
        HoodieSchemaField.of("c", HoodieSchema.createChar(10)),
        HoodieSchemaField.of("cs", HoodieSchema.createArray(HoodieSchema.createChar(3))),
        HoodieSchemaField.of("ss", HoodieSchema.createMap(HoodieSchema.createInt16())),
        HoodieSchemaField.of("n", nested),
        HoodieSchemaField.of("i", HoodieSchema.create(HoodieSchemaType.INT))));
    StructType struct = HoodieSchemaConversionUtils.convertHoodieSchemaToStructType(record);
    assertEquals(DataTypes.ByteType, struct.apply("b").dataType());
    assertEquals("INT8", struct.apply("b").metadata().getString(HoodieSchema.TYPE_METADATA_FIELD));
    assertEquals(DataTypes.ShortType, struct.apply("s").dataType());
    assertEquals("INT16", struct.apply("s").metadata().getString(HoodieSchema.TYPE_METADATA_FIELD));
    assertEquals(DataTypes.StringType, struct.apply("c").dataType());
    assertEquals("CHAR(10)", struct.apply("c").metadata().getString(HoodieSchema.TYPE_METADATA_FIELD));
    assertEquals(new CharType(10), CharVarcharUtils.getRawType(struct.apply("c").metadata()).get());
    assertEquals(new ArrayType(DataTypes.StringType, false), struct.apply("cs").dataType());
    assertEquals("CHAR(3)", struct.apply("cs").metadata().getString(HoodieSchema.TYPE_METADATA_FIELD));
    assertEquals("array<char(3)>", CharVarcharUtils.getRawTypeString(struct.apply("cs").metadata()).get());
    assertEquals("INT16", struct.apply("ss").metadata().getString(HoodieSchema.TYPE_METADATA_FIELD));
    // Spark pads a CHAR inside a struct from the raw type of the top-level field, as CharVarcharUtils writes it
    assertEquals("struct<code:char(2)>", CharVarcharUtils.getRawTypeString(struct.apply("n").metadata()).get());
    assertFalse(struct.apply("n").metadata().contains(HoodieSchema.TYPE_METADATA_FIELD));
    assertEquals(Metadata.empty(), struct.apply("i").metadata());

    HoodieSchema converted = new SparkTypeMapper().fromTypeSystem(struct, Option.empty());
    assertEquals(HoodieSchemaType.INT8, converted.getField("b").get().schema().getNonNullType().getType());
    assertEquals(HoodieSchemaType.INT16, converted.getField("s").get().schema().getType());
    assertEquals(HoodieSchema.createChar(10), converted.getField("c").get().schema());
    assertEquals(HoodieSchema.createChar(3), converted.getField("cs").get().schema().getElementType());
    assertEquals(HoodieSchemaType.INT16, converted.getField("ss").get().schema().getValueType().getType());
    assertEquals(HoodieSchema.createChar(2), converted.getField("n").get().schema().getField("code").get().schema());
  }

  /**
   * A Spark schema without markers: ByteType, ShortType, CharType(n) and Spark's in-schema form of CHAR(n) (StringType
   * with the raw type in the metadata of the top-level field, nested CHARs included).
   */
  private static StructType unmarkedSparkSchema() {
    StructType withChars = CharVarcharUtils.replaceCharVarcharWithStringInSchema(new StructType(new StructField[] {
        field("c2", new CharType(3), false),
        field("cs", new ArrayType(new CharType(4), true), false),
        field("cm", new MapType(DataTypes.StringType, new CharType(5), true), false),
        field("n", new StructType(new StructField[] {
            field("code", new CharType(2), true), field("tiny", DataTypes.ByteType, true)}), true),
        field("v", new VarcharType(8), true),
        field("v2", DataTypes.StringType, true)}));
    StructField[] fields = new StructField[] {
        field("b", DataTypes.ByteType, false), field("s", DataTypes.ShortType, false), field("c", new CharType(3), false),
        withChars.apply("c2"), withChars.apply("cs"), withChars.apply("cm"), withChars.apply("n"), withChars.apply("v"),
        field("v3", new VarcharType(8), true),
        new StructField("i", DataTypes.IntegerType, false,
            new MetadataBuilder().putString(HoodieSchema.TYPE_METADATA_FIELD, "INT16").build())};
    return new StructType(fields);
  }

  /**
   * The mapper maps Spark's ByteType, ShortType and CHAR(n) natively, without the marker (RFC-88 rollout step 2c).
   * VARCHAR(n) maps to STRING (its length is a per-field detail, rule 5). A marker on a field of another Spark type is
   * ignored.
   */
  @Test
  void sparkTypesWithoutMarkerMapNatively() {
    HoodieSchema converted = new SparkTypeMapper().fromTypeSystem(unmarkedSparkSchema(), Option.empty());
    assertEquals(HoodieSchema.createInt8(), converted.getField("b").get().schema());
    assertEquals(HoodieSchema.createInt16(), converted.getField("s").get().schema());
    assertEquals(HoodieSchema.createChar(3), converted.getField("c").get().schema());
    assertEquals(HoodieSchema.createChar(3), converted.getField("c2").get().schema());
    assertEquals(HoodieSchema.createChar(4), converted.getField("cs").get().schema().getElementType().getNonNullType());
    assertEquals(HoodieSchema.createChar(5), converted.getField("cm").get().schema().getValueType().getNonNullType());
    HoodieSchema nested = converted.getField("n").get().schema().getNonNullType();
    assertEquals(HoodieSchema.createChar(2), nested.getField("code").get().schema().getNonNullType());
    assertEquals(HoodieSchema.createInt8(), nested.getField("tiny").get().schema().getNonNullType());
    assertEquals(HoodieSchema.create(HoodieSchemaType.STRING), converted.getField("v").get().schema().getNonNullType());
    assertEquals(HoodieSchema.create(HoodieSchemaType.STRING), converted.getField("v3").get().schema().getNonNullType());
    assertEquals(HoodieSchemaType.INT, converted.getField("i").get().schema().getType());
  }

  /**
   * Today's converter path, which tables created before {@code hoodie.table.type.system.version} keep: without the
   * marker, ByteType, ShortType and CHAR(n) map to INT and STRING, exactly as before INT8, INT16 and CHAR existed.
   */
  @Test
  void todaysConverterPathKeepsIntAndString() {
    HoodieSchema converted = HoodieSchemaConversionUtils.convertStructTypeToHoodieSchema(unmarkedSparkSchema(), "r", "ns");
    assertEquals(converted, HoodieSchemaConversionUtils.convertStructTypeToHoodieSchema(unmarkedSparkSchema(), "r", "ns", false, false));
    assertEquals(HoodieSchemaType.INT, converted.getField("b").get().schema().getType());
    assertEquals(HoodieSchemaType.INT, converted.getField("s").get().schema().getType());
    assertEquals(HoodieSchemaType.STRING, converted.getField("c").get().schema().getType());
    assertEquals(HoodieSchemaType.STRING, converted.getField("c2").get().schema().getType());
    assertEquals(HoodieSchemaType.STRING, converted.getField("cs").get().schema().getElementType().getNonNullType().getType());
    assertEquals(HoodieSchemaType.STRING, converted.getField("cm").get().schema().getValueType().getNonNullType().getType());
    HoodieSchema nested = converted.getField("n").get().schema().getNonNullType();
    assertEquals(HoodieSchemaType.STRING, nested.getField("code").get().schema().getNonNullType().getType());
    assertEquals(HoodieSchemaType.INT, nested.getField("tiny").get().schema().getNonNullType().getType());
    assertEquals(HoodieSchemaType.STRING, converted.getField("v").get().schema().getNonNullType().getType());
    assertEquals(HoodieSchemaType.INT, converted.getField("i").get().schema().getType());
    // the markers written for INT8, INT16 and CHAR(n) still restore them
    HoodieSchema record = HoodieSchema.createRecord("r", "ns", null, Arrays.asList(
        HoodieSchemaField.of("b", HoodieSchema.createInt8()), HoodieSchemaField.of("c", HoodieSchema.createChar(2))));
    assertEquals(record, HoodieSchemaConversionUtils.convertStructTypeToHoodieSchema(
        HoodieSchemaConversionUtils.convertHoodieSchemaToStructType(record), "r", "ns"));
  }

  /**
   * Which mapping callers use for a table: the new one for tables with type system version 1, and for tables about to
   * be created at a table version that keeps the property; today's for the rest.
   */
  @Test
  void callersFollowTheTableTypeSystemVersion() {
    assertFalse(HoodieSchemaConversionUtils.usesNarrowIntAndCharTypes(null));
    assertTrue(HoodieSchemaConversionUtils.usesNarrowIntAndCharTypesForNewTable(HoodieTableVersion.current()));
    assertTrue(HoodieSchemaConversionUtils.usesNarrowIntAndCharTypesForNewTable(HoodieTableVersion.TEN));
    assertFalse(HoodieSchemaConversionUtils.usesNarrowIntAndCharTypesForNewTable(HoodieTableVersion.NINE));
    assertFalse(HoodieSchemaConversionUtils.usesNarrowIntAndCharTypesForNewTable(HoodieTableVersion.SIX));
    HoodieTableConfig existing = new HoodieTableConfig();
    assertFalse(HoodieSchemaConversionUtils.usesNarrowIntAndCharTypes(existing));
    existing.setValue(HoodieTableConfig.TYPE_SYSTEM_VERSION, "1");
    assertTrue(HoodieSchemaConversionUtils.usesNarrowIntAndCharTypes(existing));
  }

  private static StructField field(String name, DataType type, boolean nullable) {
    return new StructField(name, type, nullable, Metadata.empty());
  }
}
