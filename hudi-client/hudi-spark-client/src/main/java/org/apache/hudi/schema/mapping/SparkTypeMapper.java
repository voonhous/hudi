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
import org.apache.hudi.common.schema.mapping.HoodieSchemaEquivalence;
import org.apache.hudi.common.schema.mapping.HoodieSchemaHints;
import org.apache.hudi.common.schema.mapping.MappingOptions;
import org.apache.hudi.common.schema.mapping.TypeSupport;
import org.apache.hudi.common.schema.mapping.TypeSystemMapper;
import org.apache.hudi.common.schema.mapping.TypeSystems;
import org.apache.hudi.common.util.Option;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;

import java.util.Collections;

/**
 * Maps between {@link HoodieSchema} and Spark {@link DataType} (RFC-88, type plane). Records map to
 * {@link StructType}.
 *
 * <p>This step only wraps today's converter, {@code org.apache.spark.sql.avro.HoodieSparkSchemaConverters}, through
 * the entry points callers use ({@link HoodieSchemaConversionUtils#convertHoodieSchemaToDataType} and
 * {@link HoodieSchemaConversionUtils#convertStructTypeToHoodieSchema(DataType, String, String, boolean, boolean)}), and adds
 * the hint: {@link #fromTypeSystem} restores the types Spark drops from the table schema with
 * {@link HoodieSchemaHints#restore}. Once callers switch over (RFC-88 rollout step 2), this class replaces
 * {@code HoodieSparkSchemaConverters} and {@code SparkFilterHelper.convertDataType}, and
 * {@code SparkInternalSchemaConverter} once RFC-104 retires {@code InternalSchema}.
 *
 * <p>Record name and namespace on the way back come from the hint's top-level record. Without a hint they are
 * {@value #DEFAULT_RECORD_NAME} and an empty namespace, the defaults of {@code HoodieSparkSchemaConverters.toHoodieType}.
 *
 * <p>Spark keeps the {@value HoodieSchema#TYPE_METADATA_FIELD} marker for VECTOR and BLOB in the metadata of the
 * enclosing {@code StructField}, so the marker only survives when the type is a field of a record.
 *
 * <p>INT8, INT16 and CHAR(n) map to Spark's own types: {@code ByteType}, {@code ShortType} and CHAR(n), which inside a
 * {@code StructType} is {@code StringType} with the raw type {@code char(n)} in the field metadata (Spark's
 * {@code CharVarcharUtils} form). {@link #fromTypeSystem} maps them back natively, with or without the
 * {@value HoodieSchema#TYPE_METADATA_FIELD} marker ({@code INT8}, {@code INT16}, {@code CHAR(n)}) the converter writes
 * on a record field of these types, or an array or map of one. VARCHAR(n) maps to STRING (RFC-88, rule 5).
 *
 * <p>The mapper is stateless and always uses this mapping, the one of tables created with
 * {@code hoodie.table.type.system.version} 1. Which mapping a table uses is its callers' business: paths that write to a
 * table created before that property existed keep today's converter path
 * ({@link HoodieSchemaConversionUtils#convertStructTypeToHoodieSchema(DataType, String, String, boolean, boolean)} with
 * {@code false}), where ByteType and ShortType give INT and CHAR gives STRING.
 *
 * <p>There are no options yet: {@link MappingOptions} are accepted and ignored, which is today's behaviour.
 */
public class SparkTypeMapper implements TypeSystemMapper<DataType> {

  private static final long serialVersionUID = 1L;

  /**
   * Record name used by {@link #fromTypeSystem} when there is no hint.
   */
  public static final String DEFAULT_RECORD_NAME = "topLevelRecord";
  /**
   * Record namespace used by {@link #fromTypeSystem} when there is no hint.
   */
  public static final String DEFAULT_RECORD_NAMESPACE = "";

  private static final String IMAGE_COLUMN = "col";

  @Override
  public String typeSystem() {
    return TypeSystems.SPARK;
  }

  @Override
  public DataType toTypeSystem(HoodieSchema schema, MappingOptions opts) {
    return HoodieSchemaConversionUtils.convertHoodieSchemaToDataType(schema);
  }

  @Override
  public HoodieSchema fromTypeSystem(DataType type, Option<HoodieSchema> hint) {
    String name = DEFAULT_RECORD_NAME;
    String namespace = DEFAULT_RECORD_NAMESPACE;
    if (hint.isPresent()) {
      HoodieSchema h = hint.get().isNullable() && !hint.get().isComplexUnion() ? hint.get().getNonNullType() : hint.get();
      if (h.getType() == HoodieSchemaType.RECORD) {
        name = h.getName();
        namespace = h.getNamespace().orElse(DEFAULT_RECORD_NAMESPACE);
      }
    }
    HoodieSchema converted = HoodieSchemaConversionUtils.convertStructTypeToHoodieSchema(type, name, namespace, false, true);
    return HoodieSchemaHints.restore(converted, hint, t -> support(t).isLossy(), this::image, HoodieSchemaEquivalence.Mode.LOGICAL);
  }

  /**
   * What a column type becomes through Spark without a hint. The column is wrapped in a one-field record because
   * the {@value HoodieSchema#TYPE_METADATA_FIELD} marker lives on the enclosing {@code StructField}.
   */
  private HoodieSchema image(HoodieSchema column) {
    HoodieSchema record = HoodieSchema.createRecord(DEFAULT_RECORD_NAME, null, null,
        Collections.singletonList(HoodieSchemaField.of(IMAGE_COLUMN, column)));
    return fromTypeSystem(toTypeSystem(record), Option.empty()).getField(IMAGE_COLUMN).get().schema();
  }

  @Override
  public TypeSupport support(HoodieSchemaType type, MappingOptions opts) {
    switch (type) {
      case ENUM:
        return TypeSupport.lossy("ENUM is mapped to StringType without its symbols and no marker; it reads back as STRING");
      case FIXED:
        return TypeSupport.lossy("FIXED is mapped to BinaryType without its size and no marker; it reads back as BYTES");
      case UUID:
        return TypeSupport.lossy("UUID is mapped to StringType with no marker; it reads back as STRING");
      case TIME:
        return TypeSupport.lossy("TIME(millis) is mapped to IntegerType and TIME(micros) to LongType with no marker; they read back as INT and LONG");
      case TIMESTAMP:
        return TypeSupport.lossy("TIMESTAMP(millis) and LOCAL TIMESTAMP(millis) are mapped to TimestampType and TimestampNTZType, which are micros,"
            + " with no marker; they read back as micros. Micros timestamps map exactly");
      case UNION:
        return TypeSupport.lossy("Spark has no union type: a union of several types is mapped to a struct of nullable member0..memberN fields,"
            + " read back as a union by those field names; [int, long] and [float, double] are widened to LongType and DoubleType and read back"
            + " as LONG and DOUBLE", "struct field names member0..memberN");
      case VECTOR:
        return TypeSupport.lossy("VECTOR is mapped to ArrayType of its element type (containsNull = false); the hudi_type StructField metadata"
            + " restores dimension and element type, so it survives only as a record field", HoodieSchema.TYPE_METADATA_FIELD);
      case BLOB:
        return TypeSupport.lossy("BLOB is mapped to its canonical StructType with every field nullable; the hudi_type StructField metadata"
            + " restores it, so it survives only as a record field (or as the element or value of one)", HoodieSchema.TYPE_METADATA_FIELD);
      case VARIANT:
        return HoodieSparkUtils.gteqSpark4_0()
            ? TypeSupport.nativeType()
            : TypeSupport.unsupported("Spark 3.x has no VariantType; HoodieSparkSchemaConverters.toSqlType fails with"
                + " 'VARIANT type is only supported in Spark 4.0+' and writes no struct<metadata, value> with a hudi_type marker instead");
      default:
        // RECORD, ARRAY, MAP, STRING, BYTES, INT, LONG, FLOAT, DOUBLE, BOOLEAN, DATE, NULL, DECIMAL (DecimalType keeps
        // precision and scale; the bytes or fixed backing is physical and restored from the hint), and INT8, INT16 and
        // CHAR(n) (ByteType, ShortType and CHAR(n); see the class comment for the way back).
        return TypeSupport.nativeType();
    }
  }
}
