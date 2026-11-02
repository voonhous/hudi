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

import org.apache.hudi.adapter.DataTypeAdapter;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.schema.mapping.HoodieSchemaEquivalence;
import org.apache.hudi.common.schema.mapping.HoodieSchemaHints;
import org.apache.hudi.common.schema.mapping.MappingOptions;
import org.apache.hudi.common.schema.mapping.TypeSupport;
import org.apache.hudi.common.schema.mapping.TypeSystemMapper;
import org.apache.hudi.common.schema.mapping.TypeSystems;
import org.apache.hudi.common.util.Option;

import org.apache.flink.table.types.DataType;

/**
 * {@link TypeSystemMapper} for Flink (RFC-88, type plane), keyed {@link TypeSystems#FLINK}.
 *
 * <p>Wraps {@link HoodieSchemaConverter} in both directions: {@link HoodieSchemaConverter#convertToDataType} for
 * {@link #toTypeSystem} and {@link HoodieSchemaConverter#convertToSchema(org.apache.flink.table.types.logical.LogicalType, String)}
 * for {@link #fromTypeSystem}. It replaces {@code HoodieSchemaConverter} once callers switch to the mapper (RFC-88
 * rollout step 2); until then nothing calls it and behaviour is unchanged.
 *
 * <p>The type parameter is {@link DataType}, the type {@code convertToDataType} returns and most callers hold; the
 * reverse direction converts its {@link DataType#getLogicalType() logical type}.
 *
 * <p>Record names: the callers name the top-level record after the table
 * ({@code HoodieSchemaUtils.getRecordQualifiedName(tableName)}), and the hint (the table schema) carries that name, so
 * {@link #fromTypeSystem} takes the hint's full name. Without a hint, or when the hint is not a record, it uses
 * {@value #DEFAULT_RECORD_NAME}, the converter's own default.
 *
 * <p>Flink types have no field metadata, so this mapper has no markers: LOSSY types (ENUM, FIXED, UUID, VECTOR, BLOB)
 * come back only from the hint, through {@link HoodieSchemaHints#restore}. The converter's {@code vectorColumns}
 * argument is not used: it is a table option of the write path, not part of the Flink type.
 */
public final class FlinkTypeMapper implements TypeSystemMapper<DataType> {

  private static final long serialVersionUID = 1L;

  /**
   * Top-level record name used by {@link #fromTypeSystem} when the hint gives none.
   */
  public static final String DEFAULT_RECORD_NAME = "record";

  /**
   * Whether the Flink on the classpath has a VARIANT type (Flink 2.1+).
   */
  private static final boolean HAS_VARIANT = hasVariantType();

  @Override
  public String typeSystem() {
    return TypeSystems.FLINK;
  }

  @Override
  public DataType toTypeSystem(HoodieSchema schema, MappingOptions opts) {
    return HoodieSchemaConverter.convertToDataType(schema);
  }

  @Override
  public HoodieSchema fromTypeSystem(DataType type, Option<HoodieSchema> hint) {
    String recordName = hint.isPresent() && hint.get().getNonNullType().getType() == HoodieSchemaType.RECORD
        ? hint.get().getNonNullType().getFullName()
        : DEFAULT_RECORD_NAME;
    HoodieSchema converted = HoodieSchemaConverter.convertToSchema(type.getLogicalType(), recordName);
    return HoodieSchemaHints.restore(converted, hint, t -> support(t).isLossy(),
        n -> fromTypeSystem(toTypeSystem(n), Option.empty()), HoodieSchemaEquivalence.Mode.LOGICAL);
  }

  @Override
  public TypeSupport support(HoodieSchemaType type, MappingOptions opts) {
    switch (type) {
      case ENUM:
        return TypeSupport.lossy("ENUM stored as Flink STRING; the symbols are lost");
      case FIXED:
        return TypeSupport.lossy("FIXED(n) stored as Flink VARBINARY(n), which reads back as BYTES");
      case UUID:
        return TypeSupport.lossy("UUID stored as Flink STRING");
      case VECTOR:
        return TypeSupport.lossy("VECTOR stored as Flink ARRAY<FLOAT|DOUBLE|TINYINT>; dimension is lost and the array reads back as ARRAY "
            + "(TINYINT elements as INT)");
      case BLOB:
        return TypeSupport.lossy("BLOB stored as Flink ROW<type STRING, data BYTES, reference ROW<...>>; read back as BLOB by its field "
            + "structure, so any ROW of that shape becomes BLOB");
      case INT8:
        return TypeSupport.lossy("INT8 stored as Flink INT, not TINYINT (HoodieSchemaConverter maps it as INT)");
      case INT16:
        return TypeSupport.lossy("INT16 stored as Flink INT, not SMALLINT (HoodieSchemaConverter maps it as INT)");
      case CHAR:
        return TypeSupport.lossy("CHAR(n) stored as Flink STRING, not CHAR(n) (HoodieSchemaConverter maps it as STRING)");
      case UNION:
        return TypeSupport.unsupported("Flink has no union type: a union with more than one non-null branch is written as "
            + "RAW(Object), which convertToSchema cannot read back");
      case VARIANT:
        return HAS_VARIANT
            ? TypeSupport.nativeType()
            : TypeSupport.unsupported("Flink before 2.1 has no VARIANT type");
      default:
        // TIMESTAMP and TIME keep millis / micros precision and the UTC adjustment (as TIMESTAMP vs
        // TIMESTAMP_LTZ); DECIMAL keeps precision and scale (the backing is a physical detail).
        return TypeSupport.nativeType();
    }
  }

  private static boolean hasVariantType() {
    try {
      DataTypeAdapter.createVariantType();
      return true;
    } catch (UnsupportedOperationException e) {
      return false;
    }
  }
}
