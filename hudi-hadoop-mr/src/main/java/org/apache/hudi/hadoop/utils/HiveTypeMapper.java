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

package org.apache.hudi.hadoop.utils;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.schema.mapping.HoodieSchemaEquivalence;
import org.apache.hudi.common.schema.mapping.HoodieSchemaHints;
import org.apache.hudi.common.schema.mapping.MappingOptions;
import org.apache.hudi.common.schema.mapping.TypeSupport;
import org.apache.hudi.common.schema.mapping.TypeSystemMapper;
import org.apache.hudi.common.schema.mapping.TypeSystems;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;

import org.apache.avro.Schema;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeException;
import org.apache.hadoop.hive.serde2.avro.TypeInfoToSchema;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.Collections;

/**
 * {@link TypeSystemMapper} for Hive {@link TypeInfo} (RFC-88, type plane), as used by the Hive readers in
 * {@code hudi-hadoop-mr}.
 *
 * <p>{@code HoodieSchema} to Hive delegates to {@link HiveTypeUtils#generateTypeInfo}. Hive to {@code HoodieSchema}
 * has no Hudi converter today, so it delegates to Hive's own Avro serde converter, {@link TypeInfoToSchema}, and
 * reads the Avro schema with {@link HoodieSchema#fromAvroSchema}. Hive types carry no nullability, so
 * {@code TypeInfoToSchema} reads every column, struct field, array element and map value back as nullable.
 *
 * <p>Hive has no metadata on a type, so LOSSY types are restored from the hint only.
 *
 * <p>Replaces, in RFC-88 step 2, {@code HiveTypeUtils} and the type part of {@code HiveSchemaUtil} (Hive sync) and
 * of the Flink catalog's {@code HiveSchemaUtils}, which map the same types differently today.
 */
public final class HiveTypeMapper implements TypeSystemMapper<TypeInfo> {

  private static final long serialVersionUID = 1L;

  private static final String WRAPPER_RECORD = "hive_type_mapper_column";
  private static final String WRAPPER_COLUMN = "col";

  @Override
  public String typeSystem() {
    return TypeSystems.HIVE;
  }

  @Override
  public TypeInfo toTypeSystem(HoodieSchema schema, MappingOptions opts) {
    try {
      return HiveTypeUtils.generateTypeInfo(schema, null);
    } catch (AvroSerdeException e) {
      throw new HoodieException("Cannot convert to a Hive type: " + schema, e);
    }
  }

  @Override
  public HoodieSchema fromTypeSystem(TypeInfo type, Option<HoodieSchema> hint) {
    HoodieSchema converted = convert(type);
    return HoodieSchemaHints.restore(converted, hint, t -> support(t, MappingOptions.DEFAULT).isLossy(),
        n -> convert(toTypeSystem(n, MappingOptions.DEFAULT)), HoodieSchemaEquivalence.Mode.LOGICAL);
  }

  private static HoodieSchema convert(TypeInfo type) {
    if (type instanceof StructTypeInfo) {
      StructTypeInfo struct = (StructTypeInfo) type;
      Schema record = new TypeInfoToSchema().convert(struct.getAllStructFieldNames(), struct.getAllStructFieldTypeInfos(),
          Collections.emptyList(), null, null, null);
      return HoodieSchema.fromAvroSchema(record);
    }
    // TypeInfoToSchema converts columns of a table only: wrap a single type in a one-column record and unwrap it.
    Schema record = new TypeInfoToSchema().convert(Collections.singletonList(WRAPPER_COLUMN), Collections.singletonList(type),
        Collections.emptyList(), null, WRAPPER_RECORD, null);
    return HoodieSchema.fromAvroSchema(record.getField(WRAPPER_COLUMN).schema());
  }

  @Override
  public TypeSupport support(HoodieSchemaType type, MappingOptions opts) {
    switch (type) {
      case FIXED:
        return TypeSupport.lossy("FIXED stored as Hive binary; the size is dropped");
      case ENUM:
        return TypeSupport.lossy("ENUM stored as Hive string; the symbols are dropped");
      case TIME:
        return TypeSupport.lossy("TIME(MILLIS) stored as Hive int and TIME(MICROS) as bigint: Hive has no time type");
      case TIMESTAMP:
        return TypeSupport.lossy("TIMESTAMP(MICROS) and LOCAL_TIMESTAMP(MICROS) stored as Hive bigint; LOCAL_TIMESTAMP(MILLIS) stored as Hive "
            + "timestamp, read back as TIMESTAMP(MILLIS); only TIMESTAMP(MILLIS) is exact");
      case UUID:
      case VARIANT:
      case BLOB:
      case VECTOR:
        return TypeSupport.unsupported("HiveTypeUtils has no case for " + type + " and throws AvroSerdeException \"Do not yet support\"");
      default:
        // BOOLEAN, INT, LONG, FLOAT, DOUBLE, STRING, BYTES, DECIMAL, DATE, RECORD, ARRAY, MAP (string keys), UNION
        // (uniontype) and NULL (void) have a Hive type of their own.
        return TypeSupport.nativeType();
    }
  }
}
