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

package org.apache.hudi.common.schema.internal.utils;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.schema.internal.Type;
import org.apache.hudi.common.schema.internal.Types;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

public class Conversions {

  private static final HashSet<Type.TypeID> SUPPORTED_PARTITION_TYPES = new HashSet<>(
      Arrays.asList(Type.TypeID.INT,
          Type.TypeID.LONG,
          Type.TypeID.BOOLEAN,
          Type.TypeID.FLOAT,
          Type.TypeID.DECIMAL,
          Type.TypeID.DECIMAL_FIXED,
          Type.TypeID.DECIMAL_BYTES,
          Type.TypeID.DOUBLE,
          Type.TypeID.UUID,
          Type.TypeID.DATE,
          Type.TypeID.STRING));

  // The HoodieSchema types of SUPPORTED_PARTITION_TYPES: DECIMAL covers the three decimal type ids, and
  // ENUM and CHAR are read as STRING and INT8 and INT16 as INT, as InternalSchemaConverter maps them.
  private static final Set<HoodieSchemaType> SUPPORTED_PARTITION_SCHEMA_TYPES = EnumSet.of(
      HoodieSchemaType.INT,
      HoodieSchemaType.INT8,
      HoodieSchemaType.INT16,
      HoodieSchemaType.CHAR,
      HoodieSchemaType.LONG,
      HoodieSchemaType.BOOLEAN,
      HoodieSchemaType.FLOAT,
      HoodieSchemaType.DECIMAL,
      HoodieSchemaType.DOUBLE,
      HoodieSchemaType.UUID,
      HoodieSchemaType.DATE,
      HoodieSchemaType.STRING,
      HoodieSchemaType.ENUM);

  /**
   * Whether every partition value of the given partition record schema can be parsed from a partition
   * path, judged on each field's non-null type.
   */
  public static boolean isPartitionSchemaSupportedConversion(HoodieSchema schema) {
    for (HoodieSchemaField field : schema.getFields()) {
      if (!SUPPORTED_PARTITION_SCHEMA_TYPES.contains(field.getNonNullSchema().getType())) {
        return false;
      }
    }

    return true;
  }

  public static boolean isPartitionSchemaSupportedConversion(Types.RecordType schema) {
    for (Types.Field field: schema.fields()) {
      if (!SUPPORTED_PARTITION_TYPES.contains(field.type().typeId())) {
        return false;
      }
    }

    return true;
  }
}
