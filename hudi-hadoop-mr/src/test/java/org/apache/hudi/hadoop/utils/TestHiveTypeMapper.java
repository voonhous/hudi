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
import org.apache.hudi.common.schema.mapping.TypeSupport;
import org.apache.hudi.common.schema.mapping.TypeSystemMapper;
import org.apache.hudi.common.schema.mapping.testkit.SchemaSamples;
import org.apache.hudi.common.schema.mapping.testkit.TypeSystemMapperContractTest;

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.hudi.common.schema.HoodieSchemaType.BLOB;
import static org.apache.hudi.common.schema.HoodieSchemaType.ENUM;
import static org.apache.hudi.common.schema.HoodieSchemaType.FIXED;
import static org.apache.hudi.common.schema.HoodieSchemaType.TIME;
import static org.apache.hudi.common.schema.HoodieSchemaType.TIMESTAMP;
import static org.apache.hudi.common.schema.HoodieSchemaType.UUID;
import static org.apache.hudi.common.schema.HoodieSchemaType.VARIANT;
import static org.apache.hudi.common.schema.HoodieSchemaType.VECTOR;
import static org.apache.hudi.common.schema.mapping.TypeSupport.Level.LOSSY;

/**
 * Contract test for {@link HiveTypeMapper}.
 *
 * <p>RFC-99 has no Hive column, so the expected support follows RFC-88 ("Hive: VECTOR, UUID and FIXED as binary,
 * ENUM as string, BLOB as struct") and is NATIVE wherever Hive has the type. Hive has no time or variant type and its
 * timestamp has neither a precision nor a time zone flag, so TIME, VARIANT and TIMESTAMP are LOSSY too. INT8, INT16
 * and CHAR(n) are NATIVE: Hive has tinyint, smallint and char(n).
 */
class TestHiveTypeMapper extends TypeSystemMapperContractTest<TypeInfo> {

  @Override
  protected TypeSystemMapper<TypeInfo> mapper() {
    return new HiveTypeMapper();
  }

  @Override
  protected Map<HoodieSchemaType, TypeSupport.Level> expectedSupport() {
    return nativeExcept(
        FIXED, LOSSY,
        ENUM, LOSSY,
        UUID, LOSSY,
        VECTOR, LOSSY,
        BLOB, LOSSY,
        TIME, LOSSY,
        TIMESTAMP, LOSSY,
        VARIANT, LOSSY);
  }

  @Override
  protected Map<String, String> knownGaps() {
    Map<String, String> gaps = new LinkedHashMap<>();
    String unsupported = "HiveTypeUtils has no case for %s and throws \"Do not yet support\"; HiveSchemaUtil (Hive sync) maps it to %s";
    gaps.put(SUPPORT_EXPECTED + "/UUID", String.format(unsupported, "UUID", "binary"));
    gaps.put(SUPPORT_EXPECTED + "/VECTOR", String.format(unsupported, "VECTOR", "binary"));
    gaps.put(SUPPORT_EXPECTED + "/BLOB", String.format(unsupported, "BLOB", "a struct"));
    gaps.put(SUPPORT_EXPECTED + "/VARIANT", String.format(unsupported, "VARIANT", "a struct"));
    String widened = "HiveTypeUtils maps %s to Hive %s, not %s, and Hive's TypeInfoToSchema reads %s back as %s";
    gaps.put(SUPPORT_EXPECTED + "/INT8", String.format(widened, "INT8", "int", "tinyint", "tinyint", "a plain int"));
    gaps.put(SUPPORT_EXPECTED + "/INT16", String.format(widened, "INT16", "int", "smallint", "smallint", "a plain int"));
    gaps.put(SUPPORT_EXPECTED + "/CHAR", String.format(widened, "CHAR(n)", "string", "char(n)", "char(n)",
        "Hive's own {\"logicalType\":\"char\",\"maxLength\":n}, which is STRING"));
    // Hive types have no NOT NULL, and HoodieSchemaHints never restores nullability from the hint, so every case with
    // a required column, struct field, array element or map value reads back nullable. Types that fail fast are exempt.
    for (SchemaSamples.SchemaCase c : SchemaSamples.allCases()) {
      if (!support(c.sample().type()).isUnsupported() && c.schema().getFields().stream().anyMatch(f -> hasRequiredNode(f.schema()))) {
        gaps.put(ROUND_TRIP + "/" + c.id(), "Hive TypeInfo has no NOT NULL: Hive's TypeInfoToSchema reads every column, struct field, "
            + "array element and map value back as nullable, and the hint does not restore nullability");
      }
    }
    return gaps;
  }

  /**
   * Whether the schema, or a field, element or value below it, is required (not nullable).
   */
  private static boolean hasRequiredNode(HoodieSchema schema) {
    if (schema.getType() == HoodieSchemaType.NULL) {
      return false;
    }
    if (!schema.isNullable()) {
      return true;
    }
    HoodieSchema type = schema.getNonNullType();
    switch (type.getType()) {
      case RECORD:
        return type.getFields().stream().anyMatch(f -> hasRequiredNode(f.schema()));
      case ARRAY:
        return hasRequiredNode(type.getElementType());
      case MAP:
        return hasRequiredNode(type.getValueType());
      default:
        return false;
    }
  }
}
