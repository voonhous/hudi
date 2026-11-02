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

package org.apache.hudi.gcp.bigquery;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.schema.mapping.HoodieSchemaEquivalence;
import org.apache.hudi.common.schema.mapping.TypeSupport;
import org.apache.hudi.common.schema.mapping.TypeSystemMapper;
import org.apache.hudi.common.schema.mapping.testkit.SchemaSample;
import org.apache.hudi.common.schema.mapping.testkit.SchemaSamples;
import org.apache.hudi.common.schema.mapping.testkit.TypeSystemMapperContractTest;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.StandardSQLTypeName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.hudi.common.schema.HoodieSchemaType.BLOB;
import static org.apache.hudi.common.schema.HoodieSchemaType.ENUM;
import static org.apache.hudi.common.schema.HoodieSchemaType.FIXED;
import static org.apache.hudi.common.schema.HoodieSchemaType.FLOAT;
import static org.apache.hudi.common.schema.HoodieSchemaType.INT;
import static org.apache.hudi.common.schema.HoodieSchemaType.MAP;
import static org.apache.hudi.common.schema.HoodieSchemaType.NULL;
import static org.apache.hudi.common.schema.HoodieSchemaType.TIME;
import static org.apache.hudi.common.schema.HoodieSchemaType.TIMESTAMP;
import static org.apache.hudi.common.schema.HoodieSchemaType.UNION;
import static org.apache.hudi.common.schema.HoodieSchemaType.UUID;
import static org.apache.hudi.common.schema.HoodieSchemaType.VARIANT;
import static org.apache.hudi.common.schema.HoodieSchemaType.VECTOR;
import static org.apache.hudi.common.schema.mapping.TypeSupport.Level.LOSSY;
import static org.apache.hudi.common.schema.mapping.TypeSupport.Level.UNSUPPORTED;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Contract test for {@link BigQueryTypeMapper}.
 *
 * <p>RFC-99 has no BigQuery column. The expected support is NATIVE wherever BigQuery has the type, including DECIMAL
 * (NUMERIC(P, S) and BIGNUMERIC(P, S)), and LOSSY where it has none (INT and FLOAT have only 64-bit forms, TIME and
 * TIMESTAMP only microseconds, and there is no FIXED, ENUM, UUID, MAP, VARIANT, BLOB or VECTOR). BigQuery has no union
 * and no null type, so UNION and NULL are UNSUPPORTED.
 */
class TestBigQueryTypeMapper extends TypeSystemMapperContractTest<Field> {

  @Override
  protected TypeSystemMapper<Field> mapper() {
    return new BigQueryTypeMapper();
  }

  @Override
  protected Map<HoodieSchemaType, TypeSupport.Level> expectedSupport() {
    return nativeExcept(
        INT, LOSSY,
        FLOAT, LOSSY,
        FIXED, LOSSY,
        ENUM, LOSSY,
        UUID, LOSSY,
        TIME, LOSSY,
        TIMESTAMP, LOSSY,
        MAP, LOSSY,
        VARIANT, LOSSY,
        BLOB, LOSSY,
        VECTOR, LOSSY,
        UNION, UNSUPPORTED,
        NULL, UNSUPPORTED);
  }

  @Override
  protected Map<String, String> knownGaps() {
    Map<String, String> gaps = new LinkedHashMap<>();
    String decimal = "BigQuerySchemaResolver maps every DECIMAL to NUMERIC with no precision or scale (scale 9 at most) instead of "
        + "NUMERIC(p, s) / BIGNUMERIC(p, s), and does not fail fast for decimals NUMERIC cannot hold";
    gaps.put(SUPPORT_EXPECTED + "/DECIMAL", decimal);
    gaps.put(ROUND_TRIP + "/decimal_10_2_bytes.*", decimal);
    gaps.put(ROUND_TRIP + "/decimal_10_2_fixed_5.*", decimal);
    gaps.put(ROUND_TRIP + "/decimal_38_10_fixed_16.*", decimal);

    String mapNullable = "BigQuerySchemaResolver writes every MAP as a NULLABLE STRUCT, so a required map reads back nullable";
    for (SchemaSample sample : SchemaSamples.all()) {
      if (sample.nesting() == SchemaSample.Nesting.ANY && !support(sample.type()).isUnsupported()) {
        gaps.put(ROUND_TRIP + "/" + sample.id() + ".in_map", mapNullable);
      }
    }
    gaps.put(ROUND_TRIP + "/map_long", mapNullable);
    gaps.put(ROUND_TRIP + "/map_long.in_struct", mapNullable);

    // The kit checks a sample by its top-level type: ARRAY and RECORD are NATIVE, so array_int and struct are round
    // tripped without the hint, but they hold an INT, which BigQuery stores as INT64 (LOSSY) and only the hint restores.
    String nestedInt = "[kit] NATIVE container checked without the hint holds an INT, which reads back as LONG (INT64)";
    gaps.put(ROUND_TRIP + "/array_int.*", nestedInt);
    gaps.put(ROUND_TRIP + "/struct.*", nestedInt);
    gaps.put(ROUND_TRIP + "/array_int.nullable", "BigQuery REPEATED fields are never NULL, so a nullable array reads back required; " + nestedInt);
    gaps.put(ROUND_TRIP + "/array_int.in_array", "BigQuery has no array of arrays and BigQuerySchemaResolver does not fail fast: "
        + "array<array<int>> is written as one REPEATED INT64, dropping the inner level");
    gaps.put(ROUND_TRIP + "/array_int.in_map", mapNullable + "; " + nestedInt);
    gaps.put(ROUND_TRIP + "/struct.in_map", mapNullable + "; " + nestedInt);
    return gaps;
  }

  /**
   * BigQuery to HoodieSchema, which the contract reaches only through what BigQuerySchemaResolver writes: BigQuery's
   * own types, parameterized decimals and the modes.
   */
  @Test
  void fromBigQueryTypes() {
    Field table = Field.newBuilder("t", StandardSQLTypeName.STRUCT,
        Field.newBuilder("dt", StandardSQLTypeName.DATETIME).setMode(Field.Mode.REQUIRED).build(),
        Field.newBuilder("t", StandardSQLTypeName.TIME).build(),
        Field.newBuilder("n", StandardSQLTypeName.NUMERIC).setMode(Field.Mode.REQUIRED).build(),
        Field.newBuilder("n_10_2", StandardSQLTypeName.NUMERIC).setPrecision(10L).setScale(2L).setMode(Field.Mode.REQUIRED).build(),
        Field.newBuilder("bn_40_20", StandardSQLTypeName.BIGNUMERIC).setPrecision(40L).setScale(20L).setMode(Field.Mode.REQUIRED).build(),
        Field.newBuilder("ints", StandardSQLTypeName.INT64).setMode(Field.Mode.REPEATED).build(),
        Field.newBuilder("s", StandardSQLTypeName.STRUCT,
            Field.newBuilder("x", StandardSQLTypeName.BOOL).setMode(Field.Mode.NULLABLE).build()).setMode(Field.Mode.REQUIRED).build())
        .setMode(Field.Mode.REQUIRED).build();
    HoodieSchema expected = HoodieSchema.createRecord("t", null, null, Arrays.asList(
        HoodieSchemaField.of("dt", HoodieSchema.createLocalTimestampMicros()),
        HoodieSchemaField.of("t", HoodieSchema.createNullable(HoodieSchema.createTimeMicros())),
        HoodieSchemaField.of("n", HoodieSchema.createDecimal(38, 9)),
        HoodieSchemaField.of("n_10_2", HoodieSchema.createDecimal(10, 2)),
        HoodieSchemaField.of("bn_40_20", HoodieSchema.createDecimal(40, 20)),
        HoodieSchemaField.of("ints", HoodieSchema.createArray(HoodieSchema.create(HoodieSchemaType.LONG))),
        HoodieSchemaField.of("s", HoodieSchema.createRecord("s", "t", null, Arrays.asList(
            HoodieSchemaField.of("x", HoodieSchema.createNullable(HoodieSchemaType.BOOLEAN)))))));
    HoodieSchemaEquivalence.assertEquivalent(expected, mapper().fromTypeSystem(table), HoodieSchemaEquivalence.Mode.LOGICAL);
    assertThrows(RuntimeException.class, () -> mapper().fromTypeSystem(Field.newBuilder("j", StandardSQLTypeName.JSON).build()));
  }
}
