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

package org.apache.hudi.sync.common.util;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.schema.mapping.SyncOptions;
import org.apache.hudi.sync.common.schema.CatalogSchemaRenderer;
import org.apache.hudi.sync.common.schema.CatalogSchemaRenderers;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link SparkPropsRenderer} produces what {@link SparkDataSourceTableUtils} produces today.
 */
class TestSparkPropsRenderer {

  private static final SparkPropsRenderer RENDERER = new SparkPropsRenderer();

  private static HoodieSchema tableSchema() {
    HoodieSchema nested = HoodieSchema.createRecord("nested", "test", null, Arrays.asList(
        HoodieSchemaField.of("a", HoodieSchema.create(HoodieSchemaType.INT), "inner \"doc\"", null),
        HoodieSchemaField.of("b", HoodieSchema.createNullable(HoodieSchemaType.STRING))));
    return HoodieSchema.createRecord("trips", "test", null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.LONG), "the key", null),
        HoodieSchemaField.of("dt", HoodieSchema.create(HoodieSchemaType.STRING), "partition date", null),
        HoodieSchemaField.of("ts", HoodieSchema.createTimestampMicros()),
        HoodieSchemaField.of("local_ts", HoodieSchema.createNullable(HoodieSchema.createLocalTimestampMillis())),
        HoodieSchemaField.of("price", HoodieSchema.createDecimal(10, 2)),
        HoodieSchemaField.of("uid", HoodieSchema.createUUID()),
        HoodieSchemaField.of("emb", HoodieSchema.createVector(4, HoodieSchema.Vector.VectorElementType.FLOAT)),
        HoodieSchemaField.of("payload", HoodieSchema.createBlob()),
        HoodieSchemaField.of("tags", HoodieSchema.createArray(HoodieSchema.createNullable(HoodieSchemaType.STRING))),
        HoodieSchemaField.of("attrs", HoodieSchema.createMap(HoodieSchema.create(HoodieSchemaType.INT))),
        HoodieSchemaField.of("nested", nested),
        HoodieSchemaField.of("nothing", HoodieSchema.create(HoodieSchemaType.NULL))));
  }

  @ParameterizedTest
  @CsvSource({
      "dt,,4000,false",
      "dt;datestr,3.5.1,4000,true",
      ",3.5.1,100,true",
      "dt,,50,false"})
  void renderEqualsSparkTableProperties(String partitions, String sparkVersion, int threshold, boolean syncComment) {
    List<String> partitionFields = partitions == null ? Collections.emptyList() : Arrays.asList(partitions.split(";"));
    String version = sparkVersion == null ? "" : sparkVersion;
    SyncOptions opts = SyncOptions.builder()
        .set(SparkPropsRenderer.SPARK_VERSION, version)
        .set(SparkPropsRenderer.SCHEMA_STRING_LENGTH_THRESHOLD, String.valueOf(threshold))
        .set(SparkPropsRenderer.SYNC_COMMENT, syncComment)
        .build();

    Map<String, String> rendered = RENDERER.render(tableSchema(), partitionFields, opts);

    assertEquals(SparkDataSourceTableUtils.getSparkTableProperties(partitionFields, version, threshold, tableSchema(), syncComment), rendered);
    if (partitionFields.isEmpty()) {
      assertFalse(rendered.containsKey("spark.sql.sources.schema.numPartCols"));
    } else {
      assertEquals(String.valueOf(partitionFields.size()), rendered.get("spark.sql.sources.schema.numPartCols"));
    }
  }

  @Test
  void renderDefaultsToTodaysDefaults() {
    List<String> partitionFields = Collections.singletonList("dt");
    Map<String, String> rendered = RENDERER.render(tableSchema(), partitionFields, SyncOptions.DEFAULT);
    // HiveSyncTool's defaults: no spark version, a 4000 character threshold, no comments.
    assertEquals(SparkDataSourceTableUtils.getSparkTableProperties(partitionFields, "", 4000, tableSchema(), false), rendered);
    assertFalse(rendered.containsKey("spark.sql.create.version"));
    assertEquals("1", rendered.get("spark.sql.sources.schema.numParts"));
    String schemaJson = rendered.get("spark.sql.sources.schema.part.0");
    assertEquals(SparkSchemaUtils.convertToSparkSchemaJson(reordered(), false), schemaJson);
    assertFalse(schemaJson.contains("\"comment\""));
  }

  @Test
  void diffIsNotSupported() {
    assertThrows(UnsupportedOperationException.class,
        () -> RENDERER.diff(tableSchema(), Collections.emptyMap(), Collections.emptyList(), SyncOptions.DEFAULT));
  }

  @Test
  void isRegistered() {
    assertTrue(CatalogSchemaRenderers.catalogs().contains(CatalogSchemaRenderers.SPARK_PROPERTIES));
    CatalogSchemaRenderer<Map<String, String>> renderer = CatalogSchemaRenderers.get(CatalogSchemaRenderers.SPARK_PROPERTIES);
    assertInstanceOf(SparkPropsRenderer.class, renderer);
  }

  /**
   * The table schema with the partition column moved last, as Spark expects it.
   */
  private static HoodieSchema reordered() {
    HoodieSchema schema = tableSchema();
    List<HoodieSchemaField> fields = new ArrayList<>();
    schema.getFields().stream().filter(f -> !f.name().equals("dt")).forEach(f -> fields.add(f.withSchema(f.schema())));
    fields.add(schema.getField("dt").get().withSchema(schema.getField("dt").get().schema()));
    return HoodieSchema.createRecord(schema.getName(), null, null, fields);
  }
}
