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

package org.apache.hudi.hive.util;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.schema.mapping.SyncOptions;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.sync.common.schema.CatalogSchemaRenderer;
import org.apache.hudi.sync.common.schema.CatalogSchemaRenderers;
import org.apache.hudi.sync.common.util.SparkPropsRenderer;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_SUPPORT_TIMESTAMP_TYPE;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_SYNC_COMMENT;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_SYNC_SCHEMA_STRING_LENGTH_THRESHOLD;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_PARTITION_FIELDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link HiveCatalogRenderer} produces what Hive sync produces today.
 */
class TestHiveCatalogRenderer {

  private static final HiveCatalogRenderer RENDERER = new HiveCatalogRenderer();

  /**
   * A table with the types Hive sync maps specially: timestamps (by option), decimals, UUID and VECTOR (binary),
   * nested structs, a NULL column (skipped), field docs and a partition column.
   */
  private static HoodieSchema tableSchema() {
    HoodieSchema nested = HoodieSchema.createRecord("nested", "test", null, Arrays.asList(
        HoodieSchemaField.of("a", HoodieSchema.create(HoodieSchemaType.INT)),
        HoodieSchemaField.of("c", HoodieSchema.createNullable(HoodieSchemaType.STRING))));
    return HoodieSchema.createRecord("trips", "test", null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.LONG), "the key", null),
        HoodieSchemaField.of("Name", HoodieSchema.createNullable(HoodieSchemaType.STRING), "it's the name", null),
        HoodieSchemaField.of("ts", HoodieSchema.createTimestampMillis()),
        HoodieSchemaField.of("ts_micros", HoodieSchema.createNullable(HoodieSchema.createTimestampMicros())),
        HoodieSchemaField.of("price", HoodieSchema.createDecimal(10, 2)),
        HoodieSchemaField.of("uid", HoodieSchema.createUUID()),
        HoodieSchemaField.of("emb", HoodieSchema.createVector(4, HoodieSchema.Vector.VectorElementType.FLOAT)),
        HoodieSchemaField.of("tags", HoodieSchema.createArray(HoodieSchema.create(HoodieSchemaType.STRING))),
        HoodieSchemaField.of("attrs", HoodieSchema.createMap(HoodieSchema.create(HoodieSchemaType.INT))),
        HoodieSchemaField.of("nested", nested),
        HoodieSchemaField.of("nothing", HoodieSchema.create(HoodieSchemaType.NULL)),
        HoodieSchemaField.of("dt", HoodieSchema.create(HoodieSchemaType.STRING), "partition date", null)));
  }

  @ParameterizedTest
  @CsvSource({"false,false,dt", "true,false,dt", "false,true,dt", "true,true,dt", "false,true,", "true,true,dt;datestr"})
  void renderEqualsHmsColumns(boolean supportTimestamp, boolean syncComment, String partitions) {
    List<String> partitionFields = partitions == null ? Collections.emptyList() : Arrays.asList(partitions.split(";"));
    HoodieSchema schema = tableSchema();
    SyncOptions opts = SyncOptions.builder()
        .set(HiveCatalogRenderer.SUPPORT_TIMESTAMP, supportTimestamp)
        .set(HiveCatalogRenderer.SYNC_COMMENT, syncComment)
        .build();

    List<FieldSchema> rendered = RENDERER.render(schema, partitionFields, opts);

    assertEquals(hmsCreateTableColumns(schema, partitionFields, supportTimestamp, syncComment), rendered);
    // The data columns are also what HMSDDLExecutor#updateTableDefinition sets.
    HiveSyncConfig config = config(partitionFields, supportTimestamp, syncComment);
    List<FieldSchema> dataColumns = rendered.stream().filter(c -> !partitionFields.contains(c.getName()))
        .map(c -> new FieldSchema(c.getName(), c.getType(), "")).collect(Collectors.toList());
    assertEquals(HiveSchemaUtil.convertSchemaToHiveFieldSchema(schema, config), dataColumns);
  }

  @Test
  void renderDefaultsToTodaysDefaults() {
    List<String> partitionFields = Collections.singletonList("dt");
    assertEquals(hmsCreateTableColumns(tableSchema(), partitionFields, false, false),
        RENDERER.render(tableSchema(), partitionFields, SyncOptions.DEFAULT));
    // Spot-check the types today's Hive sync writes.
    Map<String, String> types = new LinkedHashMap<>();
    RENDERER.render(tableSchema(), partitionFields, SyncOptions.DEFAULT).forEach(c -> types.put(c.getName(), c.getType()));
    assertEquals("bigint", types.get("ts"));
    assertEquals("binary", types.get("uid"));
    assertEquals("binary", types.get("emb"));
    assertEquals("decimal(10,2)", types.get("price"));
    assertEquals("struct<a:int,c:string>", types.get("nested"));
    assertFalse(types.containsKey("nothing"));
    assertEquals(Arrays.asList("id", "Name", "ts", "ts_micros", "price", "uid", "emb", "tags", "attrs", "nested", "dt"),
        new ArrayList<>(types.keySet()));
  }

  @ParameterizedTest
  @CsvSource({"false", "true"})
  void diffEqualsGetSchemaDifference(boolean supportTimestamp) {
    HoodieSchema schema = tableSchema();
    List<String> partitionFields = Collections.singletonList("dt");
    SyncOptions opts = SyncOptions.builder().set(HiveCatalogRenderer.SUPPORT_TIMESTAMP, supportTimestamp).build();

    // In sync: what render wrote is what the metastore returns.
    List<FieldSchema> inSync = RENDERER.render(schema, partitionFields, opts);
    assertTrue(RENDERER.diff(schema, inSync, partitionFields, opts).isEmpty());

    // Drifted: a dropped column, a changed type and an extra column, in metastore (upper) case.
    List<FieldSchema> drifted = new ArrayList<>();
    for (FieldSchema column : inSync) {
      if (column.getName().equals("tags")) {
        continue;
      }
      String type = column.getName().equals("price") ? "DECIMAL(8,2)" : column.getType().toUpperCase(Locale.ROOT);
      drifted.add(new FieldSchema(column.getName(), type, column.getComment()));
    }
    drifted.add(new FieldSchema("legacy", "STRING", ""));
    Map<String, String> metastoreSchema = new LinkedHashMap<>();
    drifted.forEach(c -> metastoreSchema.put(c.getName(), c.getType()));

    assertEquals(HiveSchemaUtil.getSchemaDifference(schema, metastoreSchema, partitionFields, supportTimestamp).toString(),
        RENDERER.diff(schema, drifted, partitionFields, opts).toString());
    assertFalse(RENDERER.diff(schema, drifted, partitionFields, opts).isEmpty());
  }

  @Test
  void isRegistered() {
    assertTrue(CatalogSchemaRenderers.catalogs().contains(CatalogSchemaRenderers.HIVE));
    CatalogSchemaRenderer<List<FieldSchema>> renderer = CatalogSchemaRenderers.get(CatalogSchemaRenderers.HIVE);
    assertInstanceOf(HiveCatalogRenderer.class, renderer);
    assertEquals(CatalogSchemaRenderers.HIVE, renderer.catalog());
    // hudi-sync-common's renderer is discovered alongside, from its own service file.
    assertInstanceOf(SparkPropsRenderer.class, CatalogSchemaRenderers.get(CatalogSchemaRenderers.SPARK_PROPERTIES));
  }

  /**
   * SparkPropsRenderer cannot reference HiveSyncConfigHolder, so it repeats the keys Hive sync passes to
   * SparkDataSourceTableUtils.
   */
  @Test
  void sparkPropsRendererUsesHiveSyncKeys() {
    assertEquals(HIVE_SYNC_SCHEMA_STRING_LENGTH_THRESHOLD.key(), SparkPropsRenderer.SCHEMA_STRING_LENGTH_THRESHOLD);
    assertEquals((int) HIVE_SYNC_SCHEMA_STRING_LENGTH_THRESHOLD.defaultValue(), SparkPropsRenderer.DEFAULT_SCHEMA_STRING_LENGTH_THRESHOLD);
    assertEquals(HIVE_SYNC_COMMENT.key(), SparkPropsRenderer.SYNC_COMMENT);
  }

  /**
   * The columns HMSDDLExecutor#createTable sets today: sd cols, then partition keys, with field docs as comments when
   * comment sync is on.
   */
  private static List<FieldSchema> hmsCreateTableColumns(HoodieSchema schema, List<String> partitionFields, boolean supportTimestamp,
                                                         boolean syncComment) {
    HiveSyncConfig config = config(partitionFields, supportTimestamp, syncComment);
    LinkedHashMap<String, String> mapSchema = HiveSchemaUtil.hoodieSchemaToMapSchema(schema, supportTimestamp, false);
    List<FieldSchema> columns = new ArrayList<>(HiveSchemaUtil.convertMapSchemaToHiveFieldSchema(mapSchema, config));
    for (String partitionKey : partitionFields) {
      columns.add(new FieldSchema(partitionKey, HiveSchemaUtil.getPartitionKeyType(mapSchema, partitionKey).toLowerCase(), ""));
    }
    if (syncComment) {
      Map<String, String> fieldDocs = HiveSchemaUtil.getFieldDocs(schema);
      for (FieldSchema column : columns) {
        String doc = fieldDocs.get(column.getName().toLowerCase(Locale.ROOT));
        if (doc != null) {
          column.setComment(doc);
        }
      }
    }
    return columns;
  }

  private static HiveSyncConfig config(List<String> partitionFields, boolean supportTimestamp, boolean syncComment) {
    Properties props = new Properties();
    props.setProperty(META_SYNC_PARTITION_FIELDS.key(), String.join(",", partitionFields));
    props.setProperty(HIVE_SUPPORT_TIMESTAMP_TYPE.key(), String.valueOf(supportTimestamp));
    props.setProperty(HIVE_SYNC_COMMENT.key(), String.valueOf(syncComment));
    return new HiveSyncConfig(props);
  }
}
