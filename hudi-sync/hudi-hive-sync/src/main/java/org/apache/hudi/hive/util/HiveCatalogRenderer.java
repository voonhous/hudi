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
import org.apache.hudi.common.schema.mapping.SyncOptions;
import org.apache.hudi.hive.HiveSyncConfigHolder;
import org.apache.hudi.hive.SchemaDifference;
import org.apache.hudi.sync.common.schema.CatalogSchemaRenderer;
import org.apache.hudi.sync.common.schema.CatalogSchemaRenderers;

import org.apache.hadoop.hive.metastore.api.FieldSchema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * {@link CatalogSchemaRenderer} for the Hive Metastore and the catalogs that share its column model (RFC-88, type
 * plane). Wraps {@link HiveSchemaUtil}.
 *
 * <p>{@link #render} returns the HMS column list exactly as {@code HMSDDLExecutor#createTable} builds it: the data
 * columns ({@link HiveSchemaUtil#hoodieSchemaToMapSchema} without formatting, partition fields removed, types lower
 * case) followed by the partition columns (typed by {@link HiveSchemaUtil#getPartitionKeyType}, {@code string} when
 * the field is not in the schema), each with the field doc as comment when {@link #SYNC_COMMENT} is on and an empty
 * comment otherwise. A {@link FieldSchema} list is what the metastore API takes ({@code sd.cols} plus
 * {@code partitionKeys}) and returns ({@code getSchema}), so the same representation is both {@code render}'s output
 * and {@code diff}'s current schema; it is also the only one of today's representations that carries comments.
 *
 * <p>{@link #diff} delegates to {@link HiveSchemaUtil#getSchemaDifference} with the current columns as a name to type
 * map, as {@code HiveSyncTool#syncSchema} does with {@code getMetastoreSchema}.
 *
 * <p>Column types come from {@code HiveSchemaUtil}'s own type switch, not from {@code HiveTypeMapper}: the two map
 * UUID, VECTOR, BLOB, VARIANT, TIMESTAMP, UNION and NULL differently today, and delegating would change what Hive
 * sync writes. RFC-88 step 2 converges them, and this renderer then replaces {@code HiveSchemaUtil} and the HMS
 * schema diff for HMS, Glue, ADB and Flink's {@code HoodieHiveCatalog}.
 */
public final class HiveCatalogRenderer implements CatalogSchemaRenderer<List<FieldSchema>> {

  private static final long serialVersionUID = 1L;

  /**
   * Render timestamps as Hive {@code timestamp} instead of {@code bigint}; default {@code false}, as today.
   */
  public static final String SUPPORT_TIMESTAMP = HiveSyncConfigHolder.HIVE_SUPPORT_TIMESTAMP_TYPE.key();
  /**
   * Use field docs as column comments; default {@code false}, as today.
   */
  public static final String SYNC_COMMENT = HiveSyncConfigHolder.HIVE_SYNC_COMMENT.key();

  @Override
  public String catalog() {
    return CatalogSchemaRenderers.HIVE;
  }

  @Override
  public List<FieldSchema> render(HoodieSchema schema, List<String> partitionFields, SyncOptions opts) {
    LinkedHashMap<String, String> mapSchema = HiveSchemaUtil.hoodieSchemaToMapSchema(schema, supportTimestamp(opts), false);
    Map<String, String> fieldDocs = syncComment(opts) ? HiveSchemaUtil.getFieldDocs(schema) : Collections.emptyMap();
    List<FieldSchema> columns = new ArrayList<>(mapSchema.size() + partitionFields.size());
    mapSchema.forEach((name, type) -> {
      if (!partitionFields.contains(name)) {
        columns.add(column(name, type, fieldDocs));
      }
    });
    for (String partitionField : partitionFields) {
      columns.add(column(partitionField, HiveSchemaUtil.getPartitionKeyType(mapSchema, partitionField), fieldDocs));
    }
    return columns;
  }

  @Override
  public SchemaDifference diff(HoodieSchema tableSchema, List<FieldSchema> current, List<String> partitionFields, SyncOptions opts) {
    Map<String, String> currentSchema = new LinkedHashMap<>();
    current.forEach(column -> currentSchema.put(column.getName(), column.getType()));
    return HiveSchemaUtil.getSchemaDifference(tableSchema, currentSchema, partitionFields, supportTimestamp(opts));
  }

  private static boolean supportTimestamp(SyncOptions opts) {
    return opts.getBoolean(SUPPORT_TIMESTAMP, Boolean.parseBoolean(HiveSyncConfigHolder.HIVE_SUPPORT_TIMESTAMP_TYPE.defaultValue()));
  }

  private static boolean syncComment(SyncOptions opts) {
    return opts.getBoolean(SYNC_COMMENT, Boolean.parseBoolean(HiveSyncConfigHolder.HIVE_SYNC_COMMENT.defaultValue()));
  }

  private static FieldSchema column(String name, String type, Map<String, String> fieldDocs) {
    return new FieldSchema(name, type.toLowerCase(), fieldDocs.getOrDefault(name.toLowerCase(Locale.ROOT), ""));
  }
}
