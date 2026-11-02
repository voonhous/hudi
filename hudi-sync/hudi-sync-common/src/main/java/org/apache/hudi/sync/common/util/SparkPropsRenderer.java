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
import org.apache.hudi.common.schema.mapping.SyncOptions;
import org.apache.hudi.sync.common.HoodieSyncConfig;
import org.apache.hudi.sync.common.schema.CatalogSchemaRenderer;
import org.apache.hudi.sync.common.schema.CatalogSchemaRenderers;

import java.util.List;
import java.util.Map;

/**
 * {@link CatalogSchemaRenderer} for the Spark data source table properties that HMS, Glue, ADB, DataHub and Flink's
 * Hive catalog store with a table ({@code spark.sql.sources.provider}, {@code spark.sql.create.version},
 * {@code spark.sql.sources.schema.numParts} / {@code part.N} and {@code numPartCols} / {@code partCol.N}), so Spark
 * reads the table as a Hudi data source (RFC-88, type plane).
 *
 * <p>Wraps {@link SparkDataSourceTableUtils#getSparkTableProperties}, which serializes the schema with
 * {@link SparkSchemaUtils#convertToSparkSchemaJson}. Column types come from {@code SparkSchemaUtils}'s own type
 * switch, not from the Spark {@code TypeSystemMapper}, which {@code hudi-sync-common} cannot depend on; RFC-88 step 2
 * converges them, and this renderer then replaces both utilities. The properties are rewritten on every sync, never
 * evolved in place, so {@link #diff} is not supported.
 */
public final class SparkPropsRenderer implements CatalogSchemaRenderer<Map<String, String>> {

  private static final long serialVersionUID = 1L;

  /**
   * The Spark version stored as {@code spark.sql.create.version}; not stored when empty, the default.
   */
  public static final String SPARK_VERSION = HoodieSyncConfig.META_SYNC_SPARK_VERSION.key();
  /**
   * Maximum length of one {@code spark.sql.sources.schema.part.N} value; default 4000. The same key as Hive sync's
   * {@code HiveSyncConfigHolder.HIVE_SYNC_SCHEMA_STRING_LENGTH_THRESHOLD}, which this module cannot reference.
   */
  public static final String SCHEMA_STRING_LENGTH_THRESHOLD = "hoodie.datasource.hive_sync.schema_string_length_thresh";
  /**
   * Store field docs as {@code comment} field metadata in the schema JSON; default {@code false}. The same key as
   * Hive sync's {@code HiveSyncConfigHolder.HIVE_SYNC_COMMENT}, which this module cannot reference.
   */
  public static final String SYNC_COMMENT = "hoodie.datasource.hive_sync.sync_comment";

  public static final int DEFAULT_SCHEMA_STRING_LENGTH_THRESHOLD = 4000;

  @Override
  public String catalog() {
    return CatalogSchemaRenderers.SPARK_PROPERTIES;
  }

  @Override
  public Map<String, String> render(HoodieSchema schema, List<String> partitionFields, SyncOptions opts) {
    return SparkDataSourceTableUtils.getSparkTableProperties(partitionFields,
        opts.getString(SPARK_VERSION, HoodieSyncConfig.META_SYNC_SPARK_VERSION.defaultValue()),
        opts.getInt(SCHEMA_STRING_LENGTH_THRESHOLD, DEFAULT_SCHEMA_STRING_LENGTH_THRESHOLD),
        schema,
        opts.getBoolean(SYNC_COMMENT, false));
  }
}
