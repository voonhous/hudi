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

package org.apache.hudi.sync.common.schema;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.mapping.SyncOptions;
import org.apache.hudi.hive.SchemaDifference;

import java.io.Serializable;
import java.util.List;

/**
 * Renders a {@link HoodieSchema} as a catalog's table schema (RFC-88, type plane): HMS and Glue column lists,
 * Spark data source table properties, BigQuery schemas.
 *
 * <p>A renderer owns the table-level rules (partition columns, field comments, case handling) and delegates every
 * column type to the catalog's {@link org.apache.hudi.common.schema.mapping.TypeSystemMapper}, so a type turns
 * lossy only inside the mapper. Implementations are discovered through {@link CatalogSchemaRenderers}.
 *
 * @param <C> the catalog's table schema representation
 */
public interface CatalogSchemaRenderer<C> extends Serializable {

  /**
   * The registry key, e.g. {@code "hive"}; see {@link CatalogSchemaRenderers}.
   */
  String catalog();

  /**
   * Renders the table schema.
   *
   * @param schema          the table schema
   * @param partitionFields the partition columns, in order
   * @param opts            sync options
   */
  C render(HoodieSchema schema, List<String> partitionFields, SyncOptions opts);

  /**
   * The changes that bring the catalog's current schema in line with the table schema. Only catalogs that evolve a
   * column list in place (the HMS family) implement it.
   *
   * @param tableSchema     the table schema
   * @param current         the catalog's current schema
   * @param partitionFields the partition columns, in order
   * @param opts            sync options
   */
  default SchemaDifference diff(HoodieSchema tableSchema, C current, List<String> partitionFields, SyncOptions opts) {
    throw new UnsupportedOperationException("Catalog '" + catalog() + "' does not diff schemas");
  }
}
