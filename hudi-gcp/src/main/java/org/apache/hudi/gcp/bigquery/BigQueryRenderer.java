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
import org.apache.hudi.common.schema.mapping.MappingOptions;
import org.apache.hudi.common.schema.mapping.SyncOptions;
import org.apache.hudi.sync.common.schema.CatalogSchemaRenderer;
import org.apache.hudi.sync.common.schema.CatalogSchemaRenderers;

import com.google.cloud.bigquery.Schema;

import java.util.List;
import java.util.stream.Collectors;

/**
 * {@link CatalogSchemaRenderer} for BigQuery (RFC-88, type plane): the table part of {@link BigQuerySchemaResolver}.
 *
 * <p>{@link #render} returns what {@code BigQuerySchemaResolver#getTableSchema} returns for the table schema: every
 * column except the partition fields, which BigQuery derives from the source URI (BigQuery rejects them as duplicate
 * columns). Column types come from {@link BigQueryTypeMapper}, which wraps the same resolver. BigQuery sync has no
 * schema options today. The sync client replaces the whole schema on change, so {@link #diff} is not supported.
 *
 * <p>Replaces, in RFC-88 step 2, the table part of {@code BigQuerySchemaResolver}.
 */
public final class BigQueryRenderer implements CatalogSchemaRenderer<Schema> {

  private static final long serialVersionUID = 1L;

  private static final BigQueryTypeMapper TYPES = new BigQueryTypeMapper();

  @Override
  public String catalog() {
    return CatalogSchemaRenderers.BIGQUERY;
  }

  @Override
  public Schema render(HoodieSchema schema, List<String> partitionFields, SyncOptions opts) {
    return Schema.of(TYPES.toTypeSystem(schema, MappingOptions.DEFAULT).getSubFields().stream()
        .filter(field -> !partitionFields.contains(field.getName()))
        .collect(Collectors.toList()));
  }
}
