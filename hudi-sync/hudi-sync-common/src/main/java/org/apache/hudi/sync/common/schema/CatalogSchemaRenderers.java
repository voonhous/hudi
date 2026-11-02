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

import org.apache.hudi.common.schema.mapping.SchemaMappers;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;

import java.util.Map;
import java.util.Set;

/**
 * Registry of {@link CatalogSchemaRenderer}s, discovered with {@link java.util.ServiceLoader} from
 * {@code META-INF/services/org.apache.hudi.sync.common.schema.CatalogSchemaRenderer}.
 */
public final class CatalogSchemaRenderers {

  /**
   * Hive Metastore and the catalogs that share its column model: Glue, ADB, Flink {@code HoodieHiveCatalog}.
   */
  public static final String HIVE = "hive";
  /**
   * Spark data source table properties ({@code spark.sql.sources.schema.*}).
   */
  public static final String SPARK_PROPERTIES = "spark_properties";
  public static final String BIGQUERY = "bigquery";

  private CatalogSchemaRenderers() {
  }

  @SuppressWarnings("unchecked")
  public static <C> CatalogSchemaRenderer<C> get(String catalog) {
    CatalogSchemaRenderer<?> renderer = all().get(catalog);
    if (renderer == null) {
      throw new HoodieException("No CatalogSchemaRenderer registered for '" + catalog + "'. Registered: "
          + all().keySet() + ". Is the module that provides it on the classpath?");
    }
    return (CatalogSchemaRenderer<C>) renderer;
  }

  public static Option<CatalogSchemaRenderer<?>> find(String catalog) {
    return Option.ofNullable(all().get(catalog));
  }

  public static Set<String> catalogs() {
    return all().keySet();
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static Map<String, CatalogSchemaRenderer<?>> all() {
    return (Map) SchemaMappers.load(CatalogSchemaRenderer.class, CatalogSchemaRenderer::catalog);
  }
}
