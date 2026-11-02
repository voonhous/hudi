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

package org.apache.hudi.common.schema.mapping;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.util.Option;

import java.io.Serializable;

/**
 * Maps between {@link HoodieSchema} and the type system of one engine or catalog: Spark, Flink, Hive, Trino,
 * BigQuery (RFC-88, type plane).
 *
 * <p>Rules every implementation follows:
 * <ol>
 *   <li>It maps only between {@link HoodieSchema} and its own type system, never to another engine, format or
 *   catalog.</li>
 *   <li>{@link #support} declares, per {@link HoodieSchemaType} and options, whether the type maps
 *   {@code NATIVE}, {@code LOSSY} or {@code UNSUPPORTED}.</li>
 *   <li>{@code fromTypeSystem(toTypeSystem(s), empty)} is equivalent to {@code s} for every {@code NATIVE} type, and
 *   {@code fromTypeSystem(toTypeSystem(s), Option.of(s))} is equivalent to {@code s} for every {@code LOSSY} type.</li>
 *   <li>It passes the shared mapper contract tests in {@code hudi-common}'s test jar.</li>
 * </ol>
 *
 * <p>Implementations are discovered through {@link SchemaMappers} with {@link java.util.ServiceLoader}, so they must
 * have a public no-arg constructor and be listed in
 * {@code META-INF/services/org.apache.hudi.common.schema.mapping.TypeSystemMapper}.
 *
 * @param <T> the type system's schema type, e.g. Spark {@code DataType}
 */
public interface TypeSystemMapper<T> extends Serializable {

  /**
   * The registry key, e.g. {@code "spark"}; see {@link TypeSystems}.
   */
  String typeSystem();

  /**
   * Converts a Hudi schema to this type system.
   */
  T toTypeSystem(HoodieSchema schema, MappingOptions opts);

  /**
   * Converts a type of this type system to a Hudi schema.
   *
   * @param type the type to convert
   * @param hint the table schema, when one exists; it restores types this type system drops (LOSSY)
   */
  HoodieSchema fromTypeSystem(T type, Option<HoodieSchema> hint);

  /**
   * How this type system maps the given Hudi type under the given options.
   */
  TypeSupport support(HoodieSchemaType type, MappingOptions opts);

  default T toTypeSystem(HoodieSchema schema) {
    return toTypeSystem(schema, MappingOptions.DEFAULT);
  }

  default HoodieSchema fromTypeSystem(T type) {
    return fromTypeSystem(type, Option.empty());
  }

  default TypeSupport support(HoodieSchemaType type) {
    return support(type, MappingOptions.DEFAULT);
  }
}
