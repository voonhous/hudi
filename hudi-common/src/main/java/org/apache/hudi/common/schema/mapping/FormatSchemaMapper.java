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
 * Maps between {@link HoodieSchema} and the physical schema of one file format: Parquet {@code MessageType}, ORC
 * {@code TypeDescription}, Arrow {@code Schema} (Lance), Avro {@code Schema} (RFC-88, type plane). This is the only
 * place a physical schema is decided.
 *
 * <p>The same rules as {@link TypeSystemMapper} apply. Physical layout knobs that exist today (legacy list and
 * decimal layouts, timestamp NTZ, variant shredding, blob encoding) are {@link PhysicalOptions} with today's
 * defaults.
 *
 * <p>Implementations are discovered through {@link SchemaMappers} with {@link java.util.ServiceLoader}, so they must
 * have a public no-arg constructor and be listed in
 * {@code META-INF/services/org.apache.hudi.common.schema.mapping.FormatSchemaMapper}.
 *
 * @param <P> the format's physical schema type, e.g. Parquet {@code MessageType}
 */
public interface FormatSchemaMapper<P> extends Serializable {

  /**
   * The registry key, e.g. {@code "parquet"}; see {@link FileFormats}.
   *
   * <p>A string rather than {@link org.apache.hudi.common.model.HoodieFileFormat}: Avro is a format here (data
   * blocks, the persisted schema) but not a {@code HoodieFileFormat} member until the reader / writer step of RFC-88
   * adds it. For every other format the key is the {@code HoodieFileFormat} name in lower case.
   */
  String format();

  /**
   * Converts a Hudi schema to this format's physical schema.
   */
  P toPhysicalSchema(HoodieSchema schema, PhysicalOptions opts);

  /**
   * Converts a physical schema read from a file to a Hudi schema.
   *
   * @param physical the physical schema, e.g. from a file footer
   * @param hint     the table schema, when one exists; it restores types the format drops (LOSSY)
   */
  HoodieSchema fromPhysicalSchema(P physical, Option<HoodieSchema> hint);

  /**
   * How this format maps the given Hudi type under the given options.
   */
  TypeSupport support(HoodieSchemaType type, PhysicalOptions opts);

  default P toPhysicalSchema(HoodieSchema schema) {
    return toPhysicalSchema(schema, PhysicalOptions.DEFAULT);
  }

  default HoodieSchema fromPhysicalSchema(P physical) {
    return fromPhysicalSchema(physical, Option.empty());
  }

  default TypeSupport support(HoodieSchemaType type) {
    return support(type, PhysicalOptions.DEFAULT);
  }
}
