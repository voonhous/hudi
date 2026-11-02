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

package org.apache.hudi.common.schema.mapping.testkit;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaType;

import java.util.Objects;

/**
 * One column type the mapper contract tests exercise, e.g. {@code DECIMAL(10,2)} backed by {@code fixed(5)}.
 */
public final class SchemaSample {

  private final String id;
  private final HoodieSchemaType type;
  private final HoodieSchema schema;
  private final Nesting nesting;

  /**
   * Which shapes a sample is checked in.
   */
  public enum Nesting {
    /**
     * Only as a required top-level column.
     */
    NONE,
    /**
     * As a required or nullable top-level column; for types {@code HoodieSchema} allows only at the top level, such
     * as VECTOR.
     */
    TOP_LEVEL,
    /**
     * Also nested in a struct, as an array element and as a map value.
     */
    ANY
  }

  SchemaSample(String id, HoodieSchema schema, Nesting nesting) {
    this.id = id;
    this.type = schema.getType();
    this.schema = schema;
    this.nesting = nesting;
  }

  /**
   * Stable id, used in test names and as the key of {@code expectedColumnTypes()} and {@code knownGaps()}.
   */
  public String id() {
    return id;
  }

  /**
   * The {@link HoodieSchemaType} whose declared support decides how the sample is checked.
   */
  public HoodieSchemaType type() {
    return type;
  }

  /**
   * The column's schema, not nullable.
   */
  public HoodieSchema schema() {
    return schema;
  }

  public Nesting nesting() {
    return nesting;
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof SchemaSample && id.equals(((SchemaSample) o).id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id);
  }

  @Override
  public String toString() {
    return id + "(" + type + ")";
  }
}
