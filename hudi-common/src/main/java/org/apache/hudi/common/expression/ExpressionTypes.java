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

package org.apache.hudi.common.expression;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.internal.Types;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

/**
 * Type helpers for {@link Expression}s typed with {@link HoodieSchema}.
 *
 * <p>Expressions used to be typed with InternalSchema types. These helpers keep the runtime behaviour of
 * that typing: {@link #classTag(HoodieSchema)} returns the Java class that {@code Type.typeId().getClassTag()}
 * gave for the InternalSchema type {@code InternalSchemaConverter} maps the schema to, and
 * {@link #isNested(HoodieSchema)} answers what {@code Type.isNestedType()} answered for it.
 */
public final class ExpressionTypes {

  private ExpressionTypes() {
  }

  /**
   * Returns the Java class a value of the given leaf schema is read as from a {@link StructLike}.
   *
   * @param schema a non-null, non-union schema
   * @return the class passed to {@link StructLike#get(int, Class)}
   * @throws IllegalArgumentException for a {@code null} schema, the NULL type or a union
   */
  public static Class<?> classTag(HoodieSchema schema) {
    if (schema == null) {
      throw new IllegalArgumentException("Expression type cannot be null");
    }
    switch (schema.getType()) {
      case RECORD:
      case VARIANT:
      case BLOB:
        // RFC-88 bug list: kept from the InternalSchema typing, whose class tag for a record is the schema
        // class Types.RecordType rather than a class of record values, so ArrayData cannot return a
        // non-null record value.
        return Types.RecordType.class;
      case ARRAY:
        return List.class;
      case MAP:
        return Map.class;
      case FIXED:
      case BYTES:
      case VECTOR:
        return ByteBuffer.class;
      case STRING:
      case ENUM:
        return String.class;
      case INT:
      case DATE:
        return Integer.class;
      case LONG:
      case TIMESTAMP:
        return Long.class;
      case TIME:
        return ((HoodieSchema.Time) schema).getPrecision() == HoodieSchema.TimePrecision.MILLIS
            ? Integer.class : Long.class;
      case FLOAT:
        return Float.class;
      case DOUBLE:
        return Double.class;
      case BOOLEAN:
        return Boolean.class;
      case DECIMAL:
        return BigDecimal.class;
      case UUID:
        return UUID.class;
      default:
        throw new IllegalArgumentException("Expression type must be a non-null leaf type, got: " + schema);
    }
  }

  /**
   * Whether the schema is a nested type, i.e. one that maps to an InternalSchema record, array or map.
   */
  public static boolean isNested(HoodieSchema schema) {
    switch (schema.getType()) {
      case RECORD:
      case VARIANT:
      case BLOB:
      case ARRAY:
      case MAP:
        return true;
      default:
        return false;
    }
  }

  /**
   * Short lower-case name of the schema's type, used in expression descriptions.
   */
  static String typeName(HoodieSchema schema) {
    return schema.getType().name().toLowerCase(Locale.ROOT);
  }
}
