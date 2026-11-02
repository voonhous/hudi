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

package org.apache.hudi.util;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.schema.mapping.HoodieSchemaEquivalence;
import org.apache.hudi.common.schema.mapping.HoodieSchemaHints;
import org.apache.hudi.common.util.Option;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * Restores INT8, INT16 and CHAR(n) after a round trip through {@code InternalSchema}, which has none of them and turns
 * them into INT and STRING (RFC-88; {@code InternalSchema} itself stays as it is until RFC-104 replaces it).
 *
 * <p>A column takes its type back from the source's schema (the writer's) where the table's schema has the same type
 * for it or no such column, so a write never narrows an existing column, and from the table's schema for the columns
 * the source does not have, so a write does not widen them either. Tables whose schemas have none of these types
 * (every table created before {@code hoodie.table.type.system.version} existed, and whose writers' schemas come with
 * the mapping of that table) are returned as they are.
 */
public final class NarrowIntAndCharTypes {

  private static final Set<HoodieSchemaType> NARROW_TYPES = EnumSet.of(HoodieSchemaType.INT8, HoodieSchemaType.INT16, HoodieSchemaType.CHAR);

  private NarrowIntAndCharTypes() {
  }

  /**
   * @param roundTripped the schema that went through {@code InternalSchema}
   * @param source       the writer's schema, if any
   * @param table        the table's schema before the write, if any
   */
  public static HoodieSchema restore(HoodieSchema roundTripped, Option<HoodieSchema> source, Option<HoodieSchema> table) {
    boolean sourceHasThem = source.isPresent() && containsNarrowType(source.get());
    boolean tableHasThem = table.isPresent() && containsNarrowType(table.get());
    if (!sourceHasThem && !tableHasThem) {
      return roundTripped;
    }
    HoodieSchema hint;
    if (!source.isPresent()) {
      hint = table.get();
    } else if (!table.isPresent()) {
      hint = source.get();
    } else {
      hint = agreeing(source.get(), table.get());
    }
    return restoreFrom(roundTripped, hint);
  }

  /**
   * Takes INT8, INT16 and CHAR(n) from the hint wherever the schema has the type they become in {@code InternalSchema}
   * (INT, STRING) for the same column.
   */
  public static HoodieSchema restoreFrom(HoodieSchema schema, HoodieSchema hint) {
    return HoodieSchemaHints.restore(schema, Option.ofNullable(hint), NARROW_TYPES::contains, NarrowIntAndCharTypes::widened,
        HoodieSchemaEquivalence.Mode.LOGICAL);
  }

  /**
   * The type INT8, INT16 or CHAR(n) becomes in {@code InternalSchema}.
   */
  public static HoodieSchema widened(HoodieSchema schema) {
    return HoodieSchema.create(schema.getType() == HoodieSchemaType.CHAR ? HoodieSchemaType.STRING : HoodieSchemaType.INT);
  }

  public static boolean containsNarrowType(HoodieSchema schema) {
    if (schema == null) {
      return false;
    }
    if (NARROW_TYPES.contains(schema.getType())) {
      return true;
    }
    switch (schema.getType()) {
      case RECORD:
        return schema.getFields().stream().anyMatch(f -> containsNarrowType(f.schema()));
      case ARRAY:
        return containsNarrowType(schema.getElementType());
      case MAP:
        return containsNarrowType(schema.getValueType());
      case UNION:
        return schema.getTypes().stream().anyMatch(NarrowIntAndCharTypes::containsNarrowType);
      default:
        return false;
    }
  }

  /**
   * The source's schema with each INT8, INT16 or CHAR(n) column widened where the table has another type for it, and
   * with the table's columns the source does not have.
   */
  private static HoodieSchema agreeing(HoodieSchema source, HoodieSchema table) {
    HoodieSchema s = nonNull(source);
    HoodieSchema t = table == null ? null : nonNull(table);
    HoodieSchema result;
    if (NARROW_TYPES.contains(s.getType())) {
      result = t == null || t.equals(s) ? s : widened(s);
    } else if (s.getType() == HoodieSchemaType.RECORD && t != null && t.getType() == HoodieSchemaType.RECORD) {
      List<HoodieSchemaField> fields = new ArrayList<>();
      for (HoodieSchemaField f : s.getFields()) {
        Option<HoodieSchemaField> tableField = t.getField(f.name());
        fields.add(f.withSchema(agreeing(f.schema(), tableField.isPresent() ? tableField.get().schema() : null)));
      }
      for (HoodieSchemaField f : t.getFields()) {
        if (!s.getField(f.name()).isPresent()) {
          fields.add(f.withSchema(f.schema()));
        }
      }
      result = HoodieSchema.createRecord(s.getName(), s.getNamespace().orElse(null), s.getDoc().orElse(null), fields);
    } else if (s.getType() == HoodieSchemaType.ARRAY && t != null) {
      result = HoodieSchema.createArray(agreeing(s.getElementType(),
          t.getType() == HoodieSchemaType.ARRAY ? t.getElementType() : HoodieSchema.create(HoodieSchemaType.NULL)));
    } else if (s.getType() == HoodieSchemaType.MAP && t != null) {
      result = HoodieSchema.createMap(agreeing(s.getValueType(),
          t.getType() == HoodieSchemaType.MAP ? t.getValueType() : HoodieSchema.create(HoodieSchemaType.NULL)));
    } else {
      result = s;
    }
    if (result == s) {
      return source;
    }
    return source.isNullable() && !source.isComplexUnion() ? HoodieSchema.createNullable(result) : result;
  }

  private static HoodieSchema nonNull(HoodieSchema schema) {
    return schema.isNullable() && !schema.isComplexUnion() ? schema.getNonNullType() : schema;
  }
}
