/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.hudi.common.util.Option;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Takes INT8, INT16 and CHAR(n) back into a schema converted from an InternalSchema (RFC-88).
 *
 * <p>InternalSchema has no INT8, INT16 or CHAR: converted back, such columns are INT and STRING. For a table that maps
 * Flink TINYINT, SMALLINT and CHAR(n) to them, {@link #restore} takes them back from the schemas the InternalSchema
 * was evolved from (the writer schema, the table schema). A column the sources agree on keeps its type: INT8 and
 * INT16 give the wider of the two, CHAR(n) its length; when a source has INT (or STRING, or another CHAR length) for
 * it, the column stays INT (STRING), as a type widening does.
 */
public final class NarrowTypeRestorer {

  private NarrowTypeRestorer() {
  }

  /**
   * {@code converted} with INT8, INT16 and CHAR(n) taken back from {@code sources} where it has INT or STRING.
   *
   * @param converted the schema converted from an InternalSchema
   * @param sources   the schemas to take the types from; a null source is skipped
   * @return {@code converted} itself if nothing is taken back
   */
  public static HoodieSchema restore(HoodieSchema converted, List<HoodieSchema> sources) {
    List<HoodieSchema> present = sources.stream().filter(Objects::nonNull).collect(Collectors.toList());
    return present.isEmpty() ? converted : restoreField(converted, present);
  }

  private static HoodieSchema restoreField(HoodieSchema converted, List<HoodieSchema> sources) {
    if (converted.isNullable() && converted.isComplexUnion()) {
      return converted;
    }
    HoodieSchema nonNull = converted.getNonNullType();
    List<HoodieSchema> nonNullSources = sources.stream()
        .filter(s -> !(s.isNullable() && s.isComplexUnion()))
        .map(HoodieSchema::getNonNullType)
        .collect(Collectors.toList());
    if (nonNullSources.isEmpty()) {
      return converted;
    }
    HoodieSchema restored = restoreNonNull(nonNull, nonNullSources);
    if (restored == nonNull) {
      return converted;
    }
    return converted.isNullable() ? HoodieSchema.createNullable(restored) : restored;
  }

  private static HoodieSchema restoreNonNull(HoodieSchema converted, List<HoodieSchema> sources) {
    switch (converted.getType()) {
      case INT: {
        HoodieSchemaType widest = null;
        for (HoodieSchema source : sources) {
          HoodieSchemaType type = source.getType();
          if (type != HoodieSchemaType.INT8 && type != HoodieSchemaType.INT16) {
            return converted;
          }
          widest = widest == HoodieSchemaType.INT16 ? widest : type;
        }
        return widest == HoodieSchemaType.INT8 ? HoodieSchema.createInt8() : HoodieSchema.createInt16();
      }
      case STRING: {
        Integer length = null;
        for (HoodieSchema source : sources) {
          if (source.getType() != HoodieSchemaType.CHAR) {
            return converted;
          }
          int sourceLength = ((HoodieSchema.Char) source).getLength();
          if (length != null && length != sourceLength) {
            return converted;
          }
          length = sourceLength;
        }
        return HoodieSchema.createChar(length);
      }
      case RECORD: {
        List<HoodieSchemaField> fields = converted.getFields();
        List<HoodieSchemaField> restoredFields = new ArrayList<>(fields.size());
        boolean changed = false;
        for (HoodieSchemaField field : fields) {
          List<HoodieSchema> fieldSources = new ArrayList<>();
          for (HoodieSchema source : sources) {
            if (source.getType() == HoodieSchemaType.RECORD) {
              Option<HoodieSchemaField> sourceField = source.getField(field.name());
              if (sourceField.isPresent()) {
                fieldSources.add(sourceField.get().schema());
              }
            }
          }
          HoodieSchema fieldSchema = field.schema();
          HoodieSchema restored = fieldSources.isEmpty() ? fieldSchema : restoreField(fieldSchema, fieldSources);
          changed |= restored != fieldSchema;
          restoredFields.add(field.withSchema(restored));
        }
        return changed
            ? HoodieSchema.createRecord(converted.getName(), converted.getNamespace().orElse(null), converted.getDoc().orElse(null), restoredFields)
            : converted;
      }
      case ARRAY: {
        List<HoodieSchema> elements = sources.stream().filter(s -> s.getType() == HoodieSchemaType.ARRAY)
            .map(HoodieSchema::getElementType).collect(Collectors.toList());
        HoodieSchema element = converted.getElementType();
        HoodieSchema restored = elements.isEmpty() ? element : restoreField(element, elements);
        return restored == element ? converted : HoodieSchema.createArray(restored);
      }
      case MAP: {
        List<HoodieSchema> values = sources.stream().filter(s -> s.getType() == HoodieSchemaType.MAP)
            .map(HoodieSchema::getValueType).collect(Collectors.toList());
        HoodieSchema value = converted.getValueType();
        HoodieSchema restored = values.isEmpty() ? value : restoreField(value, values);
        return restored == value ? converted : HoodieSchema.createMap(restored);
      }
      default:
        return converted;
    }
  }

  /**
   * As {@link #restore(HoodieSchema, List)} with one source.
   */
  public static HoodieSchema restore(HoodieSchema converted, HoodieSchema source) {
    return restore(converted, Collections.singletonList(source));
  }
}
