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

package org.apache.hudi.sink.transform;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.util.Option;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Widens the TINYINT and SMALLINT values of the rows a Flink sink receives to the integer type the write schema
 * stores them as.
 *
 * <p>A table whose write schema stores a TINYINT or SMALLINT column as INT (or INT16 / LONG) is written by writers
 * that read the column with the width of the write schema. Rows coming from the Flink table definition keep the
 * TINYINT / SMALLINT layout, so a binary row's one or two byte value was read as an unsigned int: -1 was stored as
 * 255 (TINYINT) or 65535 (SMALLINT), -128 as 128. This converts such rows to the write schema's types first, so the
 * values are stored sign extended. Rows whose types already match the write schema are not converted.
 */
public final class NarrowIntWidening {

  private NarrowIntWidening() {
  }

  /**
   * The row type the sink writes: {@code rowType} with every TINYINT or SMALLINT (at any depth) whose write schema
   * counterpart is a wider integer replaced by the Flink type of that integer.
   *
   * @param rowType     the row type of the rows the sink receives
   * @param writeSchema the write schema
   * @return the widened row type, or {@code rowType} itself when nothing needs widening
   */
  public static RowType widenedRowType(RowType rowType, HoodieSchema writeSchema) {
    return (RowType) widen(rowType, writeSchema);
  }

  private static LogicalType widen(LogicalType type, HoodieSchema schema) {
    if (schema == null) {
      return type;
    }
    HoodieSchema nonNull = schema.getNonNullType();
    switch (type.getTypeRoot()) {
      case TINYINT:
        switch (nonNull.getType()) {
          case INT16:
            return new SmallIntType(type.isNullable());
          case INT:
            return new IntType(type.isNullable());
          case LONG:
            return new BigIntType(type.isNullable());
          default:
            return type;
        }
      case SMALLINT:
        switch (nonNull.getType()) {
          case INT:
            return new IntType(type.isNullable());
          case LONG:
            return new BigIntType(type.isNullable());
          default:
            return type;
        }
      case ROW: {
        if (nonNull.getType() != HoodieSchemaType.RECORD) {
          return type;
        }
        RowType rowType = (RowType) type;
        List<RowType.RowField> fields = new ArrayList<>(rowType.getFieldCount());
        boolean changed = false;
        for (RowType.RowField field : rowType.getFields()) {
          Option<HoodieSchemaField> schemaField = nonNull.getField(field.getName());
          LogicalType widened = schemaField.isPresent() ? widen(field.getType(), schemaField.get().schema()) : field.getType();
          changed |= widened != field.getType();
          fields.add(new RowType.RowField(field.getName(), widened, field.getDescription().orElse(null)));
        }
        return changed ? new RowType(type.isNullable(), fields) : type;
      }
      case ARRAY: {
        if (nonNull.getType() != HoodieSchemaType.ARRAY) {
          return type;
        }
        LogicalType element = ((ArrayType) type).getElementType();
        LogicalType widened = widen(element, nonNull.getElementType());
        return widened == element ? type : new ArrayType(type.isNullable(), widened);
      }
      case MAP: {
        if (nonNull.getType() != HoodieSchemaType.MAP) {
          return type;
        }
        MapType mapType = (MapType) type;
        LogicalType value = mapType.getValueType();
        LogicalType widened = widen(value, nonNull.getValueType());
        return widened == value ? type : new MapType(type.isNullable(), mapType.getKeyType(), widened);
      }
      default:
        return type;
    }
  }

  /**
   * A function converting rows of {@code from} to rows of {@code to}, a widened row type of {@code from}.
   */
  public static MapFunction<RowData, RowData> converter(RowType from, RowType to) {
    return new WideningFunction(from, to);
  }

  private static class WideningFunction implements MapFunction<RowData, RowData> {
    private static final long serialVersionUID = 1L;

    private final ValueConverter converter;

    WideningFunction(RowType from, RowType to) {
      this.converter = createConverter(from, to);
    }

    @Override
    public RowData map(RowData row) {
      return (RowData) converter.convert(row);
    }
  }

  /**
   * Converts a non-null internal value of one Flink type to the internal value of its widened type.
   */
  private interface ValueConverter extends Serializable {
    Object convert(Object value);
  }

  private static ValueConverter createConverter(LogicalType from, LogicalType to) {
    if (from.equals(to)) {
      return value -> value;
    }
    switch (to.getTypeRoot()) {
      case SMALLINT:
        return value -> ((Number) value).shortValue();
      case INTEGER:
        return value -> ((Number) value).intValue();
      case BIGINT:
        return value -> ((Number) value).longValue();
      case ROW:
        return createRowConverter((RowType) from, (RowType) to);
      case ARRAY: {
        LogicalType fromElement = ((ArrayType) from).getElementType();
        ValueConverter element = createConverter(fromElement, ((ArrayType) to).getElementType());
        ArrayData.ElementGetter getter = ArrayData.createElementGetter(fromElement);
        return value -> convertArray((ArrayData) value, getter, element);
      }
      case MAP: {
        MapType fromMap = (MapType) from;
        ArrayData.ElementGetter keyGetter = ArrayData.createElementGetter(fromMap.getKeyType());
        ArrayData.ElementGetter valueGetter = ArrayData.createElementGetter(fromMap.getValueType());
        ValueConverter valueConverter = createConverter(fromMap.getValueType(), ((MapType) to).getValueType());
        return value -> {
          MapData map = (MapData) value;
          ArrayData keys = map.keyArray();
          ArrayData values = map.valueArray();
          Map<Object, Object> converted = new HashMap<>(map.size());
          for (int i = 0; i < map.size(); i++) {
            Object v = valueGetter.getElementOrNull(values, i);
            converted.put(keyGetter.getElementOrNull(keys, i), v == null ? null : valueConverter.convert(v));
          }
          return new GenericMapData(converted);
        };
      }
      default:
        throw new IllegalArgumentException("Cannot widen " + from + " to " + to);
    }
  }

  private static Object convertArray(ArrayData array, ArrayData.ElementGetter getter, ValueConverter element) {
    Object[] converted = new Object[array.size()];
    for (int i = 0; i < converted.length; i++) {
      Object v = getter.getElementOrNull(array, i);
      converted[i] = v == null ? null : element.convert(v);
    }
    return new GenericArrayData(converted);
  }

  private static ValueConverter createRowConverter(RowType from, RowType to) {
    int arity = from.getFieldCount();
    RowData.FieldGetter[] getters = new RowData.FieldGetter[arity];
    ValueConverter[] converters = new ValueConverter[arity];
    for (int i = 0; i < arity; i++) {
      getters[i] = RowData.createFieldGetter(from.getTypeAt(i), i);
      converters[i] = createConverter(from.getTypeAt(i), to.getTypeAt(i));
    }
    return value -> {
      RowData row = (RowData) value;
      GenericRowData converted = new GenericRowData(row.getRowKind(), arity);
      for (int i = 0; i < arity; i++) {
        Object v = getters[i].getFieldOrNull(row);
        converted.setField(i, v == null ? null : converters[i].convert(v));
      }
      return converted;
    };
  }
}
