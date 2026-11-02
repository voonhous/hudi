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
import org.apache.hudi.util.HoodieSchemaConverter;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Tests {@link NarrowIntWidening}.
 */
public class TestNarrowIntWidening {

  private static final RowType ROW_TYPE = (RowType) DataTypes.ROW(
      DataTypes.FIELD("id", DataTypes.INT().notNull()),
      DataTypes.FIELD("f_tiny", DataTypes.TINYINT()),
      DataTypes.FIELD("f_small", DataTypes.SMALLINT()),
      DataTypes.FIELD("f_array", DataTypes.ARRAY(DataTypes.TINYINT())),
      DataTypes.FIELD("f_map", DataTypes.MAP(DataTypes.STRING(), DataTypes.SMALLINT())),
      DataTypes.FIELD("f_row", DataTypes.ROW(DataTypes.FIELD("f_nested", DataTypes.TINYINT())))).notNull().getLogicalType();

  @Test
  void testWidensToTheWriteSchema() throws Exception {
    // the schema convertToSchema infers today: TINYINT and SMALLINT as INT, at every depth
    HoodieSchema intSchema = HoodieSchemaConverter.convertToSchema(ROW_TYPE);
    RowType widened = NarrowIntWidening.widenedRowType(ROW_TYPE, intSchema);
    RowType expected = (RowType) DataTypes.ROW(
        DataTypes.FIELD("id", DataTypes.INT().notNull()),
        DataTypes.FIELD("f_tiny", DataTypes.INT()),
        DataTypes.FIELD("f_small", DataTypes.INT()),
        DataTypes.FIELD("f_array", DataTypes.ARRAY(DataTypes.INT())),
        DataTypes.FIELD("f_map", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT())),
        DataTypes.FIELD("f_row", DataTypes.ROW(DataTypes.FIELD("f_nested", DataTypes.INT())))).notNull().getLogicalType();
    assertEquals(expected, widened);

    // a binary row, as the sink receives it: the converter reads the narrow values and widens them sign extended
    GenericRowData row = GenericRowData.ofKind(RowKind.UPDATE_AFTER, 1, (byte) -1, (short) -32768,
        new GenericArrayData(new Object[] {(byte) -128, null, (byte) 127}),
        new GenericMapData(Collections.singletonMap(StringData.fromString("k"), (short) -1)),
        GenericRowData.of((byte) -2));
    BinaryRowData binary = new RowDataSerializer(ROW_TYPE).toBinaryRow(row);
    RowData converted = NarrowIntWidening.converter(ROW_TYPE, widened).map(binary);

    assertEquals(RowKind.UPDATE_AFTER, converted.getRowKind());
    assertEquals(1, converted.getInt(0));
    assertEquals(-1, converted.getInt(1));
    assertEquals(-32768, converted.getInt(2));
    assertEquals(-128, converted.getArray(3).getInt(0));
    assertEquals(true, converted.getArray(3).isNullAt(1));
    assertEquals(127, converted.getArray(3).getInt(2));
    assertEquals(-1, converted.getMap(4).valueArray().getInt(0));
    assertEquals("k", converted.getMap(4).keyArray().getString(0).toString());
    assertEquals(-2, converted.getRow(5, 1).getInt(0));
    // the widened row serializes with the widened type
    assertEquals(-1, new RowDataSerializer(widened).toBinaryRow(converted).getInt(1));

    GenericRowData nulls = GenericRowData.of(2, null, null, null, null, null);
    RowData convertedNulls = NarrowIntWidening.converter(ROW_TYPE, widened).map(nulls);
    assertEquals(true, convertedNulls.isNullAt(1));
    assertNull(((GenericRowData) convertedNulls).getField(3));
  }

  @Test
  void testKeepsTypesTheWriteSchemaStores() {
    HoodieSchema narrowSchema = HoodieSchema.parse("{\"type\":\"record\",\"name\":\"r\",\"fields\":["
        + "{\"name\":\"id\",\"type\":\"int\"},"
        + "{\"name\":\"f_tiny\",\"type\":[\"null\",{\"type\":\"int\",\"logicalType\":\"int8\"}],\"default\":null},"
        + "{\"name\":\"f_small\",\"type\":[\"null\",{\"type\":\"int\",\"logicalType\":\"int16\"}],\"default\":null}]}");
    RowType rowType = (RowType) DataTypes.ROW(
        DataTypes.FIELD("id", DataTypes.INT().notNull()),
        DataTypes.FIELD("f_tiny", DataTypes.TINYINT()),
        DataTypes.FIELD("f_small", DataTypes.SMALLINT())).notNull().getLogicalType();
    assertSame(rowType, NarrowIntWidening.widenedRowType(rowType, narrowSchema));

    // TINYINT into INT16 and SMALLINT into LONG
    HoodieSchema wider = HoodieSchema.parse("{\"type\":\"record\",\"name\":\"r\",\"fields\":["
        + "{\"name\":\"id\",\"type\":\"int\"},"
        + "{\"name\":\"f_tiny\",\"type\":[\"null\",{\"type\":\"int\",\"logicalType\":\"int16\"}],\"default\":null},"
        + "{\"name\":\"f_small\",\"type\":[\"null\",\"long\"],\"default\":null}]}");
    assertEquals(DataTypes.ROW(
        DataTypes.FIELD("id", DataTypes.INT().notNull()),
        DataTypes.FIELD("f_tiny", DataTypes.SMALLINT()),
        DataTypes.FIELD("f_small", DataTypes.BIGINT())).notNull().getLogicalType(), NarrowIntWidening.widenedRowType(rowType, wider));
  }
}
