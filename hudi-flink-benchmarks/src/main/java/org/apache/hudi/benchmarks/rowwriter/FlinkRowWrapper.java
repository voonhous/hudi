/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.benchmarks.rowwriter;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.util.List;

/** A Flink RowData seen as a HoodieRow. Getters are chosen from Flink's own types (getShort for SMALLINT). */
public final class FlinkRowWrapper implements HoodieRow {
  private final int[] kinds;
  private final int[] precisions;
  private final FlinkRowWrapper[] childRows;
  private final FlinkArrayWrapper[] childArrays;
  private RowData data;

  private FlinkRowWrapper(int[] kinds, int[] precisions, FlinkRowWrapper[] childRows, FlinkArrayWrapper[] childArrays) {
    this.kinds = kinds;
    this.precisions = precisions;
    this.childRows = childRows;
    this.childArrays = childArrays;
  }

  public static FlinkRowWrapper of(RowType rowType) {
    List<LogicalType> types = rowType.getChildren();
    int n = types.size();
    int[] kinds = new int[n];
    int[] precisions = new int[n];
    FlinkRowWrapper[] rows = new FlinkRowWrapper[n];
    FlinkArrayWrapper[] arrays = new FlinkArrayWrapper[n];
    for (int i = 0; i < n; i++) {
      kinds[i] = FlinkKinds.kindOf(types.get(i));
      precisions[i] = FlinkKinds.precisionOf(types.get(i));
      rows[i] = FlinkKinds.rowWrapperOrNull(types.get(i));
      arrays[i] = FlinkKinds.arrayWrapperOrNull(types.get(i));
    }
    return new FlinkRowWrapper(kinds, precisions, rows, arrays);
  }

  public FlinkRowWrapper replace(RowData row) {
    this.data = row;
    return this;
  }

  @Override
  public int getFieldCount() {
    return kinds.length;
  }

  @Override
  public boolean isNullAt(int i) {
    return data.isNullAt(i);
  }

  @Override
  public boolean getBoolean(int i) {
    return data.getBoolean(i);
  }

  @Override
  public int getInt(int i) {
    switch (kinds[i]) {
      case FlinkKinds.SHORT:
        return data.getShort(i);
      case FlinkKinds.BYTE:
        return data.getByte(i);
      default:
        return data.getInt(i);
    }
  }

  @Override
  public long getLong(int i) {
    return kinds[i] == FlinkKinds.TIMESTAMP
        ? FlinkKinds.timestampValue(data.getTimestamp(i, precisions[i]), precisions[i])
        : data.getLong(i);
  }

  @Override
  public float getFloat(int i) {
    return data.getFloat(i);
  }

  @Override
  public double getDouble(int i) {
    return data.getDouble(i);
  }

  @Override
  public void getBinaryRef(int i, BinaryRef ref) {
    if (kinds[i] == FlinkKinds.STRING) {
      FlinkKinds.binaryRef(data.getString(i), ref);
    } else {
      byte[] bytes = data.getBinary(i);
      ref.set(bytes, 0, bytes.length);
    }
  }

  @Override
  public byte[] getBinary(int i) {
    return kinds[i] == FlinkKinds.STRING ? data.getString(i).toBytes() : data.getBinary(i);
  }

  @Override
  public long getDecimalUnscaledLong(int i, int precision, int scale) {
    return data.getDecimal(i, precision, scale).toUnscaledLong();
  }

  @Override
  public byte[] getDecimalUnscaledBytes(int i, int precision, int scale) {
    return data.getDecimal(i, precision, scale).toUnscaledBytes();
  }

  @Override
  public HoodieRow getRow(int i, int numFields) {
    return childRows[i].replace(data.getRow(i, numFields));
  }

  @Override
  public HoodieArray getArray(int i) {
    return childArrays[i].replace(data.getArray(i));
  }
}
