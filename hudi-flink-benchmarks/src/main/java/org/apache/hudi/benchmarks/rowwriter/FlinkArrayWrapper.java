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

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.types.logical.LogicalType;

/** A Flink ArrayData seen as a HoodieArray. */
public final class FlinkArrayWrapper implements HoodieArray {
  private final int kind;
  private final int precision;
  private final FlinkRowWrapper elementRow;
  private final FlinkArrayWrapper elementArray;
  private ArrayData data;

  private FlinkArrayWrapper(int kind, int precision, FlinkRowWrapper elementRow, FlinkArrayWrapper elementArray) {
    this.kind = kind;
    this.precision = precision;
    this.elementRow = elementRow;
    this.elementArray = elementArray;
  }

  static FlinkArrayWrapper of(LogicalType elementType) {
    return new FlinkArrayWrapper(FlinkKinds.kindOf(elementType), FlinkKinds.precisionOf(elementType),
        FlinkKinds.rowWrapperOrNull(elementType), FlinkKinds.arrayWrapperOrNull(elementType));
  }

  FlinkArrayWrapper replace(ArrayData array) {
    this.data = array;
    return this;
  }

  @Override
  public int numElements() {
    return data.size();
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
    switch (kind) {
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
    return kind == FlinkKinds.TIMESTAMP
        ? FlinkKinds.timestampValue(data.getTimestamp(i, precision), precision)
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
    if (kind == FlinkKinds.STRING) {
      FlinkKinds.binaryRef(data.getString(i), ref);
    } else {
      byte[] bytes = data.getBinary(i);
      ref.set(bytes, 0, bytes.length);
    }
  }

  @Override
  public byte[] getBinary(int i) {
    return kind == FlinkKinds.STRING ? data.getString(i).toBytes() : data.getBinary(i);
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
    return elementRow.replace(data.getRow(i, numFields));
  }

  @Override
  public HoodieArray getArray(int i) {
    return elementArray.replace(data.getArray(i));
  }
}
