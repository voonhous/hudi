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

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;

/** Getter choice for Flink values, made once from Flink's own logical type. */
final class FlinkKinds {
  static final int OTHER = 0;
  static final int BYTE = 1;
  static final int SHORT = 2;
  static final int STRING = 3;
  static final int TIMESTAMP = 4;

  private FlinkKinds() {
  }

  static int kindOf(LogicalType type) {
    switch (type.getTypeRoot()) {
      case TINYINT:
        return BYTE;
      case SMALLINT:
        return SHORT;
      case CHAR:
      case VARCHAR:
        return STRING;
      case TIMESTAMP_WITHOUT_TIME_ZONE:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return TIMESTAMP;
      default:
        return OTHER;
    }
  }

  static int precisionOf(LogicalType type) {
    if (type instanceof TimestampType) {
      return ((TimestampType) type).getPrecision();
    } else if (type instanceof LocalZonedTimestampType) {
      return ((LocalZonedTimestampType) type).getPrecision();
    }
    return 0;
  }

  static FlinkRowWrapper rowWrapperOrNull(LogicalType type) {
    return type instanceof RowType ? FlinkRowWrapper.of((RowType) type) : null;
  }

  static FlinkArrayWrapper arrayWrapperOrNull(LogicalType type) {
    return type instanceof ArrayType ? FlinkArrayWrapper.of(((ArrayType) type).getElementType()) : null;
  }

  /** The column's unit: millis for precision 3, micros otherwise (precision up to 6). */
  static long timestampValue(TimestampData t, int precision) {
    return precision <= 3 ? t.getMillisecond() : t.getMillisecond() * 1000 + t.getNanoOfMillisecond() / 1000;
  }

  static void binaryRef(StringData s, BinaryRef ref) {
    if (s instanceof BinaryStringData) {
      BinaryStringData b = (BinaryStringData) s;
      b.ensureMaterialized();
      MemorySegment[] segments = b.getSegments();
      if (segments.length == 1 && !segments[0].isOffHeap()) {
        ref.set(segments[0].getArray(), b.getOffset(), b.getSizeInBytes());
        return;
      }
    }
    byte[] bytes = s.toBytes();
    ref.set(bytes, 0, bytes.length);
  }
}
