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

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/** A Spark InternalRow seen as a HoodieRow. Reused across rows; nested wrappers reused per field. */
public final class SparkRowWrapper extends SparkGetters implements HoodieRow {
  private final int[] kinds;
  private final SparkRowWrapper[] childRows;
  private final SparkArrayWrapper[] childArrays;

  private SparkRowWrapper(int[] kinds, SparkRowWrapper[] childRows, SparkArrayWrapper[] childArrays) {
    this.kinds = kinds;
    this.childRows = childRows;
    this.childArrays = childArrays;
  }

  public static SparkRowWrapper of(StructType structType) {
    StructField[] fields = structType.fields();
    int[] kinds = new int[fields.length];
    SparkRowWrapper[] rows = new SparkRowWrapper[fields.length];
    SparkArrayWrapper[] arrays = new SparkArrayWrapper[fields.length];
    for (int i = 0; i < fields.length; i++) {
      kinds[i] = kindOf(fields[i].dataType());
      rows[i] = rowWrapperOrNull(fields[i].dataType());
      arrays[i] = arrayWrapperOrNull(fields[i].dataType());
    }
    return new SparkRowWrapper(kinds, rows, arrays);
  }

  public SparkRowWrapper replace(InternalRow row) {
    this.data = row;
    return this;
  }

  @Override
  public int getFieldCount() {
    return kinds.length;
  }

  @Override
  int kindAt(int i) {
    return kinds[i];
  }

  @Override
  SparkRowWrapper rowAt(int i) {
    return childRows[i];
  }

  @Override
  SparkArrayWrapper arrayAt(int i) {
    return childArrays[i];
  }
}
