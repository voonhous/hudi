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

import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.types.DataType;

/** A Spark ArrayData seen as a HoodieArray. Reused per field; the element wrapper is reused per element. */
public final class SparkArrayWrapper extends SparkGetters implements HoodieArray {
  private final int kind;
  private final SparkRowWrapper elementRow;
  private final SparkArrayWrapper elementArray;

  private SparkArrayWrapper(int kind, SparkRowWrapper elementRow, SparkArrayWrapper elementArray) {
    this.kind = kind;
    this.elementRow = elementRow;
    this.elementArray = elementArray;
  }

  static SparkArrayWrapper of(DataType elementType) {
    return new SparkArrayWrapper(kindOf(elementType), rowWrapperOrNull(elementType), arrayWrapperOrNull(elementType));
  }

  SparkArrayWrapper replace(ArrayData array) {
    this.data = array;
    return this;
  }

  @Override
  public int numElements() {
    return ((ArrayData) data).numElements();
  }

  @Override
  int kindAt(int i) {
    return kind;
  }

  @Override
  SparkRowWrapper rowAt(int i) {
    return elementRow;
  }

  @Override
  SparkArrayWrapper arrayAt(int i) {
    return elementArray;
  }
}
