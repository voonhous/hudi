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

import org.apache.avro.Schema;

import java.util.List;

/** An Avro array (a java.util.List) seen as a HoodieArray. */
public final class AvroArrayWrapper extends AvroGetters implements HoodieArray {
  private final AvroRowWrapper elementRow;
  private final AvroArrayWrapper elementArray;
  private List<?> list;

  private AvroArrayWrapper(AvroRowWrapper elementRow, AvroArrayWrapper elementArray) {
    this.elementRow = elementRow;
    this.elementArray = elementArray;
  }

  static AvroArrayWrapper of(Schema elementSchema) {
    return new AvroArrayWrapper(rowWrapperOrNull(elementSchema), arrayWrapperOrNull(elementSchema));
  }

  AvroArrayWrapper replace(List<?> list) {
    this.list = list;
    return this;
  }

  @Override
  public int numElements() {
    return list.size();
  }

  @Override
  Object value(int i) {
    return list.get(i);
  }

  @Override
  AvroRowWrapper rowAt(int i) {
    return elementRow;
  }

  @Override
  AvroArrayWrapper arrayAt(int i) {
    return elementArray;
  }
}
