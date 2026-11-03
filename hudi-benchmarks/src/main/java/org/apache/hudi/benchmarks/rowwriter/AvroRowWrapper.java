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
import org.apache.avro.generic.IndexedRecord;

import java.util.List;

/** An Avro IndexedRecord seen as a HoodieRow. */
public final class AvroRowWrapper extends AvroGetters implements HoodieRow {
  private final AvroRowWrapper[] childRows;
  private final AvroArrayWrapper[] childArrays;
  private IndexedRecord record;

  private AvroRowWrapper(AvroRowWrapper[] childRows, AvroArrayWrapper[] childArrays) {
    this.childRows = childRows;
    this.childArrays = childArrays;
  }

  public static AvroRowWrapper of(Schema recordSchema) {
    List<Schema.Field> fields = AvroGetters.nonNull(recordSchema).getFields();
    AvroRowWrapper[] rows = new AvroRowWrapper[fields.size()];
    AvroArrayWrapper[] arrays = new AvroArrayWrapper[fields.size()];
    for (int i = 0; i < rows.length; i++) {
      rows[i] = rowWrapperOrNull(fields.get(i).schema());
      arrays[i] = arrayWrapperOrNull(fields.get(i).schema());
    }
    return new AvroRowWrapper(rows, arrays);
  }

  public AvroRowWrapper replace(IndexedRecord record) {
    this.record = record;
    return this;
  }

  @Override
  public int getFieldCount() {
    return childRows.length;
  }

  @Override
  Object value(int i) {
    return record.get(i);
  }

  @Override
  AvroRowWrapper rowAt(int i) {
    return childRows[i];
  }

  @Override
  AvroArrayWrapper arrayAt(int i) {
    return childArrays[i];
  }
}
