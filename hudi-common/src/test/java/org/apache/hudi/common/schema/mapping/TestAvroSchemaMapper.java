/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.schema.mapping;

import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.schema.mapping.testkit.FormatSchemaMapperContractTest;

import org.apache.avro.Schema;

import java.util.HashMap;
import java.util.Map;

/**
 * Avro carries every Hudi type natively, so the reference mapper passes the contract with no known gaps. It also
 * keeps the test kit honest: a kit change that breaks a lossless mapper shows up here first.
 */
class TestAvroSchemaMapper extends FormatSchemaMapperContractTest<Schema> {

  @Override
  protected FormatSchemaMapper<Schema> mapper() {
    return new AvroSchemaMapper();
  }

  @Override
  protected Map<HoodieSchemaType, TypeSupport.Level> expectedSupport() {
    return nativeExcept();
  }

  /**
   * The persisted form of the Hudi logical types: the Avro base type plus the {@code logicalType} property (and its
   * parameters), which readers without them ignore.
   */
  @Override
  protected Map<String, Schema> expectedColumnTypes() {
    Map<String, Schema> types = new HashMap<>();
    types.put("int8", new Schema.Parser().parse("{\"type\":\"int\",\"logicalType\":\"int8\"}"));
    types.put("int16", new Schema.Parser().parse("{\"type\":\"int\",\"logicalType\":\"int16\"}"));
    types.put("char_10", new Schema.Parser().parse("{\"type\":\"string\",\"logicalType\":\"char\",\"length\":10}"));
    types.put("int", Schema.create(Schema.Type.INT));
    types.put("string", Schema.create(Schema.Type.STRING));
    return types;
  }

  @Override
  protected Schema columnType(Schema record, String column) {
    return record.getField(column).schema();
  }
}
