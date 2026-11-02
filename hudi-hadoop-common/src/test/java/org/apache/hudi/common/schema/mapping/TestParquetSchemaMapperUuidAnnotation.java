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

import org.apache.hudi.common.schema.mapping.testkit.SchemaSamples;

import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;

import java.util.Map;

/**
 * {@link TestParquetSchemaMapper} with {@code parquet.avro.write-parquet-uuid=true}: UUID is written as
 * {@code FIXED_LEN_BYTE_ARRAY(16) + UUID} and maps NATIVE (RFC-88 section 1 calls UUID lossy only because the
 * annotation is off by default).
 */
class TestParquetSchemaMapperUuidAnnotation extends TestParquetSchemaMapper {

  @Override
  protected PhysicalOptions options() {
    return PhysicalOptions.builder().set(ParquetSchemaMapper.WRITE_PARQUET_UUID, true).build();
  }

  @Override
  protected TypeSupport.Level uuidLevel() {
    return TypeSupport.Level.NATIVE;
  }

  @Override
  protected Map<String, MessageType> expectedColumnTypes() {
    Map<String, MessageType> expected = super.expectedColumnTypes();
    expected.put("uuid", new MessageType(SchemaSamples.RECORD_NAME, Types.required(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
        .length(16).as(LogicalTypeAnnotation.uuidType()).named(SchemaSamples.COLUMN)));
    return expected;
  }
}
