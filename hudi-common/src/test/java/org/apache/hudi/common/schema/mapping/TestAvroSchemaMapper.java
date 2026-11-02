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
}
