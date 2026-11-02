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

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.util.Option;

import org.apache.avro.Schema;

/**
 * {@link FormatSchemaMapper} for Avro: data blocks and the persisted schema (Avro JSON plus Hudi logical-type
 * properties). Wraps {@link HoodieSchema#toAvroSchema()} and {@link HoodieSchema#fromAvroSchema(Schema)}.
 *
 * <p>Every type {@code HoodieSchema} has today is backed by an Avro schema, so every type is {@code NATIVE} and the
 * hint is not needed.
 */
public final class AvroSchemaMapper implements FormatSchemaMapper<Schema> {

  private static final long serialVersionUID = 1L;

  @Override
  public String format() {
    return FileFormats.AVRO;
  }

  @Override
  public Schema toPhysicalSchema(HoodieSchema schema, PhysicalOptions opts) {
    return schema.toAvroSchema();
  }

  @Override
  public HoodieSchema fromPhysicalSchema(Schema physical, Option<HoodieSchema> hint) {
    return HoodieSchema.fromAvroSchema(physical);
  }

  @Override
  public TypeSupport support(HoodieSchemaType type, PhysicalOptions opts) {
    return TypeSupport.nativeType();
  }
}
