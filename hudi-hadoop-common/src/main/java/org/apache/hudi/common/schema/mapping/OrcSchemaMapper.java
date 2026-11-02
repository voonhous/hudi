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
import org.apache.hudi.common.util.AvroOrcUtils;
import org.apache.hudi.common.util.Option;

import org.apache.orc.TypeDescription;

/**
 * {@link FormatSchemaMapper} for ORC (RFC-88, type plane). Wraps {@link AvroOrcUtils#createOrcSchema(HoodieSchema)}
 * and {@link AvroOrcUtils#createSchema(TypeDescription)}, then restores LOSSY types from the hint with
 * {@link HoodieSchemaHints#restore}. The converter has no options.
 *
 * <p>Today's ORC readers prefer the Avro schema stored in the {@code orc.avro.schema} file metadata and fall back
 * to {@code createSchema} only without it; that metadata is outside the {@link TypeDescription}, so this mapper
 * sees only the fallback.
 *
 * <p>Replaces later: the schema part of {@code AvroOrcUtils}.
 */
public final class OrcSchemaMapper implements FormatSchemaMapper<TypeDescription> {

  private static final long serialVersionUID = 1L;

  @Override
  public String format() {
    return FileFormats.ORC;
  }

  @Override
  public TypeDescription toPhysicalSchema(HoodieSchema schema, PhysicalOptions opts) {
    return AvroOrcUtils.createOrcSchema(schema);
  }

  @Override
  public HoodieSchema fromPhysicalSchema(TypeDescription physical, Option<HoodieSchema> hint) {
    HoodieSchema converted = AvroOrcUtils.createSchema(physical);
    return HoodieSchemaHints.restore(converted, hint, t -> support(t).isLossy(),
        n -> AvroOrcUtils.createSchema(AvroOrcUtils.createOrcSchema(n)), HoodieSchemaEquivalence.Mode.PHYSICAL);
  }

  @Override
  public TypeSupport support(HoodieSchemaType type, PhysicalOptions opts) {
    switch (type) {
      case FIXED:
        return TypeSupport.lossy("FIXED written as ORC binary; read back as BYTES");
      case ENUM:
        return TypeSupport.lossy("ENUM written as ORC string; read back as STRING");
      case DECIMAL:
        return TypeSupport.lossy("ORC decimal(p,s) has no fixed or bytes backing; read back as a bytes-backed decimal");
      case TIME:
        return TypeSupport.lossy("TIME(millis) written as ORC int and TIME(micros) as ORC bigint; read back as INT / LONG");
      case TIMESTAMP:
        return TypeSupport.lossy("UTC-adjusted timestamps written as ORC timestamp and read back as timestamp-millis (micros precision "
            + "lost); local timestamps written as ORC bigint and read back as LONG");
      // Written natively; createSchema still reads the ORC types back as INT and STRING (RFC-88 step 2c switches that
      // read), so the hint restores them
      case INT8:
        return TypeSupport.lossy("INT8 written as ORC tinyint; read back as INT without a hint");
      case INT16:
        return TypeSupport.lossy("INT16 written as ORC smallint; read back as INT without a hint");
      case CHAR:
        return TypeSupport.lossy("CHAR(n) written as ORC char(n); read back as STRING without a hint");
      case UUID:
      case VARIANT:
      case BLOB:
      case VECTOR:
        return TypeSupport.unsupported("AvroOrcUtils.createOrcSchema has no case for " + type + " and throws");
      case NULL:
        return TypeSupport.unsupported("ORC has no null type; AvroOrcUtils.createOrcSchema writes an empty union");
      default:
        return TypeSupport.nativeType();
    }
  }
}
