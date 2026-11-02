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
import org.apache.hudi.common.schema.mapping.testkit.SchemaSample;
import org.apache.hudi.common.schema.mapping.testkit.SchemaSamples;

import org.apache.orc.TypeDescription;

import java.util.HashMap;
import java.util.Map;

/**
 * Contract test for {@link OrcSchemaMapper}. RFC-99 has no ORC column, so the expected support is "lossless where ORC
 * has the type": NATIVE for the types ORC has with all their parameters (INT8 as tinyint, INT16 as smallint, CHAR(n)
 * as char(n) included), LOSSY where ORC has no type (FIXED, ENUM, TIME, UUID, VARIANT, BLOB, VECTOR) or drops a
 * parameter (DECIMAL backing, TIMESTAMP precision), UNSUPPORTED for NULL. There is no expected output.
 */
class TestOrcSchemaMapper extends FormatSchemaMapperContractTest<TypeDescription> {

  private static final String NULLABILITY_LOST = "ORC types carry no nullability and AvroOrcUtils.createSchema returns every column "
      + "required; HoodieSchemaHints.restore takes nullability from the converted schema, never from the hint";

  @Override
  protected FormatSchemaMapper<TypeDescription> mapper() {
    return new OrcSchemaMapper();
  }

  @Override
  protected Map<HoodieSchemaType, TypeSupport.Level> expectedSupport() {
    return nativeExcept(
        HoodieSchemaType.FIXED, TypeSupport.Level.LOSSY,
        HoodieSchemaType.ENUM, TypeSupport.Level.LOSSY,
        HoodieSchemaType.DECIMAL, TypeSupport.Level.LOSSY,
        HoodieSchemaType.TIME, TypeSupport.Level.LOSSY,
        HoodieSchemaType.TIMESTAMP, TypeSupport.Level.LOSSY,
        HoodieSchemaType.UUID, TypeSupport.Level.LOSSY,
        HoodieSchemaType.VARIANT, TypeSupport.Level.LOSSY,
        HoodieSchemaType.BLOB, TypeSupport.Level.LOSSY,
        HoodieSchemaType.VECTOR, TypeSupport.Level.LOSSY,
        HoodieSchemaType.NULL, TypeSupport.Level.UNSUPPORTED);
  }

  @Override
  protected Map<String, String> knownGaps() {
    Map<String, String> gaps = new HashMap<>();
    for (String type : new String[] {"UUID", "VARIANT", "BLOB", "VECTOR"}) {
      gaps.put(SUPPORT_EXPECTED + "/" + type, "AvroOrcUtils.createOrcSchema has no case for " + type + " and throws IllegalStateException; "
          + "ORC could keep the values (as string, struct or binary) and the hint restore the type");
    }
    gaps.put(SUPPORT_EXPECTED + "/INT8", "AvroOrcUtils.createOrcSchema writes INT8 as ORC int, not tinyint, and createSchema reads tinyint back as INT");
    gaps.put(SUPPORT_EXPECTED + "/INT16", "AvroOrcUtils.createOrcSchema writes INT16 as ORC int, not smallint, and createSchema reads smallint back as INT");
    gaps.put(SUPPORT_EXPECTED + "/CHAR", "AvroOrcUtils.createOrcSchema writes CHAR(n) as ORC string, not char(n), and createSchema reads char(n) back "
        + "as STRING");
    // Every column reads back required; nullability is not a HoodieSchemaType, so HoodieSchemaHints does not restore it.
    for (SchemaSample sample : SchemaSamples.all()) {
      if (sample.nesting() != SchemaSample.Nesting.NONE && !support(sample.type()).isUnsupported() && !sample.id().equals("struct")) {
        gaps.put(ROUND_TRIP + "/" + sample.id() + ".nullable", NULLABILITY_LOST);
      }
    }
    gaps.put(ROUND_TRIP + "/struct.*", NULLABILITY_LOST + " (field b of the struct is nullable)");
    gaps.put(ROUND_TRIP + "/null", "AvroOrcUtils.createOrcSchema writes NULL as an empty ORC union instead of failing fast");
    return gaps;
  }
}
