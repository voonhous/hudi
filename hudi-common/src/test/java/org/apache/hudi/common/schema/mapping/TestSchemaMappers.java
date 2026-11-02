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

import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.exception.HoodieException;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestSchemaMappers {

  @Test
  void avroMapperIsRegistered() {
    assertTrue(SchemaMappers.formats().contains(FileFormats.AVRO));
    assertInstanceOf(AvroSchemaMapper.class, SchemaMappers.format(FileFormats.AVRO));
  }

  @Test
  void missingMapperFailsWithRegisteredKeys() {
    HoodieException e = assertThrows(HoodieException.class, () -> SchemaMappers.typeSystem("no-such-engine"));
    assertTrue(e.getMessage().contains("no-such-engine"), e.getMessage());
    assertFalse(SchemaMappers.findFormat("no-such-format").isPresent());
  }

  @Test
  void fileFormatKeys() {
    assertEquals("parquet", FileFormats.key(HoodieFileFormat.PARQUET));
    assertEquals(FileFormats.ORC, FileFormats.key(HoodieFileFormat.ORC));
  }

  @Test
  void loadIsCachedPerService() {
    Map<String, FormatSchemaMapper> first = SchemaMappers.load(FormatSchemaMapper.class, FormatSchemaMapper::format);
    assertTrue(first == SchemaMappers.load(FormatSchemaMapper.class, FormatSchemaMapper::format));
    assertThrows(UnsupportedOperationException.class, () -> first.put("x", new AvroSchemaMapper()));
  }

  @Test
  void options() {
    MappingOptions opts = MappingOptions.builder().set("a", true).set("b", "x").build();
    assertTrue(opts.getBoolean("a", false));
    assertEquals("x", opts.getString("b", "y"));
    assertEquals(7, opts.getInt("c", 7));
    assertEquals(MappingOptions.DEFAULT, MappingOptions.of(Collections.emptyMap()));
    assertEquals(opts, MappingOptions.builder().set("b", "x").set("a", "true").build());
  }
}
