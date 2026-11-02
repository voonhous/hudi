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

package org.apache.hudi.common.schema.mapping.testkit;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.schema.mapping.MappingOptions;
import org.apache.hudi.common.schema.mapping.SchemaMappers;
import org.apache.hudi.common.schema.mapping.TypeSupport;
import org.apache.hudi.common.schema.mapping.TypeSystemMapper;
import org.apache.hudi.common.util.Option;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Contract test for a {@link TypeSystemMapper}. Extend it in the module that provides the mapper:
 *
 * <pre>{@code
 * class TestSparkTypeMapper extends TypeSystemMapperContractTest<DataType> {
 *   protected TypeSystemMapper<DataType> mapper() { return new SparkTypeMapper(); }
 *   protected Map<HoodieSchemaType, TypeSupport.Level> expectedSupport() { return nativeExcept(UUID, LOSSY, ...); }
 *   protected Map<String, String> knownGaps() { ... }
 * }
 * }</pre>
 *
 * <p>Override {@link #options()} and run the test class once more per option set that changes the mapping.
 *
 * @param <T> the type system's schema type
 */
public abstract class TypeSystemMapperContractTest<T> extends AbstractMapperContractTest<T> {

  protected abstract TypeSystemMapper<T> mapper();

  protected MappingOptions options() {
    return MappingOptions.DEFAULT;
  }

  @Override
  protected T to(HoodieSchema schema) {
    return mapper().toTypeSystem(schema, options());
  }

  @Override
  protected HoodieSchema from(T target, Option<HoodieSchema> hint) {
    return mapper().fromTypeSystem(target, hint);
  }

  @Override
  protected TypeSupport support(HoodieSchemaType type) {
    return mapper().support(type, options());
  }

  @Override
  protected HoodieSchemaEquivalence.Mode equivalenceMode() {
    return HoodieSchemaEquivalence.Mode.LOGICAL;
  }

  /**
   * The mapper is registered with {@link SchemaMappers} under its key.
   */
  @Test
  void isRegistered() {
    Option<TypeSystemMapper<?>> registered = SchemaMappers.findTypeSystem(mapper().typeSystem());
    assertTrue(registered.isPresent(), "No TypeSystemMapper registered for '" + mapper().typeSystem()
        + "'; list it in META-INF/services/" + TypeSystemMapper.class.getName());
    assertSame(mapper().getClass(), registered.get().getClass());
  }
}
