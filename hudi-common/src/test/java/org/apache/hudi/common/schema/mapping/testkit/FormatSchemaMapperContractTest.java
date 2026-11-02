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
import org.apache.hudi.common.schema.mapping.FormatSchemaMapper;
import org.apache.hudi.common.schema.mapping.HoodieSchemaEquivalence;
import org.apache.hudi.common.schema.mapping.PhysicalOptions;
import org.apache.hudi.common.schema.mapping.SchemaMappers;
import org.apache.hudi.common.schema.mapping.TypeSupport;
import org.apache.hudi.common.util.Option;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Contract test for a {@link FormatSchemaMapper}. Extend it in the module that provides the mapper; see
 * {@link TypeSystemMapperContractTest} for the shape. Round trips are compared in
 * {@link HoodieSchemaEquivalence.Mode#PHYSICAL} mode, so decimal backing must survive.
 *
 * <p>Override {@link #options()} and run the test class once more per option set that changes the layout.
 *
 * @param <P> the format's physical schema type
 */
public abstract class FormatSchemaMapperContractTest<P> extends AbstractMapperContractTest<P> {

  protected abstract FormatSchemaMapper<P> mapper();

  protected PhysicalOptions options() {
    return PhysicalOptions.DEFAULT;
  }

  @Override
  protected P to(HoodieSchema schema) {
    return mapper().toPhysicalSchema(schema, options());
  }

  @Override
  protected HoodieSchema from(P target, Option<HoodieSchema> hint) {
    return mapper().fromPhysicalSchema(target, hint);
  }

  @Override
  protected TypeSupport support(HoodieSchemaType type) {
    return mapper().support(type, options());
  }

  @Override
  protected HoodieSchemaEquivalence.Mode equivalenceMode() {
    return HoodieSchemaEquivalence.Mode.PHYSICAL;
  }

  /**
   * The mapper is registered with {@link SchemaMappers} under its key.
   */
  @Test
  void isRegistered() {
    Option<FormatSchemaMapper<?>> registered = SchemaMappers.findFormat(mapper().format());
    assertTrue(registered.isPresent(), "No FormatSchemaMapper registered for '" + mapper().format()
        + "'; list it in META-INF/services/" + FormatSchemaMapper.class.getName());
    assertSame(mapper().getClass(), registered.get().getClass());
  }
}
