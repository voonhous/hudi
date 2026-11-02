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

package org.apache.hudi.metadata.planner;

import org.apache.hudi.common.expression.Expression;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.metadata.MetadataPartitionType;

import java.util.List;
import java.util.Set;

/**
 * Chooses which index lookups run for a filter, and in which order (RFC-88 section 4.3). Engines may override the
 * order.
 */
public interface IndexPlanner {

  /**
   * @param filter    the query filter as a Hudi expression (from an {@code ExpressionTranslator})
   * @param schema    the table schema
   * @param available the metadata-table indexes that exist and are fully built
   * @return the lookups to run, in order
   */
  List<IndexLookup> plan(Expression filter, HoodieSchema schema, Set<MetadataPartitionType> available);
}
