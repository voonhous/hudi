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
import org.apache.hudi.metadata.MetadataPartitionType;

/**
 * Prunes candidate files with one metadata-table index (RFC-88 section 4.3): column stats, partition stats, record
 * index, bloom filters, secondary index, expression index. One implementation per index type, shared by every
 * engine.
 */
public interface IndexLookup {

  MetadataPartitionType index();

  /**
   * Whether this lookup can do anything for the filter, e.g. it references an indexed column or the record key.
   * Must be cheap: no metadata-table reads.
   */
  boolean canPrune(Expression filter, IndexLookupContext context);

  /**
   * The files that might hold a matching row. Files the index has no entry for are kept.
   */
  CandidateFiles lookup(Expression filter, IndexLookupContext context);
}
