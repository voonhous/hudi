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

import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.metadata.HoodieTableMetadata;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * What an {@link IndexLookup} needs: the table, its metadata table, the engine context that distributes key probes,
 * the evaluator for stats-based lookups, and the files it may prune.
 */
public final class IndexLookupContext {

  private final HoodieTableMetaClient metaClient;
  private final HoodieTableMetadata metadata;
  private final HoodieEngineContext engineContext;
  private final HoodieSchema tableSchema;
  private final StatsEvaluator evaluator;
  private final Map<String, List<String>> filesByPartition;

  /**
   * @param filesByPartition the candidate files before this lookup: file names (base and log files) by relative
   *                         partition path, after partition pruning
   */
  public IndexLookupContext(HoodieTableMetaClient metaClient, HoodieTableMetadata metadata, HoodieEngineContext engineContext,
                            HoodieSchema tableSchema, StatsEvaluator evaluator, Map<String, List<String>> filesByPartition) {
    this.metaClient = metaClient;
    this.metadata = metadata;
    this.engineContext = engineContext;
    this.tableSchema = tableSchema;
    this.evaluator = evaluator;
    this.filesByPartition = Collections.unmodifiableMap(new LinkedHashMap<>(filesByPartition));
  }

  public HoodieTableMetaClient metaClient() {
    return metaClient;
  }

  public HoodieTableMetadata metadata() {
    return metadata;
  }

  public HoodieEngineContext engineContext() {
    return engineContext;
  }

  public HoodieSchema tableSchema() {
    return tableSchema;
  }

  public StatsEvaluator evaluator() {
    return evaluator;
  }

  public Map<String, List<String>> filesByPartition() {
    return filesByPartition;
  }
}
