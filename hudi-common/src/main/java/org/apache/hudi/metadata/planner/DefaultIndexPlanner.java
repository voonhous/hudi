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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * The planner shared by every engine for the indexes it covers: PARTITION_STATS, RECORD_INDEX and COLUMN_STATS (bloom
 * filters, secondary index, expression index and bucket index stay with the engines for now).
 *
 * <p>The plan follows Spark's data skipping today ({@code HoodieFileIndex}):
 * <ol>
 *   <li>PARTITION_STATS, which Spark applies during partition pruning, before any file-level index, so the
 *       file-level indexes only see the partitions it keeps;</li>
 *   <li>RECORD_INDEX, the first entry of Spark's ordered {@code indicesSupport};</li>
 *   <li>COLUMN_STATS, the last one.</li>
 * </ol>
 * {@link #lookup} runs a plan the way Spark does, so that {@link IndexPlannerMode#COMPARE} compares like with like:
 * partition-level lookups narrow the candidates, then the first file-level lookup that applies and prunes wins.
 *
 * <p>Later improvement: every lookup only removes files it has proof against, so intersecting the results of all the
 * file-level lookups ({@link CandidateFiles#intersect}) prunes more than the first one alone, e.g. a record key filter
 * combined with a range filter on an indexed column. {@link #lookupAll} does that; it is also how Flink's
 * {@code FileIndex} combines the record index and column stats today.
 */
public class DefaultIndexPlanner implements IndexPlanner {

  private final PartitionStatsLookup partitionStats;
  private final RecordIndexLookup recordIndex;
  private final ColumnStatsLookup columnStats;

  public DefaultIndexPlanner() {
    this(new RecordIndexLookup());
  }

  /**
   * @param recordIndex the record index lookup, e.g. with the engine's resolver of complex record key encodings
   */
  public DefaultIndexPlanner(RecordIndexLookup recordIndex) {
    this.partitionStats = new PartitionStatsLookup();
    this.recordIndex = recordIndex;
    this.columnStats = new ColumnStatsLookup();
  }

  @Override
  public List<IndexLookup> plan(Expression filter, HoodieSchema schema, Set<MetadataPartitionType> available) {
    List<IndexLookup> plan = new ArrayList<>();
    if (filter == null) {
      return plan;
    }
    if (available.contains(MetadataPartitionType.PARTITION_STATS)) {
      plan.add(partitionStats);
    }
    if (available.contains(MetadataPartitionType.RECORD_INDEX)) {
      plan.add(recordIndex);
    }
    if (available.contains(MetadataPartitionType.COLUMN_STATS)) {
      plan.add(columnStats);
    }
    return plan;
  }

  /**
   * Whether a lookup prunes partitions as a whole, which Spark does during partition pruning rather than as one of its
   * file-level indexes.
   */
  public static boolean isPartitionLevel(IndexLookup lookup) {
    return lookup.index() == MetadataPartitionType.PARTITION_STATS;
  }

  /**
   * Runs a plan the way Spark's data skipping does today:
   * <ol>
   *   <li>every partition-level lookup ({@link #isPartitionLevel}) that can prune narrows the candidates, as partition
   *       pruning does; the following lookups only see the files it keeps;</li>
   *   <li>then the first file-level lookup, in plan order, whose {@link IndexLookup#canPrune} holds and whose result is
   *       not {@link CandidateFiles#all()} gives the result, as the first non-empty result of Spark's
   *       {@code indicesSupport} does;</li>
   *   <li>if none does, the result of the partition-level lookups stands.</li>
   * </ol>
   * Spark uses partition stats only when the query has no partition filter; a caller that mirrors this leaves
   * PARTITION_STATS out of the plan otherwise.
   *
   * @return the candidate files, among those of the context
   */
  public static CandidateFiles lookup(List<IndexLookup> plan, Expression filter, IndexLookupContext context) {
    CandidateFiles partitionLevel = CandidateFiles.all();
    IndexLookupContext current = context;
    for (IndexLookup lookup : plan) {
      if (isPartitionLevel(lookup) && lookup.canPrune(filter, current)) {
        CandidateFiles result = lookup.lookup(filter, current);
        if (!result.isAll()) {
          partitionLevel = partitionLevel.intersect(result);
          current = LookupUtils.narrow(current, result);
        }
      }
    }
    for (IndexLookup lookup : plan) {
      if (!isPartitionLevel(lookup) && lookup.canPrune(filter, current)) {
        CandidateFiles result = lookup.lookup(filter, current);
        if (!result.isAll()) {
          return partitionLevel.intersect(result);
        }
      }
    }
    return partitionLevel;
  }

  /**
   * Runs every lookup of a plan that can prune and intersects their results: partition-level lookups first, then
   * the file-level ones in plan order, each seeing only the files the previous ones kept. This is how Flink's
   * {@code FileIndex} applies the record index and then column stats, and prunes at least as much as {@link #lookup}.
   *
   * @return the candidate files, among those of the context
   */
  public static CandidateFiles lookupAll(List<IndexLookup> plan, Expression filter, IndexLookupContext context) {
    CandidateFiles result = CandidateFiles.all();
    IndexLookupContext current = context;
    for (boolean partitionLevel : new boolean[] {true, false}) {
      for (IndexLookup lookup : plan) {
        if (isPartitionLevel(lookup) == partitionLevel && lookup.canPrune(filter, current)) {
          CandidateFiles candidates = lookup.lookup(filter, current);
          if (!candidates.isAll()) {
            result = result.intersect(candidates);
            current = LookupUtils.narrow(current, candidates);
          }
        }
      }
    }
    return result;
  }
}
