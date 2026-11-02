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

import org.apache.hudi.avro.model.HoodieMetadataColumnStats;
import org.apache.hudi.common.expression.Expression;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.MetadataPartitionType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Prunes files with the COLUMN_STATS index: the per-file min, max, null count and value count of the indexed
 * columns the filter references. Mirrors Spark's {@code ColumnStatsIndexSupport}:
 * <ul>
 *   <li>only the referenced columns that are indexed ({@link LookupUtils#validIndexedColumns}) are read; a filter on
 *       other columns does not prune;</li>
 *   <li>base and log files are treated alike: a file is evaluated on its own stats (a MOR file slice is read when any
 *       of its files is kept, so a log file's stats cover the rows it changes);</li>
 *   <li>a file without any stats entry for the read columns is kept.</li>
 * </ul>
 *
 * <p>Differences from Spark, all keeping more files: a file with stats for some of the read columns but not for
 * another one (e.g. a column added later) leaves that column unknown, where Spark reads its min/max as null and prunes
 * the file on any comparison with it; and a range with no min/max prunes a comparison only when its counts prove the
 * column is all null. See {@link HudiStatsEvaluator} for the evaluation itself.
 */
public class ColumnStatsLookup implements IndexLookup {

  @Override
  public MetadataPartitionType index() {
    return MetadataPartitionType.COLUMN_STATS;
  }

  @Override
  public boolean canPrune(Expression filter, IndexLookupContext context) {
    return filter != null
        && context.metaClient().getTableConfig().isMetadataPartitionAvailable(MetadataPartitionType.COLUMN_STATS)
        && !LookupUtils.prunableColumns(filter, context, HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS).isEmpty();
  }

  @Override
  public CandidateFiles lookup(Expression filter, IndexLookupContext context) {
    Map<String, HoodieSchema> columns = LookupUtils.prunableColumns(filter, context, HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS);
    if (columns.isEmpty()) {
      return CandidateFiles.all();
    }
    List<Pair<String, String>> partitionAndFiles = new ArrayList<>();
    context.filesByPartition().forEach((partition, files) -> files.forEach(file -> partitionAndFiles.add(Pair.of(partition, file))));
    if (partitionAndFiles.isEmpty()) {
      return CandidateFiles.of(new HashSet<>());
    }
    Map<Pair<String, String>, List<HoodieMetadataColumnStats>> statsByFile =
        context.metadata().getColumnStats(partitionAndFiles, new ArrayList<>(columns.keySet()));

    Map<String, Map<String, ColumnRange>> rangesByFile = new HashMap<>();
    for (Map.Entry<Pair<String, String>, List<HoodieMetadataColumnStats>> entry : statsByFile.entrySet()) {
      Map<String, ColumnRange> ranges = new HashMap<>();
      for (HoodieMetadataColumnStats stats : entry.getValue()) {
        HoodieSchema type = columns.get(String.valueOf(stats.getColumnName()));
        if (type != null && !Boolean.TRUE.equals(stats.getIsDeleted())) {
          ranges.put(String.valueOf(stats.getColumnName()), ColumnRanges.fromColumnStats(stats, type));
        }
      }
      if (!ranges.isEmpty()) {
        rangesByFile.put(entry.getKey().getRight(), ranges);
      }
    }

    Set<String> kept = new HashSet<>(context.evaluator().prune(filter, rangesByFile));
    for (Pair<String, String> partitionAndFile : partitionAndFiles) {
      if (!rangesByFile.containsKey(partitionAndFile.getRight())) {
        // not indexed (yet): kept
        kept.add(partitionAndFile.getRight());
      }
    }
    return CandidateFiles.of(kept);
  }
}
