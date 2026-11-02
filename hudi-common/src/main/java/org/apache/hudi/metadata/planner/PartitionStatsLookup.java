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
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.expression.Expression;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.metadata.ColumnStatsIndexPrefixRawKey;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.metadata.RawKey;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Prunes whole partitions with the PARTITION_STATS index: the min and max of the indexed columns over every file of
 * a partition. Every file of a partition that might match is kept, and so is every file of a partition without
 * stats.
 *
 * <p>Spark uses partition stats for partition pruning ({@code HoodieFileIndex.prunePartitionsAndGetFileSlices}, through
 * {@code PartitionStatsIndexSupport.prunePartitions}), before and independently of the file-level indexes, and only
 * on partitioned tables whose query has no partition filter. This lookup mirrors it with these differences:
 * <ul>
 *   <li>like Spark, which does not look up filters that use null or value counts, the counts are dropped (they are not
 *       exact after updates and deletes); a filter that needs them, such as {@code IS NULL}, keeps the partition.
 *       Consequently a partition whose column is all null is kept where Spark prunes it for a comparison;</li>
 *   <li>a partition without stats is kept; Spark prunes it (it keeps only the partitions found in the index);</li>
 *   <li>the lookup itself does not know whether the query had a partition filter: callers that mirror Spark leave
 *       PARTITION_STATS out of the plan in that case.</li>
 * </ul>
 */
public class PartitionStatsLookup implements IndexLookup {

  @Override
  public MetadataPartitionType index() {
    return MetadataPartitionType.PARTITION_STATS;
  }

  @Override
  public boolean canPrune(Expression filter, IndexLookupContext context) {
    Option<String[]> partitionFields = context.metaClient().getTableConfig().getPartitionFields();
    return filter != null
        && partitionFields.isPresent() && partitionFields.get().length > 0
        && context.metaClient().getTableConfig().isMetadataPartitionAvailable(MetadataPartitionType.PARTITION_STATS)
        && !LookupUtils.prunableColumns(filter, context, HoodieTableMetadataUtil.PARTITION_NAME_PARTITION_STATS).isEmpty();
  }

  @Override
  public CandidateFiles lookup(Expression filter, IndexLookupContext context) {
    Map<String, HoodieSchema> columns = LookupUtils.prunableColumns(filter, context, HoodieTableMetadataUtil.PARTITION_NAME_PARTITION_STATS);
    if (columns.isEmpty()) {
      return CandidateFiles.all();
    }
    Map<String, List<String>> filesByPartition = context.filesByPartition();
    // the partition stats key of (column, partition) is the column stats key prefix of the same pair
    Map<String, Pair<String, String>> partitionAndColumnByKey = new HashMap<>();
    List<RawKey> keys = new ArrayList<>();
    for (String partition : filesByPartition.keySet()) {
      for (String column : columns.keySet()) {
        ColumnStatsIndexPrefixRawKey key = new ColumnStatsIndexPrefixRawKey(column, partition);
        keys.add(key);
        partitionAndColumnByKey.put(HoodieTableMetadataUtil.getPartitionStatsIndexKey(partition, column), Pair.of(partition, column));
      }
    }
    if (keys.isEmpty()) {
      return CandidateFiles.of(new HashSet<>());
    }
    List<HoodieRecord<HoodieMetadataPayload>> records = context.metadata()
        .getRecordsByKeyPrefixes(HoodieListData.eager(keys), MetadataPartitionType.PARTITION_STATS.getPartitionPath(), true)
        .collectAsList();

    Map<String, Map<String, ColumnRange>> rangesByPartition = new HashMap<>();
    for (HoodieRecord<HoodieMetadataPayload> record : records) {
      Pair<String, String> partitionAndColumn = partitionAndColumnByKey.get(record.getRecordKey());
      Option<HoodieMetadataColumnStats> stats = record.getData().getColumnStatMetadata();
      if (partitionAndColumn == null || !stats.isPresent() || Boolean.TRUE.equals(stats.get().getIsDeleted())) {
        continue;
      }
      HoodieSchema type = columns.get(partitionAndColumn.getRight());
      rangesByPartition.computeIfAbsent(partitionAndColumn.getLeft(), p -> new HashMap<>())
          .put(partitionAndColumn.getRight(), ColumnRanges.fromColumnStats(stats.get(), type, false));
    }

    Set<String> keptPartitions = new HashSet<>(context.evaluator().prune(filter, rangesByPartition));
    Set<String> kept = new HashSet<>();
    for (Map.Entry<String, List<String>> partition : filesByPartition.entrySet()) {
      if (!rangesByPartition.containsKey(partition.getKey()) || keptPartitions.contains(partition.getKey())) {
        kept.addAll(partition.getValue());
      }
    }
    return CandidateFiles.of(kept);
  }
}
