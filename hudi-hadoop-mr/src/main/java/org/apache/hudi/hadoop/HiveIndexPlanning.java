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

package org.apache.hudi.hadoop;

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.expression.Expression;
import org.apache.hudi.common.expression.translate.ExpressionMappers;
import org.apache.hudi.common.expression.translate.ExpressionTranslator;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.schema.mapping.TypeSystems;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.metadata.planner.CandidateFiles;
import org.apache.hudi.metadata.planner.DefaultIndexPlanner;
import org.apache.hudi.metadata.planner.HudiStatsEvaluator;
import org.apache.hudi.metadata.planner.IndexLookup;
import org.apache.hudi.metadata.planner.IndexLookupContext;
import org.apache.hudi.metadata.planner.IndexPlannerComparison;
import org.apache.hudi.metadata.planner.IndexPlannerMode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.common.config.HoodieMetadataConfig.DEFAULT_METADATA_ENABLE_FOR_READERS;
import static org.apache.hudi.common.config.HoodieMetadataConfig.ENABLE;

/**
 * Data skipping for Hive snapshot queries through the RFC-88 index planner, set by
 * {@link HoodieMetadataConfig#INDEX_PLANNER_MODE}. Hive has no metadata-table data skipping of its own, so:
 * <ul>
 *   <li>OFF (default): nothing here runs;</li>
 *   <li>COMPARE: the planner runs and its result is logged against "no pruning" (the engine side of
 *       {@link IndexPlannerComparison#compare} is empty); every file slice is still read;</li>
 *   <li>ON: only the file slices the planner keeps are read.</li>
 * </ul>
 *
 * <p>The filter is the one Hive pushes into the input format, {@link TableScanDesc#FILTER_EXPR_CONF_STR}, translated by
 * the Hive {@link ExpressionTranslator} against the table schema. It is read when the splits are listed, before
 * {@code SchemaEvolutionContext} renames its columns to file-schema names in the record readers, so it uses the query
 * schema names. Hive fills partition columns from the partition path, not from the data files, and the two may differ
 * (key generators, partition value extractors), so partition columns are left out of the schema the filter is
 * translated against: their leaves are unknown. Hive prunes partitions itself, through the input paths.
 *
 * <p>The planner runs only where it is correct; otherwise every listed file slice is kept:
 * <ul>
 *   <li>snapshot listings only: incremental queries list through other code;</li>
 *   <li>not with a query instant ({@code hoodie.TABLE.consume.commit}, time travel) or pending commits included
 *       ({@code hoodie.TABLE.consume.pending.commits}): the indexes describe the latest completed state;</li>
 *   <li>not on bootstrapped tables, whose skeleton and source files are stitched (the readers unset the filter there),
 *       nor on tables with an evolved schema, whose column stats are keyed by the column names at write time;</li>
 *   <li>only when the metadata table is enabled for readers and its files partition is built; each index only when it
 *       is fully built ({@link HoodieTableConfig#isMetadataPartitionAvailable});</li>
 *   <li>the record index only when log files are read: a read-optimized query of a MOR table reads base files the
 *       record index no longer describes (e.g. a key deleted in a log file).</li>
 * </ul>
 * The record index, partition stats and column stats lookups are all run and intersected
 * ({@link DefaultIndexPlanner#lookupAll}), which prunes the most; Hive has no existing pruning to mirror. A failure
 * of the planner is logged and every file slice is kept, in both modes.
 */
public final class HiveIndexPlanning {

  private static final Logger LOG = LoggerFactory.getLogger(HiveIndexPlanning.class);

  private HiveIndexPlanning() {
  }

  /**
   * The planner mode of the job; OFF when it is not set.
   */
  public static IndexPlannerMode mode(Configuration conf) {
    String mode = conf.get(HoodieMetadataConfig.INDEX_PLANNER_MODE.key());
    return StringUtils.isNullOrEmpty(mode) ? IndexPlannerMode.OFF : IndexPlannerMode.valueOf(mode.trim().toUpperCase(Locale.ROOT));
  }

  /**
   * Applies the planner to the file slices listed for a snapshot query.
   *
   * @param fileSlices     the listed file slices by relative partition path
   * @param readsLogFiles  whether the query reads the log files of a file slice (MOR snapshot), or only its base file
   * @param latestSnapshot whether the query reads the latest completed state: no query instant, no pending commits
   * @return the file slices to read: all of them for OFF and COMPARE, the planner's candidates for ON
   */
  public static Map<String, List<FileSlice>> apply(IndexPlannerMode mode, Configuration job, HoodieTableMetaClient metaClient,
                                                   HoodieEngineContext engineContext, Map<String, List<FileSlice>> fileSlices,
                                                   boolean readsLogFiles, boolean latestSnapshot) {
    if (mode == IndexPlannerMode.OFF) {
      return fileSlices;
    }
    String query = metaClient.getBasePath() + " with filter " + job.get(TableScanDesc.FILTER_TEXT_CONF_STR);
    try {
      Map<String, List<String>> filesByPartition = files(fileSlices, readsLogFiles);
      Option<CandidateFiles> candidates = latestSnapshot
          ? candidates(job, metaClient, engineContext, filesByPartition, readsLogFiles)
          : Option.empty();
      if (!candidates.isPresent() || candidates.get().isAll()) {
        return fileSlices;
      }
      Map<String, List<FileSlice>> kept = keep(fileSlices, candidates.get(), readsLogFiles);
      long all = count(fileSlices);
      long read = count(kept);
      if (mode == IndexPlannerMode.COMPARE) {
        Set<String> allFiles = filesByPartition.values().stream().flatMap(List::stream).collect(Collectors.toSet());
        IndexPlannerComparison.compare(allFiles, Option.empty(), candidates.get()).log(query);
        LOG.info("Index planner COMPARE for {}: the planner would read {} of {} file slices; all are read", query, read, all);
        return fileSlices;
      }
      LOG.info("Index planner ON for {}: reading {} of {} file slices", query, read, all);
      return kept;
    } catch (Exception e) {
      // the query must never fail or change because of the planner: keep every file slice
      LOG.warn("Index planner {} failed for {}; reading every file slice", mode, query, e);
      return fileSlices;
    }
  }

  /**
   * The planner's candidate files for the job's filter, or empty when the planner does not apply to the query.
   *
   * @param filesByPartition the listed file names (base files, and log files if they are read) by relative partition path
   */
  public static Option<CandidateFiles> candidates(Configuration job, HoodieTableMetaClient metaClient, HoodieEngineContext engineContext,
                                                  Map<String, List<String>> filesByPartition, boolean readsLogFiles) throws Exception {
    String serializedFilter = job.get(TableScanDesc.FILTER_EXPR_CONF_STR);
    HoodieTableConfig tableConfig = metaClient.getTableConfig();
    if (StringUtils.isNullOrEmpty(serializedFilter)
        || !job.getBoolean(ENABLE.key(), DEFAULT_METADATA_ENABLE_FOR_READERS)
        || !HoodieTableMetadataUtil.isFilesPartitionAvailable(metaClient)
        || tableConfig.getBootstrapBasePath().isPresent()) {
      return Option.empty();
    }
    Set<MetadataPartitionType> available = EnumSet.noneOf(MetadataPartitionType.class);
    for (MetadataPartitionType index : Arrays.asList(MetadataPartitionType.PARTITION_STATS, MetadataPartitionType.RECORD_INDEX,
        MetadataPartitionType.COLUMN_STATS)) {
      if (tableConfig.isMetadataPartitionAvailable(index)) {
        available.add(index);
      }
    }
    if (!readsLogFiles && metaClient.getTableType() == HoodieTableType.MERGE_ON_READ) {
      available.remove(MetadataPartitionType.RECORD_INDEX);
    }
    if (available.isEmpty()) {
      return Option.empty();
    }
    TableSchemaResolver schemaResolver = new TableSchemaResolver(metaClient);
    if (schemaResolver.getTableInternalSchemaFromCommitMetadata().isPresent()) {
      return Option.empty();
    }
    HoodieSchema schema = schemaResolver.getTableSchema();
    ExprNodeDesc hiveFilter = SerializationUtilities.deserializeExpression(serializedFilter);
    ExpressionTranslator<ExprNodeDesc> translator = ExpressionMappers.translator(TypeSystems.HIVE);
    Option<Expression> filter = translator.toHudi(hiveFilter, withoutPartitionColumns(schema, tableConfig, job, hiveFilter));
    if (!filter.isPresent()) {
      return Option.empty();
    }
    List<IndexLookup> plan = new DefaultIndexPlanner().plan(filter.get(), schema, available);
    if (plan.isEmpty()) {
      return Option.empty();
    }
    HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder().enable(true).build();
    try (HoodieTableMetadata metadata = metaClient.getTableFormat().getMetadataFactory()
        .create(engineContext, metaClient.getStorage(), metadataConfig, metaClient.getBasePath().toString(), true)) {
      IndexLookupContext context = new IndexLookupContext(metaClient, metadata, engineContext, schema,
          HudiStatsEvaluator.getInstance(), filesByPartition);
      return Option.of(DefaultIndexPlanner.lookupAll(plan, filter.get(), context));
    }
  }

  /**
   * The schema without the table's partition columns, nor Hive's (the two can differ by Hive sync config; Hive's are
   * those of the job and the filter columns Hive marks as partition or virtual columns), matched case-insensitively as
   * Hive column names are.
   */
  private static HoodieSchema withoutPartitionColumns(HoodieSchema schema, HoodieTableConfig tableConfig, Configuration job,
                                                      ExprNodeDesc hiveFilter) {
    Set<String> partitionColumns = new HashSet<>();
    collectPartitionColumns(hiveFilter, partitionColumns);
    tableConfig.getPartitionFields().ifPresent(fields -> Arrays.stream(fields).forEach(f -> partitionColumns.add(f.toLowerCase(Locale.ROOT))));
    String hivePartitionColumns = job.get(hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS);
    if (!StringUtils.isNullOrEmpty(hivePartitionColumns)) {
      Arrays.stream(hivePartitionColumns.split("/")).map(String::trim).filter(c -> !c.isEmpty())
          .forEach(c -> partitionColumns.add(c.toLowerCase(Locale.ROOT)));
    }
    if (partitionColumns.isEmpty()) {
      return schema;
    }
    Set<String> toRemove = schema.getFields().stream().map(HoodieSchemaField::name)
        .filter(name -> partitionColumns.contains(name.toLowerCase(Locale.ROOT)))
        .collect(Collectors.toSet());
    return HoodieSchemaUtils.removeFields(schema, toRemove);
  }

  private static void collectPartitionColumns(ExprNodeDesc node, Set<String> partitionColumns) {
    if (node instanceof ExprNodeColumnDesc) {
      ExprNodeColumnDesc column = (ExprNodeColumnDesc) node;
      if (column.getIsPartitionColOrVirtualCol() && column.getColumn() != null) {
        partitionColumns.add(column.getColumn().toLowerCase(Locale.ROOT));
      }
    }
    if (node != null && node.getChildren() != null) {
      node.getChildren().forEach(child -> collectPartitionColumns(child, partitionColumns));
    }
  }

  /**
   * The file names of each file slice the query reads.
   */
  private static Map<String, List<String>> files(Map<String, List<FileSlice>> fileSlices, boolean readsLogFiles) {
    Map<String, List<String>> filesByPartition = new LinkedHashMap<>();
    fileSlices.forEach((partition, slices) -> {
      List<String> files = new ArrayList<>();
      slices.forEach(slice -> files.addAll(files(slice, readsLogFiles)));
      filesByPartition.put(partition, files);
    });
    return filesByPartition;
  }

  private static List<String> files(FileSlice slice, boolean readsLogFiles) {
    List<String> files = new ArrayList<>();
    slice.getBaseFile().ifPresent(base -> files.add(base.getFileName()));
    if (readsLogFiles) {
      slice.getLogFiles().forEach(log -> files.add(log.getFileName()));
    }
    return files;
  }

  /**
   * The file slices any of whose read files is a candidate, and those without read files (the caller decides about
   * them as without the planner).
   */
  private static Map<String, List<FileSlice>> keep(Map<String, List<FileSlice>> fileSlices, CandidateFiles candidates, boolean readsLogFiles) {
    Map<String, List<FileSlice>> kept = new LinkedHashMap<>();
    fileSlices.forEach((partition, slices) -> kept.put(partition, slices.stream()
        .filter(slice -> {
          List<String> files = files(slice, readsLogFiles);
          return files.isEmpty() || files.stream().anyMatch(candidates.fileNames()::contains);
        })
        .collect(Collectors.toList())));
    return kept;
  }

  private static long count(Map<String, List<FileSlice>> fileSlices) {
    return fileSlices.values().stream().mapToLong(List::size).sum();
  }
}
