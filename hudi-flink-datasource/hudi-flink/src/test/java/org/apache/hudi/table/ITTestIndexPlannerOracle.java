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

package org.apache.hudi.table;

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.metadata.planner.IndexPlannerMode;
import org.apache.hudi.source.FileIndex;
import org.apache.hudi.source.IndexPlannerSupport;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.FlinkMiniCluster;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.hudi.utils.TestTableEnvs;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The expression oracle of RFC-88 for Flink: the index planner ({@code hoodie.metadata.index.planner.mode}) must
 * return the rows of a query without data skipping (ON), must not change today's result (COMPARE), and should prune
 * at least as much as Flink's own data skipping.
 *
 * <p>The table is written with the metadata table, column stats, partition stats and the record index, in several
 * commits (updates in the last one, so MOR tables have log files), and read through SQL with:
 * <ul>
 *   <li>a baseline table: metadata table and data skipping off, so no index prunes anything;</li>
 *   <li>OFF, COMPARE and ON tables: metadata table and data skipping on, with the given planner mode.</li>
 * </ul>
 */
@ExtendWith(FlinkMiniCluster.class)
@Slf4j
public class ITTestIndexPlannerOracle {

  private static final List<String> FIELDS = Arrays.asList(
      "uuid varchar(20)",
      "name varchar(10)",
      "age int",
      "score double",
      "price decimal(10, 2)",
      "dt date",
      "ts timestamp(3)",
      "`partition` varchar(10)");

  private static final List<String> COMMIT_1 = Arrays.asList(
      "('id1', 'Danny', 11, 1.5, 1.00, DATE '2024-01-01', TIMESTAMP '1970-01-01 00:00:01', 'par1')",
      "('id2', 'Stephen', 12, 2.5, 2.00, DATE '2024-01-02', TIMESTAMP '1970-01-01 00:00:02', 'par1')",
      "('id3', 'Julian', 23, 3.5, 3.00, DATE '2024-01-03', TIMESTAMP '1970-01-01 00:00:03', 'par2')",
      "('id4', cast(null as varchar(10)), 24, 4.5, 4.00, DATE '2024-01-04', TIMESTAMP '1970-01-01 00:00:04', 'par2')",
      "('id5', 'Sophia', 35, 5.5, 5.00, DATE '2024-01-05', TIMESTAMP '1970-01-01 00:00:05', 'par3')",
      "('id6', 'Emma', 36, 6.5, 6.00, DATE '2024-01-06', TIMESTAMP '1970-01-01 00:00:06', 'par3')",
      "('id7', 'Bob', 47, 7.5, 7.00, DATE '2024-01-07', TIMESTAMP '1970-01-01 00:00:07', 'par4')",
      "('id8', 'Han', 48, 8.5, 8.00, DATE '2024-01-08', TIMESTAMP '1970-01-01 00:00:08', 'par4')");

  private static final List<String> COMMIT_2 = Arrays.asList(
      "('id9', 'Dave', 51, 91.5, 101.00, DATE '2024-02-01', TIMESTAMP '1970-01-01 00:01:41', 'par1')",
      "('id10', 'Eve', 52, 92.5, 102.00, DATE '2024-02-02', TIMESTAMP '1970-01-01 00:01:42', 'par1')",
      "('id11', 'Frank', 63, 93.5, 103.00, DATE '2024-02-03', TIMESTAMP '1970-01-01 00:01:43', 'par2')",
      "('id12', 'Grace', 64, 94.5, 104.00, DATE '2024-02-04', TIMESTAMP '1970-01-01 00:01:44', 'par2')",
      "('id13', cast(null as varchar(10)), 75, 95.5, 105.00, DATE '2024-02-05', TIMESTAMP '1970-01-01 00:01:45', 'par3')",
      "('id14', 'Ivan', 76, 96.5, 106.00, DATE '2024-02-06', TIMESTAMP '1970-01-01 00:01:46', 'par3')",
      "('id15', 'Judy', 87, 97.5, 107.00, DATE '2024-02-07', TIMESTAMP '1970-01-01 00:01:47', 'par4')",
      "('id16', 'Mallory', 88, 98.5, 108.00, DATE '2024-02-08', TIMESTAMP '1970-01-01 00:01:48', 'par4')");

  // updates in place (same partitions): new file slices in COW, log files in MOR
  private static final List<String> COMMIT_3 = Arrays.asList(
      "('id2', 'Stephen', 13, 2.75, 2.50, DATE '2024-01-02', TIMESTAMP '1970-01-01 00:00:09', 'par1')",
      "('id11', 'Frank', 65, 93.75, 103.50, DATE '2024-02-03', TIMESTAMP '1970-01-01 00:01:49', 'par2')",
      "('id15', 'Judy', 99, 97.75, 107.50, DATE '2024-02-09', TIMESTAMP '1970-01-01 00:01:50', 'par4')");

  private static final List<String> QUERIES = Arrays.asList(
      // record keys
      "uuid = 'id3'",
      "uuid in ('id1', 'id12')",
      "uuid = 'id1' or uuid = 'id12'",
      "uuid = 'id15' and age > 90",
      // equality, ranges, IN
      "age = 36",
      "age > 70",
      "age >= 20 and age < 30",
      "age between 13 and 24",
      "age in (12, 99)",
      "age = 12",
      // nulls
      "name is null",
      "name is not null",
      // NOT
      "not (age > 30)",
      "age <> 36",
      // OR
      "age < 12 or score > 97",
      "age < 12 or name = 'Ivan'",
      // string prefix and strings
      "name like 'Da%'",
      "name >= 'J' and name < 'K'",
      "uuid > 'id5' and age > 15",
      // dates
      "dt = DATE '2024-01-02'",
      "dt > DATE '2024-02-05'",
      // timestamps
      "ts > TIMESTAMP '1970-01-01 00:01:45'",
      "ts <= TIMESTAMP '1970-01-01 00:00:02'",
      // decimals and doubles
      "price = 2.50",
      "price > 106",
      "score < 3.0",
      // partition filters
      "`partition` = 'par2' and age > 30",
      "`partition` in ('par1', 'par3') and uuid = 'id5'",
      "`partition` = 'par4' and price < 100");

  @TempDir
  File tempFile;

  private TableEnvironment tableEnv;
  private final List<IndexPlannerSupport.Report> reports = Collections.synchronizedList(new ArrayList<>());

  @BeforeEach
  void beforeEach() {
    tableEnv = TestTableEnvs.getBatchTableEnv();
    tableEnv.getConfig().getConfiguration().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 2);
    IndexPlannerSupport.setTestListener(reports::add);
  }

  @AfterEach
  void afterEach() {
    IndexPlannerSupport.setTestListener(null);
  }

  /**
   * The table type, the record index type, and whether column stats are enabled from the first commit.
   *
   * <p>A Flink writer never updates the source fields of the column stats index definition (HUDI-8801: its
   * {@code updateColumnsToIndexWithColStats} is a no-op), so when column stats are initialized on the empty table, at
   * the first commit, the definition lists no column, while every commit indexes all columns. The planner, like Spark's
   * {@code ColumnStatsIndexSupport}, only uses the columns of the definition ({@code getValidIndexedColumns}), and then
   * prunes nothing with column stats or partition stats. Enabling column stats at the second commit initializes them
   * from the existing files, which registers the indexed columns.
   */
  private static Stream<Arguments> tableTypeAndIndex() {
    return Stream.of(
        Arguments.of(HoodieTableType.COPY_ON_WRITE, HoodieIndex.IndexType.GLOBAL_RECORD_LEVEL_INDEX, true),
        Arguments.of(HoodieTableType.COPY_ON_WRITE, HoodieIndex.IndexType.GLOBAL_RECORD_LEVEL_INDEX, false),
        Arguments.of(HoodieTableType.MERGE_ON_READ, HoodieIndex.IndexType.GLOBAL_RECORD_LEVEL_INDEX, false),
        Arguments.of(HoodieTableType.MERGE_ON_READ, HoodieIndex.IndexType.RECORD_LEVEL_INDEX, false));
  }

  @ParameterizedTest
  @MethodSource("tableTypeAndIndex")
  void testPlannerOracle(HoodieTableType tableType, HoodieIndex.IndexType indexType, boolean colStatsFromStart) {
    String path = tempFile.getAbsolutePath();
    tableEnv.executeSql(ddl("t_write1", tableType, indexType, true, colStatsFromStart, null));
    tableEnv.executeSql(ddl("t_write", tableType, indexType, true, true, null));
    execInsert("insert into t_write1 values " + String.join(", ", COMMIT_1));
    execInsert("insert into t_write values " + String.join(", ", COMMIT_2));
    execInsert("insert into t_write values " + String.join(", ", COMMIT_3));

    HoodieTableMetaClient metaClient = StreamerUtil.createMetaClient(path, new org.apache.hadoop.conf.Configuration());
    Set<String> builtIndexes = metaClient.getTableConfig().getMetadataPartitions();
    assertTrue(builtIndexes.contains(MetadataPartitionType.COLUMN_STATS.getPartitionPath()), "column stats: " + builtIndexes);
    assertTrue(builtIndexes.contains(MetadataPartitionType.PARTITION_STATS.getPartitionPath()), "partition stats: " + builtIndexes);
    assertTrue(builtIndexes.contains(MetadataPartitionType.RECORD_INDEX.getPartitionPath()), "record index: " + builtIndexes);
    List<String> indexedColumns = metaClient.getIndexForMetadataPartition(MetadataPartitionType.COLUMN_STATS.getPartitionPath())
        .map(HoodieIndexDefinition::getSourceFields).orElse(null);
    if (colStatsFromStart) {
      assertEquals(Collections.emptyList(), indexedColumns, "column stats index definition of a Flink table initialized empty (HUDI-8801)");
    } else {
      assertTrue(indexedColumns != null && indexedColumns.containsAll(Arrays.asList("uuid", "age", "price", "dt", "ts")), "indexed columns: " + indexedColumns);
    }
    int totalFiles = countFiles(metaClient);

    tableEnv.executeSql(ddl("t_base", tableType, indexType, false, true, null));
    for (IndexPlannerMode mode : IndexPlannerMode.values()) {
      tableEnv.executeSql(ddl("t_" + mode.name().toLowerCase(), tableType, indexType, true, true, mode));
    }

    List<String> table = new ArrayList<>();
    table.add(String.format("%-50s | %4s | %5s | %3s | %3s | %s", "query (" + tableType + ", " + indexType + ", col stats from start: "
        + colStatsFromStart + ")", "rows", "files", "OFF", "ON", "COMPARE"));
    List<String> plannerOnly = new ArrayList<>();
    List<String> prunesLess = new ArrayList<>();
    for (String query : QUERIES) {
      List<String> baseline = select("t_base", query);

      reports.clear();
      List<String> off = select("t_off", query);
      assertTrue(reports.isEmpty(), "OFF must not run the planner: " + reports);
      assertEquals(baseline, off, "OFF rows of " + query);

      reports.clear();
      List<String> compare = select("t_compare", query);
      assertEquals(off, compare, "COMPARE rows of " + query);
      List<IndexPlannerSupport.Report> compareReports = new ArrayList<>(reports);

      reports.clear();
      List<String> on = select("t_on", query);
      assertEquals(baseline, on, "ON rows of " + query);
      List<IndexPlannerSupport.Report> onReports = new ArrayList<>(reports);

      for (IndexPlannerSupport.Report report : compareReports) {
        assertTrue(report.failure() == null, "COMPARE failed for " + query + ": " + report);
        if (!report.comparison().prunedOnlyByPlanner().isEmpty() && !isExplainedPlannerOnly(indexType, query, report)) {
          plannerOnly.add(query + ": " + report);
        }
      }
      onReports.forEach(report -> assertTrue(report.failure() == null, "ON failed for " + query + ": " + report));

      // the files read: COMPARE's engine side is today's (OFF) result, ON's planner side the planner's
      int offFiles = lastFiles(compareReports, totalFiles, true);
      int onFiles = lastFiles(onReports, totalFiles, false);
      if (onFiles > offFiles) {
        prunesLess.add(query);
      }
      String differences = compareReports.stream()
          .filter(r -> !r.comparison().isSame())
          .map(r -> r.stage() + " onlyPlanner=" + r.comparison().prunedOnlyByPlanner().size() + " onlyEngine=" + r.comparison().prunedOnlyByEngine().size())
          .collect(Collectors.joining("; "));
      table.add(String.format("%-50s | %4d | %5d | %3d | %3d | %s", query, baseline.size(), totalFiles, offFiles, onFiles, differences.isEmpty() ? "same" : differences));
    }
    log.warn("Index planner oracle:\n{}", String.join("\n", table));
    System.out.println("Index planner oracle:\n" + String.join("\n", table));
    assertTrue(plannerOnly.isEmpty(), "files pruned only by the planner:\n" + String.join("\n", plannerOnly));
    if (!colStatsFromStart) {
      assertTrue(prunesLess.size() * 5 <= QUERIES.size(), "ON prunes less than OFF for too many queries: " + prunesLess);
    }
  }

  /**
   * The files the planner prunes and Flink keeps, whose rows are the same (checked above), for two reasons:
   * <ul>
   *   <li>a Flink bug on DATE columns: {@code ColumnStatsProbe#getValAsJavaObj} reads DATE stats with
   *       {@code getLong}, but they are Integer epoch days; the {@code ClassCastException} is caught by
   *       {@code FileStatsIndex#computeCandidateFiles}, so Flink prunes nothing with column stats or partition stats
   *       on a DATE filter, while the planner does;</li>
   *   <li>Flink probes a partitioned record index only when there are at most 3 candidate partitions
   *       ({@code RecordLevelIndex.MAX_PARTITIONS}), the planner, like Spark, up to 10: on a query by record key over
   *       the 4 partitions the planner prunes the file slices that the record index proves do not hold the keys, which
   *       Flink keeps (and only prunes with column stats).</li>
   * </ul>
   */
  private static boolean isExplainedPlannerOnly(HoodieIndex.IndexType indexType, String query, IndexPlannerSupport.Report report) {
    if (query.startsWith("dt ")) {
      return true;
    }
    return indexType == HoodieIndex.IndexType.RECORD_LEVEL_INDEX
        && report.stage() == IndexPlannerSupport.Stage.FILES
        && (query.startsWith("uuid = ") || query.startsWith("uuid in "));
  }

  /**
   * The files read after the planner's stages: the last file stage report, or none if a partition stage pruned every
   * partition (the file stage is then skipped), or every file without any report (nothing to prune).
   */
  private static int lastFiles(List<IndexPlannerSupport.Report> reports, int totalFiles, boolean engine) {
    IndexPlannerSupport.Report files = null;
    IndexPlannerSupport.Report partitions = null;
    for (IndexPlannerSupport.Report report : reports) {
      if (report.stage() == IndexPlannerSupport.Stage.FILES) {
        files = report;
      } else {
        partitions = report;
      }
    }
    if (files != null) {
      return engine ? files.engineKept().size() : files.plannerKeptSet().size();
    }
    if (partitions != null) {
      Set<String> kept = engine ? partitions.engineKept() : partitions.plannerKeptSet();
      return kept.isEmpty() ? 0 : totalFiles;
    }
    return totalFiles;
  }

  private String ddl(String name, HoodieTableType tableType, HoodieIndex.IndexType indexType, boolean dataSkipping, boolean colStats, IndexPlannerMode mode) {
    Map<String, String> keys = new HashMap<>();
    keys.put(FlinkOptions.RECORD_KEY_FIELD.key(), "uuid");
    keys.put(FlinkOptions.ORDERING_FIELDS.key(), "ts");
    TestConfigurations.Sql sql = TestConfigurations.sql(name)
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .options(keys)
        .option(FlinkOptions.TABLE_TYPE, tableType.name())
        .option(FlinkOptions.INDEX_TYPE, indexType.name())
        .option(FlinkOptions.METADATA_ENABLED, dataSkipping)
        .option(FlinkOptions.READ_DATA_SKIPPING_ENABLED, dataSkipping)
        .option(HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key(), colStats)
        .option(HoodieMetadataConfig.ENABLE_METADATA_INDEX_PARTITION_STATS.key(), colStats)
        .option("hoodie.parquet.small.file.limit", 0)
        .option(FlinkOptions.COMPACTION_ASYNC_ENABLED, false);
    if (mode != null) {
      sql.option(HoodieMetadataConfig.INDEX_PLANNER_MODE.key(), mode.name());
    }
    FIELDS.forEach(sql::field);
    return sql.end();
  }

  private void execInsert(String insert) {
    TableResult result = tableEnv.executeSql(insert);
    try {
      result.await();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private List<String> select(String table, String where) {
    List<Row> rows = CollectionUtil.iterableToList(() -> tableEnv.sqlQuery("select * from " + table + " where " + where).execute().collect());
    return rows.stream().map(Row::toString).sorted().collect(Collectors.toList());
  }

  /**
   * Every file of the latest file slices, as the file index lists them before data skipping.
   */
  private int countFiles(HoodieTableMetaClient metaClient) {
    metaClient.reloadActiveTimeline();
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.set(FlinkOptions.METADATA_ENABLED, true);
    try (FileIndex fileIndex = FileIndex.builder().path(new StoragePath(tempFile.getAbsolutePath())).conf(conf)
        .rowType(TestConfigurations.ROW_TYPE).metaClient(metaClient).build()) {
      List<String> partitions = fileIndex.getOrBuildPartitionPaths();
      List<StoragePathInfo> files = fileIndex.getFilesInPartitions();
      HoodieTimeline timeline = metaClient.getCommitsAndCompactionTimeline().filterCompletedAndCompactionInstants();
      String latest = timeline.lastInstant().get().requestedTime();
      try (HoodieTableFileSystemView fsView = new HoodieTableFileSystemView(metaClient, timeline, files)) {
        return partitions.stream()
            .flatMap(partition -> fsView.getLatestMergedFileSlicesBeforeOrOn(partition, latest))
            .mapToInt(slice -> slice.getAllFileNames().size())
            .sum();
      }
    }
  }
}
