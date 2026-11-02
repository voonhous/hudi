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

package org.apache.hudi.source;

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.expression.Expression;
import org.apache.hudi.common.expression.Predicates;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.metadata.planner.IndexPlannerMode;
import org.apache.hudi.source.prune.ColumnStatsProbe;
import org.apache.hudi.source.prune.PartitionPruners;
import org.apache.hudi.source.stats.FileStatsIndex;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.hudi.utils.TestData;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedConstruction;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.when;

/**
 * Tests the wiring of the RFC-88 index planner into Flink's {@link FileIndex} and partition pruning
 * ({@link IndexPlannerSupport}).
 */
public class TestIndexPlannerSupport {

  @TempDir
  File tempFile;

  private final List<IndexPlannerSupport.Report> reports = Collections.synchronizedList(new ArrayList<>());

  @BeforeEach
  void beforeEach() {
    IndexPlannerSupport.setTestListener(reports::add);
  }

  @AfterEach
  void afterEach() {
    IndexPlannerSupport.setTestListener(null);
  }

  @Test
  void testMode() {
    Configuration conf = new Configuration();
    assertEquals(IndexPlannerMode.OFF, IndexPlannerSupport.mode(conf));
    conf.setString(HoodieMetadataConfig.INDEX_PLANNER_MODE.key(), "compare");
    assertEquals(IndexPlannerMode.COMPARE, IndexPlannerSupport.mode(conf));
    conf.setString(HoodieMetadataConfig.INDEX_PLANNER_MODE.key(), "ON");
    assertEquals(IndexPlannerMode.ON, IndexPlannerSupport.mode(conf));
  }

  @Test
  void testTranslate() {
    // two pushed filters form a conjunction; an untranslatable one (a column not in the schema) is left out
    List<ResolvedExpression> filters = Arrays.asList(gt("uuid", "id5"), lt("age", 30), eq("unknown", 1));
    Expression expr = IndexPlannerSupport.translate(filters, IndexPlannerSupport.schemaOf(TestConfigurations.ROW_TYPE)).get();
    assertInstanceOf(Predicates.And.class, expr);
    Predicates.And and = (Predicates.And) expr;
    assertEquals(Expression.Operator.GT, ((Predicates.BinaryComparison) and.getLeft()).getOperator());
    assertEquals(Expression.Operator.LT, ((Predicates.BinaryComparison) and.getRight()).getOperator());

    // Flink pushes IN as a disjunction of equalities, the record index lookup reads it as IN
    ResolvedExpression or = CallExpression.permanent(BuiltInFunctionDefinitions.OR,
        Arrays.asList(eq("uuid", "id1"), eq("uuid", "id3")), DataTypes.BOOLEAN());
    Expression in = IndexPlannerSupport.translate(Collections.singletonList(or), IndexPlannerSupport.schemaOf(TestConfigurations.ROW_TYPE)).get();
    assertInstanceOf(Predicates.In.class, in);
    assertEquals(2, ((Predicates.In) in).getRightChildren().size());

    // a disjunction over two columns stays as it is
    ResolvedExpression mixed = CallExpression.permanent(BuiltInFunctionDefinitions.OR,
        Arrays.asList(eq("uuid", "id1"), eq("name", "Danny")), DataTypes.BOOLEAN());
    assertInstanceOf(Predicates.Or.class,
        IndexPlannerSupport.translate(Collections.singletonList(mixed), IndexPlannerSupport.schemaOf(TestConfigurations.ROW_TYPE)).get());

    assertFalse(IndexPlannerSupport.translate(Collections.singletonList(eq("unknown", 1)), IndexPlannerSupport.schemaOf(TestConfigurations.ROW_TYPE)).isPresent());
  }

  @Test
  void testOffDoesNotTranslate() {
    Configuration conf = conf(IndexPlannerMode.OFF);
    List<ResolvedExpression> filters = Collections.singletonList(gt("uuid", "id5"));
    ColumnStatsProbe probe = ColumnStatsProbe.newInstance(filters);
    assertSame(probe, IndexPlannerSupport.withPlannerFilter(probe, filters, conf, TestConfigurations.ROW_TYPE));
    assertNull(probe.getPlannerFilter());
    assertNotNull(IndexPlannerSupport.withPlannerFilter(probe, filters, conf(IndexPlannerMode.ON), TestConfigurations.ROW_TYPE).getPlannerFilter());
  }

  @Test
  void testModes() throws Exception {
    writeTable();
    // uuid > 'id5' and age < 30: only par3 (id6, age 20) matches
    List<ResolvedExpression> filters = Arrays.asList(gt("uuid", "id5"), lt("age", 30));

    List<FileSlice> off = query(IndexPlannerMode.OFF, filters);
    assertTrue(reports.isEmpty(), "OFF runs no planner code: " + reports);
    assertEquals(Collections.singleton("par3"), partitionsOf(off));

    List<FileSlice> compare = query(IndexPlannerMode.COMPARE, filters);
    assertEquals(off, compare);
    assertEquals(2, reports.size(), reports.toString());
    for (IndexPlannerSupport.Report report : reports) {
      assertNull(report.failure());
      assertTrue(report.comparison().isSame(), report.toString());
    }
    assertEquals(IndexPlannerSupport.Stage.PARTITIONS, reports.get(0).stage());
    assertEquals(Collections.singleton("par3"), reports.get(0).plannerKeptSet());

    reports.clear();
    List<FileSlice> on = query(IndexPlannerMode.ON, filters);
    assertEquals(off, on);
    assertEquals(2, reports.size(), reports.toString());
    reports.forEach(report -> assertNull(report.failure()));
  }

  @Test
  void testFailures() throws Exception {
    writeTable();
    List<ResolvedExpression> filters = Arrays.asList(gt("uuid", "id5"), lt("age", 30));
    // the metadata table of the file stage fails: Flink's own column stats lookup (mocked) prunes nothing
    try (MockedConstruction<FileStatsIndex> ignored = mockConstruction(FileStatsIndex.class,
        (mock, context) -> {
          when(mock.getMetadataTable()).thenThrow(new IllegalStateException("injected"));
          when(mock.computeCandidateFiles(any(), anyList(), anyList())).thenReturn(null);
        })) {
      // ON: no pruning from the planner, every file slice of the candidate partitions is kept
      List<FileSlice> on = queryWithoutPartitionPruning(IndexPlannerMode.ON, filters);
      assertEquals(4, on.size());
      assertEquals(1, reports.size());
      assertInstanceOf(IllegalStateException.class, reports.get(0).failure());

      // COMPARE: the failure is ignored, Flink's result is used
      reports.clear();
      List<FileSlice> compare = queryWithoutPartitionPruning(IndexPlannerMode.COMPARE, filters);
      assertEquals(4, compare.size());
      assertEquals(1, reports.size());
      assertInstanceOf(IllegalStateException.class, reports.get(0).failure());
    }
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

  /**
   * Writes the 8 rows of {@link TestData#DATA_SET_INSERT}, one file per partition, twice: column stats are enabled at
   * the second commit, so that they are initialized from the existing files and their index definition lists the
   * columns (a Flink writer never updates it, see HUDI-8801).
   */
  private void writeTable() throws Exception {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.set(FlinkOptions.METADATA_ENABLED, true);
    conf.setString(HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key(), "false");
    conf.setString(HoodieMetadataConfig.ENABLE_METADATA_INDEX_PARTITION_STATS.key(), "false");
    TestData.writeData(TestData.DATA_SET_INSERT, conf);
    conf.setString(HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key(), "true");
    conf.setString(HoodieMetadataConfig.ENABLE_METADATA_INDEX_PARTITION_STATS.key(), "true");
    TestData.writeData(TestData.DATA_SET_INSERT, conf);
  }

  private Configuration conf(IndexPlannerMode mode) {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.set(FlinkOptions.METADATA_ENABLED, true);
    conf.set(FlinkOptions.READ_DATA_SKIPPING_ENABLED, true);
    conf.setString(HoodieMetadataConfig.INDEX_PLANNER_MODE.key(), mode.name());
    return conf;
  }

  private List<FileSlice> query(IndexPlannerMode mode, List<ResolvedExpression> filters) {
    Configuration conf = conf(mode);
    HoodieTableMetaClient metaClient = StreamerUtil.createMetaClient(conf);
    ColumnStatsProbe probe = IndexPlannerSupport.withPlannerFilter(ColumnStatsProbe.newInstance(filters), filters, conf, TestConfigurations.ROW_TYPE);
    PartitionPruners.PartitionPruner pruner = PartitionPruners.builder().rowType(TestConfigurations.ROW_TYPE)
        .basePath(tempFile.getAbsolutePath()).metaClient(metaClient).conf(conf).columnStatsProbe(probe).build();
    try (FileIndex fileIndex = FileIndex.builder().path(new StoragePath(tempFile.getAbsolutePath())).conf(conf)
        .rowType(TestConfigurations.ROW_TYPE).metaClient(metaClient).columnStatsProbe(probe).partitionPruner(pruner).build()) {
      return fileSlices(metaClient, fileIndex);
    }
  }

  private List<FileSlice> queryWithoutPartitionPruning(IndexPlannerMode mode, List<ResolvedExpression> filters) {
    Configuration conf = conf(mode);
    HoodieTableMetaClient metaClient = StreamerUtil.createMetaClient(conf);
    ColumnStatsProbe probe = IndexPlannerSupport.withPlannerFilter(ColumnStatsProbe.newInstance(filters), filters, conf, TestConfigurations.ROW_TYPE);
    try (FileIndex fileIndex = FileIndex.builder().path(new StoragePath(tempFile.getAbsolutePath())).conf(conf)
        .rowType(TestConfigurations.ROW_TYPE).metaClient(metaClient).columnStatsProbe(probe).build()) {
      return fileSlices(metaClient, fileIndex);
    }
  }

  private static List<FileSlice> fileSlices(HoodieTableMetaClient metaClient, FileIndex fileIndex) {
    List<String> partitions = fileIndex.getOrBuildPartitionPaths();
    List<StoragePathInfo> files = fileIndex.getFilesInPartitions();
    HoodieTimeline timeline = metaClient.getCommitsAndCompactionTimeline().filterCompletedAndCompactionInstants();
    String latest = timeline.lastInstant().get().requestedTime();
    List<FileSlice> slices;
    try (HoodieTableFileSystemView fsView = new HoodieTableFileSystemView(metaClient, timeline, files)) {
      slices = partitions.stream().sorted()
          .flatMap(partition -> fsView.getLatestMergedFileSlicesBeforeOrOn(partition, latest))
          .collect(Collectors.toList());
    }
    return fileIndex.filterFileSlices(slices);
  }

  private static Set<String> partitionsOf(List<FileSlice> slices) {
    return slices.stream().map(FileSlice::getPartitionPath).collect(Collectors.toCollection(HashSet::new));
  }

  private static ResolvedExpression gt(String field, Object value) {
    return call(BuiltInFunctionDefinitions.GREATER_THAN, field, value);
  }

  private static ResolvedExpression lt(String field, Object value) {
    return call(BuiltInFunctionDefinitions.LESS_THAN, field, value);
  }

  private static ResolvedExpression eq(String field, Object value) {
    return call(BuiltInFunctionDefinitions.EQUALS, field, value);
  }

  private static ResolvedExpression call(BuiltInFunctionDefinition def, String field, Object value) {
    boolean string = value instanceof String;
    return CallExpression.permanent(def,
        Arrays.asList(
            new FieldReferenceExpression(field, string ? DataTypes.STRING() : DataTypes.INT(), 0, 0),
            new ValueLiteralExpression(value, string ? DataTypes.STRING().notNull() : DataTypes.INT().notNull())),
        DataTypes.BOOLEAN());
  }
}
