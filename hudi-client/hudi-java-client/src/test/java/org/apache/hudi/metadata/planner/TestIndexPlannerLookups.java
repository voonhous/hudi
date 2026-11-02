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
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.expression.Expression;
import org.apache.hudi.common.expression.Literal;
import org.apache.hudi.common.expression.NameReference;
import org.apache.hudi.common.expression.Predicates;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordDelegate;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.metadata.stats.HoodieColumnRangeMetadata;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.testutils.TestHoodieMetadataBase;

import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests the index lookups of the RFC-88 planner against a real metadata table written by the Java write client, with
 * column stats, partition stats and the record index: every file group holding a matching row is kept, files are
 * pruned, and the result equals what today's engine code computes where that is cheap to get.
 */
public class TestIndexPlannerLookups extends TestHoodieMetadataBase {

  private static final String FIRST_PARTITION = HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;
  private static final HoodieSchema TABLE_SCHEMA = HoodieSchema.parse(TRIP_EXAMPLE_SCHEMA);
  private static final Schema AVRO_SCHEMA = new Schema.Parser().parse(TRIP_EXAMPLE_SCHEMA);
  private static final String INDEXED_COLUMNS = "timestamp,_row_key,rider,begin_lat,distance_in_meters,current_date,weight,height";

  private static final HoodieSchema LONG = HoodieSchema.create(HoodieSchemaType.LONG);
  private static final HoodieSchema INT = HoodieSchema.create(HoodieSchemaType.INT);
  private static final HoodieSchema FLOAT = HoodieSchema.create(HoodieSchemaType.FLOAT);
  private static final HoodieSchema DOUBLE = HoodieSchema.create(HoodieSchemaType.DOUBLE);
  private static final HoodieSchema STRING = HoodieSchema.create(HoodieSchemaType.STRING);
  private static final HoodieSchema DATE = HoodieSchema.createDate();

  /**
   * The latest version of each record (the updates win, having the greatest ordering value), with its file group.
   */
  private final Map<String, Pair<String, GenericRecord>> latestByKey = new HashMap<>();

  /**
   * The rows of each file group, from {@link #latestByKey}.
   */
  private final Map<String, List<GenericRecord>> rowsByFileId = new HashMap<>();

  static Stream<Arguments> tables() {
    return Stream.of(
        Arguments.of(HoodieTableType.COPY_ON_WRITE, false),
        Arguments.of(HoodieTableType.MERGE_ON_READ, false),
        Arguments.of(HoodieTableType.COPY_ON_WRITE, true),
        Arguments.of(HoodieTableType.MERGE_ON_READ, true));
  }

  @ParameterizedTest(name = "{0}, partitioned record index: {1}")
  @MethodSource("tables")
  void lookupsKeepEveryMatchingFileAndPrune(HoodieTableType type, boolean partitionedRecordIndex) throws Exception {
    this.tableType = type;
    initPath();
    initFileSystem(basePath, storageConf);
    storage.createDirectory(new StoragePath(basePath));
    Properties tableProperties = new Properties();
    tableProperties.setProperty(HoodieTableConfig.RECORDKEY_FIELDS.key(), "_row_key");
    tableProperties.setProperty(HoodieTableConfig.PARTITION_FIELDS.key(), "partition_path");
    metaClient = HoodieTestUtils.init(storageConf, basePath, type, tableProperties);
    dataGen = new HoodieTestDataGenerator(20261102L);

    HoodieJavaEngineContext engineContext = new HoodieJavaEngineContext(storageConf);
    HoodieMetadataConfig.Builder metadataConfig = HoodieMetadataConfig.newBuilder()
        .enable(true)
        .withMetadataIndexColumnStats(true)
        .withMetadataIndexPartitionStats(true)
        .withColumnStatsIndexForColumns(INDEXED_COLUMNS);
    if (partitionedRecordIndex) {
      metadataConfig.withEnableRecordLevelIndex(true);
    } else {
      metadataConfig.withEnableGlobalRecordLevelIndex(true).withRecordIndexFileGroupCount(2, 2);
    }
    HoodieWriteConfig config = getWriteConfigBuilder(true, true, false).withMetadataConfig(metadataConfig.build()).build();

    List<String> instants = new ArrayList<>();
    try (HoodieJavaWriteClient client = new HoodieJavaWriteClient(engineContext, config)) {
      // the first commit writes one partition only, so that partition stats can prune the others
      instants.add(write(client, instant -> dataGen.generateInsertsForPartition(instant, 30, FIRST_PARTITION), false));
      instants.add(write(client, instant -> dataGen.generateInserts(instant, 30, 2000L), false));
      instants.add(write(client, instant -> dataGen.generateInserts(instant, 30, 3000L), false));
      // updates: log files for MOR, new file versions for COW
      instants.add(write(client, instant -> dataGen.generateUniqueUpdates(instant, 10, Long.MAX_VALUE / 2), true));
    }

    latestByKey.values().forEach(row -> rowsByFileId.computeIfAbsent(row.getLeft(), id -> new ArrayList<>()).add(row.getRight()));
    metaClient = HoodieTableMetaClient.reload(metaClient);
    // the Java client does not record the indexed columns in the column stats index definition (HUDI-8801), and
    // without them neither Spark nor the planner uses column or partition stats
    HoodieIndexDefinition columnStatsDefinition = metaClient.getIndexForMetadataPartition(HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS).get();
    try (HoodieTableMetadata tableMetadata = metadata(config, engineContext)) {
      IndexLookupContext context = new IndexLookupContext(metaClient, tableMetadata, engineContext, TABLE_SCHEMA,
          HudiStatsEvaluator.getInstance(), new HashMap<>());
      assertEquals(columnStatsDefinition.getSourceFields().isEmpty(), !new ColumnStatsLookup().canPrune(eq("timestamp", 2000L, LONG), context));
    }
    metaClient.buildIndexDefinition(HoodieIndexDefinition.newBuilder()
        .withIndexName(columnStatsDefinition.getIndexName())
        .withIndexType(columnStatsDefinition.getIndexType())
        .withIndexFunction(columnStatsDefinition.getIndexFunction())
        .withSourceFields(Arrays.asList(INDEXED_COLUMNS.split(",")))
        .withVersion(columnStatsDefinition.getVersion())
        .withIndexOptions(new HashMap<>())
        .build());
    metaClient = HoodieTableMetaClient.reload(metaClient);
    for (MetadataPartitionType index : Arrays.asList(MetadataPartitionType.COLUMN_STATS, MetadataPartitionType.PARTITION_STATS,
        MetadataPartitionType.RECORD_INDEX)) {
      assertTrue(metaClient.getTableConfig().isMetadataPartitionAvailable(index), index + " is available");
    }

    try (HoodieTableMetadata tableMetadata = metadata(config, engineContext);
         HoodieTableFileSystemView fsView = new HoodieTableFileSystemView(tableMetadata, metaClient, metaClient.reloadActiveTimeline())) {
      Map<String, List<String>> filesByPartition = new LinkedHashMap<>();
      Map<String, List<String>> filesByFileId = new HashMap<>();
      boolean anyLogFile = false;
      for (String partition : tableMetadata.getAllPartitionPaths()) {
        List<String> files = new ArrayList<>();
        for (FileSlice slice : fsView.getLatestFileSlices(partition).collect(Collectors.toList())) {
          List<String> sliceFiles = new ArrayList<>();
          slice.getBaseFile().ifPresent(base -> sliceFiles.add(base.getFileName()));
          slice.getLogFiles().forEach(log -> sliceFiles.add(log.getFileName()));
          anyLogFile |= slice.getLogFiles().count() > 0;
          files.addAll(sliceFiles);
          filesByFileId.put(slice.getFileId(), sliceFiles);
        }
        filesByPartition.put(partition, files);
      }
      assertEquals(3, filesByPartition.size());
      assertEquals(type == HoodieTableType.MERGE_ON_READ, anyLogFile, "log files exist only for MOR");
      Set<String> allFiles = filesByPartition.values().stream().flatMap(List::stream).collect(Collectors.toSet());
      IndexLookupContext context = new IndexLookupContext(metaClient, tableMetadata, engineContext, TABLE_SCHEMA,
          HudiStatsEvaluator.getInstance(), filesByPartition);

      ColumnStatsLookup columnStats = new ColumnStatsLookup();
      PartitionStatsLookup partitionStats = new PartitionStatsLookup();
      RecordIndexLookup recordIndex = new RecordIndexLookup();
      DefaultIndexPlanner planner = new DefaultIndexPlanner();

      String firstRider = "rider-" + instants.get(0);
      GenericRecord someRow = rowsByFileId.values().iterator().next().get(0);
      String someKey = someRow.get("_row_key").toString();
      double someLat = (Double) someRow.get("begin_lat");
      int someDistance = (Integer) someRow.get("distance_in_meters");
      int someDate = (Integer) someRow.get("current_date");
      float someWeight = (Float) someRow.get("weight");
      BigDecimal someHeight = height(someRow);

      List<Expression> filters = Arrays.asList(
          eq("timestamp", 2000L, LONG),
          eq("rider", firstRider, STRING),
          Predicates.lt(col("timestamp"), lit(2500L, LONG)),
          Predicates.not(eq("timestamp", 2000L, LONG)),
          Predicates.and(eq("rider", firstRider, STRING), Predicates.lt(col("begin_lat"), lit(0.5, DOUBLE))),
          Predicates.or(eq("timestamp", 3000L, LONG), eq("rider", firstRider, STRING)),
          eq("begin_lat", someLat, DOUBLE),
          eq("distance_in_meters", someDistance, INT),
          eq("current_date", someDate, DATE),
          eq("weight", someWeight, FLOAT),
          eq("height", someHeight, HoodieSchema.createDecimal(10, 6)),
          Predicates.startsWith(col("rider"), lit("rider-" + instants.get(1), STRING)),
          eq("_row_key", someKey, STRING),
          Predicates.in(col("_row_key"), Arrays.asList(lit(someKey, STRING), lit("no-such-key", STRING))),
          Predicates.and(eq("_row_key", someKey, STRING), Predicates.gt(col("timestamp"), lit(0L, LONG))),
          Predicates.gteq(col("timestamp"), lit(3000L, LONG)));

      // filters on the per-commit columns, which column stats must prune whatever the random values
      List<Expression> mustPrune = Arrays.asList(filters.get(0), filters.get(1), filters.get(2), filters.get(11));
      int prunedByColumnStats = 0;
      for (Expression filter : filters) {
        Set<String> matchingFileIds = new HashSet<>();
        rowsByFileId.forEach((fileId, rows) -> {
          if (rows.stream().anyMatch(row -> matches(filter, row))) {
            matchingFileIds.add(fileId);
          }
        });
        assertFalse(matchingFileIds.isEmpty(), "some row matches " + filter);

        for (IndexLookup lookup : Arrays.<IndexLookup>asList(columnStats, partitionStats, recordIndex)) {
          CandidateFiles result = lookup.canPrune(filter, context) ? lookup.lookup(filter, context) : CandidateFiles.all();
          assertKeepsMatchingFileGroups(lookup.index() + " for " + filter, result, matchingFileIds, filesByFileId);
          boolean pruned = !result.isAll() && result.fileNames().size() < allFiles.size();
          if (lookup == columnStats && pruned) {
            prunedByColumnStats++;
          }
          if (lookup == columnStats && mustPrune.contains(filter)) {
            assertTrue(pruned, "column stats prune for " + filter);
          }
        }
        List<IndexLookup> plan = planner.plan(filter, TABLE_SCHEMA, EnumSet.of(MetadataPartitionType.COLUMN_STATS,
            MetadataPartitionType.PARTITION_STATS, MetadataPartitionType.RECORD_INDEX));
        CandidateFiles first = DefaultIndexPlanner.lookup(plan, filter, context);
        CandidateFiles all = DefaultIndexPlanner.lookupAll(plan, filter, context);
        assertKeepsMatchingFileGroups("plan for " + filter, first, matchingFileIds, filesByFileId);
        assertKeepsMatchingFileGroups("whole plan for " + filter, all, matchingFileIds, filesByFileId);
        assertTrue(first.isAll() || first.fileNames().containsAll(all.isAll() ? allFiles : all.fileNames()), "intersecting prunes at least as much");
      }
      assertTrue(prunedByColumnStats >= mustPrune.size(), "column stats pruned for " + prunedByColumnStats + " filters");

      // column stats: the same files as Spark's translation (min <= v <= max, files without stats kept)
      assertEquals(sparkEqualTo(tableMetadata, filesByPartition, "timestamp", 2000L),
          columnStats.lookup(eq("timestamp", 2000L, LONG), context).fileNames());
      assertEquals(sparkEqualTo(tableMetadata, filesByPartition, "rider", firstRider),
          columnStats.lookup(eq("rider", firstRider, STRING), context).fileNames());
      assertTrue(columnStats.lookup(eq("rider", firstRider, STRING), context).fileNames().size() < allFiles.size());

      // partition stats: only the first partition holds the first commit's rider
      assertEquals(new HashSet<>(filesByPartition.get(FIRST_PARTITION)),
          partitionStats.lookup(eq("rider", firstRider, STRING), context).fileNames());
      assertTrue(partitionStats.canPrune(eq("rider", firstRider, STRING), context));
      assertFalse(partitionStats.canPrune(eq("nation", "x", STRING), context));

      // record index: exactly the files of the key's file group, as Spark's record index support computes
      Set<String> keyFileIds = new HashSet<>();
      rowsByFileId.forEach((fileId, rows) -> {
        if (rows.stream().anyMatch(row -> someKey.equals(row.get("_row_key").toString()))) {
          keyFileIds.add(fileId);
        }
      });
      Set<String> expected = keyFileIds.stream().flatMap(id -> filesByFileId.get(id).stream()).collect(Collectors.toSet());
      assertTrue(recordIndex.canPrune(eq("_row_key", someKey, STRING), context));
      assertEquals(expected, recordIndex.lookup(eq("_row_key", someKey, STRING), context).fileNames());
      if (!partitionedRecordIndex) {
        Set<String> sparkFileIds = tableMetadata.readRecordIndexLocationsWithKeys(HoodieListData.eager(Arrays.asList(someKey, "no-such-key")))
            .collectAsList().stream().map(p -> p.getValue().getFileId()).collect(Collectors.toSet());
        Set<String> spark = sparkFileIds.stream().flatMap(id -> filesByFileId.getOrDefault(id, new ArrayList<>()).stream()).collect(Collectors.toSet());
        assertEquals(spark, recordIndex.lookup(Predicates.in(col("_row_key"), Arrays.asList(lit(someKey, STRING), lit("no-such-key", STRING))),
            context).fileNames());
      }
      assertTrue(recordIndex.lookup(eq("_row_key", "no-such-key", STRING), context).fileNames().isEmpty());
      assertFalse(recordIndex.canPrune(eq("rider", firstRider, STRING), context));

      // the planner: the record index wins over column stats, after partition stats
      Expression keyAndRider = Predicates.and(eq("_row_key", someKey, STRING), Predicates.gt(col("timestamp"), lit(0L, LONG)));
      List<IndexLookup> plan = planner.plan(keyAndRider, TABLE_SCHEMA, EnumSet.allOf(MetadataPartitionType.class));
      assertEquals(Arrays.asList(MetadataPartitionType.PARTITION_STATS, MetadataPartitionType.RECORD_INDEX, MetadataPartitionType.COLUMN_STATS),
          plan.stream().map(IndexLookup::index).collect(Collectors.toList()));
      assertEquals(expected, DefaultIndexPlanner.lookup(plan, keyAndRider, context).fileNames());
    }
  }

  private String write(HoodieJavaWriteClient client, Function<String, List<HoodieRecord>> generator, boolean upsert) {
    String instant = client.startCommit();
    List<HoodieRecord> records = generator.apply(instant);
    Map<String, GenericRecord> rowsByKey = new HashMap<>();
    for (HoodieRecord record : records) {
      rowsByKey.put(record.getRecordKey(), (GenericRecord) record.getData());
    }
    List<WriteStatus> statuses = upsert ? client.upsert(records, instant) : client.insert(records, instant);
    client.commit(instant, statuses);
    assertNoWriteErrors(statuses);
    int tracked = 0;
    for (WriteStatus status : statuses) {
      for (HoodieRecordDelegate delegate : status.getWrittenRecordDelegates()) {
        String fileId = delegate.getNewLocation().get().getFileId();
        latestByKey.put(delegate.getRecordKey(), Pair.of(fileId, rowsByKey.get(delegate.getRecordKey())));
        tracked++;
      }
    }
    assertEquals(records.size(), tracked, "every written record is tracked");
    return instant;
  }

  private static void assertKeepsMatchingFileGroups(String what, CandidateFiles result, Set<String> matchingFileIds,
                                                    Map<String, List<String>> filesByFileId) {
    if (result.isAll()) {
      return;
    }
    for (String fileId : matchingFileIds) {
      List<String> files = filesByFileId.get(fileId);
      assertTrue(files.stream().anyMatch(result.fileNames()::contains), what + " keeps file group " + fileId + " " + files);
    }
  }

  /**
   * Spark's {@code DataSkippingUtils} translation of {@code column = value}: files whose stats have
   * {@code min <= value <= max}, plus the files without stats.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  private static Set<String> sparkEqualTo(HoodieTableMetadata metadata, Map<String, List<String>> filesByPartition, String column, Comparable value) {
    List<Pair<String, String>> pairs = new ArrayList<>();
    filesByPartition.forEach((partition, files) -> files.forEach(file -> pairs.add(Pair.of(partition, file))));
    Map<Pair<String, String>, HoodieMetadataColumnStats> stats = metadata.getColumnStats(pairs, column);
    Set<String> kept = new HashSet<>();
    for (Pair<String, String> pair : pairs) {
      HoodieMetadataColumnStats entry = stats.get(pair);
      if (entry == null) {
        kept.add(pair.getRight());
        continue;
      }
      HoodieColumnRangeMetadata<Comparable> range = HoodieColumnRangeMetadata.fromColumnStats(entry);
      Comparable min = range.getMinValue() == null ? null : normalize(range.getMinValue());
      Comparable max = range.getMaxValue() == null ? null : normalize(range.getMaxValue());
      if (min != null && max != null && min.compareTo(value) <= 0 && max.compareTo(value) >= 0) {
        kept.add(pair.getRight());
      }
    }
    return kept;
  }

  private static Comparable<?> normalize(Comparable<?> value) {
    return value instanceof CharSequence ? value.toString() : value;
  }

  // -------------------------------------------------------------------------
  //  Filters and their evaluation on rows
  // -------------------------------------------------------------------------

  private static NameReference col(String name) {
    return new NameReference(name);
  }

  private static <T> Literal<T> lit(T value, HoodieSchema type) {
    return new Literal<>(value, type);
  }

  private static Expression eq(String column, Object value, HoodieSchema type) {
    return Predicates.eq(col(column), lit(value, type));
  }

  private static BigDecimal height(GenericRecord row) {
    Schema schema = AVRO_SCHEMA.getField("height").schema();
    return new Conversions.DecimalConversion().fromFixed((GenericFixed) row.get("height"), schema, LogicalTypes.decimal(10, 6));
  }

  private static Object value(GenericRecord row, String column) {
    if ("height".equals(column)) {
      return height(row);
    }
    Object value = row.get(column);
    return value instanceof CharSequence ? value.toString() : value;
  }

  /**
   * The filters here have a column on the left of comparisons and no nulls in the data.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  private static boolean matches(Expression expr, GenericRecord row) {
    if (expr instanceof Predicates.And) {
      return matches(((Predicates.And) expr).getLeft(), row) && matches(((Predicates.And) expr).getRight(), row);
    }
    if (expr instanceof Predicates.Or) {
      return matches(((Predicates.Or) expr).getLeft(), row) || matches(((Predicates.Or) expr).getRight(), row);
    }
    if (expr instanceof Predicates.Not) {
      return !matches(expr.getChildren().get(0), row);
    }
    if (expr instanceof Predicates.In) {
      Object value = value(row, ((NameReference) expr.getChildren().get(0)).getName());
      Collection<Expression> values = ((Predicates.In) expr).getRightChildren();
      return values.stream().anyMatch(v -> ((Comparable) ((Literal<?>) v).getValue()).compareTo(value) == 0);
    }
    if (expr instanceof Predicates.StringStartsWith) {
      Predicates.StringStartsWith startsWith = (Predicates.StringStartsWith) expr;
      return value(row, ((NameReference) startsWith.getLeft()).getName()).toString().startsWith(((Literal<?>) startsWith.getRight()).getValue().toString());
    }
    Predicates.BinaryComparison cmp = (Predicates.BinaryComparison) expr;
    Comparable left = (Comparable) value(row, ((NameReference) cmp.getLeft()).getName());
    int c = left.compareTo(((Literal<?>) cmp.getRight()).getValue());
    switch (cmp.getOperator()) {
      case EQ:
        return c == 0;
      case LT:
        return c < 0;
      case LT_EQ:
        return c <= 0;
      case GT:
        return c > 0;
      case GT_EQ:
        return c >= 0;
      default:
        throw new IllegalArgumentException(cmp.toString());
    }
  }
}
