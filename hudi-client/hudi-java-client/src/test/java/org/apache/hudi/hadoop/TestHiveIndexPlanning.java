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

import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordDelegate;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.hadoop.utils.HoodieHiveUtils;
import org.apache.hudi.hadoop.utils.HoodieInputFormatUtils;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.metadata.planner.CandidateFiles;
import org.apache.hudi.metadata.planner.IndexPlannerComparison;
import org.apache.hudi.metadata.planner.IndexPlannerMode;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.testutils.HoodieMergeOnReadTestUtils;
import org.apache.hudi.testutils.TestHoodieMetadataBase;

import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.ql.udf.UDFLike;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBetween;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFIn;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNot;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotNull;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNull;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.sql.Date;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The expression oracle of RFC-88 for Hive: a table written by the Java write client with the metadata table, column
 * stats, partition stats and the record index is read through Hive's input formats with the filter Hive pushes
 * ({@link TableScanDesc#FILTER_EXPR_CONF_STR}), under each {@link IndexPlannerMode}. For every query:
 * <ul>
 *   <li>ON reads the same matching rows as OFF, and lists every file group holding a matching row;</li>
 *   <li>COMPARE lists and reads exactly what OFF does, and the files the planner would prune hold no matching row;</li>
 *   <li>ON lists fewer splits than OFF where the indexes can prove files irrelevant.</li>
 * </ul>
 *
 * <p>The test lives here rather than in hudi-hadoop-mr because hudi-java-client depends on hudi-hadoop-mr: the Java
 * write client, which builds the metadata-table indexes, cannot be a test dependency of hudi-hadoop-mr.
 */
public class TestHiveIndexPlanning extends TestHoodieMetadataBase {

  private static final String FIRST_PARTITION = HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;
  private static final Schema AVRO_SCHEMA = new Schema.Parser().parse(TRIP_EXAMPLE_SCHEMA);
  private static final String INDEXED_COLUMNS = "timestamp,_row_key,rider,begin_lat,distance_in_meters,current_date,weight,height,current_ts";

  /**
   * The latest version of each record, with its file group.
   */
  private final Map<String, String> fileIdByKey = new HashMap<>();
  private final Map<String, GenericRecord> latestByKey = new HashMap<>();
  /**
   * Every version of every record written.
   */
  private final List<GenericRecord> written = new ArrayList<>();

  static Stream<Arguments> tables() {
    return Stream.of(
        Arguments.of(HoodieTableType.COPY_ON_WRITE, false),
        Arguments.of(HoodieTableType.MERGE_ON_READ, true),
        Arguments.of(HoodieTableType.MERGE_ON_READ, false));
  }

  @ParameterizedTest(name = "{0}, realtime: {1}")
  @MethodSource("tables")
  void plannerKeepsEveryMatchingRowAndPrunes(HoodieTableType type, boolean realtime) throws Exception {
    List<String> instants = writeTable(type);
    List<String> allPartitions = Arrays.asList(HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS);
    List<String> allInputPaths = allPartitions.stream().map(p -> new StoragePath(basePath, p).toString()).collect(Collectors.toList());

    String firstRider = "rider-" + instants.get(0);
    GenericRecord someRow = latestByKey.values().stream().filter(r -> ((Long) r.get("timestamp")) == 2000L).findFirst().get();
    String someKey = someRow.get("_row_key").toString();
    double someLat = (Double) someRow.get("begin_lat");
    int someDistance = (Integer) someRow.get("distance_in_meters");
    int someDate = (Integer) someRow.get("current_date");
    float someWeight = (Float) someRow.get("weight");
    BigDecimal someHeight = height(someRow);

    Map<String, Query> queries = new LinkedHashMap<>();
    queries.put("timestamp = 2000", eq("timestamp", 2000L));
    queries.put("rider = first", eq("rider", firstRider));
    queries.put("timestamp < 2500", cmp(new GenericUDFOPLessThan(), "timestamp", 2500L, c -> c < 0));
    queries.put("timestamp >= 3000", cmp(new GenericUDFOPEqualOrGreaterThan(), "timestamp", 3000L, c -> c >= 0));
    queries.put("timestamp <> 2000", cmp(new GenericUDFOPNotEqual(), "timestamp", 2000L, c -> c != 0));
    queries.put("NOT(timestamp > 2000)", not(cmp(new GenericUDFOPGreaterThan(), "timestamp", 2000L, c -> c > 0)));
    queries.put("timestamp BETWEEN 2000 AND 2999", between("timestamp", 2000L, 2999L));
    queries.put("timestamp IN (2000, 3000)", in("timestamp", 2000L, 3000L));
    queries.put("rider = first AND begin_lat < 0.5", and(eq("rider", firstRider), cmp(new GenericUDFOPLessThan(), "begin_lat", 0.5, c -> c < 0)));
    queries.put("timestamp = 3000 OR rider = first", or(eq("timestamp", 3000L), eq("rider", firstRider)));
    queries.put("rider LIKE second%", like("rider", "rider-" + instants.get(1)));
    queries.put("rider >= third", cmp(new GenericUDFOPEqualOrGreaterThan(), "rider", "rider-" + instants.get(2), c -> c >= 0));
    queries.put("rider IS NULL", isNull("rider"));
    queries.put("rider IS NOT NULL", isNotNull("rider"));
    queries.put("begin_lat = some", eq("begin_lat", someLat));
    queries.put("distance_in_meters = some", eq("distance_in_meters", someDistance));
    queries.put("current_date = some (date)", eq("current_date", new DateDays(someDate)));
    queries.put("weight = some (float)", eq("weight", someWeight));
    queries.put("height = some (decimal)", eq("height", someHeight));
    // beyond the greatest value ever written: no file can match, so every file is pruned if the constant translates
    queries.put("distance_in_meters > max (int)", cmp(new GenericUDFOPGreaterThan(), "distance_in_meters",
        max(r -> (Integer) r.get("distance_in_meters")), c -> c > 0));
    queries.put("begin_lat > max (double)", cmp(new GenericUDFOPGreaterThan(), "begin_lat", max(r -> (Double) r.get("begin_lat")), c -> c > 0));
    queries.put("weight > max (float)", cmp(new GenericUDFOPGreaterThan(), "weight", max(r -> (Float) r.get("weight")), c -> c > 0));
    queries.put("current_date > max (date)", cmp(new GenericUDFOPGreaterThan(), "current_date",
        new DateDays(max(r -> (Integer) r.get("current_date"))), c -> c > 0));
    queries.put("height > max (decimal)", cmp(new GenericUDFOPGreaterThan(), "height", max(TestHiveIndexPlanning::height), c -> c > 0));
    queries.put("current_ts > 0 AND timestamp = 2000", and(cmp(new GenericUDFOPGreaterThan(), "current_ts", 0L, c -> c > 0), eq("timestamp", 2000L)));
    queries.put("_row_key = some", eq("_row_key", someKey));
    queries.put("_row_key IN (some, missing)", in("_row_key", someKey, "no-such-key"));
    queries.put("_row_key = some AND timestamp > 0", and(eq("_row_key", someKey), cmp(new GenericUDFOPGreaterThan(), "timestamp", 0L, c -> c > 0)));
    queries.put("_row_key = some OR timestamp = 3000", or(eq("_row_key", someKey), eq("timestamp", 3000L)));
    queries.put("driver = x (not indexed)", eq("driver", "driver-" + instants.get(0)));
    queries.put("partition_path = first AND timestamp = 2000 (first partition)",
        and(eq("partition_path", FIRST_PARTITION), eq("timestamp", 2000L)).inPartitions(Collections.singletonList(FIRST_PARTITION)));
    queries.put("partition_path = first (all partitions)", eq("partition_path", FIRST_PARTITION));

    // filters on the per-commit columns, which the planner must prune whatever the random values; and the record key
    Set<String> mustPrune = new HashSet<>(Arrays.asList("timestamp = 2000", "rider = first", "timestamp < 2500",
        "timestamp BETWEEN 2000 AND 2999", "rider LIKE second%", "rider IS NULL",
        "distance_in_meters > max (int)", "begin_lat > max (double)", "weight > max (float)", "current_date > max (date)", "height > max (decimal)", "current_ts > 0 AND timestamp = 2000",
        "partition_path = first AND timestamp = 2000 (first partition)"));
    if (realtime || type == HoodieTableType.COPY_ON_WRITE) {
      // the record index is not used by read-optimized queries
      mustPrune.addAll(Arrays.asList("_row_key = some", "_row_key IN (some, missing)", "_row_key = some AND timestamp > 0"));
    }

    StringBuilder report = new StringBuilder(String.format("%n%s realtime=%s%n%-62s %8s %8s %8s %8s %8s%n", type, realtime,
        "query", "rows", "rowsON", "splitOFF", "splitON", "cmpOnlyP"));
    int prunedQueries = 0;
    List<String> notPruned = new ArrayList<>();
    for (Map.Entry<String, Query> entry : queries.entrySet()) {
      String name = entry.getKey();
      Query query = entry.getValue();
      List<String> inputPaths = query.partitions == null ? allInputPaths
          : query.partitions.stream().map(p -> new StoragePath(basePath, p).toString()).collect(Collectors.toList());

      Map<String, String> off = matchingRows(read(query, IndexPlannerMode.OFF, inputPaths, realtime), query);
      Map<String, String> on = matchingRows(read(query, IndexPlannerMode.ON, inputPaths, realtime), query);
      List<GenericRecord> compareRows = read(query, IndexPlannerMode.COMPARE, inputPaths, realtime);
      List<GenericRecord> offRows = read(query, IndexPlannerMode.OFF, inputPaths, realtime);
      assertEquals(rowStrings(offRows), rowStrings(compareRows), "COMPARE reads what OFF reads for " + name);
      assertEquals(off, on, "ON reads the matching rows OFF reads for " + name);

      Set<String> splitsOff = splitFiles(query, IndexPlannerMode.OFF, inputPaths, realtime);
      Set<String> splitsOn = splitFiles(query, IndexPlannerMode.ON, inputPaths, realtime);
      Set<String> splitsCompare = splitFiles(query, IndexPlannerMode.COMPARE, inputPaths, realtime);
      assertEquals(splitsOff, splitsCompare, "COMPARE lists what OFF lists for " + name);
      assertTrue(splitsOff.containsAll(splitsOn), "ON lists a subset of OFF for " + name);
      Set<String> onFileIds = splitsOn.stream().map(FSUtils::getFileIdFromFileName).collect(Collectors.toSet());
      Set<String> matchingFileIds = off.keySet().stream().map(fileIdByKey::get).collect(Collectors.toSet());
      assertTrue(onFileIds.containsAll(matchingFileIds), "ON lists every file group with a matching row for " + name
          + ": " + matchingFileIds + " listed " + onFileIds);

      // what COMPARE reports: the engine does not prune, so every file the planner prunes is "pruned only by planner";
      // none of them may hold a matching row
      Map<String, List<String>> filesByFileId = new HashMap<>();
      IndexPlannerComparison comparison = compare(query, inputPaths, realtime, filesByFileId);
      for (String fileId : matchingFileIds) {
        assertFalse(comparison.prunedOnlyByPlanner().containsAll(filesByFileId.get(fileId)),
            "the planner prunes every file of " + fileId + " " + filesByFileId.get(fileId) + ", which holds a match of " + name);
      }
      assertTrue(comparison.prunedOnlyByEngine().isEmpty());

      if (mustPrune.contains(name) && splitsOn.size() >= splitsOff.size()) {
        notPruned.add(name + ": " + splitsOn.size() + " of " + splitsOff.size());
      }
      if (splitsOn.size() < splitsOff.size()) {
        prunedQueries++;
      }
      report.append(String.format("%-62s %8d %8d %8d %8d %8d%n", name, off.size(), on.size(), splitsOff.size(), splitsOn.size(),
          comparison.prunedOnlyByPlanner().size()));
    }
    System.out.println(report);
    assertTrue(notPruned.isEmpty(), "ON prunes for " + notPruned);
    assertTrue(prunedQueries >= mustPrune.size(), "pruned for " + prunedQueries + " queries");

    // the planner is off for listings that are not of the latest snapshot, and for failures
    Query timestamp2000 = eq("timestamp", 2000L);
    String tableName = metaClient.getTableConfig().getTableName();
    Map<String, String> timeTravel = Collections.singletonMap(String.format(HoodieHiveUtils.HOODIE_CONSUME_COMMIT, tableName), instants.get(instants.size() - 1));
    assertEquals(splitFiles(timestamp2000, IndexPlannerMode.OFF, allInputPaths, realtime, timeTravel),
        splitFiles(timestamp2000, IndexPlannerMode.ON, allInputPaths, realtime, timeTravel), "no pruning with a query instant");
    Map<String, String> pending = Collections.singletonMap(String.format(HoodieHiveUtils.HOODIE_CONSUME_PENDING_COMMITS, tableName), "true");
    assertEquals(splitFiles(timestamp2000, IndexPlannerMode.OFF, allInputPaths, realtime, pending),
        splitFiles(timestamp2000, IndexPlannerMode.ON, allInputPaths, realtime, pending), "no pruning with pending commits");
    Map<String, String> incremental = new HashMap<>();
    incremental.put(String.format(HoodieHiveUtils.HOODIE_CONSUME_MODE_PATTERN, tableName), HoodieHiveUtils.INCREMENTAL_SCAN_MODE);
    incremental.put(String.format(HoodieHiveUtils.HOODIE_START_COMMIT_PATTERN, tableName), "000");
    incremental.put(String.format(HoodieHiveUtils.HOODIE_MAX_COMMIT_PATTERN, tableName), "-1");
    assertEquals(splitFiles(timestamp2000, IndexPlannerMode.OFF, allInputPaths, realtime, incremental),
        splitFiles(timestamp2000, IndexPlannerMode.ON, allInputPaths, realtime, incremental), "no pruning for incremental queries");
    Query broken = new Query(null, row -> true) {
      @Override
      String serialized() {
        return "not a serialized filter";
      }
    };
    assertEquals(splitFiles(timestamp2000, IndexPlannerMode.OFF, allInputPaths, realtime),
        splitFiles(broken, IndexPlannerMode.ON, allInputPaths, realtime), "a planner failure keeps every file");
  }

  // -------------------------------------------------------------------------
  //  The table
  // -------------------------------------------------------------------------

  private List<String> writeTable(HoodieTableType type) throws Exception {
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
    HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder()
        .enable(true)
        .withMetadataIndexColumnStats(true)
        .withMetadataIndexPartitionStats(true)
        .withColumnStatsIndexForColumns(INDEXED_COLUMNS)
        .withEnableGlobalRecordLevelIndex(true).withRecordIndexFileGroupCount(2, 2)
        .build();
    HoodieWriteConfig config = getWriteConfigBuilder(true, true, false).withMetadataConfig(metadataConfig).build();

    List<String> instants = new ArrayList<>();
    try (HoodieJavaWriteClient client = new HoodieJavaWriteClient(engineContext, config)) {
      // the first commit writes one partition only, so that partition stats can prune the others
      instants.add(write(client, instant -> dataGen.generateInsertsForPartition(instant, 30, FIRST_PARTITION), false));
      instants.add(write(client, instant -> dataGen.generateInserts(instant, 30, 2000L), false));
      instants.add(write(client, instant -> dataGen.generateInserts(instant, 30, 3000L), false));
      // updates: log files for MOR, new file versions for COW
      instants.add(write(client, instant -> dataGen.generateUniqueUpdates(instant, 10, Long.MAX_VALUE / 2), true));
    }
    metaClient = HoodieTableMetaClient.reload(metaClient);
    // the Java client does not record the indexed columns in the column stats index definition (HUDI-8801); without
    // them no engine uses column or partition stats
    HoodieIndexDefinition columnStatsDefinition = metaClient.getIndexForMetadataPartition(HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS).get();
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
    if (type == HoodieTableType.MERGE_ON_READ) {
      try (HoodieTableMetadata tableMetadata = metadata(config, engineContext);
           HoodieTableFileSystemView fsView = new HoodieTableFileSystemView(tableMetadata, metaClient, metaClient.reloadActiveTimeline())) {
        assertTrue(tableMetadata.getAllPartitionPaths().stream().flatMap(fsView::getLatestFileSlices).anyMatch(s -> s.getLogFiles().count() > 0),
            "the MOR table has log files");
      }
    }
    return instants;
  }

  private String write(HoodieJavaWriteClient client, Function<String, List<HoodieRecord>> generator, boolean upsert) {
    String instant = client.startCommit();
    List<HoodieRecord> records = generator.apply(instant);
    Map<String, GenericRecord> rowsByKey = new HashMap<>();
    for (HoodieRecord record : records) {
      rowsByKey.put(record.getRecordKey(), (GenericRecord) record.getData());
      written.add((GenericRecord) record.getData());
    }
    List<WriteStatus> statuses = upsert ? client.upsert(records, instant) : client.insert(records, instant);
    client.commit(instant, statuses);
    assertNoWriteErrors(statuses);
    for (WriteStatus status : statuses) {
      for (HoodieRecordDelegate delegate : status.getWrittenRecordDelegates()) {
        fileIdByKey.put(delegate.getRecordKey(), delegate.getNewLocation().get().getFileId());
        latestByKey.put(delegate.getRecordKey(), rowsByKey.get(delegate.getRecordKey()));
      }
    }
    return instant;
  }

  private <T extends Comparable<T>> T max(Function<GenericRecord, T> column) {
    return written.stream().map(column).max(Comparable::compareTo).get();
  }

  private static BigDecimal height(GenericRecord row) {
    Schema schema = AVRO_SCHEMA.getField("height").schema();
    return new Conversions.DecimalConversion().fromFixed((GenericFixed) row.get("height"), schema, LogicalTypes.decimal(10, 6));
  }

  // -------------------------------------------------------------------------
  //  Reading through the Hive input formats
  // -------------------------------------------------------------------------

  private JobConf jobConf(Query query, IndexPlannerMode mode, Map<String, String> extra) {
    JobConf jobConf = new JobConf(storageConf.unwrapAs(Configuration.class));
    jobConf.set(TableScanDesc.FILTER_EXPR_CONF_STR, query.serialized());
    if (query.filter != null) {
      jobConf.set(TableScanDesc.FILTER_TEXT_CONF_STR, query.filter.getExprString());
    }
    if (mode != IndexPlannerMode.OFF) {
      jobConf.set(HoodieMetadataConfig.INDEX_PLANNER_MODE.key(), mode.name());
    }
    extra.forEach(jobConf::set);
    return jobConf;
  }

  private List<GenericRecord> read(Query query, IndexPlannerMode mode, List<String> inputPaths, boolean realtime) {
    return HoodieMergeOnReadTestUtils.getRecordsUsingInputFormat(storageConf, inputPaths, basePath,
        jobConf(query, mode, Collections.emptyMap()), realtime);
  }

  private Set<String> splitFiles(Query query, IndexPlannerMode mode, List<String> inputPaths, boolean realtime) throws Exception {
    return splitFiles(query, mode, inputPaths, realtime, Collections.emptyMap());
  }

  /**
   * The names of the files the splits start from: base files, or log files of file slices without one.
   */
  private Set<String> splitFiles(Query query, IndexPlannerMode mode, List<String> inputPaths, boolean realtime,
                                 Map<String, String> extra) throws Exception {
    JobConf jobConf = jobConf(query, mode, extra);
    FileInputFormat<?, ?> inputFormat = HoodieInputFormatUtils.getInputFormat(metaClient.getTableConfig().getBaseFileFormat(), realtime, jobConf);
    ((Configurable) inputFormat).setConf(jobConf);
    FileInputFormat.setInputPaths(jobConf, String.join(",", inputPaths));
    Set<String> files = new TreeSet<>();
    for (InputSplit split : inputFormat.getSplits(jobConf, 1)) {
      files.add(((FileSplit) split).getPath().getName());
    }
    return files;
  }

  /**
   * What COMPARE reports for the query, over the file slices the query reads.
   */
  private IndexPlannerComparison compare(Query query, List<String> inputPaths, boolean realtime,
                                         Map<String, List<String>> filesByFileId) throws Exception {
    HoodieJavaEngineContext engineContext = new HoodieJavaEngineContext(storageConf);
    JobConf jobConf = jobConf(query, IndexPlannerMode.COMPARE, Collections.emptyMap());
    Map<String, List<String>> filesByPartition = new LinkedHashMap<>();
    try (HoodieTableMetadata tableMetadata = metaClient.getTableFormat().getMetadataFactory().create(engineContext, metaClient.getStorage(),
        HoodieMetadataConfig.newBuilder().enable(true).build(), basePath, true);
         HoodieTableFileSystemView fsView = new HoodieTableFileSystemView(tableMetadata, metaClient, metaClient.reloadActiveTimeline())) {
      for (String inputPath : inputPaths) {
        String partition = FSUtils.getRelativePartitionPath(new StoragePath(basePath), new StoragePath(inputPath));
        List<String> files = new ArrayList<>();
        for (FileSlice slice : fsView.getLatestFileSlices(partition).collect(Collectors.toList())) {
          List<String> sliceFiles = new ArrayList<>();
          slice.getBaseFile().ifPresent(base -> sliceFiles.add(base.getFileName()));
          if (realtime) {
            slice.getLogFiles().forEach(log -> sliceFiles.add(log.getFileName()));
          }
          files.addAll(sliceFiles);
          filesByFileId.put(slice.getFileId(), sliceFiles);
        }
        filesByPartition.put(partition, files);
      }
    }
    Set<String> allFiles = filesByPartition.values().stream().flatMap(List::stream).collect(Collectors.toSet());
    Option<CandidateFiles> candidates = HiveIndexPlanning.candidates(jobConf, metaClient, engineContext, filesByPartition, realtime);
    return IndexPlannerComparison.compare(allFiles, Option.empty(), candidates.orElse(CandidateFiles.all()));
  }

  /**
   * The rows matching the query by record key, as strings.
   */
  private static Map<String, String> matchingRows(List<GenericRecord> rows, Query query) {
    Map<String, String> matching = new TreeMap<>();
    for (GenericRecord row : rows) {
      if (query.matches.test(row)) {
        String previous = matching.put(row.get("_row_key").toString(), rowString(row));
        assertTrue(previous == null, "one row per key");
      }
    }
    return matching;
  }

  private static List<String> rowStrings(List<GenericRecord> rows) {
    return rows.stream().map(TestHiveIndexPlanning::rowString).sorted().collect(Collectors.toList());
  }

  /**
   * The row with its nested values ({@link ArrayWritable}s, which have no {@code toString}) spelled out.
   */
  private static String rowString(GenericRecord row) {
    return row.getSchema().getFields().stream().map(f -> f.name() + "=" + valueString(row.get(f.name()))).collect(Collectors.joining(", "));
  }

  private static String valueString(Object value) {
    if (value instanceof ArrayWritable) {
      return Arrays.stream(((ArrayWritable) value).get()).map(TestHiveIndexPlanning::valueString).collect(Collectors.joining(", ", "[", "]"));
    }
    return String.valueOf(value);
  }

  // -------------------------------------------------------------------------
  //  Queries: the Hive filter and its evaluation on the rows Hive reads
  // -------------------------------------------------------------------------

  /**
   * A date constant, as days since the epoch.
   */
  private static final class DateDays implements Comparable<DateDays> {
    private final int days;

    private DateDays(int days) {
      this.days = days;
    }

    @Override
    public int compareTo(DateDays o) {
      return Integer.compare(days, o.days);
    }
  }

  private static class Query {
    private final ExprNodeGenericFuncDesc filter;
    private final Predicate<GenericRecord> matches;
    private List<String> partitions;

    Query(ExprNodeDesc filter, Predicate<GenericRecord> matches) {
      this.filter = (ExprNodeGenericFuncDesc) filter;
      this.matches = matches;
    }

    String serialized() {
      return SerializationUtilities.serializeExpression(filter);
    }

    Query inPartitions(List<String> partitions) {
      this.partitions = partitions;
      return this;
    }
  }

  private static TypeInfo hiveType(Object value) {
    if (value instanceof Long) {
      return TypeInfoFactory.longTypeInfo;
    } else if (value instanceof Integer) {
      return TypeInfoFactory.intTypeInfo;
    } else if (value instanceof Double) {
      return TypeInfoFactory.doubleTypeInfo;
    } else if (value instanceof Float) {
      return TypeInfoFactory.floatTypeInfo;
    } else if (value instanceof BigDecimal) {
      return TypeInfoFactory.getDecimalTypeInfo(10, 6);
    } else if (value instanceof DateDays) {
      return TypeInfoFactory.dateTypeInfo;
    }
    return TypeInfoFactory.stringTypeInfo;
  }

  private static ExprNodeDesc column(String name, Object value) {
    return new ExprNodeColumnDesc(hiveType(value), name, "t", "partition_path".equals(name));
  }

  private static ExprNodeDesc constant(Object value) {
    Object hive = value;
    if (value instanceof BigDecimal) {
      hive = HiveDecimal.create((BigDecimal) value);
    } else if (value instanceof DateDays) {
      hive = new Date(DateWritable.daysToMillis(((DateDays) value).days));
    }
    return new ExprNodeConstantDesc(hiveType(value), hive);
  }

  private static ExprNodeDesc call(GenericUDF udf, ExprNodeDesc... children) {
    return new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo, udf, new ArrayList<>(Arrays.asList(children)));
  }

  /**
   * The value Hive reads, as the Java type of the query constants.
   */
  @SuppressWarnings({"rawtypes"})
  private static Comparable value(GenericRecord row, String column) {
    Object value = row.get(column);
    if (value == null) {
      return null;
    } else if (value instanceof LongWritable) {
      return ((LongWritable) value).get();
    } else if (value instanceof IntWritable) {
      return "current_date".equals(column) ? new DateDays(((IntWritable) value).get()) : (Comparable) ((IntWritable) value).get();
    } else if (value instanceof DateWritable) {
      return new DateDays(((DateWritable) value).getDays());
    } else if (value instanceof DoubleWritable) {
      return ((DoubleWritable) value).get();
    } else if (value instanceof org.apache.hadoop.hive.serde2.io.DoubleWritable) {
      return ((org.apache.hadoop.hive.serde2.io.DoubleWritable) value).get();
    } else if (value instanceof FloatWritable) {
      return ((FloatWritable) value).get();
    } else if (value instanceof HiveDecimalWritable) {
      return ((HiveDecimalWritable) value).getHiveDecimal().bigDecimalValue().setScale(6);
    } else if (value instanceof BooleanWritable) {
      return ((BooleanWritable) value).get();
    } else if (value instanceof Text) {
      return value.toString();
    }
    throw new IllegalArgumentException("Unexpected value " + value.getClass() + " of " + column);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static Query cmp(GenericUDF udf, String column, Object value, Predicate<Integer> outcome) {
    Comparable constant = value instanceof BigDecimal ? ((BigDecimal) value).setScale(6) : (Comparable) value;
    return new Query(call(udf, column(column, value), constant(value)), row -> {
      Comparable read = value(row, column);
      return read != null && outcome.test(Integer.signum(read.compareTo(constant)));
    });
  }

  private static Query eq(String column, Object value) {
    return cmp(new GenericUDFOPEqual(), column, value, c -> c == 0);
  }

  private static Query not(Query query) {
    // the filters here are over non-null columns
    return new Query(call(new GenericUDFOPNot(), query.filter), query.matches.negate());
  }

  private static Query and(Query left, Query right) {
    return new Query(call(new GenericUDFOPAnd(), left.filter, right.filter), left.matches.and(right.matches));
  }

  private static Query or(Query left, Query right) {
    return new Query(call(new GenericUDFOPOr(), left.filter, right.filter), left.matches.or(right.matches));
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static Query between(String column, Object low, Object high) {
    return new Query(call(new GenericUDFBetween(), new ExprNodeConstantDesc(TypeInfoFactory.booleanTypeInfo, false),
        column(column, low), constant(low), constant(high)), row -> {
          Comparable read = value(row, column);
          return read != null && read.compareTo(low) >= 0 && read.compareTo(high) <= 0;
        });
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static Query in(String column, Object... values) {
    List<ExprNodeDesc> children = new ArrayList<>();
    children.add(column(column, values[0]));
    Arrays.stream(values).forEach(v -> children.add(constant(v)));
    return new Query(new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo, new GenericUDFIn(), children), row -> {
      Comparable read = value(row, column);
      return read != null && Arrays.stream(values).anyMatch(v -> read.compareTo(v) == 0);
    });
  }

  private static Query like(String column, String prefix) {
    return new Query(call(new GenericUDFBridge("like", true, UDFLike.class.getName()), column(column, prefix),
        new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, prefix + "%")), row -> {
          Object read = value(row, column);
          return read != null && read.toString().startsWith(prefix);
        });
  }

  private static Query isNull(String column) {
    return new Query(call(new GenericUDFOPNull(), column(column, "")), row -> value(row, column) == null);
  }

  private static Query isNotNull(String column) {
    return new Query(call(new GenericUDFOPNotNull(), column(column, "")), row -> value(row, column) != null);
  }
}
