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

package org.apache.hudi.integ2.testcontainers;

import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.keygen.SimpleKeyGenerator;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Integration test for Hudi demo using Testcontainers. Migrated from ITTestHoodieDemo to use testcontainers framework.
 */
public class ITTestHoodieDemoTestcontainers extends ITTestBaseTestcontainers {

  private static final String HOODIE_WS_ROOT = "/var/hoodie/ws";

  private static final String HDFS_DATA_DIR = "/usr/hive/data/input";
  private static final String HDFS_BATCH_PATH1 = HDFS_DATA_DIR + "/batch_1.json";
  private static final String HDFS_BATCH_PATH2 = HDFS_DATA_DIR + "/batch_2.json";
  private static final String HDFS_PRESTO_INPUT_TABLE_CHECK_PATH = HDFS_DATA_DIR + "/presto-table-check.commands";
  private static final String HDFS_PRESTO_INPUT_BATCH1_PATH = HDFS_DATA_DIR + "/presto-batch1.commands";
  private static final String HDFS_PRESTO_INPUT_BATCH2_PATH = HDFS_DATA_DIR + "/presto-batch2-after-compaction.commands";
  private static final String HDFS_TRINO_INPUT_TABLE_CHECK_PATH = HDFS_DATA_DIR + "/trino-table-check.commands";
  private static final String HDFS_TRINO_INPUT_BATCH1_PATH = HDFS_DATA_DIR + "/trino-batch1.commands";
  private static final String HDFS_TRINO_INPUT_BATCH2_PATH = HDFS_DATA_DIR + "/trino-batch2-after-compaction.commands";

  private static final String INPUT_BATCH_PATH1 = HOODIE_WS_ROOT + "/docker/demo/data/batch_1.json";
  private static final String INPUT_BATCH_PATH2 = HOODIE_WS_ROOT + "/docker/demo/data/batch_2.json";

  private static final String COW_BASE_PATH = "/user/hive/warehouse/stock_ticks_cow";
  private static final String MOR_BASE_PATH = "/user/hive/warehouse/stock_ticks_mor";
  private static final String COW_TABLE_NAME = "stock_ticks_cow";
  private static final String MOR_TABLE_NAME = "stock_ticks_mor";

  private static final String BOOTSTRAPPED_SRC_PATH = "/user/hive/warehouse/stock_ticks_cow_bs_src";
  private static final String COW_BOOTSTRAPPED_BASE_PATH = "/user/hive/warehouse/stock_ticks_cow_bs";
  private static final String MOR_BOOTSTRAPPED_BASE_PATH = "/user/hive/warehouse/stock_ticks_mor_bs";
  private static final String COW_BOOTSTRAPPED_TABLE_NAME = "stock_ticks_cow_bs";
  private static final String MOR_BOOTSTRAPPED_TABLE_NAME = "stock_ticks_mor_bs";

  private static final String DEMO_CONTAINER_SCRIPT = HOODIE_WS_ROOT + "/docker/demo/setup_demo_container.sh";
  private static final String MIN_COMMIT_TIME_COW_SCRIPT = HOODIE_WS_ROOT + "/docker/demo/get_min_commit_time_cow.sh";
  private static final String MIN_COMMIT_TIME_MOR_SCRIPT = HOODIE_WS_ROOT + "/docker/demo/get_min_commit_time_mor.sh";
  private static final String HUDI_CLI_TOOL = HOODIE_WS_ROOT + "/packaging/hudi-cli-bundle/hudi-cli-with-bundle.sh";
  private static final String COMPACTION_COMMANDS = HOODIE_WS_ROOT + "/docker/demo/compaction.commands";
  private static final String COMPACTION_BOOTSTRAP_COMMANDS = HOODIE_WS_ROOT + "/docker/demo/compaction-bootstrap.commands";
  private static final String SPARKSQL_BS_PREP_COMMANDS = HOODIE_WS_ROOT + "/docker/demo/sparksql-bootstrap-prep-source.commands";
  private static final String SPARKSQL_BATCH1_COMMANDS = HOODIE_WS_ROOT + "/docker/demo/sparksql-batch1.commands";
  private static final String SPARKSQL_BATCH2_COMMANDS = HOODIE_WS_ROOT + "/docker/demo/sparksql-batch2.commands";
  private static final String SPARKSQL_INCREMENTAL_COMMANDS = HOODIE_WS_ROOT + "/docker/demo/sparksql-incremental.commands";
  private static final String HIVE_TBLCHECK_COMMANDS = HOODIE_WS_ROOT + "/docker/demo/hive-table-check.commands";
  private static final String HIVE_BATCH1_COMMANDS = HOODIE_WS_ROOT + "/docker/demo/hive-batch1.commands";
  private static final String HIVE_BATCH2_COMMANDS = HOODIE_WS_ROOT + "/docker/demo/hive-batch2-after-compaction.commands";
  private static final String HIVE_INCREMENTAL_COW_COMMANDS = HOODIE_WS_ROOT + "/docker/demo/hive-incremental-cow.commands";
  private static final String HIVE_INCREMENTAL_MOR_RO_COMMANDS = HOODIE_WS_ROOT + "/docker/demo/hive-incremental-mor-ro.commands";
  private static final String HIVE_INCREMENTAL_MOR_RT_COMMANDS = HOODIE_WS_ROOT + "/docker/demo/hive-incremental-mor-rt.commands";


  protected static final String HUDI_HADOOP_BUNDLE =
      HOODIE_WS_ROOT + "/docker/hoodie/hadoop/hive_base/target/hoodie-hadoop-mr-bundle.jar";
  protected static final String HUDI_HIVE_SYNC_BUNDLE =
      HOODIE_WS_ROOT + "/docker/hoodie/hadoop/hive_base/target/hoodie-hive-sync-bundle.jar";
  protected static final String HUDI_SPARK_BUNDLE =
      HOODIE_WS_ROOT + "/docker/hoodie/hadoop/hive_base/target/hoodie-spark-bundle.jar";
  protected static final String HUDI_UTILITIES_BUNDLE =
      HOODIE_WS_ROOT + "/docker/hoodie/hadoop/hive_base/target/hoodie-utilities.jar";

  private HoodieFileFormat baseFileFormat;

  private static final String HIVE_SYNC_CMD_FMT =
      " --enable-hive-sync --hoodie-conf hoodie.datasource.hive_sync.jdbcurl=jdbc:hive2://hiveserver:10000/ "
          + " --hoodie-conf hoodie.datasource.hive_sync.partition_extractor_class=org.apache.hudi.hive.SlashEncodedDayPartitionValueExtractor "
          + " --hoodie-conf hoodie.datasource.hive_sync.username=hive "
          + " --hoodie-conf hoodie.datasource.hive_sync.password=hive "
          + " --hoodie-conf hoodie.datasource.hive_sync.partition_fields=%s "
          + " --hoodie-conf hoodie.datasource.hive_sync.database=default "
          + " --hoodie-conf hoodie.datasource.hive_sync.table=%s";

  // Helper methods for building complex commands

  private String buildDeltaStreamerCommand(HoodieTableType tableType, String basePath, String tableName, String additionalArgs) {
    return "spark-submit --class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer " + HUDI_UTILITIES_BUNDLE
        + " --table-type " + tableType.name()
        + " --base-file-format " + baseFileFormat.toString()
        + " --source-class org.apache.hudi.utilities.sources.JsonDFSSource --source-ordering-field ts"
        + " --target-base-path " + basePath + " --target-table " + tableName
        + " --props /var/demo/config/dfs-source.properties"
        + " --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider"
        + (additionalArgs != null ? " " + additionalArgs : "");
  }

  private String buildHiveSyncCommand(String tableName, String basePath) {
    return "spark-submit --class org.apache.hudi.hive.HiveSyncTool " + HUDI_HIVE_SYNC_BUNDLE
        + " --database default"
        + " --table " + tableName
        + " --base-path " + basePath
        + " --base-file-format " + baseFileFormat.toString()
        + " --user hive"
        + " --pass hive"
        + " --jdbc-url jdbc:hive2://hiveserver:10000"
        + " --partition-value-extractor org.apache.hudi.hive.SlashEncodedDayPartitionValueExtractor"
        + " --partitioned-by dt";
  }

  private String buildBootstrapCommand(HoodieTableType tableType, String basePath, String tableName, String partitionField) {
    return "spark-submit --class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer " + HUDI_UTILITIES_BUNDLE
        + " --table-type " + tableType.name()
        + " --run-bootstrap"
        + " --source-class org.apache.hudi.utilities.sources.JsonDFSSource --source-ordering-field ts"
        + " --target-base-path " + basePath + " --target-table " + tableName
        + " --props /var/demo/config/dfs-source.properties"
        + " --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider"
        + " --initial-checkpoint-provider org.apache.hudi.utilities.checkpointing.InitialCheckpointFromAnotherHoodieTimelineProvider"
        + " --hoodie-conf hoodie.bootstrap.base.path=" + BOOTSTRAPPED_SRC_PATH
        + " --hoodie-conf hoodie.deltastreamer.checkpoint.provider.path=" + COW_BASE_PATH
        + " --hoodie-conf hoodie.bootstrap.parallelism=2"
        + " --hoodie-conf hoodie.datasource.write.keygenerator.class=" + SimpleKeyGenerator.class.getName()
        + " --hoodie-conf hoodie.metadata.index.column.stats.enable=false" // col stats is not supported with bootstrap operation
        + String.format(HIVE_SYNC_CMD_FMT, partitionField, tableName);
  }

  @BeforeEach
  public void setup() {
    initializeServices();
  }

  @AfterEach
  public void clean() throws Exception {
    // Use -f to silently skip non-existent paths (not all tests create all tables)
    final String hdfsCmd = "hdfs dfs -rm -R -f ";
    List<String> tablePaths = CollectionUtils.createImmutableList(
        COW_BASE_PATH, MOR_BASE_PATH, COW_BOOTSTRAPPED_BASE_PATH, MOR_BOOTSTRAPPED_BASE_PATH);
    for (String tablePath : tablePaths) {
      sparkAdhoc1.executeShellCommand(hdfsCmd + tablePath)
          .expectToSucceed();
    }
  }

  @Test
  @Disabled("HUDI-8440: This test is disabled because it is failing in the CI pipeline. It is working fine in the local setup.")
  public void testParquetDemo() throws Exception {
    baseFileFormat = HoodieFileFormat.PARQUET;

    setupDemo();

    // batch 1
    ingestFirstBatchAndHiveSync();
    // Hive-on-MR is deprecated in Hive 2, selects will fail even on local mode, with spark engine, scala class not found error is thrown
    // testHiveAfterFirstBatch();
    // TODO(HUDI-8269, HUDI-8270): fix integration tests with Presto and Trino
    // testPrestoAfterFirstBatch();
    // testTrinoAfterFirstBatch();
//    testSparkSQLAfterFirstBatch();

    // batch 2
    ingestSecondBatchAndHiveSync();
    // Hive-on-MR is deprecated in Hive 2, selects will fail even on local mode, with spark engine, scala class not found error is thrown
    // testHiveAfterSecondBatch();
    // testPrestoAfterSecondBatch();
    // testTrinoAfterSecondBatch();
    testSparkSQLAfterSecondBatch();

    // TODO: HUDI-8572
    // testIncrementalHiveQueryBeforeCompaction();
    testIncrementalSparkSQLQuery();

    // compaction
    scheduleAndRunCompaction();

    testIncrementalSparkSQLQuery();
    // TODO: Hive data queries disabled - MapReduce requires YARN which is not available
    // testHiveAfterSecondBatchAfterCompaction();
    // testPrestoAfterSecondBatchAfterCompaction();
    // testTrinoAfterSecondBatchAfterCompaction();
    // TODO: HUDI-8572
    // testIncrementalHiveQueryAfterCompaction();
  }

  @Test
  @Disabled
  public void testHFileDemo() throws Exception {
    baseFileFormat = HoodieFileFormat.HFILE;

    setupDemo();

    // batch 1
    ingestFirstBatchAndHiveSync();
    testHiveAfterFirstBatch();

    // batch 2
    ingestSecondBatchAndHiveSync();
    testHiveAfterSecondBatch();
    testIncrementalHiveQueryBeforeCompaction();

    // compaction
    scheduleAndRunCompaction();
    testHiveAfterSecondBatchAfterCompaction();
  }

  private void setupDemo() throws Exception {
    String workspaceRoot = System.getProperty("user.dir") + "/..";

    // Setup HDFS and core infrastructure
    final List<String> cmds = CollectionUtils.createImmutableList(
        "hdfs dfsadmin -safemode wait",
        "hdfs dfs -mkdir -p " + HDFS_DATA_DIR,
        "hdfs dfs -copyFromLocal -f " + INPUT_BATCH_PATH1 + " " + HDFS_BATCH_PATH1,
        "/bin/bash " + DEMO_CONTAINER_SCRIPT,
        "mkdir -p " + HDFS_DATA_DIR);
    for (String cmd : cmds) {
      sparkAdhoc1.executeShellCommand(cmd)
          .expectToSucceed();
    }

    // Setup Presto and Trino
//    presto.setupDemo(workspaceRoot, HDFS_DATA_DIR);
//    trino.setupDemo(workspaceRoot, HDFS_DATA_DIR);
  }

  private void ingestFirstBatchAndHiveSync() throws Exception {
    // Ingest COW table and sync to Hive
    final List<String> cmds = CollectionUtils.createImmutableList(
        buildDeltaStreamerCommand(HoodieTableType.COPY_ON_WRITE, COW_BASE_PATH, COW_TABLE_NAME, null),
        buildHiveSyncCommand(COW_TABLE_NAME, COW_BASE_PATH),
        buildDeltaStreamerCommand(HoodieTableType.MERGE_ON_READ, MOR_BASE_PATH, MOR_TABLE_NAME,
            "--disable-compaction" + String.format(HIVE_SYNC_CMD_FMT, "dt", MOR_TABLE_NAME)));

    for (String cmd : cmds) {
      sparkAdhoc1.executeShellCommand(cmd)
          .expectToSucceed();
    };

    // Prepare bootstrap source data
    sparkAdhoc1.executeSQLFile(SPARKSQL_BS_PREP_COMMANDS)
        .expectToSucceed();

    // Bootstrap COW and MOR tables
    final List<String> bootstrapCmds = CollectionUtils.createImmutableList(
        buildBootstrapCommand(HoodieTableType.COPY_ON_WRITE, COW_BOOTSTRAPPED_BASE_PATH, COW_BOOTSTRAPPED_TABLE_NAME, "dt"),
        buildBootstrapCommand(HoodieTableType.MERGE_ON_READ, MOR_BOOTSTRAPPED_BASE_PATH, MOR_BOOTSTRAPPED_TABLE_NAME, "dt"));
    for (String bootstrapCmd : bootstrapCmds) {
      sparkAdhoc1.executeShellCommand(bootstrapCmd)
          .expectToSucceed();
    }
  }

  private void testHiveAfterFirstBatch() throws Exception {
    hive.executeFile(HIVE_TBLCHECK_COMMANDS)
        .expectToSucceed()
        .assertStdOutContains("| stock_ticks_cow     |")
        .assertStdOutContains("| stock_ticks_cow_bs  |")
        .assertStdOutContains("| stock_ticks_mor_ro  |")
        .assertStdOutContains("| stock_ticks_mor_rt  |")
        .assertStdOutContains("| stock_ticks_mor_bs_ro  |")
        .assertStdOutContains("| stock_ticks_mor_bs_rt  |")
        .assertStdOutContains("|   partition    |\n+----------------+\n| dt=2018-08-31  |\n+----------------+\n", 3)
        .assertStdOutContains("'spark.sql.sources.provider'='hudi'", 6);

    hive.executeFile(HIVE_BATCH1_COMMANDS)
        .expectToSucceed()
        .assertStdOutContains("| symbol  |         _c1          |\n+---------+----------------------+\n"
            + "| GOOG    | 2018-08-31 10:29:00  |\n", 6)
        .assertStdOutContains("| symbol  |          ts          | volume  |    open    |   close   |\n"
            + "+---------+----------------------+---------+------------+-----------+\n"
            + "| GOOG    | 2018-08-31 09:59:00  | 6330    | 1230.5     | 1230.02   |\n"
            + "| GOOG    | 2018-08-31 10:29:00  | 3391    | 1230.1899  | 1230.085  |\n", 6);
  }

  private void testSparkSQLAfterFirstBatch() throws Exception {
    sparkAdhoc1.executeSQLFile(SPARKSQL_BATCH1_COMMANDS)
        .expectToSucceed()
        .assertStdOutContains("|default  |stock_ticks_cow      |false      |\n"
                + "|default  |stock_ticks_cow_bs   |false      |\n"
                + "|default  |stock_ticks_mor      |false      |\n"
                + "|default  |stock_ticks_mor_bs   |false      |\n"
                + "|default  |stock_ticks_mor_bs_ro|false      |\n"
                + "|default  |stock_ticks_mor_bs_rt|false      |\n"
                + "|default  |stock_ticks_mor_ro   |false      |\n"
                + "|default  |stock_ticks_mor_rt   |false      |")
        .assertStdOutContains("+------+-------------------+\n"
            + "|GOOG  |2018-08-31 10:29:00|\n"
            + "+------+-------------------+", 6)
        .assertStdOutContains("|GOOG  |2018-08-31 09:59:00|6330  |1230.5   |1230.02 |", 6)
        .assertStdOutContains("|GOOG  |2018-08-31 10:29:00|3391  |1230.1899|1230.085|", 6);
  }

  private void ingestSecondBatchAndHiveSync() throws Exception {
    // Copy second batch data and ingest into all tables
    final List<String> cmds = CollectionUtils.createImmutableList(
        "hdfs dfs -copyFromLocal -f " + INPUT_BATCH_PATH2 + " " + HDFS_BATCH_PATH2,
        buildDeltaStreamerCommand(HoodieTableType.COPY_ON_WRITE, COW_BASE_PATH, COW_TABLE_NAME,
            String.format(HIVE_SYNC_CMD_FMT, "dt", COW_TABLE_NAME)),
        buildDeltaStreamerCommand(HoodieTableType.MERGE_ON_READ, MOR_BASE_PATH, MOR_TABLE_NAME,
            "--disable-compaction" + String.format(HIVE_SYNC_CMD_FMT, "dt", MOR_TABLE_NAME)),
        buildDeltaStreamerCommand(HoodieTableType.COPY_ON_WRITE, COW_BOOTSTRAPPED_BASE_PATH, COW_BOOTSTRAPPED_TABLE_NAME,
            String.format(HIVE_SYNC_CMD_FMT, "dt", COW_BOOTSTRAPPED_TABLE_NAME)),
        buildDeltaStreamerCommand(HoodieTableType.MERGE_ON_READ, MOR_BOOTSTRAPPED_BASE_PATH, MOR_BOOTSTRAPPED_TABLE_NAME,
            "--disable-compaction" + String.format(HIVE_SYNC_CMD_FMT, "dt", MOR_BOOTSTRAPPED_TABLE_NAME)));
    for (String cmd : cmds) {
      sparkAdhoc1.executeShellCommand(cmd)
          .expectToSucceed();
    }
  }

  private void testPrestoAfterFirstBatch() throws Exception {
    presto.executeFile(HDFS_PRESTO_INPUT_TABLE_CHECK_PATH)
        .expectToSucceed()
        .assertStdOutContains("stock_ticks_cow", 2)
        .assertStdOutContains("stock_ticks_mor", 6);

    presto.executeFile(HDFS_PRESTO_INPUT_BATCH1_PATH)
        .expectToSucceed()
        .assertStdOutContains("\"GOOG\",\"2018-08-31 10:29:00\"", 4)
        .assertStdOutContains("\"GOOG\",\"2018-08-31 09:59:00\",\"6330\",\"1230.5\",\"1230.02\"", 2)
        .assertStdOutContains("\"GOOG\",\"2018-08-31 10:29:00\",\"3391\",\"1230.1899\",\"1230.085\"", 2);
  }

  private void testTrinoAfterFirstBatch() throws Exception {
    trino.executeFile(HDFS_TRINO_INPUT_TABLE_CHECK_PATH)
        .expectToSucceed()
        .assertStdOutContains("stock_ticks_cow", 2)
        .assertStdOutContains("stock_ticks_mor", 6);

    trino.executeFile(HDFS_TRINO_INPUT_BATCH1_PATH)
        .expectToSucceed()
        .assertStdOutContains("\"GOOG\",\"2018-08-31 10:29:00\"", 4)
        .assertStdOutContains("\"GOOG\",\"2018-08-31 09:59:00\",\"6330\",\"1230.5\",\"1230.02\"", 2)
        .assertStdOutContains("\"GOOG\",\"2018-08-31 10:29:00\",\"3391\",\"1230.1899\",\"1230.085\"", 2);
  }

  private void testHiveAfterSecondBatch() throws Exception {
    hive.executeFile(HIVE_BATCH1_COMMANDS)
        .expectToSucceed()
        .assertStdOutContains("| symbol  |         _c1          |"
            + "\n+---------+----------------------+\n"
            + "| GOOG    | 2018-08-31 10:29:00  |\n", 2)
        .assertStdOutContains("| symbol  |         _c1          |"
            + "\n+---------+----------------------+\n"
            + "| GOOG    | 2018-08-31 10:59:00  |\n", 4)
        .assertStdOutContains("| symbol  |          ts          | volume  |    open    |   close   |\n"
            + "+---------+----------------------+---------+------------+-----------+\n"
            + "| GOOG    | 2018-08-31 09:59:00  | 6330    | 1230.5     | 1230.02   |\n"
            + "| GOOG    | 2018-08-31 10:29:00  | 3391    | 1230.1899  | 1230.085  |\n", 2)
        .assertStdOutContains("| symbol  |          ts          | volume  |    open    |   close   |\n"
            + "+---------+----------------------+---------+------------+-----------+\n"
            + "| GOOG    | 2018-08-31 09:59:00  | 6330    | 1230.5     | 1230.02   |\n"
            + "| GOOG    | 2018-08-31 10:59:00  | 9021    | 1227.1993  | 1227.215  |\n", 4);
  }

  private void testPrestoAfterSecondBatch() throws Exception {
    presto.executeFile(HDFS_PRESTO_INPUT_BATCH1_PATH)
        .expectToSucceed()
        .assertStdOutContains("\"GOOG\",\"2018-08-31 10:29:00\"", 2)
        .assertStdOutContains("\"GOOG\",\"2018-08-31 10:59:00\"", 2)
        .assertStdOutContains("\"GOOG\",\"2018-08-31 09:59:00\",\"6330\",\"1230.5\",\"1230.02\"", 2)
        .assertStdOutContains("\"GOOG\",\"2018-08-31 10:29:00\",\"3391\",\"1230.1899\",\"1230.085\"")
        .assertStdOutContains("\"GOOG\",\"2018-08-31 10:59:00\",\"9021\",\"1227.1993\",\"1227.215\"");
  }

  private void testTrinoAfterSecondBatch() throws Exception {
    trino.executeFile(HDFS_TRINO_INPUT_BATCH1_PATH)
        .expectToSucceed()
        .assertStdOutContains("\"GOOG\",\"2018-08-31 10:29:00\"", 2)
        .assertStdOutContains("\"GOOG\",\"2018-08-31 10:59:00\"", 2)
        .assertStdOutContains("\"GOOG\",\"2018-08-31 09:59:00\",\"6330\",\"1230.5\",\"1230.02\"", 2)
        .assertStdOutContains("\"GOOG\",\"2018-08-31 10:29:00\",\"3391\",\"1230.1899\",\"1230.085\"")
        .assertStdOutContains("\"GOOG\",\"2018-08-31 10:59:00\",\"9021\",\"1227.1993\",\"1227.215\"");
  }

  private void testHiveAfterSecondBatchAfterCompaction() throws Exception {
    hive.executeFile(HIVE_BATCH2_COMMANDS)
        .expectToSucceed()
        .assertStdOutContains("| symbol  |         _c1          |"
            + "\n+---------+----------------------+\n"
            + "| GOOG    | 2018-08-31 10:59:00  |", 4)
        .assertStdOutContains("| symbol  |          ts          | volume  |    open    |   close   |\n"
                + "+---------+----------------------+---------+------------+-----------+\n"
                + "| GOOG    | 2018-08-31 09:59:00  | 6330    | 1230.5     | 1230.02   |\n"
                + "| GOOG    | 2018-08-31 10:59:00  | 9021    | 1227.1993  | 1227.215  |", 4);
  }

  private void testPrestoAfterSecondBatchAfterCompaction() throws Exception {
    presto.executeFile(HDFS_PRESTO_INPUT_BATCH2_PATH)
        .expectToSucceed()
        .assertStdOutContains("\"GOOG\",\"2018-08-31 10:59:00\"", 2)
        .assertStdOutContains("\"GOOG\",\"2018-08-31 09:59:00\",\"6330\",\"1230.5\",\"1230.02\"")
        .assertStdOutContains("\"GOOG\",\"2018-08-31 10:59:00\",\"9021\",\"1227.1993\",\"1227.215\"");
  }

  private void testTrinoAfterSecondBatchAfterCompaction() throws Exception {
    trino.executeFile(HDFS_TRINO_INPUT_BATCH2_PATH)
        .expectToSucceed()
        .assertStdOutContains("\"GOOG\",\"2018-08-31 10:59:00\"", 2)
        .assertStdOutContains("\"GOOG\",\"2018-08-31 09:59:00\",\"6330\",\"1230.5\",\"1230.02\"")
        .assertStdOutContains("\"GOOG\",\"2018-08-31 10:59:00\",\"9021\",\"1227.1993\",\"1227.215\"");
  }

  private void testSparkSQLAfterSecondBatch() throws Exception {
    sparkAdhoc1.executeSQLFile(SPARKSQL_BATCH2_COMMANDS)
        .expectToSucceed()
        .assertStdOutContains(
            "+------+-------------------+\n|GOOG  |2018-08-31 10:59:00|\n+------+-------------------+", 4)
        .assertStdOutContains("|GOOG  |2018-08-31 09:59:00|6330  |1230.5   |1230.02 |", 6)
        .assertStdOutContains("|GOOG  |2018-08-31 10:59:00|9021  |1227.1993|1227.215|", 4)
        .assertStdOutContains("+------+-------------------+\n|GOOG  |2018-08-31 10:29:00|\n+------+-------------------+", 2)
        .assertStdOutContains("|GOOG  |2018-08-31 10:29:00|3391  |1230.1899|1230.085|", 2);
  }

  private void testIncrementalHiveQuery(String minCommitTimeScript, String incrementalCommandsFile,
      String expectedOutput, int expectedTimes) throws Exception {
    String minCommitTime = sparkAdhoc2.executeShellCommand(minCommitTimeScript).getStdout();
    hive.executeFile(incrementalCommandsFile, "min.commit.time=" + minCommitTime + "`")
        .expectToSucceed()
        .assertStdOutContains(expectedOutput, expectedTimes);
  }

  private void testIncrementalHiveQueryBeforeCompaction() throws Exception {
    String expectedOutput = "| GOOG    | 2018-08-31 10:59:00  | 9021    | 1227.1993  | 1227.215  |";

    // verify that 10:59 is present in COW table because there is no compaction process for COW
    testIncrementalHiveQuery(MIN_COMMIT_TIME_COW_SCRIPT, HIVE_INCREMENTAL_COW_COMMANDS, expectedOutput, 2);

    // verify that 10:59 is NOT present in RO table because of pending compaction
    testIncrementalHiveQuery(MIN_COMMIT_TIME_MOR_SCRIPT, HIVE_INCREMENTAL_MOR_RO_COMMANDS, expectedOutput, 0);

    // verify that 10:59 is present in RT table even with pending compaction
    testIncrementalHiveQuery(MIN_COMMIT_TIME_MOR_SCRIPT, HIVE_INCREMENTAL_MOR_RT_COMMANDS, expectedOutput, 2);
  }

  private void testIncrementalHiveQueryAfterCompaction() throws Exception {
    String expectedOutput = "| symbol  |          ts          | volume  |    open    |   close   |\n"
        + "+---------+----------------------+---------+------------+-----------+\n"
        + "| GOOG    | 2018-08-31 10:59:00  | 9021    | 1227.1993  | 1227.215  |";

    // verify that 10:59 is present for all views because compaction is complete
    testIncrementalHiveQuery(MIN_COMMIT_TIME_COW_SCRIPT, HIVE_INCREMENTAL_COW_COMMANDS, expectedOutput, 2);
    testIncrementalHiveQuery(MIN_COMMIT_TIME_MOR_SCRIPT, HIVE_INCREMENTAL_MOR_RO_COMMANDS, expectedOutput, 2);
    testIncrementalHiveQuery(MIN_COMMIT_TIME_MOR_SCRIPT, HIVE_INCREMENTAL_MOR_RT_COMMANDS, expectedOutput, 2);
  }

  private void testIncrementalSparkSQLQuery() throws Exception {
    sparkAdhoc1.executeSQLFile(SPARKSQL_INCREMENTAL_COMMANDS)
        .expectToSucceed()
        .assertStdOutContains("stock_ticks_cow incremental count: 99", 1)
        .assertStdOutContains("stock_ticks_cow_bs incremental count: 99", 1)
        .assertStdOutContains("stock_ticks_mor incremental count: 99", 1)
        .assertStdOutContains("stock_ticks_mor_bs incremental count: 99", 1)
        .assertStdOutContains("|GOOG  |2018-08-31 10:59:00|9021  |1227.1993|1227.215|", 4);
  }

  private void scheduleAndRunCompaction() throws Exception {
    sparkAdhoc1.executeShellCommand(HUDI_CLI_TOOL + " script --file " + COMPACTION_COMMANDS)
        .expectToSucceed();
    sparkAdhoc1.executeShellCommand(HUDI_CLI_TOOL + " script --file " + COMPACTION_BOOTSTRAP_COMMANDS)
        .expectToSucceed();
  }
}
