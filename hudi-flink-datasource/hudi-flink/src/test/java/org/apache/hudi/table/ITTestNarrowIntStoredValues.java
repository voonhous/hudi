/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.table;

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.FlinkMiniCluster;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.hudi.utils.TestTableEnvs;

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.hudi.utils.TestData.assertRowsEqualsUnordered;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Flink TINYINT / SMALLINT columns that the table schema stores as INT (a table without an explicit schema created
 * before RFC-88 step 2c, or one whose schema says INT) store their values sign extended, so -1 is stored as -1 and
 * not as 255 / 65535 (rfc/rfc-88/data-skipping-issues.md, item 9).
 *
 * <p>Files written before the fix keep the unsigned values. Flink reads them back truncated, so they still read as
 * the right numbers, and filters on the columns ignore their out-of-range statistics (Parquet row group statistics
 * and the column stats index), so they do not prune rows that match.
 */
@ExtendWith(FlinkMiniCluster.class)
public class ITTestNarrowIntStoredValues {

  private static final String TABLE = "t_narrow_stored";

  @TempDir
  File tempFile;

  private TableEnvironment tableEnv;

  @BeforeEach
  void beforeEach() {
    tableEnv = TestTableEnvs.getBatchTableEnv();
    tableEnv.getConfig().getConfiguration().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
  }

  static Stream<Arguments> tableTypesFormatsAndOperations() {
    List<Arguments> args = new ArrayList<>();
    for (HoodieTableType tableType : HoodieTableType.values()) {
      for (String format : new String[] {"PARQUET", "LANCE"}) {
        args.add(Arguments.of(tableType, format, "upsert"));
      }
    }
    args.add(Arguments.of(HoodieTableType.COPY_ON_WRITE, "PARQUET", "insert"));
    args.add(Arguments.of(HoodieTableType.COPY_ON_WRITE, "PARQUET", "bulk_insert"));
    return args.stream();
  }

  /**
   * A table storing TINYINT / SMALLINT columns as INT stores the values sign extended: read through an INT table
   * definition (as other engines read the table), they are the values written, and filters on them select the rows
   * whose values match, with data skipping and without. Through the TINYINT / SMALLINT definition, Flink reads them
   * back on the paths that can read such columns of an INT table (COW Parquet, see
   * rfc/rfc-88/data-skipping-issues.md, item 9).
   */
  @ParameterizedTest
  @MethodSource("tableTypesFormatsAndOperations")
  void testWritesSignExtendedValues(HoodieTableType tableType, String format, String operation) throws Exception {
    createTable(tableType, format, operation, "tinyint", "smallint");
    exec("insert into " + TABLE + " values "
        + "(1, cast(-128 as tinyint), cast(-32768 as smallint), 1, 'p1'), "
        + "(2, cast(127 as tinyint), cast(32767 as smallint), 2, 'p1'), "
        + "(3, cast(null as tinyint), cast(null as smallint), 3, 'p3'), "
        + "(4, cast(-1 as tinyint), cast(-1 as smallint), 4, 'p4')");

    assertIntTableSchema();
    boolean cowParquet = format.equals("PARQUET") && tableType == HoodieTableType.COPY_ON_WRITE;
    if (cowParquet) {
      Map<Integer, List<Integer>> stored = storedValues();
      assertEquals(Arrays.asList(-128, -32768), stored.get(1));
      assertEquals(Arrays.asList(127, 32767), stored.get(2));
      assertEquals(Arrays.asList(null, null), stored.get(3));
      assertEquals(Arrays.asList(-1, -1), stored.get(4));
      assertParquetStatsInRange();

      assertRowsEqualsUnordered(Arrays.asList(
          Row.of(1, (byte) -128, (short) -32768, 1L, "p1"),
          Row.of(2, (byte) 127, (short) 32767, 2L, "p1"),
          Row.of(3, null, null, 3L, "p3"),
          Row.of(4, (byte) -1, (short) -1, 4L, "p4")), select("select * from " + TABLE));
      assertFilters();
    }

    tableEnv.executeSql("drop table " + TABLE);
    createTable(tableType, format, operation, "int", "int");
    assertRowsEqualsUnordered(Arrays.asList(
        Row.of(1, -128, -32768, 1L, "p1"),
        Row.of(2, 127, 32767, 2L, "p1"),
        Row.of(3, null, null, 3L, "p3"),
        Row.of(4, -1, -1, 4L, "p4")), select("select * from " + TABLE));
    assertFilter("f_tiny < 0", 1, 4);
    assertFilter("f_tiny = -1", 4);
    assertFilter("f_tiny > 127");
    assertFilter("f_small < 0", 1, 4);
    assertFilter("f_small > 32767");
  }

  /**
   * Files written before the fix store the unsigned values (simulated by writing them through an INT table
   * definition, which stores exactly what the old writer stored). Reading them through the TINYINT / SMALLINT table
   * definition gives the right values, filters keep the files whose statistics are out of range, and files written
   * after the fix are read and filtered together with them, including a file that merges old and new rows.
   */
  @Test
  void testFilesWrittenBeforeTheFix() throws Exception {
    HoodieTableType tableType = HoodieTableType.COPY_ON_WRITE;
    createTable(tableType, "PARQUET", "upsert", "int", "int");
    exec("insert into " + TABLE + " values "
        + "(1, 128, 32768, 1, 'p1'), "
        + "(2, 127, 32767, 2, 'p1'), "
        + "(3, cast(null as int), cast(null as int), 3, 'p3'), "
        + "(4, 255, 65535, 4, 'p4')");
    assertEquals(Arrays.asList(255, 65535), storedValues().get(4));
    tableEnv.executeSql("drop table " + TABLE);
    createTable(tableType, "PARQUET", "upsert", "tinyint", "smallint");

    assertIntTableSchema();
    assertRowsEqualsUnordered(Arrays.asList(
        Row.of(1, (byte) -128, (short) -32768, 1L, "p1"),
        Row.of(2, (byte) 127, (short) 32767, 2L, "p1"),
        Row.of(3, null, null, 3L, "p3"),
        Row.of(4, (byte) -1, (short) -1, 4L, "p4")), select("select * from " + TABLE));
    assertFilters();

    // new rows are stored sign extended; the file of p1 now holds an old row (128) and a new one (-7)
    exec("insert into " + TABLE + " values "
        + "(5, cast(-7 as tinyint), cast(-700 as smallint), 5, 'p1'), "
        + "(6, cast(-2 as tinyint), cast(-2 as smallint), 6, 'p5'), "
        + "(4, cast(-1 as tinyint), cast(-1 as smallint), 7, 'p4')");
    Map<Integer, List<Integer>> stored = storedValues();
    assertEquals(Arrays.asList(128, 32768), stored.get(1), "rows of old files are copied as stored");
    assertEquals(Arrays.asList(-7, -700), stored.get(5));
    assertEquals(Arrays.asList(-1, -1), stored.get(4));
    assertIntTableSchema();
    assertRowsEqualsUnordered(Arrays.asList(
        Row.of(1, (byte) -128, (short) -32768, 1L, "p1"),
        Row.of(2, (byte) 127, (short) 32767, 2L, "p1"),
        Row.of(3, null, null, 3L, "p3"),
        Row.of(4, (byte) -1, (short) -1, 7L, "p4"),
        Row.of(5, (byte) -7, (short) -700, 5L, "p1"),
        Row.of(6, (byte) -2, (short) -2, 6L, "p5")), select("select * from " + TABLE));
    assertFilter("f_tiny < 0", 1, 4, 5, 6);
    assertFilter("f_tiny = -2", 6);
    assertFilter("f_tiny = -1", 4);
    assertFilter("f_tiny > 0", 2);
    assertFilter("f_small <= -700", 1, 5);
    assertFilter("f_small = cast(-2 as smallint)", 6);
    assertFilter("not (f_tiny >= 0)", 1, 4, 5, 6);
  }

  private void assertFilters() {
    assertFilter("f_tiny < 0", 1, 4);
    assertFilter("f_tiny = -128", 1);
    assertFilter("f_tiny = -1", 4);
    assertFilter("f_tiny > 0", 2);
    assertFilter("f_tiny <> 127", 1, 4);
    assertFilter("f_tiny >= -1", 2, 4);
    assertFilter("not (f_tiny >= 0)", 1, 4);
    assertFilter("f_small < 0", 1, 4);
    assertFilter("f_small = -32768", 1);
    assertFilter("f_small in (cast(-32768 as smallint), cast(-1 as smallint))", 1, 4);
    assertFilter("f_small > 300", 2);
    assertFilter("f_tiny is null", 3);
  }

  /**
   * The filter selects the expected ids, with data skipping and without.
   */
  private void assertFilter(String filter, Integer... ids) {
    List<Row> expected = new ArrayList<>();
    for (Integer id : ids) {
      expected.add(Row.of(id));
    }
    for (boolean dataSkipping : new boolean[] {false, true}) {
      String hint = "/*+ OPTIONS('" + FlinkOptions.READ_DATA_SKIPPING_ENABLED.key() + "'='" + dataSkipping + "') */";
      List<Row> actual = select("select id from " + TABLE + " " + hint + " where " + filter);
      assertEquals(new HashSet<>(expected), new HashSet<>(actual), filter + (dataSkipping ? " with" : " without") + " data skipping");
    }
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

  private void createTable(HoodieTableType tableType, String format, String operation, String tinyType, String smallType) {
    String sql = TestConfigurations.sql(TABLE)
        .field("id int not null")
        .field("f_tiny " + tinyType)
        .field("f_small " + smallType)
        .field("ts bigint")
        .field("part varchar(10)")
        .pkField("id")
        .partitionField("part")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.TABLE_TYPE, tableType.name())
        .option(FlinkOptions.OPERATION, operation)
        .option(FlinkOptions.ORDERING_FIELDS, "ts")
        .option(FlinkOptions.METADATA_ENABLED, true)
        .option("hoodie.metadata.index.column.stats.enable", true)
        .option(FlinkOptions.READ_DATA_SKIPPING_ENABLED, true)
        .option("hoodie.table.base.file.format", format)
        .end();
    tableEnv.executeSql(sql);
  }

  private void assertIntTableSchema() throws Exception {
    HoodieSchema tableSchema = new TableSchemaResolver(
        StreamerUtil.createMetaClient(tempFile.getAbsolutePath(), new Configuration())).getTableSchema(false);
    assertEquals(HoodieSchemaType.INT, tableSchema.getField("f_tiny").get().schema().getNonNullType().getType());
    assertEquals(HoodieSchemaType.INT, tableSchema.getField("f_small").get().schema().getNonNullType().getType());
  }

  /**
   * The stored f_tiny and f_small values of the latest version of each row in the base files, by id.
   */
  private Map<Integer, List<Integer>> storedValues() throws IOException {
    Map<Integer, List<Integer>> values = new HashMap<>();
    Map<Integer, String> commitTimes = new HashMap<>();
    Configuration conf = new Configuration();
    for (Path path : baseFiles()) {
      try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), path).withConf(conf).build()) {
        Group group;
        while ((group = reader.read()) != null) {
          int id = group.getInteger("id", 0);
          String commitTime = group.getString(HoodieRecord.COMMIT_TIME_METADATA_FIELD, 0);
          if (commitTime.compareTo(commitTimes.getOrDefault(id, "")) > 0) {
            commitTimes.put(id, commitTime);
            values.put(id, Arrays.asList(intOrNull(group, "f_tiny"), intOrNull(group, "f_small")));
          }
        }
      }
    }
    return values;
  }

  private static Integer intOrNull(Group group, String field) {
    return group.getFieldRepetitionCount(field) == 0 ? null : group.getInteger(field, 0);
  }

  /**
   * The Parquet statistics of f_tiny and f_small are those of the values written.
   */
  private void assertParquetStatsInRange() throws IOException {
    List<Path> files = baseFiles();
    assertFalse(files.isEmpty(), "no base file found");
    for (Path path : files) {
      try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(path, new Configuration()))) {
        for (BlockMetaData block : reader.getFooter().getBlocks()) {
          for (ColumnChunkMetaData column : block.getColumns()) {
            String name = column.getPath().toDotString();
            Statistics<?> stats = column.getStatistics();
            if (!(name.equals("f_tiny") || name.equals("f_small")) || !stats.hasNonNullValue()) {
              continue;
            }
            int min = (Integer) stats.genericGetMin();
            int max = (Integer) stats.genericGetMax();
            if (name.equals("f_tiny")) {
              assertTrue(min >= Byte.MIN_VALUE && max <= Byte.MAX_VALUE, name + " " + stats);
            } else if (name.equals("f_small")) {
              assertTrue(min >= Short.MIN_VALUE && max <= Short.MAX_VALUE, name + " " + stats);
            }
          }
        }
      }
    }
  }

  private List<Path> baseFiles() throws IOException {
    Configuration conf = new Configuration();
    Path basePath = new Path(tempFile.toURI());
    FileSystem fs = basePath.getFileSystem(conf);
    RemoteIterator<LocatedFileStatus> files = fs.listFiles(basePath, true);
    List<Path> paths = new ArrayList<>();
    while (files.hasNext()) {
      Path path = files.next().getPath();
      if (path.getName().endsWith(".parquet") && !path.toString().contains("/.hoodie/")) {
        paths.add(path);
      }
    }
    return paths;
  }

  private void exec(String insert) {
    try {
      tableEnv.executeSql(insert).await();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private List<Row> select(String query) {
    return CollectionUtil.iterableToList(() -> tableEnv.sqlQuery(query).execute().collect());
  }
}
