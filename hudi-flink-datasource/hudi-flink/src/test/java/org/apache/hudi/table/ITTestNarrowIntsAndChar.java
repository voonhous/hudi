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

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.sink.clustering.FlinkClusteringConfig;
import org.apache.hudi.sink.clustering.HoodieFlinkClusteringJob;
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
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
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
import java.util.HashSet;
import java.util.List;
import java.util.stream.Stream;

import static org.apache.hudi.utils.TestData.assertRowsEqualsUnordered;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * End to end: a table whose HoodieSchema has INT8, INT16 and CHAR(10) columns, written by Flink SQL with a table
 * definition that uses TINYINT, SMALLINT and CHAR(10) and the HoodieSchema given through
 * {@link FlinkOptions#SOURCE_AVRO_SCHEMA}, reads back through Flink SQL with the same values (RFC-88 rollout step 2).
 * Without the option, a new table infers the same schema (step 2c).
 */
@ExtendWith(FlinkMiniCluster.class)
public class ITTestNarrowIntsAndChar {

  private static final String TABLE = "t_narrow";

  @TempDir
  File tempFile;

  private TableEnvironment tableEnv;

  @BeforeEach
  void beforeEach() {
    tableEnv = TestTableEnvs.getBatchTableEnv();
    tableEnv.getConfig().getConfiguration().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
  }

  static Stream<Arguments> tableTypesAndFormats() {
    List<Arguments> args = new ArrayList<>();
    for (HoodieTableType tableType : HoodieTableType.values()) {
      for (String format : new String[] {"PARQUET", "LANCE"}) {
        for (boolean sourceV2 : new boolean[] {false, true}) {
          args.add(Arguments.of(tableType, format, sourceV2));
        }
      }
    }
    return args.stream();
  }

  @ParameterizedTest
  @MethodSource("tableTypesAndFormats")
  void testUpsertAndRead(HoodieTableType tableType, String format, boolean sourceV2) throws Exception {
    createTable(tableType, format, "upsert", sourceV2);
    insertRows();
    exec("insert into " + TABLE + " values (1, cast(5 as tinyint), cast(300 as smallint), cast('cd' as char(10)), 10, 'p1')");

    assertRowsEqualsUnordered(Arrays.asList(
        Row.of(1, (byte) 5, (short) 300, "cd        ", 10L, "p1"),
        Row.of(2, (byte) 127, (short) 32767, "abcdefghij", 2L, "p2"),
        Row.of(3, null, null, null, 3L, "p3"),
        Row.of(4, (byte) -1, (short) -1, " x        ", 4L, "p4")), select("select * from " + TABLE));
    assertRowsEqualsUnordered(Arrays.asList(
        Row.of("cd        ", (short) 300), Row.of("abcdefghij", (short) 32767), Row.of(null, null), Row.of(" x        ", (short) -1)),
        select("select f_char, f_small from " + TABLE));
    assertTableSchema();
    if (tableType == HoodieTableType.COPY_ON_WRITE && format.equals("PARQUET")) {
      assertParquetFooters();
    }
  }

  @Test
  void testBulkInsertAndRead() throws Exception {
    createTable(HoodieTableType.COPY_ON_WRITE, "PARQUET", "bulk_insert", false);
    insertRows();

    assertRowsEqualsUnordered(Arrays.asList(
        Row.of(1, (byte) -128, (short) -32768, "ab        ", 1L, "p1"),
        Row.of(2, (byte) 127, (short) 32767, "abcdefghij", 2L, "p2"),
        Row.of(3, null, null, null, 3L, "p3"),
        Row.of(4, (byte) -1, (short) -1, " x        ", 4L, "p4")), select("select * from " + TABLE));
    assertTableSchema();
    assertParquetFooters();
  }

  /**
   * Clustering reads the base files and rewrites them with INT8, INT16 and CHAR(10) as they were.
   */
  @Test
  void testClustering() throws Exception {
    createTable(HoodieTableType.COPY_ON_WRITE, "PARQUET", "insert", false);
    insertRows();
    exec("insert into " + TABLE + " values (5, cast(-7 as tinyint), cast(-700 as smallint), cast('ef' as char(10)), 5, 'p1')");

    FlinkClusteringConfig cfg = new FlinkClusteringConfig();
    cfg.path = tempFile.getAbsolutePath();
    cfg.schedule = true;
    org.apache.flink.configuration.Configuration conf = FlinkClusteringConfig.toFlinkConfig(cfg);
    new HoodieFlinkClusteringJob(new HoodieFlinkClusteringJob.AsyncClusteringService(cfg, conf)).start(false);

    HoodieTableMetaClient metaClient = metaClient();
    assertFalse(metaClient.getActiveTimeline().getCompletedReplaceTimeline().empty(), "no clustering commit");
    assertRowsEqualsUnordered(Arrays.asList(
        Row.of(1, (byte) -128, (short) -32768, "ab        ", 1L, "p1"),
        Row.of(2, (byte) 127, (short) 32767, "abcdefghij", 2L, "p2"),
        Row.of(3, null, null, null, 3L, "p3"),
        Row.of(4, (byte) -1, (short) -1, " x        ", 4L, "p4"),
        Row.of(5, (byte) -7, (short) -700, "ef        ", 5L, "p1")), select("select * from " + TABLE));
    assertTableSchema();
    assertParquetFooters();
  }

  /**
   * Without an explicit schema, a new table infers INT8, INT16 and CHAR(10) for TINYINT, SMALLINT and CHAR(10)
   * columns (RFC-88 rollout step 2c) and writes them as INT(8) / INT(16) annotated INT32 and strings. A table created
   * before keeps INT and STRING ({@link ITTestNarrowIntAndCharTypeSystem}).
   */
  @ParameterizedTest
  @MethodSource("tableTypesAndFormats")
  void testInferredSchemaOfNewTable(HoodieTableType tableType, String format, boolean sourceV2) throws Exception {
    createTable(tableType, format, "upsert", sourceV2, false);
    insertRows();

    assertRowsEqualsUnordered(Arrays.asList(
        Row.of(1, (byte) -128, (short) -32768, "ab        ", 1L, "p1"),
        Row.of(2, (byte) 127, (short) 32767, "abcdefghij", 2L, "p2"),
        Row.of(3, null, null, null, 3L, "p3"),
        Row.of(4, (byte) -1, (short) -1, " x        ", 4L, "p4")), select("select * from " + TABLE));
    assertTableSchema();
    if (tableType == HoodieTableType.COPY_ON_WRITE && format.equals("PARQUET")) {
      assertParquetFooters();
    }
    assertFilter("f_tiny < 0", 1, 4);
    assertFilter("f_small = cast(32767 as smallint)", 2);
    assertFilter("f_char = cast('ab' as char(10))", 1);
  }

  @ParameterizedTest
  @MethodSource("tableTypesAndFormats")
  void testFiltersWithDataSkipping(HoodieTableType tableType, String format, boolean sourceV2) {
    createTable(tableType, format, "upsert", sourceV2);
    insertRows();

    assertFilter("f_tiny = -128", 1);
    assertFilter("f_tiny < 0", 1, 4);
    assertFilter("f_tiny = cast(127 as tinyint)", 2);
    assertFilter("f_small > 300", 2);
    assertFilter("f_small in (cast(-32768 as smallint), cast(-1 as smallint))", 1, 4);
    // Flink compares CHAR values with their padding, as Hudi compares their stats
    assertFilter("f_char = 'ab'");
    assertFilter("f_char = cast('ab' as char(10))", 1);
    assertFilter("f_char = 'abcdefghij'", 2);
    assertFilter("f_char > 'ab'", 1, 2);
    assertFilter("f_char < 'a'", 4);
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

  private void createTable(HoodieTableType tableType, String format, String operation, boolean sourceV2) {
    createTable(tableType, format, operation, sourceV2, true);
  }

  private void createTable(HoodieTableType tableType, String format, String operation, boolean sourceV2, boolean explicitSchema) {
    TestConfigurations.Sql sql = TestConfigurations.sql(TABLE)
        .field("id int not null")
        .field("f_tiny tinyint")
        .field("f_small smallint")
        .field("f_char char(10)")
        .field("ts bigint")
        .field("part varchar(10)")
        .pkField("id")
        .partitionField("part")
        .option(FlinkOptions.PATH, tempFile.getAbsolutePath())
        .option(FlinkOptions.TABLE_TYPE, tableType.name())
        .option(FlinkOptions.OPERATION, operation)
        .option(FlinkOptions.ORDERING_FIELDS, "ts")
        .option(FlinkOptions.READ_SOURCE_V2_ENABLED, sourceV2)
        .option(FlinkOptions.METADATA_ENABLED, true)
        .option("hoodie.metadata.index.column.stats.enable", true)
        .option(FlinkOptions.READ_DATA_SKIPPING_ENABLED, true)
        .option("hoodie.table.base.file.format", format);
    if (explicitSchema) {
      sql.option(FlinkOptions.SOURCE_AVRO_SCHEMA, schema().toString());
    }
    tableEnv.executeSql(sql.end());
  }

  private void insertRows() {
    exec("insert into " + TABLE + " values "
        + "(1, cast(-128 as tinyint), cast(-32768 as smallint), cast('ab' as char(10)), 1, 'p1'), "
        + "(2, cast(127 as tinyint), cast(32767 as smallint), cast('abcdefghij' as char(10)), 2, 'p2'), "
        + "(3, cast(null as tinyint), cast(null as smallint), cast(null as char(10)), 3, 'p3'), "
        + "(4, cast(-1 as tinyint), cast(-1 as smallint), cast(' x' as char(10)), 4, 'p4')");
  }

  /**
   * The table schema of the test table: INT8, INT16 and CHAR(10) columns.
   */
  private static HoodieSchema schema() {
    return HoodieSchema.createRecord(HoodieSchemaUtils.getRecordQualifiedName(TABLE), null, null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT)),
        HoodieSchemaField.of("f_tiny", HoodieSchema.createNullable(HoodieSchema.createInt8()), null, HoodieSchema.NULL_VALUE),
        HoodieSchemaField.of("f_small", HoodieSchema.createNullable(HoodieSchema.createInt16()), null, HoodieSchema.NULL_VALUE),
        HoodieSchemaField.of("f_char", HoodieSchema.createNullable(HoodieSchema.createChar(10)), null, HoodieSchema.NULL_VALUE),
        HoodieSchemaField.of("ts", HoodieSchema.createNullable(HoodieSchema.create(HoodieSchemaType.LONG)), null, HoodieSchema.NULL_VALUE),
        HoodieSchemaField.of("part", HoodieSchema.createNullable(HoodieSchema.create(HoodieSchemaType.STRING)), null, HoodieSchema.NULL_VALUE)));
  }

  private void assertTableSchema() throws Exception {
    HoodieSchema tableSchema = new TableSchemaResolver(metaClient()).getTableSchema(false);
    assertEquals(HoodieSchemaType.INT8, tableSchema.getField("f_tiny").get().schema().getNonNullType().getType());
    assertEquals(HoodieSchemaType.INT16, tableSchema.getField("f_small").get().schema().getNonNullType().getType());
    HoodieSchema charSchema = tableSchema.getField("f_char").get().schema().getNonNullType();
    assertEquals(HoodieSchemaType.CHAR, charSchema.getType());
    assertEquals(10, ((HoodieSchema.Char) charSchema).getLength());
  }

  private HoodieTableMetaClient metaClient() {
    return StreamerUtil.createMetaClient(tempFile.getAbsolutePath(), new Configuration());
  }

  /**
   * The base files store INT8 and INT16 as INT32 annotated INT(8) and INT(16), and CHAR as a string.
   */
  private void assertParquetFooters() throws IOException {
    List<MessageType> footers = parquetFooters();
    assertFalse(footers.isEmpty(), "no base file found");
    for (MessageType fileSchema : footers) {
      assertIntColumn(fileSchema, "f_tiny", 8);
      assertIntColumn(fileSchema, "f_small", 16);
      PrimitiveType charColumn = fileSchema.getType("f_char").asPrimitiveType();
      assertEquals(PrimitiveType.PrimitiveTypeName.BINARY, charColumn.getPrimitiveTypeName());
      assertEquals(LogicalTypeAnnotation.stringType(), charColumn.getLogicalTypeAnnotation());
    }
  }

  private List<MessageType> parquetFooters() throws IOException {
    Configuration conf = new Configuration();
    Path basePath = new Path(tempFile.toURI());
    FileSystem fs = basePath.getFileSystem(conf);
    RemoteIterator<LocatedFileStatus> files = fs.listFiles(basePath, true);
    List<MessageType> footers = new ArrayList<>();
    while (files.hasNext()) {
      Path path = files.next().getPath();
      if (!path.getName().endsWith(".parquet") || path.toString().contains("/.hoodie/")) {
        continue;
      }
      try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(path, conf))) {
        footers.add(reader.getFooter().getFileMetaData().getSchema());
      }
    }
    return footers;
  }

  private static void assertIntColumn(MessageType fileSchema, String column, int bitWidth) {
    PrimitiveType type = fileSchema.getType(column).asPrimitiveType();
    assertEquals(PrimitiveType.PrimitiveTypeName.INT32, type.getPrimitiveTypeName(), column);
    assertEquals(LogicalTypeAnnotation.intType(bitWidth, true), type.getLogicalTypeAnnotation(), column);
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
