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
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.table.catalog.HoodieCatalog;
import org.apache.hudi.table.catalog.HoodieCatalogTestUtils;
import org.apache.hudi.table.catalog.HoodieHiveCatalog;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.FlinkMiniCluster;
import org.apache.hudi.utils.TestTableEnvs;
import org.apache.hudi.utils.TypeSystemTestUtils;

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.hudi.table.catalog.CatalogOptions.CATALOG_PATH;
import static org.apache.hudi.table.catalog.CatalogOptions.DEFAULT_DATABASE;
import static org.apache.hudi.utils.TestData.assertRowsEqualsUnordered;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * RFC-88 step 2c compatibility tests for Flink: a table created from now on maps TINYINT, SMALLINT and CHAR(n) to
 * INT8, INT16 and CHAR(n); a table created before (no {@link HoodieTableConfig#TYPE_SYSTEM_VERSION}) keeps INT, INT
 * and STRING for every column, including columns added later, through Flink SQL with and without a catalog, for COW
 * and MOR tables.
 */
@ExtendWith(FlinkMiniCluster.class)
public class ITTestNarrowIntAndCharTypeSystem {

  private static final String DB = "hudi_db";
  private static final String TABLE = "t_types";

  enum CatalogKind {
    NONE, DFS, HMS
  }

  @TempDir
  File tempFile;

  private TableEnvironment tableEnv;
  private Catalog catalog;
  /**
   * Whether the table's Flink columns are TINYINT and SMALLINT (else INT).
   */
  private boolean flinkNarrow;
  private boolean newTable;

  @BeforeEach
  void beforeEach() {
    tableEnv = TestTableEnvs.getBatchTableEnv();
    tableEnv.getConfig().getConfiguration().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
  }

  @AfterEach
  void afterEach() {
    if (catalog != null) {
      catalog.close();
    }
  }

  static Stream<Arguments> params() {
    List<Arguments> args = new ArrayList<>();
    for (CatalogKind catalogKind : CatalogKind.values()) {
      for (HoodieTableType tableType : HoodieTableType.values()) {
        for (boolean newTable : new boolean[] {true, false}) {
          args.add(Arguments.of(catalogKind, tableType, newTable));
        }
      }
    }
    return args.stream();
  }

  @ParameterizedTest(name = "catalog={0}, tableType={1}, newTable={2}")
  @MethodSource("params")
  void testTableSchemaFollowsTheTable(CatalogKind catalogKind, HoodieTableType tableType, boolean newTable) throws Exception {
    // a catalog gives the table the Flink types of its schema once it has one, so the columns of an existing table
    // are INT and STRING there; without a catalog they are the DDL's
    this.flinkNarrow = newTable || catalogKind == CatalogKind.NONE;
    this.newTable = newTable;
    String path = setUpCatalog(catalogKind);
    if (!newTable && catalogKind == CatalogKind.NONE) {
      // the table exists already, created before RFC-88 step 2c
      TypeSystemTestUtils.createTableBeforeTypeSystemVersion(path, TABLE, tableType, "id", "part", "ts", "PARQUET", "upsert");
    }
    createTable(catalogKind, tableType, path, false);
    if (!newTable && catalogKind != CatalogKind.NONE) {
      // the table the catalog created becomes one created before RFC-88 step 2c, before anything is written to it
      TypeSystemTestUtils.asTableCreatedBeforeTypeSystemVersion(path);
    }
    if (catalogKind != CatalogKind.NONE || !newTable) {
      assertTypeSystemVersion(path, newTable);
    }

    exec("insert into " + TABLE + " values "
        + "(1, cast(-128 as tinyint), cast(-32768 as smallint), cast('ab' as char(10)), 'xy', "
        + "TIMESTAMP '2024-01-01 00:00:01', TIMESTAMP '2024-01-01 00:00:01.123', TIMESTAMP '2024-01-01 00:00:01.123456', 1, 'p1'), "
        + "(2, cast(127 as tinyint), cast(32767 as smallint), cast('abcdefghij' as char(10)), 'abcdefghij', "
        + "TIMESTAMP '2024-01-02 00:00:00', TIMESTAMP '2024-01-02 00:00:00.5', TIMESTAMP '2024-01-02 00:00:00.000001', 2, 'p2'), "
        + "(3, cast(null as tinyint), cast(null as smallint), cast(null as char(10)), cast(null as varchar(10)), "
        + "cast(null as timestamp(0)), cast(null as timestamp(3)), cast(null as timestamp(6)), 3, 'p1')");
    // an upsert of the edge values
    exec("insert into " + TABLE + " values "
        + "(1, cast(127 as tinyint), cast(32767 as smallint), cast(' x' as char(10)), '', "
        + "TIMESTAMP '2024-01-01 00:00:01', TIMESTAMP '2024-01-01 00:00:01.123', TIMESTAMP '2024-01-01 00:00:01.123456', 4, 'p1'), "
        + "(4, cast(-1 as tinyint), cast(-1 as smallint), cast('a' as char(10)), 'a ', "
        + "TIMESTAMP '2024-01-03 00:00:00', TIMESTAMP '2024-01-03 00:00:00', TIMESTAMP '2024-01-03 00:00:00', 4, 'p2')");

    assertTypeSystemVersion(path, newTable);
    assertTableSchema(path, newTable, false);
    List<Row> expected = Arrays.asList(
        Row.of(1, tiny(127), small(32767), " x        ", "",
            LocalDateTime.parse("2024-01-01T00:00:01"), LocalDateTime.parse("2024-01-01T00:00:01.123"), LocalDateTime.parse("2024-01-01T00:00:01.123456"), 4L, "p1"),
        Row.of(2, tiny(127), small(32767), "abcdefghij", "abcdefghij",
            LocalDateTime.parse("2024-01-02T00:00:00"), LocalDateTime.parse("2024-01-02T00:00:00.5"), LocalDateTime.parse("2024-01-02T00:00:00.000001"), 2L, "p2"),
        Row.of(3, null, null, null, null, null, null, null, 3L, "p1"),
        Row.of(4, tiny(-1), small(-1), "a         ", "a ",
            LocalDateTime.parse("2024-01-03T00:00:00"), LocalDateTime.parse("2024-01-03T00:00:00"), LocalDateTime.parse("2024-01-03T00:00:00"), 4L, "p2"));
    assertRows(tableType, expected, "select * from " + TABLE);
    assertRows(tableType, Arrays.asList(Row.of(4)), "select id from " + TABLE + " where f_tiny < 0");
    assertRows(tableType, Arrays.asList(Row.of(1), Row.of(2)), "select id from " + TABLE + " where f_small = cast(32767 as smallint)");

    // a column added later follows the table as well, and the other columns keep their types
    if (catalogKind == CatalogKind.NONE) {
      tableEnv.executeSql("drop table " + TABLE);
      createTable(catalogKind, tableType, path, true);
    } else {
      tableEnv.executeSql("alter table " + TABLE + " add (f_tiny2 tinyint, f_char2 char(3))");
      if (newTable) {
        // the ALTER commits the table schema with INT8 / INT16 / CHAR(n) taken back from the table definition
        assertTableSchema(path, true, true);
      }
    }
    exec("insert into " + TABLE + " values "
        + "(5, cast(-5 as tinyint), cast(-5 as smallint), cast('e' as char(10)), 'e', "
        + "TIMESTAMP '2024-01-05 00:00:00', TIMESTAMP '2024-01-05 00:00:00', TIMESTAMP '2024-01-05 00:00:00', 5, 'p1', "
        + "cast(-128 as tinyint), cast('z' as char(3)))");
    assertTypeSystemVersion(path, newTable);
    assertTableSchema(path, newTable, true);
    assertRows(tableType, Arrays.asList(Row.of(5, tiny(-5), tiny(-128), "z  "), Row.of(4, tiny(-1), null, null)),
        "select id, f_tiny, f_tiny2, f_char2 from " + TABLE + " where id >= 4");
    if (catalogKind == CatalogKind.HMS) {
      assertSparkTableProperties(newTable);
    }
  }

  static Stream<Arguments> tableTypesAndKinds() {
    List<Arguments> args = new ArrayList<>();
    for (HoodieTableType tableType : HoodieTableType.values()) {
      for (boolean newTable : new boolean[] {true, false}) {
        args.add(Arguments.of(tableType, newTable));
      }
    }
    return args.stream();
  }

  /**
   * With schema evolution (schema on read) enabled, writes and ALTERs go through InternalSchema, which has no INT8,
   * INT16 or CHAR: a new table keeps them in its schema, including a TINYINT column changed to SMALLINT; an existing
   * table keeps INT and STRING.
   */
  @ParameterizedTest(name = "tableType={0}, newTable={1}")
  @MethodSource("tableTypesAndKinds")
  void testSchemaOnRead(HoodieTableType tableType, boolean newTable) throws Exception {
    this.flinkNarrow = newTable;
    this.newTable = newTable;
    String path = setUpCatalog(CatalogKind.DFS);
    tableEnv.executeSql("create table " + TABLE + " ("
        + "id int not null, f_tiny tinyint, f_small smallint, f_char char(10), ts bigint, part varchar(10), "
        + "primary key (id) not enforced) partitioned by (part) with ("
        + "'connector' = 'hudi', "
        + "'" + FlinkOptions.TABLE_TYPE.key() + "' = '" + tableType.name() + "', "
        + "'" + FlinkOptions.ORDERING_FIELDS.key() + "' = 'ts', "
        + "'hoodie.schema.on.read.enable' = 'true')");
    if (!newTable) {
      TypeSystemTestUtils.asTableCreatedBeforeTypeSystemVersion(path);
    }
    exec("insert into " + TABLE + " values "
        + "(1, cast(-128 as tinyint), cast(-32768 as smallint), cast('ab' as char(10)), 1, 'p1'), "
        + "(2, cast(127 as tinyint), cast(32767 as smallint), cast('abcdefghij' as char(10)), 2, 'p2')");
    assertSchemaOnReadTypes(path, newTable ? HoodieSchemaType.INT8 : HoodieSchemaType.INT);

    tableEnv.executeSql("alter table " + TABLE + " add (f_tiny2 tinyint)");
    exec("insert into " + TABLE + " values (3, cast(-1 as tinyint), cast(-1 as smallint), cast('c' as char(10)), 3, 'p1', cast(5 as tinyint))");
    assertSchemaOnReadTypes(path, newTable ? HoodieSchemaType.INT8 : HoodieSchemaType.INT);
    HoodieSchema schema = tableSchema(path);
    assertEquals(newTable ? HoodieSchemaType.INT8 : HoodieSchemaType.INT, type(schema, "f_tiny2").getType());
    // the file group reader of a MOR table reads log files with schema evolution in the types the InternalSchema
    // gives back, INT and STRING, which a new table's INT8 / INT16 / CHAR(n) columns do not match (open item, in
    // FileGroupReaderSchemaHandler); COW reads and existing tables are not affected
    boolean readable = !newTable || tableType == HoodieTableType.COPY_ON_WRITE;
    if (readable) {
      assertRows(tableType, Arrays.asList(
          Row.of(1, tiny(-128), small(-32768), "ab        ", null),
          Row.of(2, tiny(127), small(32767), "abcdefghij", null),
          Row.of(3, tiny(-1), small(-1), "c         ", tiny(5))),
          "select id, f_tiny, f_small, f_char, f_tiny2 from " + TABLE);
    }

    // TINYINT to SMALLINT: InternalSchema sees no change, the table schema does
    tableEnv.executeSql("alter table " + TABLE + " modify f_tiny smallint");
    exec("insert into " + TABLE + " values (4, cast(300 as smallint), cast(4 as smallint), cast('d' as char(10)), 4, 'p2', cast(-5 as tinyint))");
    schema = tableSchema(path);
    assertEquals(newTable ? HoodieSchemaType.INT16 : HoodieSchemaType.INT, type(schema, "f_tiny").getType());
    assertEquals(newTable ? HoodieSchemaType.INT16 : HoodieSchemaType.INT, type(schema, "f_small").getType());
    assertEquals(newTable ? HoodieSchemaType.CHAR : HoodieSchemaType.STRING, type(schema, "f_char").getType());
    assertTypeSystemVersion(path, newTable);

    if (!readable) {
      return;
    }
    assertRows(tableType, Arrays.asList(
        Row.of(1, small(-128), small(-32768), "ab        ", null),
        Row.of(2, small(127), small(32767), "abcdefghij", null),
        Row.of(3, small(-1), small(-1), "c         ", tiny(5)),
        Row.of(4, small(300), small(4), "d         ", tiny(-5))),
        "select id, f_tiny, f_small, f_char, f_tiny2 from " + TABLE);
  }

  private static void assertSchemaOnReadTypes(String path, HoodieSchemaType narrowInt) throws Exception {
    HoodieSchema schema = tableSchema(path);
    assertEquals(narrowInt, type(schema, "f_tiny").getType());
    assertEquals(narrowInt == HoodieSchemaType.INT ? HoodieSchemaType.INT : HoodieSchemaType.INT16, type(schema, "f_small").getType());
    assertEquals(narrowInt == HoodieSchemaType.INT ? HoodieSchemaType.STRING : HoodieSchemaType.CHAR, type(schema, "f_char").getType());
  }

  private static HoodieSchema tableSchema(String path) throws Exception {
    return new TableSchemaResolver(StreamerUtil.createMetaClient(path, new Configuration())).getTableSchema(false);
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

  /**
   * Registers the catalog and returns the table path.
   */
  private String setUpCatalog(CatalogKind catalogKind) {
    switch (catalogKind) {
      case DFS: {
        Map<String, String> options = new HashMap<>();
        options.put(CATALOG_PATH.key(), tempFile.getAbsolutePath());
        options.put(DEFAULT_DATABASE.key(), DB);
        catalog = new HoodieCatalog("hudi_catalog", org.apache.flink.configuration.Configuration.fromMap(options));
        break;
      }
      case HMS:
        catalog = HoodieCatalogTestUtils.createHiveCatalog("hudi_catalog");
        break;
      default:
        return new File(tempFile, TABLE).getAbsolutePath();
    }
    catalog.open();
    tableEnv.registerCatalog("hudi_catalog", catalog);
    tableEnv.executeSql("use catalog hudi_catalog");
    tableEnv.executeSql("create database if not exists " + DB);
    tableEnv.executeSql("use " + DB);
    return catalogKind == CatalogKind.DFS
        ? new File(new File(tempFile, DB), TABLE).getAbsolutePath()
        : new File(tempFile, TABLE).getAbsolutePath();
  }

  private void createTable(CatalogKind catalogKind, HoodieTableType tableType, String path, boolean withAddedColumns) {
    StringBuilder sql = new StringBuilder("create table " + TABLE + " ("
        + "id int not null, f_tiny tinyint, f_small smallint, f_char char(10), f_varchar varchar(10), "
        + "f_ts0 timestamp(0), f_ts3 timestamp(3), f_ts6 timestamp(6), ts bigint, part varchar(10)");
    if (withAddedColumns) {
      sql.append(", f_tiny2 tinyint, f_char2 char(3)");
    }
    sql.append(", primary key (id) not enforced) partitioned by (part) with ("
        + "'connector' = 'hudi', "
        + "'" + FlinkOptions.TABLE_TYPE.key() + "' = '" + tableType.name() + "', "
        + "'" + FlinkOptions.ORDERING_FIELDS.key() + "' = 'ts', "
        + "'" + FlinkOptions.METADATA_ENABLED.key() + "' = 'true', "
        + "'hoodie.metadata.index.column.stats.enable' = 'true', "
        + "'" + FlinkOptions.READ_DATA_SKIPPING_ENABLED.key() + "' = 'true'");
    if (catalogKind != CatalogKind.DFS) {
      sql.append(", '" + FlinkOptions.PATH.key() + "' = '" + path + "'");
    }
    sql.append(")");
    tableEnv.executeSql(sql.toString());
  }

  private static void assertTypeSystemVersion(String path, boolean newTable) {
    HoodieTableConfig tableConfig = StreamerUtil.createMetaClient(path, new Configuration()).getTableConfig();
    assertEquals(newTable, tableConfig.usesNarrowIntAndCharTypes());
    if (newTable) {
      assertEquals("1", tableConfig.getString(HoodieTableConfig.TYPE_SYSTEM_VERSION));
    } else {
      assertFalse(tableConfig.contains(HoodieTableConfig.TYPE_SYSTEM_VERSION));
    }
  }

  private static void assertTableSchema(String path, boolean newTable, boolean withAddedColumns) throws Exception {
    HoodieSchema schema = new TableSchemaResolver(StreamerUtil.createMetaClient(path, new Configuration())).getTableSchema(false);
    assertEquals(newTable ? HoodieSchemaType.INT8 : HoodieSchemaType.INT, type(schema, "f_tiny").getType());
    assertEquals(newTable ? HoodieSchemaType.INT16 : HoodieSchemaType.INT, type(schema, "f_small").getType());
    assertEquals(newTable ? HoodieSchemaType.CHAR : HoodieSchemaType.STRING, type(schema, "f_char").getType());
    if (newTable) {
      assertEquals(10, ((HoodieSchema.Char) type(schema, "f_char")).getLength());
    }
    // VARCHAR(n) stays STRING (RFC-88 rule 5), and the timestamps keep their precision
    assertEquals(HoodieSchemaType.STRING, type(schema, "f_varchar").getType());
    assertEquals(HoodieSchemaType.INT, type(schema, "id").getType());
    for (String ts : new String[] {"f_ts0", "f_ts3", "f_ts6"}) {
      assertEquals(HoodieSchemaType.TIMESTAMP, type(schema, ts).getType(), ts);
    }
    assertTrue(((HoodieSchema.Timestamp) type(schema, "f_ts0")).getPrecision() == HoodieSchema.TimePrecision.MILLIS);
    assertTrue(((HoodieSchema.Timestamp) type(schema, "f_ts6")).getPrecision() == HoodieSchema.TimePrecision.MICROS);
    if (withAddedColumns) {
      assertEquals(newTable ? HoodieSchemaType.INT8 : HoodieSchemaType.INT, type(schema, "f_tiny2").getType());
      assertEquals(newTable ? HoodieSchemaType.CHAR : HoodieSchemaType.STRING, type(schema, "f_char2").getType());
    } else {
      assertFalse(schema.getField("f_tiny2").isPresent());
    }
  }

  private static HoodieSchema type(HoodieSchema schema, String column) {
    return schema.getField(column).get().schema().getNonNullType();
  }

  /**
   * The rows of the query. Flink reads TINYINT / SMALLINT columns whose table schema says INT only on the COW Parquet
   * path, as on master (rfc/rfc-88/data-skipping-issues.md, item 9), so an existing MOR table is read only where its
   * columns are INT.
   */
  private void assertRows(HoodieTableType tableType, List<Row> expected, String query) {
    if (!newTable && flinkNarrow && tableType == HoodieTableType.MERGE_ON_READ) {
      return;
    }
    assertRowsEqualsUnordered(expected, select(query));
  }

  private Object tiny(int value) {
    return flinkNarrow ? (Object) (byte) value : (Object) value;
  }

  private Object small(int value) {
    return flinkNarrow ? (Object) (short) value : (Object) value;
  }

  /**
   * The Hive columns and the Spark table properties the HMS catalog renders: Hive's and Spark's types of INT8, INT16
   * and CHAR(n) for a new table, today's int and string for an existing one.
   */
  private void assertSparkTableProperties(boolean newTable) throws Exception {
    org.apache.hadoop.hive.metastore.api.Table hiveTable = ((HoodieHiveCatalog) catalog).getHiveTable(new ObjectPath(DB, TABLE));
    Map<String, String> columns = new HashMap<>();
    hiveTable.getSd().getCols().forEach(col -> columns.put(col.getName(), col.getType()));
    assertEquals(newTable ? "tinyint" : "int", columns.get("f_tiny"));
    assertEquals(newTable ? "smallint" : "int", columns.get("f_small"));
    assertEquals(newTable ? "char(10)" : "string", columns.get("f_char"));
    assertEquals(newTable ? "tinyint" : "int", columns.get("f_tiny2"));
    assertEquals("string", columns.get("f_varchar"));
    Map<String, String> parameters = hiveTable.getParameters();
    StringBuilder sparkSchema = new StringBuilder();
    for (int i = 0; parameters.containsKey("spark.sql.sources.schema.part." + i); i++) {
      sparkSchema.append(parameters.get("spark.sql.sources.schema.part." + i));
    }
    String json = sparkSchema.toString();
    assertFalse(json.isEmpty(), "no Spark schema in " + parameters);
    if (newTable) {
      assertTrue(json.contains("{\"name\":\"f_tiny\",\"type\":\"byte\""), json);
      assertTrue(json.contains("{\"name\":\"f_small\",\"type\":\"short\""), json);
      assertTrue(json.contains("char(10)"), json);
    } else {
      assertTrue(json.contains("{\"name\":\"f_tiny\",\"type\":\"integer\""), json);
      assertTrue(json.contains("{\"name\":\"f_small\",\"type\":\"integer\""), json);
      assertFalse(json.contains("char("), json);
    }
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
