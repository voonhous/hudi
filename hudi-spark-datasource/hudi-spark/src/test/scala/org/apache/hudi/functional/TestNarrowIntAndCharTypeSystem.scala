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

package org.apache.hudi.functional

import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.common.config.HoodieCommonConfig
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.common.schema.{HoodieSchema, HoodieSchemaType}
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.hive.util.HiveSchemaUtil
import org.apache.hudi.testutils.HoodieSparkClientTestBase

import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.types._
import org.junit.jupiter.api.{AfterEach, BeforeEach}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource

import scala.collection.JavaConverters._

/**
 * RFC-88 step 2c, compatibility tests for DataFrame writes: Spark's ByteType, ShortType, CHAR(n) and VARCHAR(n) are
 * accepted for a table created before `hoodie.table.type.system.version` existed (an existing table) and for a new
 * table. The existing table keeps INT / STRING in its table schema after inserts, upserts, an added column and
 * schema reconciliation, and reads back as before (IntegerType, unpadded strings); the new table gets INT8, INT16 and
 * CHAR(n) on the same writes and reads back as ByteType, ShortType and CHAR(n). VARCHAR(n) is STRING in both.
 */
class TestNarrowIntAndCharTypeSystem extends HoodieSparkClientTestBase {

  var spark: SparkSession = _

  private val baseColumns: Seq[(String, DataType)] = Seq(
    "key" -> StringType, "part" -> StringType, "ts" -> LongType,
    "tiny" -> ByteType, "small" -> ShortType, "code" -> CharType(10), "name" -> VarcharType(8))

  @BeforeEach override def setUp(): Unit = {
    initPath()
    initSparkContexts()
    spark = sqlContext.sparkSession
    initHoodieStorage()
  }

  @AfterEach override def tearDown(): Unit = {
    cleanupSparkContexts()
    cleanupFileSystem()
  }

  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testNewTable(tableType: HoodieTableType): Unit = {
    // the first write creates the table, with the new mapping
    writeAll(tableType, SaveMode.Overwrite)
    assertEquals("1", tableConfig.getProps.getProperty(HoodieTableConfig.TYPE_SYSTEM_VERSION.key))

    val tableSchema = latestTableSchema
    assertEquals(HoodieSchema.createInt8(), column(tableSchema, "tiny"))
    assertEquals(HoodieSchema.createInt16(), column(tableSchema, "small"))
    assertEquals(HoodieSchema.createChar(10), column(tableSchema, "code"))
    assertEquals(HoodieSchema.create(HoodieSchemaType.STRING), column(tableSchema, "name"))
    assertEquals(HoodieSchema.createInt8(), column(tableSchema, "extra"))
    assertEquals(Seq("tinyint", "smallint", "char(10)", "string", "tinyint"), hiveTypes(tableSchema))

    val read = spark.read.format("hudi").load(basePath)
    assertEquals(Seq("tinyint", "smallint", "char(10)", "string", "tinyint"), sparkTypes(read.schema))
    assertRows(read, padChar = true, byteAndShort = true)
  }

  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testExistingTable(tableType: HoodieTableType): Unit = {
    // a table as master creates it: no type system version in hoodie.properties
    HoodieTableMetaClient.newTableBuilder()
      .setTableType(tableType)
      .setTableName("narrow")
      .setRecordKeyFields("key")
      .setPartitionFields("part")
      .setOrderingFields("ts")
      .initTable(storageConf, basePath)
    removeTypeSystemVersion()
    writeAll(tableType, SaveMode.Append)
    assertFalse(tableConfig.getProps.containsKey(HoodieTableConfig.TYPE_SYSTEM_VERSION.key))

    val tableSchema = latestTableSchema
    Seq("tiny", "small", "extra").foreach(c => assertEquals(HoodieSchema.create(HoodieSchemaType.INT), column(tableSchema, c)))
    Seq("code", "name").foreach(c => assertEquals(HoodieSchema.create(HoodieSchemaType.STRING), column(tableSchema, c)))
    assertEquals(Seq("int", "int", "string", "string", "int"), hiveTypes(tableSchema))

    val read = spark.read.format("hudi").load(basePath)
    assertEquals(Seq("int", "int", "string", "string", "int"), sparkTypes(read.schema))
    assertRows(read, padChar = false, byteAndShort = false)
  }

  /**
   * Writing a wider Spark type into an INT16 column of a new table widens the column, as for any type widening.
   */
  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testWiderTypeWidensNewTableColumn(tableType: HoodieTableType): Unit = {
    write(df(baseColumns, Seq(row("k1", 1L, -128, 32767, "a", "n1"))), tableType, UPSERT_OPERATION_OPT_VAL, SaveMode.Overwrite)
    assertEquals(HoodieSchema.createInt16(), column(latestTableSchema, "small"))
    val wider = baseColumns.map { case (n, t) => if (n == "small") n -> IntegerType else n -> t }
    write(df(wider, Seq(Row("k2", "p1", 2L, java.lang.Byte.valueOf(1.toByte), Integer.valueOf(100000), "b", "n2"))),
      tableType, UPSERT_OPERATION_OPT_VAL, SaveMode.Append)
    assertEquals(HoodieSchema.create(HoodieSchemaType.INT), column(latestTableSchema, "small"))
    assertEquals(HoodieSchema.createInt8(), column(latestTableSchema, "tiny"))
    val rows = spark.read.format("hudi").load(basePath).select("key", "small").orderBy("key").collect()
    assertEquals(Seq(Row("k1", 32767), Row("k2", 100000)), rows.toSeq)
  }

  /**
   * The writes of both tests: insert the edge values, upsert, add a column, then write batches without a column, with
   * hoodie.write.set.null.for.missing.columns and with hoodie.datasource.write.reconcile.schema (both go through
   * InternalSchema).
   */
  private def writeAll(tableType: HoodieTableType, firstMode: SaveMode): Unit = {
    write(df(baseColumns, Seq(
      row("k1", 1L, -128, 32767, "abcdefghij", "name1"),
      row("k2", 1L, 127, -32768, "abc", "n2"),
      Row("k3", "p1", 1L, java.lang.Byte.valueOf(0.toByte), null, "x", null))), tableType, UPSERT_OPERATION_OPT_VAL, firstMode)
    write(df(baseColumns, Seq(row("k1", 2L, 5, -5, "0123456789", "upd"))), tableType, UPSERT_OPERATION_OPT_VAL, SaveMode.Append)

    val withExtra = baseColumns :+ ("extra" -> ByteType)
    write(df(withExtra, Seq(Row.fromSeq(row("k4", 1L, 1, 1, "k4", "n4").toSeq :+ byte(-1)))), tableType,
      UPSERT_OPERATION_OPT_VAL, SaveMode.Append)
    write(df(withExtra.filterNot(_._1 == "code"), Seq(Row("k5", "p1", 1L, byte(2), short(2), "n5", byte(2)))),
      tableType, UPSERT_OPERATION_OPT_VAL, SaveMode.Append, Map(HoodieCommonConfig.SET_NULL_FOR_MISSING_COLUMNS.key -> "true"))
    write(df(withExtra.filterNot(_._1 == "small"), Seq(Row("k6", "p1", 1L, byte(3), "k6", "n6", byte(3)))),
      tableType, UPSERT_OPERATION_OPT_VAL, SaveMode.Append, Map(RECONCILE_SCHEMA.key -> "true"))
  }

  private def assertRows(read: DataFrame, padChar: Boolean, byteAndShort: Boolean): Unit = {
    def code(v: String): String = if (v == null || !padChar) v else v.padTo(10, ' ')
    def tiny(v: Int): Any = if (byteAndShort) java.lang.Byte.valueOf(v.toByte) else Integer.valueOf(v)
    def small(v: Integer): Any = if (v == null) null else if (byteAndShort) java.lang.Short.valueOf(v.toShort) else v
    val expected = Seq(
      Row("k1", tiny(5), small(-5), code("0123456789"), "upd", null),
      Row("k2", tiny(127), small(-32768), code("abc"), "n2", null),
      Row("k3", tiny(0), null, code("x"), null, null),
      Row("k4", tiny(1), small(1), code("k4"), "n4", tiny(-1)),
      Row("k5", tiny(2), small(2), null, "n5", tiny(2)),
      Row("k6", tiny(3), null, code("k6"), "n6", tiny(3)))
    assertEquals(expected, read.select("key", "tiny", "small", "code", "name", "extra").orderBy("key").collect().toSeq)
    assertEquals(Seq("k1"), read.where("tiny = 5").select("key").collect().map(_.getString(0)).toSeq)
    assertEquals(Seq("k2"), read.where("small = -32768").select("key").collect().map(_.getString(0)).toSeq)
    // CHAR(10) compares ignoring trailing spaces; STRING does not
    assertEquals(Seq("k2"), read.where("code = 'abc'").select("key").collect().map(_.getString(0)).toSeq)
    assertEquals(if (padChar) Seq("k2") else Seq.empty, read.where("code = 'abc   '").select("key").collect().map(_.getString(0)).toSeq)
  }

  private def row(key: String, ts: Long, tiny: Int, small: Int, code: String, name: String): Row =
    Row(key, "p1", ts, byte(tiny), short(small), code, name)

  private def byte(v: Int): java.lang.Byte = java.lang.Byte.valueOf(v.toByte)

  private def short(v: Int): java.lang.Short = java.lang.Short.valueOf(v.toShort)

  /**
   * A DataFrame with these columns; CHAR and VARCHAR columns are StringType with the raw type in the field metadata,
   * as Spark keeps them in a schema.
   */
  private def df(columns: Seq[(String, DataType)], rows: Seq[Row]): DataFrame = {
    val schema = CharVarcharUtils.replaceCharVarcharWithStringInSchema(
      StructType(columns.map { case (n, t) => StructField(n, t, nullable = true) }))
    spark.createDataFrame(spark.sparkContext.parallelize(rows, 1), schema)
  }

  private def write(df: DataFrame, tableType: HoodieTableType, operation: String, mode: SaveMode,
                    extraOptions: Map[String, String] = Map.empty): Unit = {
    df.write.format("hudi")
      .option(RECORDKEY_FIELD.key, "key")
      .option(PARTITIONPATH_FIELD.key, "part")
      .option(HoodieTableConfig.ORDERING_FIELDS.key, "ts")
      .option(TABLE_NAME.key, "narrow")
      .option(TABLE_TYPE.key, tableType.name)
      .option(OPERATION.key, operation)
      .option("hoodie.upsert.shuffle.parallelism", "1")
      .options(extraOptions)
      .mode(mode)
      .save(basePath)
  }

  private def loadMetaClient: HoodieTableMetaClient =
    HoodieTableMetaClient.builder().setConf(storageConf).setBasePath(basePath).build()

  private def tableConfig: HoodieTableConfig = loadMetaClient.getTableConfig

  private def removeTypeSystemVersion(): Unit = {
    HoodieTableConfig.delete(storage, loadMetaClient.getMetaPath, Set(HoodieTableConfig.TYPE_SYSTEM_VERSION.key).asJava)
    assertFalse(tableConfig.usesNarrowIntAndCharTypes())
  }

  private def latestTableSchema: HoodieSchema = new TableSchemaResolver(loadMetaClient).getTableSchema(false)

  private def column(schema: HoodieSchema, name: String): HoodieSchema = {
    val field = schema.getField(name)
    assertTrue(field.isPresent, s"column $name in $schema")
    field.get().schema().getNonNullType
  }

  private def hiveTypes(schema: HoodieSchema): Seq[String] = {
    val hive = HiveSchemaUtil.convertSchemaToHiveSchema(schema, false).asScala.map { case (k, v) => k.replace("`", "") -> v }
    Seq("tiny", "small", "code", "name", "extra").map(c => hive(c).toLowerCase)
  }

  private def sparkTypes(schema: StructType): Seq[String] =
    Seq("tiny", "small", "code", "name", "extra").map { c =>
      CharVarcharUtils.getRawType(schema(c).metadata).getOrElse(schema(c).dataType).simpleString
    }
}
