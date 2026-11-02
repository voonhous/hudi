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
import org.apache.hudi.HoodieSchemaConversionUtils
import org.apache.hudi.client.HoodieJavaWriteClient
import org.apache.hudi.client.common.HoodieJavaEngineContext
import org.apache.hudi.common.engine.EngineType
import org.apache.hudi.common.model.{HoodieAvroIndexedRecord, HoodieKey, HoodieRecord, HoodieTableType}
import org.apache.hudi.common.schema.{HoodieSchema, HoodieSchemaField, HoodieSchemaType}
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.testutils.HoodieTestUtils
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.testutils.HoodieSparkClientTestBase

import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.types.{ByteType, CharType, ShortType, StringType, StructType}
import org.junit.jupiter.api.{AfterEach, BeforeEach}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{CsvSource, EnumSource}

import java.util.Properties

import scala.collection.JavaConverters._

/**
 * End to end: a table whose HoodieSchema has INT8, INT16 and CHAR(10) columns, written by the Java write client (Avro
 * records) or by the Spark data source, reads back through the DataFrame reader and Spark SQL as ByteType, ShortType
 * and CHAR(10), with the values written (CHAR values padded to the length, Spark's CHAR semantics), for COW and MOR.
 */
class TestNarrowIntAndCharTypes extends HoodieSparkClientTestBase {

  var spark: SparkSession = _

  private val schema: HoodieSchema = HoodieSchema.createRecord("narrow_record", "hoodie.narrow", null, java.util.Arrays.asList(
    HoodieSchemaField.of("key", HoodieSchema.create(HoodieSchemaType.STRING)),
    HoodieSchemaField.of("part", HoodieSchema.create(HoodieSchemaType.STRING)),
    HoodieSchemaField.of("ts", HoodieSchema.create(HoodieSchemaType.LONG)),
    HoodieSchemaField.of("tiny", HoodieSchema.createInt8()),
    HoodieSchemaField.of("small", HoodieSchema.createNullable(HoodieSchema.createInt16()), null, HoodieSchema.NULL_VALUE),
    HoodieSchemaField.of("code", HoodieSchema.createChar(10))))

  // (key, tiny, small, code) as written; the upsert of k1 replaces the first row
  private val inserts = Seq(("k1", -128, Some(32767), "abcdefghij"), ("k2", 127, Some(-32768), "abc"), ("k3", 0, None, "x"))
  private val update = ("k1", 5, Some(-5), "0123456789")

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
  def testJavaClientWriteSparkRead(tableType: HoodieTableType): Unit = {
    val metaClient = HoodieTestUtils.init(storageConf, basePath, tableType, HoodieTableMetaClient.newTableBuilder()
      .setTableType(tableType)
      .setTableName("narrow")
      .setRecordKeyFields("key")
      .setPartitionFields("part")
      .setOrderingFields("ts")
      .build())
    val props = Map(
      RECORDKEY_FIELD.key -> "key",
      PARTITIONPATH_FIELD.key -> "part",
      HoodieTableConfig.ORDERING_FIELDS.key -> "ts",
      HoodieWriteConfig.TBL_NAME.key -> "narrow",
      "hoodie.insert.shuffle.parallelism" -> "1",
      "hoodie.upsert.shuffle.parallelism" -> "1")
    val writeConfig = HoodieWriteConfig.newBuilder()
      .withEngineType(EngineType.JAVA)
      .withPath(basePath)
      .withSchema(schema.toString)
      .withProps(props.asJava)
      .build()
    val client = new HoodieJavaWriteClient(new HoodieJavaEngineContext(storageConf), writeConfig)
    try {
      var instant = client.startCommit()
      client.commit(instant, client.insert(inserts.map(r => avroRecord(r, 1L)).asJava, instant))
      instant = client.startCommit()
      client.commit(instant, client.upsert(Seq(avroRecord(update, 2L)).asJava, instant))
    } finally {
      client.close()
    }
    metaClient.reloadActiveTimeline()
    if (tableType == HoodieTableType.MERGE_ON_READ) {
      assertTrue(hasLogFiles, "the upsert of a MOR table goes to a log file")
    }

    assertTableSchema()
    assertReadsBack()
  }

  @ParameterizedTest
  @CsvSource(Array("COPY_ON_WRITE,upsert", "MERGE_ON_READ,upsert", "COPY_ON_WRITE,bulk_insert", "MERGE_ON_READ,bulk_insert"))
  def testSparkWriteSparkRead(tableType: HoodieTableType, firstOperation: String): Unit = {
    // the Spark schema of the table schema: ByteType, ShortType and CHAR(10), with the markers that keep the types
    val structType = HoodieSchemaConversionUtils.convertHoodieSchemaToStructType(schema)
    write(df(structType, inserts), tableType, firstOperation, SaveMode.Overwrite)
    write(df(structType, Seq(update)), tableType, UPSERT_OPERATION_OPT_VAL, SaveMode.Append)
    if (tableType == HoodieTableType.MERGE_ON_READ) {
      assertTrue(hasLogFiles, "the upsert of a MOR table goes to a log file")
    }
    if (firstOperation == BULK_INSERT_OPERATION_OPT_VAL) {
      // the row writer writes INT32 + INT(8) / INT(16), as Parquet asks for TINYINT / SMALLINT
      val parquetFiles = storage.listFiles(new org.apache.hudi.storage.StoragePath(basePath)).asScala
        .map(_.getPath.toString).filter(p => p.endsWith(".parquet") && !p.contains("/.hoodie/"))
      assertFalse(parquetFiles.isEmpty)
      val footer = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(parquetFiles.head), storageConf.unwrap()))
      try {
        val parquetSchema = footer.getFileMetaData.getSchema
        assertEquals(LogicalTypeAnnotation.intType(8, true),
          parquetSchema.getType(parquetSchema.getFieldIndex("tiny")).getLogicalTypeAnnotation)
        assertEquals(LogicalTypeAnnotation.intType(16, true),
          parquetSchema.getType(parquetSchema.getFieldIndex("small")).getLogicalTypeAnnotation)
      } finally {
        footer.close()
      }
    }

    assertTableSchema()
    assertReadsBack()
  }

  private def avroRecord(row: (String, Int, Option[Int], String), ts: Long): HoodieRecord[Nothing] = {
    val record: GenericRecord = new GenericData.Record(schema.toAvroSchema)
    record.put("key", row._1)
    record.put("part", "p1")
    record.put("ts", ts)
    record.put("tiny", row._2)
    record.put("small", row._3.map(Integer.valueOf).orNull)
    record.put("code", row._4)
    new HoodieAvroIndexedRecord(new HoodieKey(row._1, "p1"), record).asInstanceOf[HoodieRecord[Nothing]]
  }

  private def df(structType: StructType, rows: Seq[(String, Int, Option[Int], String)]): DataFrame = {
    val ts = if (rows.size == 1) 2L else 1L
    val data = rows.map(r => Row(r._1, "p1", ts, r._2.toByte, r._3.map(v => java.lang.Short.valueOf(v.toShort)).orNull, r._4))
    spark.createDataFrame(spark.sparkContext.parallelize(data, 1), structType)
  }

  private def write(df: DataFrame, tableType: HoodieTableType, operation: String, mode: SaveMode): Unit = {
    df.write.format("hudi")
      .option(RECORDKEY_FIELD.key, "key")
      .option(PARTITIONPATH_FIELD.key, "part")
      .option(HoodieTableConfig.ORDERING_FIELDS.key, "ts")
      .option(TABLE_NAME.key, "narrow")
      .option(TABLE_TYPE.key, tableType.name)
      .option(OPERATION.key, operation)
      .option("hoodie.bulkinsert.shuffle.parallelism", "1")
      .option("hoodie.upsert.shuffle.parallelism", "1")
      .mode(mode)
      .save(basePath)
  }

  private def hasLogFiles: Boolean =
    storage.listFiles(new org.apache.hudi.storage.StoragePath(basePath)).asScala
      .exists(f => f.getPath.getName.contains(".log.") && !f.getPath.toString.contains("/.hoodie/"))

  private def assertTableSchema(): Unit = {
    val metaClient = HoodieTableMetaClient.builder().setConf(storageConf).setBasePath(basePath).build()
    val tableSchema = new TableSchemaResolver(metaClient).getTableSchema(false)
    assertEquals(HoodieSchemaType.INT8, tableSchema.getField("tiny").get().schema().getNonNullType.getType)
    assertEquals(HoodieSchemaType.INT16, tableSchema.getField("small").get().schema().getNonNullType.getType)
    assertEquals(HoodieSchema.createChar(10), tableSchema.getField("code").get().schema().getNonNullType)
  }

  private def assertReadsBack(): Unit = {
    val read = spark.read.format("hudi").load(basePath)
    assertEquals(ByteType, read.schema("tiny").dataType)
    assertEquals(ShortType, read.schema("small").dataType)
    assertEquals(StringType, read.schema("code").dataType)
    assertEquals(Some(CharType(10)), CharVarcharUtils.getRawType(read.schema("code").metadata))

    val expected = (update +: inserts.tail).sortBy(_._1)
    val rows = read.select("key", "tiny", "small", "code").orderBy("key").collect().toSeq
    assertEquals(expected.size, rows.size)
    expected.zip(rows).foreach { case ((key, tiny, small, code), row) =>
      assertEquals(key, row.getString(0))
      assertEquals(java.lang.Byte.valueOf(tiny.toByte), row.get(1))
      assertEquals(small.map(v => java.lang.Short.valueOf(v.toShort)).orNull, row.get(2))
      // Spark reads CHAR(10) values padded to the length
      assertEquals(code.padTo(10, ' '), row.getString(3))
    }

    read.createOrReplaceTempView("narrow_view")
    assertEquals(Seq("k2"), keys("select key from narrow_view where tiny = 127"))
    assertEquals(Seq("k1", "k2"), keys("select key from narrow_view where small < 0"))
    assertEquals(Seq("k3"), keys("select key from narrow_view where small is null"))
    // CHAR comparison pads the shorter side, so the value matches with or without its trailing spaces
    assertEquals(Seq("k2"), keys("select key from narrow_view where code = 'abc'"))
    assertEquals(Seq("k2"), keys("select key from narrow_view where code = 'abc   '"))
    assertEquals(Seq("k1"), keys("select key from narrow_view where code = '0123456789'"))
    assertEquals("tinyint,smallint,char(10)", spark.sql("select tiny, small, code from narrow_view").schema.fields
      .map(f => CharVarcharUtils.getRawType(f.metadata).getOrElse(f.dataType).simpleString).mkString(","))
  }

  private def keys(sql: String): Seq[String] =
    spark.sql(sql).collect().map(_.getString(0)).sorted.toSeq
}
