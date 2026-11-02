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

import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions, HoodieFileIndex}
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.metadata.planner.IndexPlannerComparison

import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.sql.catalyst.expressions.{And, Expression}
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource

import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate}

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * The expression oracle of RFC-88 for Spark: `hoodie.metadata.index.planner.mode` ON returns the rows of a read
 * without data skipping, COMPARE returns the rows of OFF and finds no file only the planner prunes, and ON prunes
 * at least as much as OFF.
 *
 * The table has the metadata table with column stats, partition stats and the record index, and several commits
 * (inserts, updates, deletes; log files for MOR) whose value ranges differ by commit and partition, so that every
 * index has something to prune.
 */
class TestIndexPlannerOracle extends HoodieStatsIndexTestBase {

  private val schema = StructType(Seq(
    StructField("id", StringType, nullable = false),
    StructField("part", StringType, nullable = false),
    StructField("ts", LongType, nullable = false),
    StructField("i", IntegerType, nullable = true),
    StructField("l", LongType, nullable = true),
    StructField("d", DoubleType, nullable = true),
    StructField("dec", DecimalType(10, 2), nullable = true),
    StructField("dt", DateType, nullable = true),
    StructField("tstamp", TimestampType, nullable = true),
    StructField("s", StringType, nullable = true),
    StructField("n", IntegerType, nullable = true)))

  private val dataColumns = schema.fieldNames.toSeq

  /**
   * The queries: equality, ranges, IN, IS NULL, NOT, OR, string prefix, dates, timestamps, decimals, record keys and
   * partition filters, on values of base files and of log files (updated rows), and on deleted rows.
   */
  private val queries = Seq(
    "i = 20005",
    "i = 10005",
    "i > 10100 AND i < 10200",
    "i IN (3, 20005, 10301)",
    "n IS NULL",
    "n IS NOT NULL",
    "NOT (i < 20000)",
    "i < 100 OR i > 20400",
    "s LIKE 'beta%'",
    "NOT (s LIKE 'alpha%')",
    "s = 'beta-0010'",
    "s > 'alpha-0400'",
    "dt = DATE'2024-02-15'",
    "dt >= DATE'2024-12-01'",
    "tstamp < TIMESTAMP'2024-01-02 00:00:00'",
    "tstamp >= TIMESTAMP'2024-01-18 00:00:00'",
    "dec > 900",
    "dec BETWEEN 10.00 AND 20.00",
    "d > 40.5",
    "l BETWEEN 100000 AND 120000",
    "ts = 1",
    "id = 'id-0005'",
    "id IN ('id-0005', 'id-0200', 'id-0420')",
    "_hoodie_record_key = 'id-0160'",
    "id = 'id-0035'",
    "id = 'id-0005' AND i > 0",
    "id = 'id-0310' OR i = 5",
    "part = 'p1' AND i > 10100",
    "part = 'p2' AND n IS NULL",
    "i = -1",
    "i > 10000 AND n IS NULL",
    "NOT (n IS NULL) AND i < 150")

  /**
   * Queries whose rows today's Spark data skipping (OFF) gets wrong on MOR, where the planner is right.
   *
   * `s LIKE 'beta%'`: `DataSkippingUtils` translates `StartsWith(s, 'beta')` to `min(s) <= 'beta' AND max(s) >= 'beta'`,
   * the condition for `s = 'beta'`. A file whose values all start with the prefix but are greater than it (the log
   * files of the update, with only 'beta-00xx' values) is pruned, and its rows are lost. On COW the updated base files
   * also hold 'alpha-' values, so their minimum is below the prefix and they are kept.
   */
  private val engineOverPrunesOnMor = Set("s LIKE 'beta%'")

  @ParameterizedTest
  @CsvSource(Array("COPY_ON_WRITE,true", "MERGE_ON_READ,true", "MERGE_ON_READ,false"))
  def testIndexPlannerModes(tableType: HoodieTableType, globalRecordIndex: Boolean): Unit = {
    val writeOpts = writeOptions(tableType, globalRecordIndex)
    write(writeOpts, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL, SaveMode.Overwrite, (0 until 150).map(r => row(r, 0)))
    write(writeOpts, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL, SaveMode.Append, (150 until 300).map(r => row(r, 0)))
    write(writeOpts, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL, SaveMode.Append, ((0 until 30) ++ (150 until 170)).map(r => row(r, 1)))
    write(writeOpts, DataSourceWriteOptions.DELETE_OPERATION_OPT_VAL, SaveMode.Append, (30 until 40).map(r => row(r, 2)))
    write(writeOpts, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL, SaveMode.Append, (300 until 450).map(r => row(r, 0)))
    metaClient = HoodieTableMetaClient.reload(metaClient)
    assertTrue(metaClient.getTableConfig.getMetadataPartitions.containsAll(
      Seq("column_stats", "partition_stats", "record_index").asJava), metaClient.getTableConfig.getMetadataPartitions.toString)

    val results = queries.map(query => runQuery(query))
    println(s"INDEX_PLANNER_ORACLE $tableType globalRecordIndex=$globalRecordIndex")
    println("INDEX_PLANNER_ORACLE | query | rows | files no skipping | OFF | COMPARE | ON | COMPARE: only planner / only engine |")
    results.foreach(r => println(s"INDEX_PLANNER_ORACLE | ${r.query} | ${r.rows} | ${r.filesNoSkipping} | ${r.filesOff} | ${r.filesCompare} | ${r.filesOn} | " +
      s"${r.comparisons.map { case (stage, c) => s"$stage: ${c.prunedOnlyByPlanner.size} / ${c.prunedOnlyByEngine.size}" }.mkString(", ")} |"))

    val failures = mutable.ListBuffer[String]()
    def check(condition: Boolean, message: => String): Unit = if (!condition) failures += message
    results.foreach { r =>
      val offOverPrunes = tableType == HoodieTableType.MERGE_ON_READ && engineOverPrunesOnMor.contains(r.query)
      // 1. ON reads the rows of a read without data skipping; OFF too, but where today's Spark over-prunes
      check(r.rowsOnMatch, s"rows with ON differ from rows without data skipping: ${r.query}")
      check(r.rowsOffMatch != offOverPrunes, s"rows with OFF ${if (offOverPrunes) "no longer differ" else "differ"} from rows without data skipping: ${r.query}")
      check(r.rowsCompareMatch, s"rows with COMPARE differ from rows with OFF: ${r.query}")
      // 2. COMPARE reads what OFF reads, and the planner prunes no file OFF keeps
      check(r.filesOff == r.filesCompare, s"COMPARE reads ${r.filesCompare} files, OFF ${r.filesOff}: ${r.query}")
      r.comparisons.foreach { case (stage, comparison) =>
        check(comparison.prunedOnlyByPlanner.isEmpty, s"$stage of ${r.query}: only the planner prunes ${comparison.prunedOnlyByPlanner}")
      }
      // 3. ON prunes at least as much as OFF, where OFF is right
      check(offOverPrunes || r.filesOn <= r.filesOff, s"ON reads ${r.filesOn} files, OFF ${r.filesOff}: ${r.query}")
      check(r.filesOn <= r.filesNoSkipping, s"ON reads ${r.filesOn} files, ${r.filesNoSkipping} without data skipping: ${r.query}")
    }
    assertTrue(failures.isEmpty, failures.mkString("\n"))
    // the indexes have something to prune
    assertTrue(results.count(r => r.filesOn < r.filesNoSkipping) > queries.size / 2)

    // the query types the record index does not serve: time travel to the second commit, and read optimized
    val secondCommit = metaClient.getCommitsTimeline.filterCompletedInstants.getInstants.get(1).requestedTime
    val otherQueryTypes = Seq(Map(DataSourceReadOptions.TIME_TRAVEL_AS_OF_INSTANT.key -> secondCommit)) ++
      (if (tableType == HoodieTableType.MERGE_ON_READ) {
        Seq(Map(DataSourceReadOptions.QUERY_TYPE.key -> DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL))
      } else {
        Seq.empty
      })
    for (queryType <- otherQueryTypes; query <- Seq("id = 'id-0005'", "id IN ('id-0005', 'id-0200')", "i > 10100 AND i < 10200", "n IS NULL")) {
      val expected = rows(readOptions(None) ++ queryType, query)
      Seq("OFF", "COMPARE", "ON").foreach { mode =>
        assertEquals(expected, rows(readOptions(Some(mode)) ++ queryType, query), s"$mode, $queryType: $query")
      }
    }
  }

  private case class QueryResult(query: String, rows: Int, rowsOnMatch: Boolean, rowsOffMatch: Boolean, rowsCompareMatch: Boolean,
                                 filesNoSkipping: Int, filesOff: Int, filesCompare: Int, filesOn: Int,
                                 comparisons: Seq[(String, IndexPlannerComparison)])

  private def runQuery(query: String): QueryResult = {
    val noSkipping = readOptions(None)
    val rowsNoSkipping = rows(noSkipping, query)
    val rowsOff = rows(readOptions(Some("OFF")), query)
    val rowsCompare = rows(readOptions(Some("COMPARE")), query)
    val rowsOn = rows(readOptions(Some("ON")), query)

    val (partitionFilters, dataFilters) = filters(query)
    val (filesNoSkipping, _) = listFiles(noSkipping, partitionFilters, dataFilters)
    val (filesOff, _) = listFiles(readOptions(Some("OFF")), partitionFilters, dataFilters)
    val (filesCompare, comparisons) = listFiles(readOptions(Some("COMPARE")), partitionFilters, dataFilters)
    val (filesOn, _) = listFiles(readOptions(Some("ON")), partitionFilters, dataFilters)
    QueryResult(query, rowsNoSkipping.size, rowsOn == rowsNoSkipping, rowsOff == rowsNoSkipping, rowsCompare == rowsOff,
      filesNoSkipping, filesOff, filesCompare, filesOn, comparisons)
  }

  private def readOptions(plannerMode: Option[String]): Map[String, String] = {
    Map(HoodieMetadataConfig.ENABLE.key -> "true") ++ (plannerMode match {
      case Some(mode) => Map(DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "true", HoodieMetadataConfig.INDEX_PLANNER_MODE.key -> mode)
      case None => Map(DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "false")
    })
  }

  private def read(opts: Map[String, String], query: String): DataFrame =
    spark.read.format("hudi").options(opts).load(basePath).where(query)

  private def rows(opts: Map[String, String], query: String): Seq[String] =
    read(opts, query).select(dataColumns.map(col): _*).collect().map(_.toString).toSeq.sorted

  /**
   * The partition and data filters Spark gives the file index for the query: the conjuncts of the optimized filter,
   * split by whether they reference the partition column only.
   */
  private def filters(query: String): (Seq[Expression], Seq[Expression]) = {
    val conjuncts = read(readOptions(None), query).queryExecution.optimizedPlan.collect { case f: Filter => f.condition }
      .flatMap(splitConjuncts)
    conjuncts.partition(_.references.forall(_.name == "part"))
  }

  private def splitConjuncts(expr: Expression): Seq[Expression] = expr match {
    case And(left, right) => splitConjuncts(left) ++ splitConjuncts(right)
    case other => Seq(other)
  }

  private def listFiles(opts: Map[String, String], partitionFilters: Seq[Expression],
                        dataFilters: Seq[Expression]): (Int, Seq[(String, IndexPlannerComparison)]) = {
    val fileIndex = HoodieFileIndex(spark, HoodieTableMetaClient.reload(metaClient), None, opts + ("path" -> basePath), includeLogFiles = true)
    val files = fileIndex.listFiles(partitionFilters, dataFilters).flatMap(_.files)
    (files.size, fileIndex.indexPlannerComparisons.toList)
  }

  private def writeOptions(tableType: HoodieTableType, globalRecordIndex: Boolean): Map[String, String] = Map(
    HoodieWriteConfig.TBL_NAME.key -> "index_planner_oracle",
    DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name,
    DataSourceWriteOptions.RECORDKEY_FIELD.key -> "id",
    DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "part",
    HoodieTableConfig.ORDERING_FIELDS.key -> "ts",
    "hoodie.insert.shuffle.parallelism" -> "2",
    "hoodie.upsert.shuffle.parallelism" -> "2",
    "hoodie.delete.shuffle.parallelism" -> "2",
    // a new file group per partition and insert, so that the stats differ by file
    "hoodie.parquet.small.file.limit" -> "0",
    "hoodie.compact.inline" -> "false",
    HoodieMetadataConfig.ENABLE.key -> "true",
    HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "true",
    HoodieMetadataConfig.COLUMN_STATS_INDEX_FOR_COLUMNS.key -> dataColumns.mkString(","),
    HoodieMetadataConfig.ENABLE_METADATA_INDEX_PARTITION_STATS.key -> "true",
    (if (globalRecordIndex) HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP.key
    else HoodieMetadataConfig.RECORD_LEVEL_INDEX_ENABLE_PROP.key) -> "true")

  private def write(opts: Map[String, String], operation: String, mode: SaveMode, rows: Seq[Row]): Unit = {
    spark.createDataFrame(rows.asJava, schema).write.format("hudi")
      .options(opts)
      .option(DataSourceWriteOptions.OPERATION.key, operation)
      .mode(mode)
      .save(basePath)
  }

  /**
   * Row `r` of the table: `i` ranges differ by partition, the others by commit; `n` is null in the second insert;
   * version 1 (an update) changes `s`, `dec` and `n`.
   */
  private def row(r: Int, version: Int): Row = {
    val partition = r % 3
    Row(
      f"id-$r%04d",
      s"p$partition",
      version.toLong,
      partition * 10000 + r,
      r.toLong * 1000L,
      r / 10.0,
      new java.math.BigDecimal(r * 25 + (if (version == 1) 100000 else 0)).movePointLeft(2),
      Date.valueOf(LocalDate.of(2024, 1, 1).plusDays(r)),
      Timestamp.from(Instant.parse("2024-01-01T00:00:00Z").plusSeconds(r * 3600L)),
      if (version == 1) f"beta-$r%04d" else f"alpha-$r%04d",
      if (version == 0 && r >= 150 && r < 300) null else Integer.valueOf(r))
  }
}
