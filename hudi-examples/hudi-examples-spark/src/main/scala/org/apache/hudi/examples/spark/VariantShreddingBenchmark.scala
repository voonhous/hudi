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

package org.apache.hudi.examples.spark

import org.apache.hudi.avro.VariantShreddingRuntime

import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.schema.{GroupType, LogicalTypeAnnotation, Type}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.SaveMode.Overwrite
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType}

import scala.collection.JavaConverters._

/**
 * Standalone spark-submit benchmark comparing Hudi's per-file variant shredding-schema
 * inference (branch feat-18937-variant-shredding-inference) against vanilla Spark 4.1 native
 * parquet variant shredding, on Spark 4.1.
 *
 * Both sides run over an identical cached DataFrame and, in the headline FAIR-INFER mode, both
 * INFER the typed_value schema via Spark's own InferVariantShreddingSchema engine (Hudi's
 * inferrer delegates to it), so the comparison isolates the write/read machinery rather than
 * schema differences. A FAIR-FORCED control mode pins one DDL schema on both sides instead.
 *
 * SIDE A (Hudi): bulk_insert through the InternalRow row-writer path with
 *   hoodie.parquet.variant.shredding.schema.inference.enabled=true, which wraps
 *   VariantShreddingInferenceInternalRowFileWriter.
 * SIDE B (Spark native): df.write.parquet with spark.sql.variant.writeShredding.enabled +
 *   inferShreddingSchema + allowReadingShredded.
 *
 * Metrics: write throughput, on-disk size (Hudi data-only vs total vs native), shredding
 * effectiveness (typed_value vs value/metadata bytes), read/reconstruct throughput, and
 * variant_get projection/filter latency.
 *
 * Build and run (Spark 4.1 only; the inferrer ships in hudi-spark4.1.x):
 *  <pre>
 *   1. For running in IDE, set VM options `-Dspark.master=local[*]`
 *   2. For running in shell, build with `-Pspark4.1` (JDK 17) and `spark-submit` against the
 *      hudi-spark4.1-bundle so Spark41VariantShreddingSchemaInferrer is on the classpath.
 *  </pre>
 * Args are `key=value` tokens (all optional):
 *   rows (default 5000000), fields (10), depth (1), cardinality (1000), emptyPct (0),
 *   parallelism (8), warmup (1), iters (3), out (file:///tmp/hudi-variant-bench),
 *   mode (infer|forced, default infer), ddl (forced-mode DDL, default "f0 int").
 */
object VariantShreddingBenchmark {

  private val tableName = "variant_bench"

  def main(args: Array[String]): Unit = {
    val argv = args.flatMap { a =>
      a.split("=", 2) match {
        case Array(k, v) => Some(k -> v)
        case _ => None
      }
    }.toMap
    def arg(k: String, d: String): String = argv.getOrElse(k, d)

    val rows = arg("rows", "5000000").toLong
    val fields = arg("fields", "10").toInt
    val depth = arg("depth", "1").toInt
    val cardinality = arg("cardinality", "1000").toLong
    val emptyPct = arg("emptyPct", "0").toInt
    val parallelism = arg("parallelism", "8").toInt
    val warmup = arg("warmup", "1").toInt
    val iters = arg("iters", "3").toInt
    val out = arg("out", "file:///tmp/hudi-variant-bench")
    val mode = arg("mode", "infer")
    val forcedDdl = arg("ddl", "f0 int")
    val codec = arg("codec", "zstd")

    require(mode == "infer" || mode == "forced", s"mode must be infer|forced, got $mode")

    val hudiPath = s"$out/hudi"
    val nativePath = s"$out/native"

    // Master comes from -Dspark.master (IDE) or spark-submit --master, matching the other
    // examples in this module; do not hardcode it so cluster submits are not overridden.
    val spark = SparkSession.builder()
      .appName("VariantShreddingBenchmark")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")
      .config("spark.kryoserializer.buffer.max", "512m")
      .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
      // Parity knobs shared by both sides.
      .config("spark.sql.parquet.enableVectorizedReader", "true")
      .config("spark.sql.parquet.compression.codec", codec)
      .getOrCreate()

    try {
      run(spark, rows, fields, depth, cardinality, emptyPct, parallelism, warmup, iters,
        hudiPath, nativePath, mode, forcedDdl, codec)
    } finally {
      spark.stop()
    }
  }

  private def run(spark: SparkSession, rows: Long, fields: Int, depth: Int, cardinality: Long,
                  emptyPct: Int, parallelism: Int, warmup: Int, iters: Int, hudiPath: String,
                  nativePath: String, mode: String, forcedDdl: String, codec: String): Unit = {
    val conf = spark.sparkContext.hadoopConfiguration

    // Fail fast: without an inferrer on the classpath Hudi silently writes unshredded and the
    // whole comparison is meaningless. (Only hudi-spark4.1.x ships one.)
    if (!VariantShreddingRuntime.lookupInferrer.isPresent) {
      throw new IllegalStateException(
        "No VariantShreddingSchemaInferrer on the classpath. Build with -Pspark4.1 and run "
          + "against the hudi-spark4.1-bundle so Spark41VariantShreddingSchemaInferrer is present.")
    }

    println(s"[config] rows=$rows fields=$fields depth=$depth cardinality=$cardinality "
      + s"emptyPct=$emptyPct parallelism=$parallelism warmup=$warmup iters=$iters mode=$mode "
      + s"codec=$codec")

    val df = buildDataset(spark, rows, fields, depth, cardinality, emptyPct, parallelism)
    df.persist()
    val n = df.count() // materialize so generation is excluded from timing
    println(s"[dataset] cached $n rows; sample variant: "
      + df.select(expr("cast(v as string)")).head().getString(0))

    // ---- WRITE phase (delete-before-each is untimed setup) ----
    val results = scala.collection.mutable.ArrayBuffer[Result]()
    results += timeit("write:hudi-inference", n, warmup, iters, () => cleanDir(spark, hudiPath))(
      () => writeHudi(df, hudiPath, parallelism, mode, forcedDdl, codec))
    results += timeit("write:spark-native", n, warmup, iters, () => cleanDir(spark, nativePath))(
      () => writeNative(spark, df, nativePath, parallelism, mode, forcedDdl))

    // ---- Validate shredding actually happened on BOTH sides before trusting numbers ----
    val hudiFiles = listParquet(spark, hudiPath, excludeHoodie = true)
    val nativeFiles = listParquet(spark, nativePath, excludeHoodie = false)
    require(hudiFiles.nonEmpty, s"no Hudi data parquet under $hudiPath")
    require(nativeFiles.nonEmpty, s"no native parquet under $nativePath")
    val hudiShredded = hudiFiles.forall(f => hasTypedValue(conf, f, "v"))
    val nativeShredded = nativeFiles.forall(f => hasTypedValue(conf, f, "v"))
    println(s"[shredded?] hudi=$hudiShredded (${hudiFiles.size} files)  "
      + s"native=$nativeShredded (${nativeFiles.size} files)")
    if (!hudiShredded || !nativeShredded) {
      println("[WARN] a side did not shred (no typed_value); size/read numbers below are NOT a "
        + "valid shredding comparison.")
    }

    // ---- Variant column parquet schema: verify both sides shred the SAME column identically ----
    val hudiVGroup = readVariantGroup(conf, hudiFiles.head, "v")
    val nativeVGroup = readVariantGroup(conf, nativeFiles.head, "v")
    println("\n-------------------- VARIANT COLUMN PARQUET SCHEMA --------------------")
    println(s"[hudi]   ${hudiFiles.head}")
    println(hudiVGroup.toString)
    println(s"[native] ${nativeFiles.head}")
    println(nativeVGroup.toString)
    val schemaDiffs = scala.collection.mutable.ArrayBuffer[String]()
    diffTypes("v", hudiVGroup, nativeVGroup, schemaDiffs)
    println("-------------------- SCHEMA DIFF (hudi vs native) --------------------")
    if (schemaDiffs.isEmpty) {
      println("[match] hudi and native variant column schemas are identical")
    } else {
      println(s"[MISMATCH] ${schemaDiffs.size} difference(s) (path: hudi vs native):")
      schemaDiffs.foreach(d => println("  - " + d))
    }

    // ---- SIZE + effectiveness ----
    val hudiDataBytes = parquetBytes(spark, hudiPath, excludeHoodie = true)
    val hudiTotalBytes = allBytes(spark, hudiPath)
    val nativeBytes = parquetBytes(spark, nativePath, excludeHoodie = false)
    val (hudiTyped, hudiUntyped) = shreddingBytes(conf, hudiFiles)
    val (nativeTyped, nativeUntyped) = shreddingBytes(conf, nativeFiles)

    // ---- READ phase: data already on disk from the last write iteration ----
    val readField = "f0" // first field is an int (see buildDataset)
    val filterK = (cardinality / 2).toString

    // Plans: confirm both sides prune to v.typed_value.<field>, and compare the scan. Native should
    // show a columnar parquet scan (a ColumnarToRow above FileScan parquet); Hudi shows a row-based
    // FileScan HudiFileGroup with no ColumnarToRow (vectorization is force-disabled for the variant
    // projection in HoodieFileGroupReaderBasedFileFormat.supportBatch).
    println("\n-------------------- READ PLAN: variant_get projection (hudi) --------------------")
    spark.read.format("hudi").load(hudiPath)
      .select(expr(s"variant_get(v, '$$.$readField', 'int')").as("g")).agg(sum("g")).explain("formatted")
    println("-------------------- READ PLAN: variant_get projection (native) --------------------")
    spark.read.parquet(nativePath)
      .select(expr(s"variant_get(v, '$$.$readField', 'int')").as("g")).agg(sum("g")).explain("formatted")

    Seq(
      ("read:hudi-full-reconstruct", () => readFull(spark.read.format("hudi").load(hudiPath))),
      ("read:native-full-reconstruct", () => readFull(spark.read.parquet(nativePath))),
      ("read:hudi-variant_get-proj", () => readProj(spark.read.format("hudi").load(hudiPath), readField)),
      ("read:native-variant_get-proj", () => readProj(spark.read.parquet(nativePath), readField)),
      ("read:hudi-variant_get-filter", () => readFilter(spark.read.format("hudi").load(hudiPath), readField, filterK)),
      ("read:native-variant_get-filter", () => readFilter(spark.read.parquet(nativePath), readField, filterK)),
      // Control: project a non-variant top-level column. If Hudi is also ~15% slower here, the gap
      // is general file-group-reader (row-based) overhead, not variant-specific.
      ("read:hudi-plain-proj-ts", () => readPlain(spark.read.format("hudi").load(hudiPath))),
      ("read:native-plain-proj-ts", () => readPlain(spark.read.parquet(nativePath)))
    ).foreach { case (name, body) => results += timeit(name, n, warmup, iters)(body) }

    // ---- REPORT ----
    println("\n==================== RESULTS ====================")
    println(f"${"case"}%-34s ${"best(ms)"}%12s ${"avg(ms)"}%12s ${"stdev"}%10s ${"rows/s"}%14s")
    results.foreach { r =>
      println(f"${r.name}%-34s ${r.bestMs}%12.2f ${r.avgMs}%12.2f ${r.stdevMs}%10.2f ${r.rowsPerSec}%14.0f")
    }
    val hudiVBytes = hudiTyped + hudiUntyped
    val nativeVBytes = nativeTyped + nativeUntyped
    println("\n-------------------- SIZE (data parquet) --------------------")
    println(f"hudi data-only : ${mb(hudiDataBytes)}%10.2f MB  (total incl .hoodie: ${mb(hudiTotalBytes)}%.2f MB)")
    println(f"spark native   : ${mb(nativeBytes)}%10.2f MB")
    println(f"hudi/native    : ${ratio(hudiDataBytes, nativeBytes)}%10.3f x (data-only vs native, incl Hudi meta-column tax)")
    println(f"variant column : hudi=${mb(hudiVBytes)}%8.2f MB  native=${mb(nativeVBytes)}%8.2f MB  "
      + f"ratio=${ratio(hudiVBytes, nativeVBytes)}%.3f x (v-only, normalizes out meta tax)")
    println("\n-------------------- SHREDDING EFFECTIVENESS --------------------")
    println(f"hudi   typed_value=${mb(hudiTyped)}%8.2f MB  value+metadata=${mb(hudiUntyped)}%8.2f MB  "
      + f"typed fraction=${frac(hudiTyped, hudiUntyped)}%6.3f")
    println(f"native typed_value=${mb(nativeTyped)}%8.2f MB  value+metadata=${mb(nativeUntyped)}%8.2f MB  "
      + f"typed fraction=${frac(nativeTyped, nativeUntyped)}%6.3f")
    println("================================================")
  }

  // ----------------------------------------------------------------------------------------
  // Dataset
  // ----------------------------------------------------------------------------------------

  /**
   * Builds id/ts plus a VARIANT column `v` from homogeneous, Avro-identifier-safe keys
   * (f0..f{fields-1}, plus an optional nested object). Keys must be Avro-safe because the Hudi
   * inferrer drops non-Avro keys while Spark-native does not, which would desync the two sides.
   * emptyPct percent of rows carry an empty object `{}` (inference declines these) to model a
   * realistic mixed-shape file.
   */
  private def buildDataset(spark: SparkSession, rows: Long, fields: Int, depth: Int,
                           cardinality: Long, emptyPct: Int, parallelism: Int): DataFrame = {
    val base = spark.range(0, rows).toDF("id")
      .withColumn("ts", (col("id") % 100000L) + lit(1000L))

    val typed: Seq[Column] = (0 until fields).map { i =>
      i % 3 match {
        case 0 => (col("id") + lit(i)).cast(IntegerType).as(s"f$i")
        case 1 => concat(lit("s"), (col("id") % cardinality)).as(s"f$i")
        case _ => ((col("id") % cardinality) * lit(1.5)).cast(DoubleType).as(s"f$i")
      }
    }
    val structFields: Seq[Column] =
      if (depth >= 1) typed :+ nestedObject(depth).as("nested") else typed

    val jsonStr = to_json(struct(structFields: _*))
    val vStr = when((col("id") % 100L) < lit(emptyPct), lit("{}")).otherwise(jsonStr)

    base.withColumn("v_json", vStr)
      .withColumn("v", expr("parse_json(v_json)"))
      .drop("v_json")
      .repartition(parallelism) // fix file fan-out identically for both sides
  }

  /** A nested object {c0:int, ...} wrapped `depth` levels deep so inference emits nested typed_value. */
  private def nestedObject(depth: Int): Column = {
    var c: Column = struct((col("id") % 7L).cast(IntegerType).as("c0"))
    for (_ <- 2 to depth) {
      c = struct(c.as("inner"))
    }
    c
  }

  // ----------------------------------------------------------------------------------------
  // Write paths
  // ----------------------------------------------------------------------------------------

  private def writeHudi(df: DataFrame, path: String, parallelism: Int, mode: String,
                        forcedDdl: String, codec: String): Unit = {
    var w = df.write.format("hudi")
      .option("hoodie.datasource.write.operation", "bulk_insert")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "ts")
      .option("hoodie.datasource.write.partitionpath.field", "")
      .option("hoodie.datasource.write.keygenerator.class",
        "org.apache.hudi.keygen.NonpartitionedKeyGenerator")
      .option("hoodie.table.name", tableName)
      .option("hoodie.datasource.write.table.type", "COPY_ON_WRITE")
      .option("hoodie.datasource.write.row.writer.enable", "true")
      .option("hoodie.bulkinsert.sort.mode", "NONE") // preserve input partitioning -> 1 file/partition
      .option("hoodie.bulkinsert.shuffle.parallelism", parallelism.toString)
      .option("hoodie.metadata.enable", "false")
      .option("hoodie.parquet.compression.codec", codec)
      .option("hoodie.parquet.variant.write.shredding.enabled", "true")
      .option("hoodie.parquet.variant.allow.reading.shredded", "true")
    w = if (mode == "infer") {
      w.option("hoodie.parquet.variant.shredding.schema.inference.enabled", "true")
    } else {
      // force wins over inference and makes getInferableVariantColumns empty: this exercises the
      // schema-driven shredding writer, NOT the inference decorator. Keep separate from infer mode.
      w.option("hoodie.parquet.variant.force.shredding.schema.for.test", forcedDdl)
    }
    w.mode(Overwrite).save(path)
  }

  private def writeNative(spark: SparkSession, df: DataFrame, path: String, parallelism: Int,
                          mode: String, forcedDdl: String): Unit = {
    spark.conf.set("spark.sql.variant.writeShredding.enabled", "true")
    spark.conf.set("spark.sql.variant.allowReadingShredded", "true")
    if (mode == "infer") {
      spark.conf.set("spark.sql.variant.inferShreddingSchema", "true")
      spark.conf.unset("spark.sql.variant.forceShreddingSchemaForTest")
    } else {
      spark.conf.set("spark.sql.variant.forceShreddingSchemaForTest", forcedDdl)
      spark.conf.set("spark.sql.variant.inferShreddingSchema", "false")
    }
    // df is already cached at `parallelism` partitions (see buildDataset); select is narrow so
    // it preserves that. No extra repartition here -> no shuffle that the Hudi side would not pay.
    df.select("id", "ts", "v").write.mode(Overwrite).parquet(path)
  }

  // ----------------------------------------------------------------------------------------
  // Read queries (each forces a column read via an aggregate so count() cannot prune it)
  // ----------------------------------------------------------------------------------------

  private def readFull(df: DataFrame): Unit = {
    df.select(length(expr("cast(v as string)")).as("len")).agg(max("len")).collect()
  }

  private def readProj(df: DataFrame, field: String): Unit = {
    df.select(expr(s"variant_get(v, '$$.$field', 'int')").as("g")).agg(sum("g")).collect()
  }

  private def readFilter(df: DataFrame, field: String, k: String): Unit = {
    df.where(expr(s"variant_get(v, '$$.$field', 'int') > $k")).count()
  }

  /** Control: project only a non-variant top-level column to isolate variant-specific read cost. */
  private def readPlain(df: DataFrame): Unit = {
    df.agg(sum("ts")).collect()
  }

  // ----------------------------------------------------------------------------------------
  // Timing
  // ----------------------------------------------------------------------------------------

  private case class Result(name: String, bestMs: Double, avgMs: Double, stdevMs: Double,
                            rowsPerSec: Double)

  private def timeit(name: String, rows: Long, warmup: Int, iters: Int,
                     setup: () => Unit = () => ())(body: () => Unit): Result = {
    for (_ <- 0 until warmup) {
      setup()
      body()
    }
    val times = (0 until iters).map { _ =>
      setup()
      System.gc()
      val t0 = System.nanoTime()
      body()
      (System.nanoTime() - t0) / 1e6
    }
    val best = times.min
    val avg = times.sum / times.size
    val variance = times.map(t => (t - avg) * (t - avg)).sum / times.size
    val stdev = math.sqrt(variance)
    val rps = if (best > 0) rows / (best / 1000.0) else 0.0
    Result(name, best, avg, stdev, rps)
  }

  // ----------------------------------------------------------------------------------------
  // Filesystem + parquet footer inspection
  // ----------------------------------------------------------------------------------------

  private def cleanDir(spark: SparkSession, p: String): Unit = {
    val path = new Path(p)
    val fs = path.getFileSystem(spark.sparkContext.hadoopConfiguration)
    if (fs.exists(path)) fs.delete(path, true)
  }

  private def listParquet(spark: SparkSession, p: String, excludeHoodie: Boolean): Seq[String] = {
    val path = new Path(p)
    val fs = path.getFileSystem(spark.sparkContext.hadoopConfiguration)
    if (!fs.exists(path)) return Seq.empty
    val it = fs.listFiles(path, true)
    val files = scala.collection.mutable.ArrayBuffer[String]()
    while (it.hasNext) {
      val s = it.next().getPath.toString
      if (s.endsWith(".parquet") && (!excludeHoodie || !s.contains(".hoodie"))) files += s
    }
    files.toSeq
  }

  private def parquetBytes(spark: SparkSession, p: String, excludeHoodie: Boolean): Long = {
    val path = new Path(p)
    val fs = path.getFileSystem(spark.sparkContext.hadoopConfiguration)
    if (!fs.exists(path)) return 0L
    val it = fs.listFiles(path, true)
    var total = 0L
    while (it.hasNext) {
      val f = it.next()
      val s = f.getPath.toString
      if (s.endsWith(".parquet") && (!excludeHoodie || !s.contains(".hoodie"))) total += f.getLen
    }
    total
  }

  private def allBytes(spark: SparkSession, p: String): Long = {
    val path = new Path(p)
    val fs = path.getFileSystem(spark.sparkContext.hadoopConfiguration)
    if (!fs.exists(path)) return 0L
    val it = fs.listFiles(path, true)
    var total = 0L
    while (it.hasNext) total += it.next().getLen
    total
  }

  private def hasTypedValue(conf: org.apache.hadoop.conf.Configuration, filePath: String,
                            variantField: String): Boolean = {
    val reader = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(filePath), conf))
    try {
      val schema = reader.getFooter.getFileMetaData.getSchema
      val idx = schema.getFieldIndex(variantField)
      schema.getType(idx).asGroupType().containsField("typed_value")
    } finally {
      reader.close()
    }
  }

  /** The variant column's parquet GroupType (metadata/value/typed_value...). */
  private def readVariantGroup(conf: org.apache.hadoop.conf.Configuration, filePath: String,
                               variantField: String): GroupType = {
    val reader = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(filePath), conf))
    try {
      val schema = reader.getFooter.getFileMetaData.getSchema
      schema.getType(schema.getFieldIndex(variantField)).asGroupType()
    } finally {
      reader.close()
    }
  }

  /**
   * Records every difference between the hudi and native variant parquet types (recursively by
   * field name): repetition, logical-type annotation (catches a missing VARIANT(1)), physical
   * type and byte length (catches decimal int64 vs fixed_len_byte_array), and missing/extra fields.
   */
  private def diffTypes(path: String, hudi: Type, native: Type,
                        diffs: scala.collection.mutable.ArrayBuffer[String]): Unit = {
    if (hudi.getRepetition != native.getRepetition) {
      diffs += s"$path: repetition ${hudi.getRepetition} vs ${native.getRepetition}"
    }
    if (hudi.getLogicalTypeAnnotation != native.getLogicalTypeAnnotation) {
      diffs += s"$path: logical-type ${annot(hudi.getLogicalTypeAnnotation)} vs ${annot(native.getLogicalTypeAnnotation)}"
    }
    if (hudi.isPrimitive && native.isPrimitive) {
      val ph = hudi.asPrimitiveType
      val pn = native.asPrimitiveType
      if (ph.getPrimitiveTypeName != pn.getPrimitiveTypeName) {
        diffs += s"$path: physical-type ${ph.getPrimitiveTypeName} vs ${pn.getPrimitiveTypeName}"
      } else if (ph.getTypeLength != pn.getTypeLength) {
        diffs += s"$path: byte-length ${ph.getTypeLength} vs ${pn.getTypeLength}"
      }
    } else if (!hudi.isPrimitive && !native.isPrimitive) {
      val gh = hudi.asGroupType
      val gn = native.asGroupType
      val hNames = gh.getFields.asScala.map(_.getName).toList
      val nNames = gn.getFields.asScala.map(_.getName).toList
      hNames.filterNot(nNames.contains).foreach(n => diffs += s"$path.$n: only in hudi")
      nNames.filterNot(hNames.contains).foreach(n => diffs += s"$path.$n: only in native")
      hNames.filter(nNames.contains).foreach(n => diffTypes(s"$path.$n", gh.getType(n), gn.getType(n), diffs))
    } else {
      diffs += s"$path: primitive-vs-group mismatch (hudi primitive=${hudi.isPrimitive})"
    }
  }

  private def annot(l: LogicalTypeAnnotation): String = if (l == null) "none" else l.toString

  /** Sums compressed column-chunk bytes split into (typed_value, value+metadata) across files. */
  private def shreddingBytes(conf: org.apache.hadoop.conf.Configuration,
                             files: Seq[String]): (Long, Long) = {
    var typed = 0L
    var untyped = 0L
    files.foreach { filePath =>
      val reader = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(filePath), conf))
      try {
        reader.getFooter.getBlocks.asScala.foreach { block =>
          block.getColumns.asScala.foreach { col =>
            val dot = col.getPath.toDotString
            if (dot.contains("typed_value")) {
              typed += col.getTotalSize
            } else if (dot.endsWith(".value") || dot.endsWith(".metadata")) {
              untyped += col.getTotalSize
            }
          }
        }
      } finally {
        reader.close()
      }
    }
    (typed, untyped)
  }

  private def mb(bytes: Long): Double = bytes / (1024.0 * 1024.0)
  private def ratio(a: Long, b: Long): Double = if (b == 0) 0.0 else a.toDouble / b.toDouble
  private def frac(typed: Long, untyped: Long): Double = {
    val sum = typed + untyped
    if (sum == 0) 0.0 else typed.toDouble / sum.toDouble
  }
}
