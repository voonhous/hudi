/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.benchmarks.rowwriter;

import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.io.storage.row.HoodieRowParquetWriteSupport;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.spark.sql.avro.HoodieSparkSchemaConverters;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * The HoodieRow prototype when its call sites have seen four wrapper classes: before timing, the
 * same writer writes the workload as Avro records (AvroRowWrapper, AvroArrayWrapper) interleaved with
 * Spark rows (SparkRowWrapper, SparkArrayWrapper), so the JIT compiles it with a megamorphic type
 * profile. The timed part then writes Spark rows only, as in {@link ParquetWriteBenchmark#hoodieRow}.
 * Setup also checks that Spark rows and Avro records produce byte-identical files through the one writer.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 3)
@Fork(value = 2, jvmArgsAppend = {"-Xms4g", "-Xmx4g"})
public class ParquetWriteMegamorphicBenchmark {
  static final int ROWS = ParquetWriteBenchmark.ROWS;
  static final int POLLUTION_ROUNDS = 25;

  @Param({"DECIMAL", "STRING", "NESTED", "MIXED"})
  public Workloads.Kind workload;

  private Configuration conf;
  private InternalRow[] rows;
  private GenericRecord[] records;
  private HoodieRowParquetWriter writer;
  private SparkRowWrapper sparkWrapper;
  private AvroRowWrapper avroWrapper;

  @Setup(Level.Trial)
  public void setup() throws IOException {
    conf = new Configuration();
    StructType structType = Workloads.schema(workload);
    rows = Workloads.rows(workload, ROWS);
    HoodieSchema hoodieSchema = HoodieSparkSchemaConverters.toHoodieType(structType, false, "bench_record", "hoodie.bench", Metadata.empty());

    HoodieConfig config = new HoodieConfig();
    config.setValue("hoodie.avro.schema", hoodieSchema.toString());
    WriteSupport.WriteContext context = new HoodieRowParquetWriteSupport(conf, structType, Option.empty(), config).init(conf);
    writer = new HoodieRowParquetWriter(context.getSchema(), context.getExtraMetaData());
    sparkWrapper = SparkRowWrapper.of(structType);
    avroWrapper = AvroRowWrapper.of(hoodieSchema.toAvroSchema());

    scala.Function1<InternalRow, GenericRecord> toAvro = AvroConversionUtils.createInternalRowToAvroConverter(structType, hoodieSchema, false);
    records = new GenericRecord[ROWS];
    for (int i = 0; i < ROWS; i++) {
      records[i] = toAvro.apply(rows[i]);
    }

    byte[] fromSpark = writeSpark(new MemoryOutputFile(true)).bytes();
    byte[] fromAvro = writeAvro(new MemoryOutputFile(true)).bytes();
    if (!Arrays.equals(fromSpark, fromAvro)) {
      throw new IllegalStateException(String.format("%s: Spark and Avro files differ (%d vs %d bytes, first difference at %d)",
          workload, fromSpark.length, fromAvro.length, Arrays.mismatch(fromSpark, fromAvro)));
    }
    for (int round = 0; round < POLLUTION_ROUNDS; round++) {
      writeAvro(new MemoryOutputFile(false));
      writeSpark(new MemoryOutputFile(false));
    }
  }

  @Benchmark
  @OperationsPerInvocation(ROWS)
  public long hoodieRowMegamorphic() throws IOException {
    return writeSpark(new MemoryOutputFile(false)).size();
  }

  private MemoryOutputFile writeSpark(MemoryOutputFile file) throws IOException {
    try (ParquetWriter<HoodieRow> parquet = open(file)) {
      for (InternalRow row : rows) {
        parquet.write(sparkWrapper.replace(row));
      }
    }
    return file;
  }

  private MemoryOutputFile writeAvro(MemoryOutputFile file) throws IOException {
    try (ParquetWriter<HoodieRow> parquet = open(file)) {
      for (GenericRecord record : records) {
        parquet.write(avroWrapper.replace(record));
      }
    }
    return file;
  }

  private ParquetWriter<HoodieRow> open(OutputFile file) throws IOException {
    return new Builder(file, writer)
        .withConf(conf)
        .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
        .build();
  }

  private static final class Builder extends ParquetWriter.Builder<HoodieRow, Builder> {
    private final WriteSupport<HoodieRow> support;

    Builder(OutputFile file, WriteSupport<HoodieRow> support) {
      super(file);
      this.support = support;
    }

    @Override
    protected Builder self() {
      return this;
    }

    @Override
    protected WriteSupport<HoodieRow> getWriteSupport(Configuration configuration) {
      return support;
    }
  }
}
