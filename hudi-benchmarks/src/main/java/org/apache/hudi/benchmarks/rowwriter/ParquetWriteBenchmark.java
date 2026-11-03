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

import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.io.storage.row.HoodieRowParquetWriteSupport;

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
 * Spark rows to Parquet: today's {@link HoodieRowParquetWriteSupport} against the RFC-88 prototype
 * ({@link SparkRowWrapper} read by one {@link HoodieRowParquetWriter}). Both use the same physical
 * schema and footer metadata; setup checks the two files are byte-identical before timing.
 * Scores are per row.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 3)
@Fork(value = 2, jvmArgsAppend = {"-Xms4g", "-Xmx4g"})
public class ParquetWriteBenchmark {
  static final int ROWS = 20_000;

  @Param({"DECIMAL", "STRING", "NESTED", "MIXED"})
  public Workloads.Kind workload;

  private Configuration conf;
  private InternalRow[] rows;
  private HoodieRowParquetWriteSupport baselineSupport;
  private HoodieRowParquetWriter prototypeSupport;
  private HoodieRowParquetWriterMatched matchedSupport;
  private SparkRowWrapper wrapper;

  @Setup(Level.Trial)
  public void setup() throws IOException {
    conf = new Configuration();
    StructType structType = Workloads.schema(workload);
    rows = Workloads.rows(workload, ROWS);

    HoodieConfig config = new HoodieConfig();
    config.setValue("hoodie.avro.schema",
        HoodieSparkSchemaConverters.toHoodieType(structType, false, "bench_record", "hoodie.bench", Metadata.empty()).toString());
    baselineSupport = new HoodieRowParquetWriteSupport(conf, structType, Option.empty(), config);
    WriteSupport.WriteContext context = baselineSupport.init(conf);
    prototypeSupport = new HoodieRowParquetWriter(context.getSchema(), context.getExtraMetaData());
    matchedSupport = new HoodieRowParquetWriterMatched(context.getSchema(), context.getExtraMetaData());
    wrapper = SparkRowWrapper.of(structType);

    byte[] expected = writeBaseline(new MemoryOutputFile(true)).bytes();
    byte[] actual = writePrototype(new MemoryOutputFile(true), prototypeSupport).bytes();
    if (!Arrays.equals(expected, actual)) {
      throw new IllegalStateException(String.format("%s: files differ (baseline %d bytes, prototype %d bytes, first difference at %d)",
          workload, expected.length, actual.length, Arrays.mismatch(expected, actual)));
    }
    byte[] matched = writePrototype(new MemoryOutputFile(true), matchedSupport).bytes();
    if (!Arrays.equals(expected, matched)) {
      throw new IllegalStateException(String.format("%s: files differ (baseline %d bytes, matched %d bytes, first difference at %d)",
          workload, expected.length, matched.length, Arrays.mismatch(expected, matched)));
    }
  }

  @Benchmark
  @OperationsPerInvocation(ROWS)
  public long baseline() throws IOException {
    return writeBaseline(new MemoryOutputFile(false)).size();
  }

  @Benchmark
  @OperationsPerInvocation(ROWS)
  public long hoodieRow() throws IOException {
    return writePrototype(new MemoryOutputFile(false), prototypeSupport).size();
  }

  /** The HoodieRow boundary with today's conversions: isolates the cost of the boundary itself. */
  @Benchmark
  @OperationsPerInvocation(ROWS)
  public long hoodieRowMatched() throws IOException {
    return writePrototype(new MemoryOutputFile(false), matchedSupport).size();
  }

  private MemoryOutputFile writeBaseline(MemoryOutputFile file) throws IOException {
    try (ParquetWriter<InternalRow> writer = open(file, baselineSupport)) {
      for (InternalRow row : rows) {
        writer.write(row);
      }
    }
    return file;
  }

  private MemoryOutputFile writePrototype(MemoryOutputFile file, WriteSupport<HoodieRow> support) throws IOException {
    try (ParquetWriter<HoodieRow> writer = open(file, support)) {
      for (InternalRow row : rows) {
        writer.write(wrapper.replace(row));
      }
    }
    return file;
  }

  private <T> ParquetWriter<T> open(OutputFile file, WriteSupport<T> support) throws IOException {
    return new Builder<>(file, support)
        .withConf(conf)
        .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
        .build();
  }

  private static final class Builder<T> extends ParquetWriter.Builder<T, Builder<T>> {
    private final WriteSupport<T> support;

    Builder(OutputFile file, WriteSupport<T> support) {
      super(file);
      this.support = support;
    }

    @Override
    protected Builder<T> self() {
      return this;
    }

    @Override
    protected WriteSupport<T> getWriteSupport(Configuration configuration) {
      return support;
    }
  }
}
