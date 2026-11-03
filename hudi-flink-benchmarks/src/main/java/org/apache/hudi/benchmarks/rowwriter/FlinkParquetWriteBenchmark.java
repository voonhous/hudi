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

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.io.storage.row.RowDataParquetWriteSupport;
import org.apache.hudi.util.HoodieSchemaConverter;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
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
 * Flink rows (BinaryRowData) to Parquet: today's {@link RowDataParquetWriteSupport} against the RFC-88
 * prototype ({@link FlinkRowWrapper} read by the same {@link HoodieRowParquetWriter} the Spark benchmark
 * uses). Same physical schema and footer metadata; setup checks the files are byte-identical. Per row.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 3)
@Fork(value = 2, jvmArgsAppend = {"-Xms4g", "-Xmx4g"})
public class FlinkParquetWriteBenchmark {
  static final int ROWS = 20_000;

  @Param({"DECIMAL", "STRING", "NESTED", "MIXED"})
  public Workloads.Kind workload;

  private Configuration conf;
  private RowData[] rows;
  private RowDataParquetWriteSupport baselineSupport;
  private HoodieRowParquetWriter prototypeSupport;
  private FlinkRowWrapper wrapper;

  @Setup(Level.Trial)
  public void setup() throws IOException {
    conf = new Configuration();
    RowType rowType = FlinkWorkloads.rowType(workload);
    rows = FlinkWorkloads.rows(workload, ROWS);
    HoodieSchema schema = HoodieSchemaConverter.convertToSchema(rowType, "bench_record");
    baselineSupport = new RowDataParquetWriteSupport(schema, conf);
    WriteSupport.WriteContext context = baselineSupport.init(conf);
    prototypeSupport = new HoodieRowParquetWriter(context.getSchema(), context.getExtraMetaData());
    wrapper = FlinkRowWrapper.of(rowType);

    byte[] expected = writeBaseline(new MemoryOutputFile(true)).bytes();
    byte[] actual = writePrototype(new MemoryOutputFile(true)).bytes();
    if (!Arrays.equals(expected, actual)) {
      throw new IllegalStateException(String.format("%s: files differ (baseline %d bytes, prototype %d bytes, first difference at %d)",
          workload, expected.length, actual.length, Arrays.mismatch(expected, actual)));
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
    return writePrototype(new MemoryOutputFile(false)).size();
  }

  private MemoryOutputFile writeBaseline(MemoryOutputFile file) throws IOException {
    try (ParquetWriter<RowData> writer = open(file, baselineSupport)) {
      for (RowData row : rows) {
        writer.write(row);
      }
    }
    return file;
  }

  private MemoryOutputFile writePrototype(MemoryOutputFile file) throws IOException {
    try (ParquetWriter<HoodieRow> writer = open(file, prototypeSupport)) {
      for (RowData row : rows) {
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
