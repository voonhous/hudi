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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

/**
 * Writes known SMALLINT values through Hudi's Flink Parquet writer (from BinaryRowData and from
 * GenericRowData) and through the HoodieRow prototype, reads each file back, and prints what was stored.
 */
public final class SmallIntCheck {
  private static final short[] VALUES = {-1, 300, -300, 32767, -32768};

  private SmallIntCheck() {
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    RowType rowType = (RowType) DataTypes.ROW(
        DataTypes.FIELD("id", DataTypes.BIGINT().notNull()),
        DataTypes.FIELD("small", DataTypes.SMALLINT())).notNull().getLogicalType();
    HoodieSchema schema = HoodieSchemaConverter.convertToSchema(rowType, "bench_record");
    System.out.println("Flink type: SMALLINT; HoodieSchema field: " + schema.getField("small").get().schema());

    RowDataSerializer serializer = new RowDataSerializer(rowType);
    List<RowData> generic = new ArrayList<>();
    List<RowData> binary = new ArrayList<>();
    for (int i = 0; i < VALUES.length; i++) {
      GenericRowData row = GenericRowData.of((long) i, VALUES[i]);
      generic.add(row);
      binary.add(serializer.toBinaryRow(row).copy());
    }
    System.out.print("Written values:");
    for (short v : VALUES) {
      System.out.print(" " + v);
    }
    System.out.println();

    RowDataParquetWriteSupport hudi = new RowDataParquetWriteSupport(schema, conf);
    report("Hudi Flink writer, BinaryRowData", () -> write(conf, hudi, binary));
    report("Hudi Flink writer, GenericRowData", () -> write(conf, hudi, generic));

    WriteSupport.WriteContext context = hudi.init(conf);
    HoodieRowParquetWriter prototype = new HoodieRowParquetWriter(context.getSchema(), context.getExtraMetaData());
    List<HoodieRow> wrapped = new ArrayList<>();
    for (RowData row : binary) {
      wrapped.add(FlinkRowWrapper.of(rowType).replace(row));
    }
    report("HoodieRow prototype, BinaryRowData", () -> write(conf, prototype, wrapped));
  }

  private interface Writer {
    byte[] write() throws Exception;
  }

  private static void report(String label, Writer writer) throws Exception {
    byte[] bytes;
    try {
      bytes = writer.write();
    } catch (Throwable t) {
      System.out.println(label + ": write FAILED:");
      t.printStackTrace(System.out);
      return;
    }
    File file = File.createTempFile("smallint", ".parquet");
    file.deleteOnExit();
    Files.write(file.toPath(), bytes);
    StringBuilder read = new StringBuilder();
    String columnType = null;
    try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), new Path(file.getAbsolutePath())).build()) {
      Group group;
      while ((group = reader.read()) != null) {
        columnType = group.getType().getType("small").toString();
        read.append(' ').append(group.getFieldRepetitionCount("small") == 0 ? "null" : String.valueOf(group.getInteger("small", 0)));
      }
    }
    System.out.println(label + ": column '" + columnType + "', read back:" + read);
  }

  private static <T> byte[] write(Configuration conf, WriteSupport<T> support, List<? extends T> rows) throws Exception {
    MemoryOutputFile file = new MemoryOutputFile(true);
    try (ParquetWriter<T> writer = new Builder<>(file, support).withConf(conf)
        .withCompressionCodec(CompressionCodecName.UNCOMPRESSED).withWriteMode(ParquetFileWriter.Mode.OVERWRITE).build()) {
      for (T row : rows) {
        writer.write(row);
      }
    }
    return file.bytes();
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
