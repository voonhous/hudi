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

package org.apache.hudi.io.hadoop;

import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.engine.LocalTaskContextSupplier;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.schema.mapping.OrcSchemaMapper;
import org.apache.hudi.common.schema.mapping.ParquetSchemaMapper;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.core.io.storage.HoodieAvroFileReader;
import org.apache.hudi.core.io.storage.HoodieAvroFileWriter;
import org.apache.hudi.core.io.storage.HoodieFileWriterFactory;
import org.apache.hudi.core.io.storage.HoodieIOFactory;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.hudi.common.util.ConfigUtils.DEFAULT_HUDI_CONFIG_FOR_READER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * RFC-88 step 2b, end to end for the file formats: a file written by the Avro file writers for a schema with INT8,
 * INT16 and CHAR(10) columns has the native column types (Parquet INT32 + INTEGER(8 / 16, signed) and BINARY + STRING;
 * ORC tinyint, smallint and char(10)) and reads back with the same values, with the table schema as the reader schema.
 * Without a hint the file schema still reads as INT and STRING (step 2c), and the hint restores the types.
 */
class TestInt8Int16CharFileFormats {

  private static final HoodieSchema SCHEMA = HoodieSchema.createRecord("narrow", "test", null, Arrays.asList(
      HoodieSchemaField.of("_row_key", HoodieSchema.create(HoodieSchemaType.STRING)),
      HoodieSchemaField.of("tiny", HoodieSchema.createInt8()),
      HoodieSchemaField.of("small", HoodieSchema.createNullable(HoodieSchema.createInt16())),
      HoodieSchemaField.of("code", HoodieSchema.createNullable(HoodieSchema.createChar(10)))));

  @TempDir
  java.nio.file.Path tempDir;

  @ParameterizedTest
  @EnumSource(value = HoodieFileFormat.class, names = {"PARQUET", "ORC"})
  void writesNativeTypesAndReadsBackValues(HoodieFileFormat format) throws Exception {
    StoragePath path = new StoragePath(tempDir.resolve("f1_1-0-1_000" + format.getFileExtension()).toAbsolutePath().toString());
    HoodieStorage storage = HoodieTestUtils.getStorage(path);
    HoodieConfig config = HoodieStorageConfig.newBuilder().orcCompressionCodec("NONE").build();
    config.setValue(HoodieStorageConfig.PARQUET_COMPRESSION_CODEC_NAME, "uncompressed");

    List<GenericRecord> written = new ArrayList<>();
    written.add(record("k0", Byte.MIN_VALUE, (int) Short.MIN_VALUE, "ab        "));
    written.add(record("k1", Byte.MAX_VALUE, (int) Short.MAX_VALUE, "abcdefghij"));
    written.add(record("k2", 0, null, null));
    try (HoodieAvroFileWriter writer = (HoodieAvroFileWriter) HoodieFileWriterFactory.getFileWriter(
        "000", path, storage, config, SCHEMA, new LocalTaskContextSupplier(), HoodieRecordType.AVRO)) {
      for (GenericRecord record : written) {
        writer.writeAvro(record.get("_row_key").toString(), record);
      }
    }

    Configuration hadoopConf = storage.getConf().unwrapAs(Configuration.class);
    HoodieSchema fileSchema;
    if (format == HoodieFileFormat.PARQUET) {
      try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(path.toUri()), hadoopConf))) {
        MessageType parquetSchema = reader.getFooter().getFileMetaData().getSchema();
        assertEquals(LogicalTypeAnnotation.intType(8, true), parquetSchema.getType("tiny").getLogicalTypeAnnotation());
        assertEquals(LogicalTypeAnnotation.intType(16, true), parquetSchema.getType("small").getLogicalTypeAnnotation());
        assertEquals(LogicalTypeAnnotation.stringType(), parquetSchema.getType("code").getLogicalTypeAnnotation());
        fileSchema = new ParquetSchemaMapper().fromPhysicalSchema(parquetSchema, Option.empty());
        assertEquals(SCHEMA.getFields().size(), new ParquetSchemaMapper().fromPhysicalSchema(parquetSchema, Option.of(SCHEMA)).getFields().size());
        assertEquals(HoodieSchemaType.INT16,
            new ParquetSchemaMapper().fromPhysicalSchema(parquetSchema, Option.of(SCHEMA)).getField("small").get().schema().getNonNullType().getType());
      }
    } else {
      try (Reader reader = OrcFile.createReader(new Path(path.toUri()), OrcFile.readerOptions(hadoopConf))) {
        TypeDescription orcSchema = reader.getSchema();
        assertEquals("struct<_row_key:string,tiny:tinyint,small:smallint,code:char(10)>", orcSchema.toString());
        fileSchema = new OrcSchemaMapper().fromPhysicalSchema(orcSchema, Option.empty());
        assertEquals(HoodieSchemaType.CHAR,
            new OrcSchemaMapper().fromPhysicalSchema(orcSchema, Option.of(SCHEMA)).getField("code").get().schema().getNonNullType().getType());
      }
    }
    // RFC-88 step 2c: without a hint the file schema reads as today's INT and STRING
    assertEquals(HoodieSchemaType.INT, fileSchema.getField("tiny").get().schema().getNonNullType().getType());
    assertEquals(HoodieSchemaType.INT, fileSchema.getField("small").get().schema().getNonNullType().getType());
    assertEquals(HoodieSchemaType.STRING, fileSchema.getField("code").get().schema().getNonNullType().getType());

    HoodieAvroFileReader reader = (HoodieAvroFileReader) HoodieIOFactory.getIOFactory(storage).getReaderFactory(HoodieRecordType.AVRO)
        .getFileReader(DEFAULT_HUDI_CONFIG_FOR_READER, path);
    List<GenericRecord> read = new ArrayList<>();
    try (ClosableIterator<IndexedRecord> iterator = reader.getIndexedRecordIterator(SCHEMA, SCHEMA)) {
      iterator.forEachRemaining(r -> read.add((GenericRecord) r));
    }
    reader.close();
    assertEquals(written.size(), read.size());
    for (int i = 0; i < written.size(); i++) {
      GenericRecord expected = written.get(i);
      GenericRecord actual = read.get(i);
      assertEquals(expected.get("tiny"), actual.get("tiny"), "tiny of row " + i);
      assertEquals(expected.get("small"), actual.get("small"), "small of row " + i);
      if (expected.get("code") == null) {
        assertNull(actual.get("code"));
      } else {
        assertEquals(expected.get("code"), actual.get("code").toString(), "code of row " + i);
      }
      // the values are Avro ints, as for INT columns
      assertEquals(Integer.class, actual.get("tiny").getClass());
    }
  }

  private static GenericRecord record(String key, int tiny, Integer small, String code) {
    GenericRecord record = new GenericData.Record(SCHEMA.toAvroSchema());
    record.put("_row_key", key);
    record.put("tiny", tiny);
    record.put("small", small);
    record.put("code", code);
    return record;
  }
}
