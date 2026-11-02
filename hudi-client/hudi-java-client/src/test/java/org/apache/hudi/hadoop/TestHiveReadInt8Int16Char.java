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

package org.apache.hudi.hadoop;

import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.HoodieReaderConfig;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.hadoop.utils.HiveTypeUtils;
import org.apache.hudi.hadoop.utils.HoodieInputFormatUtils;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.testutils.HoodieJavaClientTestHarness;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.parquet.serde.ArrayWritableObjectInspector;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * RFC-88 step 2b, end to end for Hive: a COW and a MOR table whose HoodieSchema has INT8, INT16 and CHAR(10) columns,
 * written by the Java write client, read through Hive's input formats with the column types Hive sync declares for
 * them (tinyint, smallint, char(10)). Hive reads the rows through the Parquet serde's object inspectors
 * ({@link ArrayWritableObjectInspector}, as {@code HoodieParquetSerde} does), which give the right Byte, Short and
 * padded HiveChar values for the base files and for the log records merged into them.
 *
 * <p>The test lives here rather than in hudi-hadoop-mr because the Java write client cannot be a test dependency of
 * hudi-hadoop-mr.
 */
public class TestHiveReadInt8Int16Char extends HoodieJavaClientTestHarness {

  private static final String PARTITION = "p1";
  private static final HoodieSchema SCHEMA = HoodieSchema.createRecord("narrow", "test", null, Arrays.asList(
      HoodieSchemaField.of("_row_key", HoodieSchema.create(HoodieSchemaType.STRING)),
      HoodieSchemaField.of("partition_path", HoodieSchema.create(HoodieSchemaType.STRING)),
      HoodieSchemaField.of("ts", HoodieSchema.create(HoodieSchemaType.LONG)),
      HoodieSchemaField.of("tiny", HoodieSchema.createInt8()),
      HoodieSchemaField.of("small", HoodieSchema.createNullable(HoodieSchema.createInt16())),
      HoodieSchemaField.of("code", HoodieSchema.createNullable(HoodieSchema.createChar(10)))));
  /**
   * What Hive sync declares for the data columns (HiveSchemaUtil), and what HiveTypeUtils gives Hive's readers.
   */
  private static final String HIVE_COLUMN_TYPES = "string,string,bigint,tinyint,smallint,char(10)";

  static Stream<Arguments> reads() {
    return Stream.of(
        Arguments.of(HoodieTableType.COPY_ON_WRITE, false, true),
        Arguments.of(HoodieTableType.COPY_ON_WRITE, false, false),
        Arguments.of(HoodieTableType.MERGE_ON_READ, false, true),
        Arguments.of(HoodieTableType.MERGE_ON_READ, true, true),
        // the realtime record reader that merges log records without the file group reader
        Arguments.of(HoodieTableType.MERGE_ON_READ, true, false));
  }

  @ParameterizedTest(name = "{0}, realtime: {1}, file group reader: {2}")
  @MethodSource("reads")
  void readsInt8Int16AndCharThroughHive(HoodieTableType type, boolean realtime, boolean fileGroupReader) throws Exception {
    assertEquals(HIVE_COLUMN_TYPES, HiveTypeUtils.generateColumnTypes(SCHEMA).stream().map(TypeInfo::getTypeName)
        .collect(Collectors.joining(",")));
    initPath();
    initFileSystem(basePath, storageConf);
    storage.createDirectory(new StoragePath(basePath));
    Properties tableProperties = new Properties();
    tableProperties.setProperty(HoodieTableConfig.RECORDKEY_FIELDS.key(), "_row_key");
    tableProperties.setProperty(HoodieTableConfig.PARTITION_FIELDS.key(), "partition_path");
    tableProperties.setProperty(HoodieTableConfig.ORDERING_FIELDS.key(), "ts");
    metaClient = HoodieTestUtils.init(storageConf, basePath, type, tableProperties);

    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(SCHEMA.toString())
        .withParallelism(2, 2).withDeleteParallelism(2)
        // an index that cannot index log files, so that MOR inserts write base files and the updates log files
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.SIMPLE).build())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().withInlineCompaction(false).compactionSmallFileSize(0).build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build())
        .build();

    Map<String, GenericRecord> expected = new HashMap<>();
    try (HoodieJavaWriteClient client = new HoodieJavaWriteClient(context, config)) {
      write(client, false, expected,
          row("k0", 1L, Byte.MIN_VALUE, (int) Short.MIN_VALUE, "ab"),
          row("k1", 1L, Byte.MAX_VALUE, (int) Short.MAX_VALUE, "abcdefghij"),
          row("k2", 1L, 0, null, null),
          row("k3", 1L, 5, 500, "keep      "));
      // updates: log files for MOR, new file versions for COW
      write(client, true, expected,
          row("k0", 2L, -1, -300, "xyz"),
          row("k2", 2L, 100, 12345, "padded    "));
    }
    metaClient = HoodieTableMetaClient.reload(metaClient);
    if (type == HoodieTableType.MERGE_ON_READ) {
      try (HoodieTableFileSystemView fsView = HoodieTableFileSystemView.fileListingBasedFileSystemView(context, metaClient,
          metaClient.getActiveTimeline().filterCompletedInstants())) {
        assertTrue(fsView.getLatestFileSlices(PARTITION).anyMatch(s -> s.getLogFiles().count() > 0), "the MOR table has log files");
      }
    }

    // a read-optimized read of a MOR table sees the base files only, i.e. the first commit
    boolean readOptimized = type == HoodieTableType.MERGE_ON_READ && !realtime;
    Map<String, Object[]> rows = read(realtime, fileGroupReader);
    assertEquals(expected.keySet(), rows.keySet());
    for (Map.Entry<String, Object[]> entry : rows.entrySet()) {
      GenericRecord row = readOptimized && (entry.getKey().equals("k0") || entry.getKey().equals("k2"))
          ? firstVersion(entry.getKey()) : expected.get(entry.getKey());
      Object[] values = entry.getValue();
      assertEquals(((Integer) row.get("tiny")).byteValue(), values[0], "tiny of " + entry.getKey());
      assertEquals(row.get("small") == null ? null : ((Integer) row.get("small")).shortValue(), values[1], "small of " + entry.getKey());
      if (row.get("code") == null) {
        assertNull(values[2]);
      } else {
        // a Hive char(10) value is padded to 10, and compares equal to the same characters with other padding
        HiveChar code = (HiveChar) values[2];
        assertEquals(new HiveChar(row.get("code").toString(), 10), code, "code of " + entry.getKey());
        assertEquals(10, code.getPaddedValue().length());
      }
    }
  }

  private void write(HoodieJavaWriteClient client, boolean upsert, Map<String, GenericRecord> expected, GenericRecord... rows) {
    String instant = client.startCommit();
    List<HoodieRecord> records = new ArrayList<>();
    for (GenericRecord row : rows) {
      records.add(new HoodieAvroIndexedRecord(new HoodieKey(row.get("_row_key").toString(), PARTITION), row));
      expected.put(row.get("_row_key").toString(), row);
    }
    List<WriteStatus> statuses = upsert ? client.upsert(records, instant) : client.insert(records, instant);
    assertNoWriteErrors(statuses);
    client.commit(instant, statuses);
  }

  private static GenericRecord firstVersion(String key) {
    return key.equals("k0") ? row("k0", 1L, Byte.MIN_VALUE, (int) Short.MIN_VALUE, "ab") : row("k2", 1L, 0, null, null);
  }

  private static GenericRecord row(String key, long ts, int tiny, Integer small, String code) {
    GenericRecord row = new GenericData.Record(SCHEMA.toAvroSchema());
    row.put("_row_key", key);
    row.put("partition_path", PARTITION);
    row.put("ts", ts);
    row.put("tiny", tiny);
    row.put("small", small);
    row.put("code", code);
    return row;
  }

  /**
   * Reads every row through the Hive input format, the way Hive does for a table synced with {@link #HIVE_COLUMN_TYPES}
   * (partitioned by a {@code datestr} column outside the files), and returns tiny, small and code by record key as Hive's
   * Parquet serde object inspectors see them.
   */
  private Map<String, Object[]> read(boolean realtime, boolean fileGroupReader) throws Exception {
    HoodieSchema schema = HoodieSchemaUtils.addMetadataFields(SCHEMA);
    List<String> names = schema.getFields().stream().map(HoodieSchemaField::name).collect(Collectors.toList());
    String hiveColumnTypes = HoodieSchemaUtils.addMetadataColumnTypes(HIVE_COLUMN_TYPES);
    JobConf jobConf = new JobConf(storageConf.unwrapAs(Configuration.class));
    jobConf.setBoolean(HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key(), fileGroupReader);
    jobConf.set(hive_metastoreConstants.META_TABLE_COLUMNS, String.join(",", names) + ",datestr");
    jobConf.set(hive_metastoreConstants.META_TABLE_COLUMN_TYPES, hiveColumnTypes + ",string");
    jobConf.set(hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS, "datestr");
    jobConf.set(IOConstants.COLUMNS, String.join(",", names) + ",datestr");
    jobConf.set(IOConstants.COLUMNS_TYPES, hiveColumnTypes + ",string");
    jobConf.set(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, String.join(",", names));
    jobConf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR,
        schema.getFields().stream().map(f -> String.valueOf(f.pos())).collect(Collectors.joining(",")));
    FileInputFormat inputFormat = HoodieInputFormatUtils.getInputFormat(metaClient.getTableConfig().getBaseFileFormat(), realtime, jobConf);
    if (inputFormat instanceof Configurable) {
      ((Configurable) inputFormat).setConf(jobConf);
    }
    FileInputFormat.setInputPaths(jobConf, new StoragePath(basePath, PARTITION).toString());

    List<TypeInfo> types = TypeInfoUtils.getTypeInfosFromTypeString(hiveColumnTypes);
    ArrayWritableObjectInspector inspector = new ArrayWritableObjectInspector(
        (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(names, types));
    Map<String, Object[]> rows = new HashMap<>();
    for (InputSplit split : inputFormat.getSplits(jobConf, 1)) {
      RecordReader<Object, ArrayWritable> reader = inputFormat.getRecordReader(split, jobConf, null);
      Object key = reader.createKey();
      ArrayWritable value = reader.createValue();
      while (reader.next(key, value)) {
        String recordKey = javaValue(inspector, value, "_row_key").toString();
        rows.put(recordKey, new Object[] {
            javaValue(inspector, value, "tiny"), javaValue(inspector, value, "small"), javaValue(inspector, value, "code")});
      }
      reader.close();
    }
    return rows;
  }

  private static Object javaValue(ArrayWritableObjectInspector inspector, ArrayWritable row, String column) {
    StructField field = inspector.getStructFieldRef(column);
    Object data = inspector.getStructFieldData(row, field);
    return data == null ? null : ((PrimitiveObjectInspector) field.getFieldObjectInspector()).getPrimitiveJavaObject(data);
  }
}
