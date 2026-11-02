/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.utils;

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.HoodieStorageUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.configuration.Configuration;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;

/**
 * Test helpers for tables created before {@link HoodieTableConfig#TYPE_SYSTEM_VERSION} existed (RFC-88), which keep
 * mapping Flink TINYINT, SMALLINT and CHAR(n) to INT, INT and STRING.
 */
public final class TypeSystemTestUtils {

  private TypeSystemTestUtils() {
  }

  /**
   * Turns the table at {@code basePath} into one created before the property existed: hoodie.properties loses
   * {@link HoodieTableConfig#TYPE_SYSTEM_VERSION}, and its create schema (if any) gets the types such a table was
   * created with (INT for INT8 / INT16, STRING for CHAR(n)). Use it on a table nothing has been written to yet.
   */
  public static void asTableCreatedBeforeTypeSystemVersion(String basePath) {
    HoodieStorage storage = HoodieStorageUtils.getStorage(basePath, HadoopFSUtils.getStorageConf(new org.apache.hadoop.conf.Configuration()));
    StoragePath metaFolder = new StoragePath(basePath, HoodieTableMetaClient.METAFOLDER_NAME);
    HoodieTableConfig tableConfig = HoodieTableConfig.loadFromHoodieProps(storage, basePath);
    Properties updated = new Properties();
    Option<HoodieSchema> createSchema = tableConfig.getTableCreateSchema();
    if (createSchema.isPresent()) {
      updated.setProperty(HoodieTableConfig.CREATE_SCHEMA.key(), withoutNarrowTypes(createSchema.get().toString()));
    }
    HoodieTableConfig.updateAndDeleteProps(storage, metaFolder, updated,
        Collections.singleton(HoodieTableConfig.TYPE_SYSTEM_VERSION.key()));
  }

  /**
   * Creates an empty table at {@code basePath} as it was created before the property existed (see
   * {@link #asTableCreatedBeforeTypeSystemVersion}), with the keys a Flink SQL table definition of it gives.
   */
  public static void createTableBeforeTypeSystemVersion(String basePath, String tableName, HoodieTableType tableType,
                                                        String recordKey, String partitionField, String orderingField,
                                                        String baseFileFormat, String operation) throws IOException {
    Configuration conf = new Configuration();
    conf.set(FlinkOptions.OPERATION, operation);
    conf.set(FlinkOptions.PATH, basePath);
    conf.set(FlinkOptions.TABLE_NAME, tableName);
    conf.set(FlinkOptions.TABLE_TYPE, tableType.name());
    conf.set(FlinkOptions.RECORD_KEY_FIELD, recordKey);
    conf.set(FlinkOptions.PARTITION_PATH_FIELD, partitionField);
    conf.set(FlinkOptions.ORDERING_FIELDS, orderingField);
    conf.setString(HoodieTableConfig.BASE_FILE_FORMAT.key(), baseFileFormat);
    StreamerUtil.initTableIfNotExists(conf);
    asTableCreatedBeforeTypeSystemVersion(basePath);
  }

  /**
   * The JSON schema {@code schema} with INT8 and INT16 as int and CHAR(n) as string.
   */
  public static String withoutNarrowTypes(String schema) {
    return schema
        .replaceAll("\\{\"type\":\"int\",\"logicalType\":\"int(8|16)\"}", "\"int\"")
        .replaceAll("\\{\"type\":\"string\",\"logicalType\":\"char\",\"length\":\\d+(,\"maxLength\":\\d+)?}", "\"string\"");
  }
}
