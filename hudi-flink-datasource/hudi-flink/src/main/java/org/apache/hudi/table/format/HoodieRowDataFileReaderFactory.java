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

package org.apache.hudi.table.format;

import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.core.io.storage.HoodieFileReader;
import org.apache.hudi.core.io.storage.HoodieFileReaderFactory;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * Factory methods to create RowData file reader.
 */
public class HoodieRowDataFileReaderFactory extends HoodieFileReaderFactory {
  public HoodieRowDataFileReaderFactory(HoodieStorage storage) {
    super(storage);
  }

  /**
   * Whether {@code config} maps Flink TINYINT, SMALLINT and CHAR(n) to INT8, INT16 and CHAR(n) (RFC-88): a table
   * config with {@link HoodieTableConfig#TYPE_SYSTEM_VERSION} 1 or more, or a write config that carries it (the Flink
   * table factory puts the table's in the job configuration). Without it, today's mapping.
   */
  public static boolean usesNarrowIntAndCharTypes(@Nullable HoodieConfig config) {
    return config != null && Integer.parseInt(config.getStringOrDefault(HoodieTableConfig.TYPE_SYSTEM_VERSION, "0")) >= 1;
  }

  @Override
  public HoodieFileReader getFileReader(HoodieConfig hoodieConfig, StoragePath path, HoodieFileFormat format,
                                        Option<HoodieSchema> schemaOption) throws IOException {
    if (format == HoodieFileFormat.PARQUET) {
      return new HoodieRowDataParquetReader(storage, path, usesNarrowIntAndCharTypes(hoodieConfig));
    }
    return super.getFileReader(hoodieConfig, path, format, schemaOption);
  }

  @Override
  public HoodieFileReader getFileReader(HoodieConfig hoodieConfig, StoragePathInfo pathInfo, HoodieFileFormat format,
                                        Option<HoodieSchema> schemaOption) throws IOException {
    if (format == HoodieFileFormat.PARQUET) {
      return new HoodieRowDataParquetReader(storage, pathInfo.getPath(), usesNarrowIntAndCharTypes(hoodieConfig));
    }
    return super.getFileReader(hoodieConfig, pathInfo, format, schemaOption);
  }

  @Override
  protected HoodieFileReader newParquetFileReader(StoragePath path) {
    return new HoodieRowDataParquetReader(storage, path);
  }

  @Override
  protected HoodieFileReader newLanceFileReader(HoodieConfig hoodieConfig, StoragePath path) {
    return new HoodieRowDataLanceReader(path, hoodieConfig);
  }
}
