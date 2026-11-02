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

package org.apache.hudi.common.schema.mapping;

import org.apache.hudi.common.model.HoodieFileFormat;

import java.util.Locale;

/**
 * Registry keys of the {@link FormatSchemaMapper}s that ship with Hudi.
 */
public final class FileFormats {

  public static final String PARQUET = key(HoodieFileFormat.PARQUET);
  public static final String ORC = key(HoodieFileFormat.ORC);
  public static final String HFILE = key(HoodieFileFormat.HFILE);
  public static final String LANCE = key(HoodieFileFormat.LANCE);
  public static final String VORTEX = key(HoodieFileFormat.VORTEX);
  /**
   * Avro: data blocks and the persisted schema. Not a {@link HoodieFileFormat} member yet.
   */
  public static final String AVRO = "avro";

  private FileFormats() {
  }

  public static String key(HoodieFileFormat format) {
    return format.name().toLowerCase(Locale.ROOT);
  }
}
