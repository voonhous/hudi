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

package org.apache.hudi.metadata;

import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.expression.ArrayData;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.util.HoodieStorageUtils;
import org.apache.hudi.common.util.PartitionPathEncodeUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public abstract class AbstractHoodieTableMetadata implements HoodieTableMetadata {

  private static final LocalDate EPOCH_DAY = LocalDate.ofEpochDay(0);

  protected transient HoodieEngineContext engineContext;
  protected transient HoodieStorage storage;

  protected final StorageConfiguration<?> storageConf;
  protected final StoragePath dataBasePath;

  // TODO get this from HoodieConfig
  protected final boolean caseSensitive = false;

  public AbstractHoodieTableMetadata(HoodieEngineContext engineContext, HoodieStorage storage, String dataBasePath) {
    this.engineContext = engineContext;
    this.storage = storage;
    this.storageConf = storage.getConf();
    this.dataBasePath = new StoragePath(dataBasePath);
  }

  public HoodieStorage getStorage() {
    if (storage == null) {
      storage = HoodieStorageUtils.getStorage(dataBasePath, storageConf);
    }
    return storage;
  }

  protected static int getPathPartitionLevel(HoodieSchema partitionFields, String path) {
    if (StringUtils.isNullOrEmpty(path) || partitionFields == null) {
      return 0;
    }

    int level = 1;
    for (int i = 1; i < path.length() - 1; i++) {
      if (path.charAt(i) == StoragePath.SEPARATOR_CHAR) {
        level++;
      }
    }
    if (path.startsWith(StoragePath.SEPARATOR)) {
      level--;
    }
    if (path.endsWith(StoragePath.SEPARATOR)) {
      level--;
    }
    return level;
  }

  protected static ArrayData extractPartitionValues(HoodieSchema partitionFields,
                                                    String relativePartitionPath,
                                                    boolean urlEncodePartitioningEnabled) {
    List<HoodieSchemaField> fields = partitionFields.getFields();
    if (fields.size() == 1) {
      // SinglePartPartitionValue, which might contain slashes.
      String partitionValue = relativePartitionPath.split("=")[1];
      return new ArrayData(Collections.singletonList(fromPartitionString(
          urlEncodePartitioningEnabled ? PartitionPathEncodeUtils.unescapePathName(partitionValue) : partitionValue,
          fields.get(0).getNonNullSchema())));
    }

    List<Object> partitionValues;
    String[] partitionFragments = relativePartitionPath.split("/");
    partitionValues = IntStream.range(0, partitionFragments.length)
        .mapToObj(idx -> {
          String partitionValue = partitionFragments[idx].split("=")[1];
          return fromPartitionString(
              urlEncodePartitioningEnabled ? PartitionPathEncodeUtils.unescapePathName(partitionValue) : partitionValue,
              fields.get(idx).getNonNullSchema());
        }).collect(Collectors.toList());

    return new ArrayData(partitionValues);
  }

  /**
   * Parses a partition path value into the Java value an expression of the given type compares against.
   */
  private static Object fromPartitionString(String partitionValue, HoodieSchema type) {
    if (partitionValue == null
        || PartitionPathEncodeUtils.DEFAULT_PARTITION_PATH.equals(partitionValue)
        || PartitionPathEncodeUtils.DEPRECATED_DEFAULT_PARTITION_PATH.equals(partitionValue)) {
      return null;
    }

    switch (type.getType()) {
      case INT:
        return Integer.parseInt(partitionValue);
      case LONG:
        return Long.parseLong(partitionValue);
      case BOOLEAN:
        return Boolean.parseBoolean(partitionValue);
      case FLOAT:
        return Float.parseFloat(partitionValue);
      case DECIMAL:
        return new BigDecimal(partitionValue);
      case DOUBLE:
        return Double.parseDouble(partitionValue);
      case UUID:
        return UUID.fromString(partitionValue);
      case DATE:
        // TODO Support different date format
        return Math.toIntExact(ChronoUnit.DAYS.between(
            EPOCH_DAY, LocalDate.parse(partitionValue, DateTimeFormatter.ISO_LOCAL_DATE)));
      case STRING:
      case ENUM:
        return partitionValue;
      default:
        throw new UnsupportedOperationException("Cast value " + partitionValue
            + " to type " + type + " is not supported yet");
    }
  }
}
