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

package org.apache.hudi.util;

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieCatalogException;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Schema.UnresolvedPhysicalColumn;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Utilities for {@link org.apache.flink.table.types.DataType}.
 */
public class DataTypeUtils {

  public static Map<String, DataType> METADATA_COLUMNS = new LinkedHashMap<String, DataType>() {{
      put(HoodieRecord.COMMIT_TIME_METADATA_FIELD, DataTypes.STRING());
      put(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD, DataTypes.STRING());
      put(HoodieRecord.RECORD_KEY_METADATA_FIELD, DataTypes.STRING());
      put(HoodieRecord.PARTITION_PATH_METADATA_FIELD, DataTypes.STRING());
      put(HoodieRecord.FILENAME_METADATA_FIELD, DataTypes.STRING());
    }};

  public static Map<String, DataType> METADATA_COLUMNS_WITH_OPERATION = new LinkedHashMap<String, DataType>() {{
      put(HoodieRecord.COMMIT_TIME_METADATA_FIELD, DataTypes.STRING());
      put(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD, DataTypes.STRING());
      put(HoodieRecord.RECORD_KEY_METADATA_FIELD, DataTypes.STRING());
      put(HoodieRecord.PARTITION_PATH_METADATA_FIELD, DataTypes.STRING());
      put(HoodieRecord.FILENAME_METADATA_FIELD, DataTypes.STRING());
      put(HoodieRecord.OPERATION_METADATA_FIELD, DataTypes.STRING());
    }};

  /**
   * Returns whether the given type is TIMESTAMP type.
   */
  public static boolean isTimestampType(DataType type) {
    return type.getLogicalType().getTypeRoot() == LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE;
  }

  /**
   * Returns the precision of the given TIMESTAMP type.
   */
  public static int precision(LogicalType logicalType) {
    if (logicalType instanceof TimestampType) {
      return ((TimestampType) logicalType).getPrecision();
    } else if (logicalType instanceof LocalZonedTimestampType) {
      return ((LocalZonedTimestampType) logicalType).getPrecision();
    } else {
      throw new AssertionError("Unexpected type: " + logicalType);
    }
  }

  /**
   * Convert the given {@link Schema} into {@link RowType}.
   */
  public static RowType toRowType(Schema schema) {
    List<RowType.RowField> rowFields =
        schema.getColumns().stream()
            .filter(col -> col instanceof UnresolvedPhysicalColumn)
            .map(col -> new RowType.RowField(col.getName(), ((DataType) ((UnresolvedPhysicalColumn) col).getDataType()).getLogicalType()))
            .collect(Collectors.toList());
    return new RowType(false, rowFields);
  }

  /**
   * Returns whether the given type is DATE type.
   */
  public static boolean isDateType(DataType type) {
    return type.getLogicalType().getTypeRoot() == LogicalTypeRoot.DATE;
  }

  /**
   * Returns whether the given type is DATETIME type.
   */
  public static boolean isDatetimeType(DataType type) {
    return isTimestampType(type) || isDateType(type);
  }

  /**
   * Returns projected field ordinals with given RowType and produced RowType.
   */
  public static int[] projectOrdinals(RowType rowType, RowType producedRowType) {
    List<String> fieldNames = rowType.getFieldNames();
    return producedRowType.getFieldNames().stream().mapToInt(fieldNames::indexOf).toArray();
  }

  /**
   * Creates a hoodie schema from a Flink row type with logical metadata from the table schema.
   *
   * <p>When a field is a hoodie specific logical type in {@code tableSchema}, this method
   * reuses the table schema field to preserve logical metadata that cannot be recovered from Flink
   * {@link RowType}, for example VECTOR element type and dimension. Other fields are taken from the
   * schema converted from {@code rowType}, so the returned schema follows the row type's field order
   * while retaining hoodie-specific logical metadata where needed.
   *
   * @param rowType     Flink row type to convert
   * @param tableSchema source table schema with hoodie logical type metadata
   * @return hoodie schema matching the row type field order
   */
  public static HoodieSchema toHoodieSchema(RowType rowType, HoodieSchema tableSchema) {
    HoodieSchema convertedSchema = HoodieSchemaConverter.convertToSchema(rowType);
    List<HoodieSchemaField> schemaFields = new ArrayList<>(rowType.getFieldCount());

    for (String fieldName : rowType.getFieldNames()) {
      HoodieSchemaField tableField = tableSchema.getField(fieldName).orElse(null);
      HoodieSchemaField field = tableField != null && useTableSchemaField(tableField)
          ? tableField : convertedSchema.getField(fieldName).get();
      schemaFields.add(HoodieSchemaUtils.createNewSchemaField(field));
    }

    HoodieSchema schema = HoodieSchema.createRecord(
        tableSchema.getName(),
        tableSchema.getNamespace().orElse(null),
        tableSchema.getDoc().orElse(null),
        schemaFields);
    return restoreNarrowTypes(schema, rowType, tableSchema);
  }

  /**
   * Takes INT8, INT16 and CHAR(n) from the table schema where the Flink type is the one they map to (TINYINT,
   * SMALLINT, CHAR(n)).
   *
   * <p>{@link HoodieSchemaConverter#convertToSchema} still maps TINYINT and SMALLINT to INT and CHAR(n) to STRING
   * (RFC-88 rollout step 2c switches that), while a table whose schema has INT8, INT16 or CHAR(n) stores and reads
   * them as TINYINT, SMALLINT and CHAR(n). Reading or writing such a table with the converted schema would use the
   * wrong width, so the table schema's type is taken. Tables without these types are unchanged, and so are columns
   * whose Flink type is another one (a TINYINT column the table stores as INT stays INT).
   *
   * @param converted   the schema converted from {@code flinkType}
   * @param flinkType   the Flink type
   * @param tableSchema the table schema, or null
   * @return the converted schema with INT8, INT16 and CHAR(n) taken from the table schema
   */
  public static HoodieSchema restoreNarrowTypes(HoodieSchema converted, LogicalType flinkType, @Nullable HoodieSchema tableSchema) {
    if (tableSchema == null || (converted.isNullable() && converted.isComplexUnion())) {
      return converted;
    }
    HoodieSchema nonNull = converted.getNonNullType();
    HoodieSchema restored = restoreNarrowTypesNonNull(nonNull, flinkType, tableSchema.getNonNullType());
    if (restored == nonNull) {
      return converted;
    }
    return converted.isNullable() ? HoodieSchema.createNullable(restored) : restored;
  }

  private static HoodieSchema restoreNarrowTypesNonNull(HoodieSchema converted, LogicalType flinkType, HoodieSchema tableSchema) {
    switch (tableSchema.getType()) {
      case INT8:
      case INT16:
      case CHAR:
        return HoodieSchemaConverter.convertToDataType(tableSchema).getLogicalType().copy(true).equals(flinkType.copy(true))
            ? tableSchema : converted;
      case RECORD: {
        if (converted.getType() != HoodieSchemaType.RECORD || !(flinkType instanceof RowType)) {
          return converted;
        }
        RowType rowType = (RowType) flinkType;
        List<HoodieSchemaField> fields = converted.getFields();
        List<HoodieSchemaField> restoredFields = new ArrayList<>(fields.size());
        boolean changed = false;
        for (HoodieSchemaField field : fields) {
          int index = rowType.getFieldIndex(field.name());
          Option<HoodieSchemaField> tableField = tableSchema.getField(field.name());
          HoodieSchema fieldSchema = field.schema();
          HoodieSchema schema = index < 0 || !tableField.isPresent()
              ? fieldSchema
              : restoreNarrowTypes(fieldSchema, rowType.getTypeAt(index), tableField.get().schema());
          changed |= schema != fieldSchema;
          restoredFields.add(field.withSchema(schema));
        }
        return changed
            ? HoodieSchema.createRecord(converted.getName(), converted.getNamespace().orElse(null), converted.getDoc().orElse(null), restoredFields)
            : converted;
      }
      case ARRAY: {
        if (converted.getType() != HoodieSchemaType.ARRAY || !(flinkType instanceof ArrayType)) {
          return converted;
        }
        HoodieSchema elementType = converted.getElementType();
        HoodieSchema element = restoreNarrowTypes(elementType, ((ArrayType) flinkType).getElementType(), tableSchema.getElementType());
        return element == elementType ? converted : HoodieSchema.createArray(element);
      }
      case MAP: {
        if (converted.getType() != HoodieSchemaType.MAP || !(flinkType instanceof MapType)) {
          return converted;
        }
        HoodieSchema valueType = converted.getValueType();
        HoodieSchema value = restoreNarrowTypes(valueType, ((MapType) flinkType).getValueType(), tableSchema.getValueType());
        return value == valueType ? converted : HoodieSchema.createMap(value);
      }
      default:
        return converted;
    }
  }

  /**
   * Returns whether the converted schema should reuse the field from the table schema.
   *
   * <p>Only types whose logical metadata cannot be fully reconstructed from Flink
   * {@link RowType} are reused from the table schema.
   */
  private static boolean useTableSchemaField(HoodieSchemaField field) {
    HoodieSchemaType fieldType = field.schema().getNonNullType().getType();
    return fieldType == HoodieSchemaType.VECTOR;
  }

  /**
   * Projects the row fields with given names.
   */
  public static RowType.RowField[] projectRowFields(RowType rowType, String[] names) {
    int[] fieldIndices = Arrays.stream(names).mapToInt(rowType::getFieldIndex).toArray();
    return Arrays.stream(fieldIndices).mapToObj(i -> rowType.getFields().get(i)).toArray(RowType.RowField[]::new);
  }

  /**
   * Returns whether the given logical type belongs to the family.
   */
  public static boolean isFamily(LogicalType logicalType, LogicalTypeFamily family) {
    return logicalType.getTypeRoot().getFamilies().contains(family);
  }

  /**
   * Resolves the partition path string into value obj with given data type.
   */
  public static Object resolvePartition(String partition, DataType type) {
    if (partition == null) {
      return null;
    }

    LogicalTypeRoot typeRoot = type.getLogicalType().getTypeRoot();
    switch (typeRoot) {
      case CHAR:
      case VARCHAR:
        return partition;
      case BOOLEAN:
        return Boolean.parseBoolean(partition);
      case TINYINT:
        return Integer.valueOf(partition).byteValue();
      case SMALLINT:
        return Short.valueOf(partition);
      case INTEGER:
        return Integer.valueOf(partition);
      case BIGINT:
        return Long.valueOf(partition);
      case FLOAT:
        return Float.valueOf(partition);
      case DOUBLE:
        return Double.valueOf(partition);
      case DATE:
        return LocalDate.parse(partition);
      case TIMESTAMP_WITHOUT_TIME_ZONE:
        return LocalDateTime.parse(partition);
      case DECIMAL:
        return new BigDecimal(partition);
      default:
        throw new RuntimeException(
            String.format(
                "Can not convert %s to type %s for partition value", partition, type));
    }
  }

  /**
   * Ensures the give columns of the row data type are not nullable(for example, the primary keys).
   *
   * @param dataType  The row data type, datatype logicaltype must be rowtype
   * @param pkColumns The primary keys
   * @return a new row data type if any column nullability is tweaked or the original data type
   */
  public static DataType ensureColumnsAsNonNullable(DataType dataType, @Nullable List<String> pkColumns) {
    if (pkColumns == null || pkColumns.isEmpty()) {
      return dataType;
    }
    LogicalType dataTypeLogicalType = dataType.getLogicalType();
    if (!(dataTypeLogicalType instanceof RowType)) {
      throw new RuntimeException("The datatype to be converted must be row type, but this type is :" + dataTypeLogicalType.getClass());
    }
    RowType rowType = (RowType) dataTypeLogicalType;
    List<DataType> originalFieldTypes = dataType.getChildren();
    List<String> fieldNames = rowType.getFieldNames();
    List<DataType> fieldTypes = new ArrayList<>();
    boolean tweaked = false;
    for (int i = 0; i < fieldNames.size(); i++) {
      if (pkColumns.contains(fieldNames.get(i)) && rowType.getTypeAt(i).isNullable()) {
        fieldTypes.add(originalFieldTypes.get(i).notNull());
        tweaked = true;
      } else {
        fieldTypes.add(originalFieldTypes.get(i));
      }
    }
    if (!tweaked) {
      return dataType;
    }
    List<DataTypes.Field> fields = new ArrayList<>();
    for (int i = 0; i < fieldNames.size(); i++) {
      fields.add(DataTypes.FIELD(fieldNames.get(i), fieldTypes.get(i)));
    }
    return DataTypes.ROW(fields.stream().toArray(DataTypes.Field[]::new)).notNull();
  }

  /**
   * Adds the Hoodie metadata fields to the given row type.
   */
  public static RowType addMetadataFields(
      RowType rowType,
      boolean withOperationField) {
    List<RowType.RowField> mergedFields = new ArrayList<>();
    LogicalType metadataFieldType = DataTypes.STRING().getLogicalType();

    RowType.RowField commitTimeField =
        new RowType.RowField(HoodieRecord.COMMIT_TIME_METADATA_FIELD, metadataFieldType, "commit time");
    RowType.RowField commitSeqnoField =
        new RowType.RowField(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD, metadataFieldType, "commit seqno");
    RowType.RowField recordKeyField =
        new RowType.RowField(HoodieRecord.RECORD_KEY_METADATA_FIELD, metadataFieldType, "record key");
    RowType.RowField partitionPathField =
        new RowType.RowField(HoodieRecord.PARTITION_PATH_METADATA_FIELD, metadataFieldType, "partition path");
    RowType.RowField fileNameField =
        new RowType.RowField(HoodieRecord.FILENAME_METADATA_FIELD, metadataFieldType, "field name");

    mergedFields.add(commitTimeField);
    mergedFields.add(commitSeqnoField);
    mergedFields.add(recordKeyField);
    mergedFields.add(partitionPathField);
    mergedFields.add(fileNameField);

    if (withOperationField) {
      RowType.RowField operationField =
          new RowType.RowField(HoodieRecord.OPERATION_METADATA_FIELD, metadataFieldType, "operation");
      mergedFields.add(operationField);
    }

    mergedFields.addAll(rowType.getFields());

    return new RowType(false, mergedFields);
  }

  /**
   * Return virtual metadata columns from given Schema.
   */
  public static List<String> getMetadataColumns(Schema schema) {
    return schema.getColumns().stream()
        .filter(c -> c instanceof Schema.UnresolvedMetadataColumn)
        .peek(c -> {
          if (!HoodieRecord.HOODIE_META_COLUMNS_WITH_OPERATION.contains(c.getName())) {
            throw new HoodieCatalogException(
                "Hudi catalog only supports VIRTUAL metadata column, valid metadata columns: " + HoodieRecord.HOODIE_META_COLUMNS_WITH_OPERATION);
          }
          if (!((Schema.UnresolvedMetadataColumn) c).isVirtual()) {
            throw new HoodieCatalogException(
                "Hudi catalog only supports VIRTUAL metadata column, usage: `_hoodie_commit_time STRING METADATA VIRTUAL`.");
          }
          if (((Schema.UnresolvedMetadataColumn) c).getMetadataKey() != null) {
            throw new HoodieCatalogException(
                "Hudi catalog doesn't support metadata key, usage: `_hoodie_commit_time STRING METADATA VIRTUAL`.");
          }
        }).map(Schema.UnresolvedColumn::getName).collect(Collectors.toList());
  }
}
