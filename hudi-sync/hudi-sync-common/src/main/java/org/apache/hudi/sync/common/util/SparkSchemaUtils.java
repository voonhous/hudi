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

package org.apache.hudi.sync.common.util;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * Convert the Hoodie schema to spark schema' json string.
 * This code is refer to org.apache.spark.sql.execution.datasources.parquet.ParquetToSparkSchemaConverter
 * in spark project.
 */
public class SparkSchemaUtils {

  /**
   * The field metadata key under which Spark (3.1+, {@code CharVarcharUtils.CHAR_VARCHAR_TYPE_STRING_METADATA_KEY})
   * keeps the type of a top-level column holding a CHAR(n): the column itself is written as a string.
   */
  public static final String CHAR_VARCHAR_TYPE_STRING_METADATA_KEY = "__CHAR_VARCHAR_TYPE_STRING";

  public static String convertToSparkSchemaJson(HoodieSchema schema) {
    return convertToSparkSchemaJson(schema, false);
  }

  /**
   * @param includeFieldDocs whether field docs should be included in the field metadata
   *                         as {@code comment}, which spark displays as the column comment
   */
  public static String convertToSparkSchemaJson(HoodieSchema schema, boolean includeFieldDocs) {
    return convertToSparkSchemaJson(schema, includeFieldDocs, true);
  }

  private static String convertToSparkSchemaJson(HoodieSchema schema, boolean includeFieldDocs, boolean topLevel) {
    // Fields of the "null" type always hold null and are not written to the data files, so they are skipped
    String fieldsJsonString = schema.getFields().stream()
        .filter(field -> !field.schema().isSchemaNull())
        .map(field -> {
          String metadata = convertFieldMetadata(field, includeFieldDocs, topLevel);
          return "{\"name\":\"" + field.name() + "\",\"type\":" + convertFieldType(field.getNonNullSchema(), includeFieldDocs)
              + ",\"nullable\":" + field.isNullable() + ",\"metadata\":" + metadata + "}";
        })
        .reduce((a, b) -> a + "," + b).orElse("");
    return "{\"type\":\"struct\",\"fields\":[" + fieldsJsonString + "]}";
  }

  private static String convertFieldMetadata(HoodieSchemaField field, boolean includeFieldDocs, boolean topLevel) {
    List<String> entries = new ArrayList<>(3);
    if (includeFieldDocs) {
      field.doc().ifPresent(doc -> {
        if (!doc.isEmpty()) {
          entries.add("\"comment\":\"" + escapeJsonString(doc) + "\"");
        }
      });
    }
    if (field.getNonNullSchema().isBlobField()) {
      entries.add(String.format("\"%s\":\"%s\"", HoodieSchema.TYPE_METADATA_FIELD, HoodieSchemaType.BLOB.name()));
    }
    if (topLevel && containsChar(field.getNonNullSchema())) {
      // as Spark's CharVarcharUtils.replaceCharVarcharWithStringInSchema writes it: the column's whole type, on the
      // top-level column only
      entries.add("\"" + CHAR_VARCHAR_TYPE_STRING_METADATA_KEY + "\":\"" + escapeJsonString(catalogString(field.getNonNullSchema())) + "\"");
    }
    return "{" + String.join(",", entries) + "}";
  }

  private static boolean containsChar(HoodieSchema originalSchema) {
    HoodieSchema schema = originalSchema.getNonNullType();
    switch (schema.getType()) {
      case CHAR:
        return true;
      case ARRAY:
        return containsChar(schema.getElementType());
      case MAP:
        return containsChar(schema.getValueType());
      case RECORD:
        return schema.getFields().stream().anyMatch(f -> !f.schema().isSchemaNull() && containsChar(f.schema()));
      default:
        return false;
    }
  }

  /**
   * Spark's {@code DataType.catalogString} of the type {@link #convertFieldType} writes, with CHAR(n) as
   * {@code char(n)}.
   */
  private static String catalogString(HoodieSchema originalSchema) {
    HoodieSchema schema = originalSchema.getNonNullType();
    switch (schema.getType()) {
      case BOOLEAN:
      case FLOAT:
      case DOUBLE:
      case DATE:
        return schema.getType().name().toLowerCase(Locale.ROOT);
      case INT:
        return "int";
      case INT8:
        return "tinyint";
      case INT16:
        return "smallint";
      case LONG:
        return "bigint";
      case STRING:
      case ENUM:
      case UUID:
        return "string";
      case CHAR:
        return "char(" + ((HoodieSchema.Char) schema).getLength() + ")";
      case BYTES:
      case FIXED:
      case VECTOR:
        return "binary";
      case TIMESTAMP:
        return ((HoodieSchema.Timestamp) schema).isUtcAdjusted() ? "timestamp" : "timestamp_ntz";
      case TIME:
        return ((HoodieSchema.Time) schema).getPrecision() == HoodieSchema.TimePrecision.MILLIS ? "int" : "bigint";
      case DECIMAL:
        HoodieSchema.Decimal decimal = (HoodieSchema.Decimal) schema;
        return "decimal(" + decimal.getPrecision() + "," + decimal.getScale() + ")";
      case ARRAY:
        return "array<" + catalogString(schema.getElementType()) + ">";
      case MAP:
        return "map<" + catalogString(schema.getKeyType()) + "," + catalogString(schema.getValueType()) + ">";
      case RECORD:
      case BLOB:
      case VARIANT:
        return schema.getFields().stream()
            .filter(f -> !f.schema().isSchemaNull())
            .map(f -> quoteIfNeeded(f.name()) + ":" + catalogString(f.schema()))
            .collect(Collectors.joining(",", "struct<", ">"));
      default:
        throw new UnsupportedOperationException("Cannot convert " + schema.getType() + " to spark sql type");
    }
  }

  /**
   * Spark's {@code QuotingUtils.quoteIfNeeded}.
   */
  private static String quoteIfNeeded(String name) {
    return name.matches("[a-zA-Z0-9_]+") && !name.matches("\\d+") ? name : "`" + name.replace("`", "``") + "`";
  }

  private static String escapeJsonString(String value) {
    StringBuilder sb = new StringBuilder(value.length());
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      switch (c) {
        case '"':
          sb.append("\\\"");
          break;
        case '\\':
          sb.append("\\\\");
          break;
        case '\n':
          sb.append("\\n");
          break;
        case '\r':
          sb.append("\\r");
          break;
        case '\t':
          sb.append("\\t");
          break;
        default:
          if (c < 0x20) {
            sb.append(String.format("\\u%04x", (int) c));
          } else {
            sb.append(c);
          }
      }
    }
    return sb.toString();
  }

  private static String convertFieldType(HoodieSchema originalFieldSchema, boolean includeFieldDocs) {
    HoodieSchema fieldSchema = originalFieldSchema.getNonNullType();
    switch (fieldSchema.getType()) {
      case BOOLEAN: return "\"boolean\"";
      case FLOAT: return "\"float\"";
      case DOUBLE: return "\"double\"";
      case INT:
        return "\"integer\"";
      // no table has INT8 / INT16 / CHAR columns before RFC-88 step 2, so no synced column changes type
      case INT8:
        return "\"byte\"";
      case INT16:
        return "\"short\"";
      case LONG:
        return "\"long\"";
      case STRING:
      case ENUM:
      case UUID:
        return "\"string\"";
      case CHAR:
        // Spark 3.1+ keeps CHAR(n) in spark.sql.sources.schema as a string column with the column's type in its
        // metadata (CharVarcharUtils), see convertFieldMetadata
        return "\"string\"";
      case BYTES:
      case FIXED:
      case VECTOR:
        return "\"binary\"";
      case DATE:
        return "\"date\"";
      case TIMESTAMP:
        HoodieSchema.Timestamp timestampSchema = (HoodieSchema.Timestamp) fieldSchema;
        if (timestampSchema.isUtcAdjusted()) {
          return "\"timestamp\"";
        } else {
          return "\"timestamp_ntz\"";
        }
      case TIME:
        HoodieSchema.Time timeSchema = (HoodieSchema.Time) fieldSchema;
        if (timeSchema.getPrecision() == HoodieSchema.TimePrecision.MILLIS) {
          return "\"integer\"";
        } else {
          return "\"long\"";
        }
      case DECIMAL:
        HoodieSchema.Decimal decimal = (HoodieSchema.Decimal) fieldSchema;
        return "\"decimal(" + decimal.getPrecision() + "," + decimal.getScale() + ")\"";
      case ARRAY:
        return arrayType(fieldSchema.getElementType(), includeFieldDocs);
      case MAP:
        HoodieSchema keyType = fieldSchema.getKeyType();
        HoodieSchema valueType = fieldSchema.getValueType();
        boolean valueOptional = valueType.isNullable();
        return "{\"type\":\"map\", \"keyType\":" + convertFieldType(keyType, includeFieldDocs)
            + ",\"valueType\":" + convertFieldType(valueType, includeFieldDocs)
            + ",\"valueContainsNull\":" + valueOptional + "}";
      case RECORD:
      case BLOB:
      case VARIANT:
        return convertToSparkSchemaJson(fieldSchema, includeFieldDocs, false);
      default:
        throw new UnsupportedOperationException("Cannot convert " + fieldSchema.getType() + " to spark sql type");
    }
  }

  private static String arrayType(HoodieSchema elementType, boolean includeFieldDocs) {
    return "{\"type\":\"array\", \"elementType\":" + convertFieldType(elementType, includeFieldDocs) + ",\"containsNull\":" + elementType.isNullable() + "}";
  }
}
