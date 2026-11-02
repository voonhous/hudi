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

package org.apache.hudi.common.schema;

import org.apache.hudi.common.schema.HoodieSchema.VariantLogicalType;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

/**
 * Enumeration of all schema types supported by Hudi schema system.
 *
 * <p>This enum wraps Avro's Schema.Type to provide a consistent interface
 * for schema type handling while maintaining binary compatibility with Avro.
 * Each Hudi schema type corresponds directly to an Avro schema type.</p>
 *
 * <p>Usage example:
 * <pre>{@code
 * HoodieSchemaType type = HoodieSchemaType.STRING;
 * Schema.Type avroType = type.toAvroType();
 * HoodieSchemaType backToHudi = HoodieSchemaType.fromAvroType(avroType);
 * }</pre></p>
 *
 * @since 1.2.0
 */
public enum HoodieSchemaType {

  /**
   * Record type - represents a collection of named fields
   */
  RECORD(Schema.Type.RECORD),

  /**
   * Enum type - represents a symbolic name from a predefined set
   */
  ENUM(Schema.Type.ENUM),

  /**
   * Array type - represents an ordered collection of objects
   */
  ARRAY(Schema.Type.ARRAY),

  /**
   * Map type - represents an associative array of key-value pairs
   */
  MAP(Schema.Type.MAP),

  /**
   * Union type - represents one of several possible schemas
   */
  UNION(Schema.Type.UNION),

  /**
   * Fixed type - represents a fixed-length byte array
   */
  FIXED(Schema.Type.FIXED),

  /**
   * String type - represents Unicode character sequences
   */
  STRING(Schema.Type.STRING),

  /**
   * Bytes type - represents arbitrary byte sequences
   */
  BYTES(Schema.Type.BYTES),

  /**
   * Integer type - represents 32-bit signed integers
   */
  INT(Schema.Type.INT),

  /**
   * Long type - represents 64-bit signed integers
   */
  LONG(Schema.Type.LONG),

  /**
   * Float type - represents single precision floating point numbers
   */
  FLOAT(Schema.Type.FLOAT),

  /**
   * Double type - represents double precision floating point numbers
   */
  DOUBLE(Schema.Type.DOUBLE),

  /**
   * Boolean type - represents true or false values
   */
  BOOLEAN(Schema.Type.BOOLEAN),

  DECIMAL(Schema.Type.BYTES),

  TIME(Schema.Type.INT),

  TIMESTAMP(Schema.Type.LONG),

  DATE(Schema.Type.INT),

  UUID(Schema.Type.STRING),

  VARIANT(Schema.Type.RECORD),

  /**
   * BLOB type - represents binary large objects, stored as records with either inline bytes or a reference to a file and range within that file.
   */
  BLOB(Schema.Type.RECORD),

  VECTOR(Schema.Type.FIXED),

  /**
   * Null type - represents the absence of a value
   */
  NULL(Schema.Type.NULL),

  // The types below are appended, not placed next to INT and STRING, so the ordinals of the types above stay as they
  // were.

  /**
   * 8-bit signed integer (SQL TINYINT): Avro {@code int} with Hudi logical type {@code int8}. Values are
   * {@link Integer}s in the range of a byte, exactly like {@link #INT}.
   */
  INT8(Schema.Type.INT),

  /**
   * 16-bit signed integer (SQL SMALLINT): Avro {@code int} with Hudi logical type {@code int16}. Values are
   * {@link Integer}s in the range of a short, exactly like {@link #INT}.
   */
  INT16(Schema.Type.INT),

  /**
   * Fixed-length character string, {@code CHAR(n)}: Avro {@code string} with Hudi logical type {@code char} and a
   * {@code length} property. Values are strings, exactly like {@link #STRING}; the value keeps whatever padding it
   * has. See {@link HoodieSchema.Char}.
   */
  CHAR(Schema.Type.STRING);

  private final Schema.Type avroType;

  HoodieSchemaType(Schema.Type avroType) {
    this.avroType = avroType;
  }

  /**
   * Converts an Avro schema to the corresponding Hudi schema type.
   *
   * @param avroSchema the Avro schema to convert
   * @return the equivalent HoodieSchemaType
   * @throws IllegalArgumentException if the Avro type is not supported
   */
  public static HoodieSchemaType fromAvro(Schema avroSchema) {
    if (avroSchema == null) {
      throw new IllegalArgumentException("Null schema provided");
    }
    LogicalType logicalType = avroSchema.getLogicalType();
    if (logicalType != null) {
      if (logicalType instanceof LogicalTypes.Decimal) {
        return DECIMAL;
      } else if (logicalType instanceof LogicalTypes.TimeMillis || logicalType instanceof LogicalTypes.TimeMicros) {
        return TIME;
      } else if (logicalType instanceof LogicalTypes.TimestampMillis || logicalType instanceof LogicalTypes.TimestampMicros
          || logicalType instanceof LogicalTypes.LocalTimestampMillis || logicalType instanceof LogicalTypes.LocalTimestampMicros) {
        return TIMESTAMP;
      } else if (logicalType instanceof LogicalTypes.Date) {
        return DATE;
      } else if (logicalType == LogicalTypes.uuid()) {
        return UUID;
      } else if (logicalType == VariantLogicalType.variant()) {
        return VARIANT;
      } else if (logicalType == HoodieSchema.BlobLogicalType.blob()) {
        return BLOB;
      } else if (logicalType instanceof HoodieSchema.VectorLogicalType) {
        return VECTOR;
      } else if (logicalType == HoodieSchema.IntLogicalType.int8()) {
        return INT8;
      } else if (logicalType == HoodieSchema.IntLogicalType.int16()) {
        return INT16;
      } else if (logicalType instanceof HoodieSchema.CharLogicalType) {
        return CHAR;
      }
    }
    switch (avroSchema.getType()) {
      case RECORD:
        return RECORD;
      case ENUM:
        return ENUM;
      case ARRAY:
        return ARRAY;
      case MAP:
        return MAP;
      case UNION:
        return UNION;
      case FIXED:
        return FIXED;
      case STRING:
        return STRING;
      case BYTES:
        return BYTES;
      case INT:
        return INT;
      case LONG:
        return LONG;
      case FLOAT:
        return FLOAT;
      case DOUBLE:
        return DOUBLE;
      case BOOLEAN:
        return BOOLEAN;
      case NULL:
        return NULL;
      default:
        throw new IllegalArgumentException("Unsupported Avro schema type: " + avroSchema.getType());
    }
  }

  /**
   * Converts this Hudi schema type to the corresponding Avro schema type.
   *
   * @return the equivalent Avro Schema.Type
   */
  public Schema.Type toAvroType() {
    return avroType;
  }

  /**
   * Checks if this schema type represents a primitive type.
   *
   * @return true if this type is a primitive type (not RECORD, ENUM, ARRAY, MAP, or UNION)
   */
  public boolean isPrimitive() {
    return !isComplex();
  }

  /**
   * Checks if this schema type represents a complex type.
   *
   * @return true if this type is a complex type (RECORD, ENUM, ARRAY, MAP, or UNION)
   */
  public boolean isComplex() {
    switch (this) {
      case RECORD:
      case ENUM:
      case ARRAY:
      case MAP:
      case UNION:
      case VARIANT:
      case BLOB:
      case VECTOR:
        return true;
      default:
        return false;
    }
  }

  /**
   * Checks if this schema type is a signed integer that is narrower than {@link #INT}: {@link #INT8} or
   * {@link #INT16}. Their values are {@link Integer}s, exactly like INT's, so most value handling treats them as INT.
   *
   * @return true for INT8 and INT16
   */
  public boolean isNarrowInt() {
    return this == INT8 || this == INT16;
  }

  /**
   * Checks if this schema type's values are Avro {@code int}s handled as plain integers: {@link #INT},
   * {@link #INT8} or {@link #INT16}. DATE and TIME(millis) are also Avro {@code int}s but are not included.
   *
   * @return true for INT, INT8 and INT16
   */
  public boolean isIntLike() {
    return this == INT || isNarrowInt();
  }

  /**
   * Checks if this schema type's values are plain strings: {@link #STRING} or {@link #CHAR}. UUID and ENUM are not
   * included.
   *
   * @return true for STRING and CHAR
   */
  public boolean isStringLike() {
    return this == STRING || this == CHAR;
  }

  /**
   * Checks if this schema type represents a numeric type.
   *
   * @return true if this type is INT8, INT16, INT, LONG, FLOAT, DOUBLE or DECIMAL
   */
  public boolean isNumeric() {
    switch (this) {
      case INT8:
      case INT16:
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case DECIMAL:
        return true;
      default:
        return false;
    }
  }
}
