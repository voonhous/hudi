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

package org.apache.hudi.gcp.bigquery;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.schema.mapping.HoodieSchemaEquivalence;
import org.apache.hudi.common.schema.mapping.HoodieSchemaHints;
import org.apache.hudi.common.schema.mapping.MappingOptions;
import org.apache.hudi.common.schema.mapping.TypeSupport;
import org.apache.hudi.common.schema.mapping.TypeSystemMapper;
import org.apache.hudi.common.schema.mapping.TypeSystems;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.StandardSQLTypeName;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * {@link TypeSystemMapper} for BigQuery (RFC-88, type plane). A type is a BigQuery {@link Field}: its standard SQL
 * type, mode (REQUIRED, NULLABLE, REPEATED for arrays) and sub-fields. A record maps to a REQUIRED STRUCT field named
 * after the record, whose sub-fields are the table columns.
 *
 * <p>{@code HoodieSchema} to BigQuery delegates to {@link BigQuerySchemaResolver#convertSchema}, wrapping a single
 * type in a one-column record. BigQuery to {@code HoodieSchema} has no converter today and is implemented here,
 * minimally: it inverts what the resolver writes (REPEATED fields as arrays, the {@code key_value} STRUCT as a map)
 * and reads BigQuery's own types at their natural precision.
 *
 * <p>BigQuery fields have a description but the resolver writes no marker into it, so LOSSY types are restored from
 * the hint only.
 *
 * <p>Replaces, in RFC-88 step 2, the type switch in {@code BigQuerySchemaResolver}.
 */
public final class BigQueryTypeMapper implements TypeSystemMapper<Field> {

  private static final long serialVersionUID = 1L;

  private static final String WRAPPER_RECORD = "bigquery_type_mapper_column";
  private static final String WRAPPER_COLUMN = "col";
  // The names BigQuerySchemaResolver gives the parts of a map.
  private static final String MAP_ENTRIES = "key_value";
  private static final String MAP_KEY = "key";
  private static final String MAP_VALUE = "value";
  // BigQuery's unparameterized NUMERIC and BIGNUMERIC.
  private static final int NUMERIC_PRECISION = 38;
  private static final int NUMERIC_SCALE = 9;
  private static final int BIGNUMERIC_PRECISION = 76;
  private static final int BIGNUMERIC_SCALE = 38;

  @Override
  public String typeSystem() {
    return TypeSystems.BIGQUERY;
  }

  @Override
  public Field toTypeSystem(HoodieSchema schema, MappingOptions opts) {
    BigQuerySchemaResolver resolver = BigQuerySchemaResolver.getInstance();
    if (schema.getType() == HoodieSchemaType.RECORD) {
      return Field.newBuilder(schema.getName(), StandardSQLTypeName.STRUCT, resolver.convertSchema(schema).getFields())
          .setMode(Field.Mode.REQUIRED).build();
    }
    HoodieSchema record = HoodieSchema.createRecord(WRAPPER_RECORD, null, null,
        Collections.singletonList(HoodieSchemaField.of(WRAPPER_COLUMN, schema)));
    FieldList columns = resolver.convertSchema(record).getFields();
    if (columns.isEmpty()) {
      throw new HoodieException("BigQuerySchemaResolver skips columns of type " + schema.getType());
    }
    return columns.get(0);
  }

  @Override
  public HoodieSchema fromTypeSystem(Field type, Option<HoodieSchema> hint) {
    HoodieSchema converted = convert(type, null);
    return HoodieSchemaHints.restore(converted, hint, t -> support(t, MappingOptions.DEFAULT).isLossy(),
        n -> convert(toTypeSystem(n, MappingOptions.DEFAULT), null), HoodieSchemaEquivalence.Mode.LOGICAL);
  }

  private static HoodieSchema convert(Field field, String namespace) {
    HoodieSchema type = convertType(field, namespace);
    if (field.getMode() == Field.Mode.REPEATED) {
      // BigQuery arrays and their elements are never NULL.
      return HoodieSchema.createArray(type);
    }
    // An unset mode means NULLABLE in BigQuery.
    return field.getMode() == Field.Mode.REQUIRED ? type : HoodieSchema.createNullable(type);
  }

  private static HoodieSchema convertType(Field field, String namespace) {
    StandardSQLTypeName type = field.getType().getStandardType();
    switch (type) {
      case BOOL:
        return HoodieSchema.create(HoodieSchemaType.BOOLEAN);
      case INT64:
        return HoodieSchema.create(HoodieSchemaType.LONG);
      case FLOAT64:
        return HoodieSchema.create(HoodieSchemaType.DOUBLE);
      case STRING:
        return HoodieSchema.create(HoodieSchemaType.STRING);
      case BYTES:
        return HoodieSchema.create(HoodieSchemaType.BYTES);
      case DATE:
        return HoodieSchema.createDate();
      case TIME:
        return HoodieSchema.createTimeMicros();
      case TIMESTAMP:
        return HoodieSchema.createTimestampMicros();
      case DATETIME:
        return HoodieSchema.createLocalTimestampMicros();
      case NUMERIC:
        return decimal(field, NUMERIC_PRECISION, NUMERIC_SCALE);
      case BIGNUMERIC:
        return decimal(field, BIGNUMERIC_PRECISION, BIGNUMERIC_SCALE);
      case STRUCT:
        return isMap(field) ? convertMap(field, namespace) : convertRecord(field, namespace);
      default:
        // JSON, GEOGRAPHY, INTERVAL, RANGE
        throw new HoodieException("BigQuery type " + type + " of field " + field.getName() + " has no HoodieSchema type");
    }
  }

  private static HoodieSchema decimal(Field field, int defaultPrecision, int defaultScale) {
    int precision = field.getPrecision() == null ? defaultPrecision : field.getPrecision().intValue();
    int scale = field.getScale() == null ? (field.getPrecision() == null ? defaultScale : 0) : field.getScale().intValue();
    return HoodieSchema.createDecimal(precision, scale);
  }

  private static HoodieSchema convertRecord(Field field, String namespace) {
    String childNamespace = namespace == null ? field.getName() : namespace + "." + field.getName();
    List<HoodieSchemaField> fields = new ArrayList<>(field.getSubFields().size());
    for (Field subField : field.getSubFields()) {
      fields.add(HoodieSchemaField.of(subField.getName(), convert(subField, childNamespace)));
    }
    return HoodieSchema.createRecord(field.getName(), namespace, null, fields);
  }

  /**
   * Whether the field is the STRUCT BigQuerySchemaResolver writes for a map: a single REPEATED {@code key_value}
   * STRUCT of a STRING {@code key} and a {@code value}.
   */
  private static boolean isMap(Field field) {
    FieldList subFields = field.getSubFields();
    if (subFields == null || subFields.size() != 1) {
      return false;
    }
    Field entries = subFields.get(0);
    if (!MAP_ENTRIES.equals(entries.getName()) || entries.getMode() != Field.Mode.REPEATED
        || entries.getType().getStandardType() != StandardSQLTypeName.STRUCT || entries.getSubFields().size() != 2) {
      return false;
    }
    Field key = entries.getSubFields().get(0);
    return MAP_KEY.equals(key.getName()) && key.getType().getStandardType() == StandardSQLTypeName.STRING
        && MAP_VALUE.equals(entries.getSubFields().get(1).getName());
  }

  private static HoodieSchema convertMap(Field field, String namespace) {
    String childNamespace = namespace == null ? field.getName() : namespace + "." + field.getName();
    return HoodieSchema.createMap(convert(field.getSubFields().get(0).getSubFields().get(1), childNamespace));
  }

  @Override
  public TypeSupport support(HoodieSchemaType type, MappingOptions opts) {
    switch (type) {
      case INT:
        return TypeSupport.lossy("INT stored as INT64");
      case FLOAT:
        return TypeSupport.lossy("FLOAT stored as FLOAT64");
      case FIXED:
        return TypeSupport.lossy("FIXED stored as BYTES; the size is dropped");
      case ENUM:
        return TypeSupport.lossy("ENUM stored as STRING; the symbols are dropped");
      case UUID:
        return TypeSupport.lossy("UUID stored as STRING");
      case TIME:
        return TypeSupport.lossy("TIME(MILLIS) stored as TIME, which has microsecond precision; only TIME(MICROS) is exact");
      case TIMESTAMP:
        return TypeSupport.lossy("TIMESTAMP(MILLIS) stored as TIMESTAMP, which has microsecond precision; LOCAL_TIMESTAMP(MILLIS) and "
            + "LOCAL_TIMESTAMP(MICROS) stored as INT64; only TIMESTAMP(MICROS) is exact");
      case MAP:
        return TypeSupport.lossy("MAP stored as a NULLABLE STRUCT holding a REPEATED key_value STRUCT; BigQuery has no map type");
      case VARIANT:
        return TypeSupport.lossy("VARIANT stored as a STRUCT of its metadata and value BYTES");
      case BLOB:
        return TypeSupport.lossy("BLOB stored as a STRUCT of its fields");
      case VECTOR:
        return TypeSupport.lossy("VECTOR stored as BYTES; the dimension and element type are dropped");
      case DECIMAL:
        return TypeSupport.unsupported("every DECIMAL(p, s) is stored as NUMERIC without precision or scale; NUMERIC holds at most "
            + "scale 9 and 29 integer digits, so values with s > 9 or p - s > 29 are not stored exactly");
      case UNION:
        return TypeSupport.unsupported("BigQuerySchemaResolver accepts only a union of one type and null");
      case NULL:
        return TypeSupport.unsupported("BigQuery has no null type; BigQuerySchemaResolver skips NULL columns");
      default:
        // BOOLEAN (BOOL), LONG (INT64), DOUBLE (FLOAT64), STRING, BYTES, DATE, RECORD (STRUCT), ARRAY (REPEATED).
        return TypeSupport.nativeType();
    }
  }
}
