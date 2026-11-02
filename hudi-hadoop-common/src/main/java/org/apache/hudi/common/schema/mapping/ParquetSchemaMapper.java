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

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaRepair;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.util.Option;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.avro.AvroWriteSupport;
import org.apache.parquet.avro.HoodieAvroParquetSchemaConverter;
import org.apache.parquet.schema.MessageType;

import java.util.Collections;

/**
 * {@link FormatSchemaMapper} for Parquet (RFC-88, type plane). Wraps the parquet-avro path Hudi uses today:
 * {@link HoodieAvroParquetSchemaConverter#getAvroSchemaConverter(Configuration)}, which picks
 * {@code AvroSchemaConverterWithTimestampNTZ} (or {@code NativeAvroSchemaConverter} on parquet-java older than
 * 1.11, where the former cannot load). It is the converter behind {@code ParquetTableSchemaResolver
 * .convertAvroSchemaToParquet}, the Avro Parquet writer ({@code HoodieAvroFileWriterFactory}) and
 * {@code ParquetUtils.readSchema} / {@code HoodieAvroParquetReader.getSchema}.
 *
 * <p>{@link #fromPhysicalSchema} converts the file schema, then, with a hint, applies
 * {@link HoodieSchemaRepair#repairLogicalTypes} the way {@code HoodieAvroParquetReader} does and finally
 * {@link HoodieSchemaHints#restore} for the LOSSY types.
 *
 * <p>Options are the parquet-avro Hadoop keys the write side reads today, with the same defaults, so a caller can
 * pass its Hadoop configuration through: {@link #WRITE_OLD_LIST_STRUCTURE}, {@link #WRITE_PARQUET_UUID} and
 * {@link #WRITE_FIXED_AS_INT96}. There is no timestamp NTZ key: local timestamps are always written as
 * {@code TIMESTAMP(isAdjustedToUTC=false)} by {@code AvroSchemaConverterWithTimestampNTZ}. The read side is fixed to
 * what Hudi's readers configure ({@code parquet.avro.add-list-element-records=false}, INT96 not readable), since
 * {@link #fromPhysicalSchema} takes no options.
 *
 * <p>Replaces later: the parquet-avro converter copy, Flink's {@code ParquetSchemaConverter} and the Spark row
 * writer's own derivation in {@code HoodieRowParquetWriteSupport}.
 */
public final class ParquetSchemaMapper implements FormatSchemaMapper<MessageType> {

  private static final long serialVersionUID = 1L;

  /**
   * Writes lists in the legacy 2-level layout ({@code repeated <type> array}) instead of the 3-level
   * {@code list / element} layout. Default {@value #WRITE_OLD_LIST_STRUCTURE_DEFAULT}, parquet-avro's default, which
   * the Avro writer path keeps unless the Hadoop configuration sets it.
   */
  public static final String WRITE_OLD_LIST_STRUCTURE = AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE;
  public static final boolean WRITE_OLD_LIST_STRUCTURE_DEFAULT = true;

  /**
   * Writes UUID as {@code FIXED_LEN_BYTE_ARRAY(16) + UUID} instead of {@code BINARY + STRING}. Default
   * {@value #WRITE_PARQUET_UUID_DEFAULT}.
   */
  public static final String WRITE_PARQUET_UUID = AvroWriteSupport.WRITE_PARQUET_UUID;
  public static final boolean WRITE_PARQUET_UUID_DEFAULT = false;

  /**
   * Comma-separated dotted paths of {@code fixed(12)} fields to write as {@code INT96}. Default: none.
   */
  public static final String WRITE_FIXED_AS_INT96 = "parquet.avro.writeFixedAsInt96";

  @Override
  public String format() {
    return FileFormats.PARQUET;
  }

  @Override
  public MessageType toPhysicalSchema(HoodieSchema schema, PhysicalOptions opts) {
    Configuration conf = new Configuration(false);
    conf.setBoolean(WRITE_OLD_LIST_STRUCTURE, opts.getBoolean(WRITE_OLD_LIST_STRUCTURE, WRITE_OLD_LIST_STRUCTURE_DEFAULT));
    conf.setBoolean(WRITE_PARQUET_UUID, opts.getBoolean(WRITE_PARQUET_UUID, WRITE_PARQUET_UUID_DEFAULT));
    opts.get(WRITE_FIXED_AS_INT96).ifPresent(paths -> conf.set(WRITE_FIXED_AS_INT96, paths));
    return HoodieAvroParquetSchemaConverter.getAvroSchemaConverter(conf).convert(schema);
  }

  @Override
  public HoodieSchema fromPhysicalSchema(MessageType physical, Option<HoodieSchema> hint) {
    HoodieSchema fileSchema = convertFileSchema(physical);
    if (!hint.isPresent()) {
      return fileSchema;
    }
    HoodieSchema repaired = HoodieSchemaRepair.repairLogicalTypes(fileSchema, hint.get());
    return HoodieSchemaHints.restore(repaired, hint, t -> support(t).isLossy(), this::image, HoodieSchemaEquivalence.Mode.PHYSICAL);
  }

  @Override
  public TypeSupport support(HoodieSchemaType type, PhysicalOptions opts) {
    switch (type) {
      case UUID:
        return opts.getBoolean(WRITE_PARQUET_UUID, WRITE_PARQUET_UUID_DEFAULT)
            ? TypeSupport.nativeType()
            : TypeSupport.lossy("UUID written as BINARY + STRING and read back as STRING; the UUID annotation needs " + WRITE_PARQUET_UUID + "=true");
      case ENUM:
        return TypeSupport.lossy("ENUM written as BINARY + ENUM, which keeps no symbols; read back as STRING");
      case UNION:
        return TypeSupport.lossy("A union with more than one non-null branch is written as a group of optional member0..memberN fields; read back as RECORD");
      case VARIANT:
        return TypeSupport.lossy("VARIANT written as a group of metadata and value (VARIANT annotation only on parquet-java 1.16+); read back as RECORD");
      case BLOB:
        return TypeSupport.lossy("BLOB written as a plain group; read back as RECORD");
      case VECTOR:
        // The hoodie.vector.columns footer key restores VECTOR without a hint, but a MessageType does not carry the
        // footer, so no marker is declared here.
        return TypeSupport.lossy("VECTOR written as bare FIXED_LEN_BYTE_ARRAY(dimension * element size); read back as FIXED");
      case FIXED:
        return opts.get(WRITE_FIXED_AS_INT96).map(p -> !p.trim().isEmpty()).orElse(false)
            ? TypeSupport.lossy("fixed(12) fields listed in " + WRITE_FIXED_AS_INT96 + " are written as INT96, which the read side "
                + "rejects unless parquet.avro.readInt96AsFixed is set")
            : TypeSupport.nativeType();
      case NULL:
        return TypeSupport.unsupported("Parquet has no null type; the converter drops NULL fields from the MessageType");
      default:
        return TypeSupport.nativeType();
    }
  }

  /**
   * File schema to {@link HoodieSchema}, with the list handling Hudi's Parquet readers set (see
   * {@code HoodieAvroParquetReader.tryOverrideDefaultConfigs}): a repeated group is a list element record only in
   * the 2-level layout, so both list layouts read back as the same array.
   */
  private static HoodieSchema convertFileSchema(MessageType physical) {
    Configuration conf = new Configuration(false);
    conf.setBoolean(AvroSchemaConverter.ADD_LIST_ELEMENT_RECORDS, false);
    return HoodieAvroParquetSchemaConverter.getAvroSchemaConverter(conf).convert(physical);
  }

  /**
   * What a single column type reads back as without a hint, for {@link HoodieSchemaHints#restore}.
   */
  private HoodieSchema image(HoodieSchema column) {
    HoodieSchema record = HoodieSchema.createRecord("image", null, null,
        Collections.singletonList(HoodieSchemaField.of("col", column)));
    HoodieSchema converted = convertFileSchema(toPhysicalSchema(record, PhysicalOptions.DEFAULT));
    return converted.getFields().isEmpty() ? null : converted.getFields().get(0).schema();
  }
}
