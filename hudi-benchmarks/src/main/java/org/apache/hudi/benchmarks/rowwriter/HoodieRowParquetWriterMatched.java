/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.benchmarks.rowwriter;

import org.apache.hudi.SparkAdapterSupport$;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.spark.sql.execution.datasources.DataSourceUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import scala.Enumeration;
import scala.Function1;

/**
 * Experimental control: the prototype writer reading through {@link HoodieRow}, but with exactly the
 * value conversions and structure of today's {@code HoodieRowParquetWriteSupport}: strings copied with
 * {@code getBinary}, every decimal through {@code BigDecimal} bytes, dates and timestamps through
 * Spark's rebase functions, and a capturing {@code Runnable} per message, group and field. The only
 * difference left from today's writer is the HoodieRow boundary.
 */
public final class HoodieRowParquetWriterMatched extends WriteSupport<HoodieRow> {

  @FunctionalInterface
  private interface FieldWriter {
    void write(HoodieGetters getters, int ordinal);
  }

  private final Enumeration.Value rebaseMode =
      (Enumeration.Value) SparkAdapterSupport$.MODULE$.sparkAdapter().getDateTimeRebaseMode();
  private final Function1<Object, Object> dateRebase = DataSourceUtils.createDateRebaseFuncInWrite(rebaseMode, "Parquet");
  private final Function1<Object, Object> timestampRebase = DataSourceUtils.createTimestampRebaseFuncInWrite(rebaseMode, "Parquet");

  private final MessageType schema;
  private final Map<String, String> extraMetadata;
  private final String[] rootNames;
  private final FieldWriter[] rootWriters;
  private RecordConsumer consumer;

  public HoodieRowParquetWriterMatched(MessageType schema, Map<String, String> extraMetadata) {
    this.schema = schema;
    this.extraMetadata = extraMetadata;
    this.rootNames = names(schema);
    this.rootWriters = writers(schema);
  }

  @Override
  public WriteContext init(Configuration configuration) {
    return new WriteContext(schema, extraMetadata);
  }

  @Override
  public void prepareForWrite(RecordConsumer recordConsumer) {
    this.consumer = recordConsumer;
  }

  @Override
  public void write(HoodieRow row) {
    consumeMessage(() -> writeFields(row, rootNames, rootWriters));
  }

  @Override
  public FinalizedWriteContext finalizeWrite() {
    return new FinalizedWriteContext(Collections.emptyMap());
  }

  private void consumeMessage(Runnable writer) {
    consumer.startMessage();
    writer.run();
    consumer.endMessage();
  }

  private void consumeGroup(Runnable writer) {
    consumer.startGroup();
    writer.run();
    consumer.endGroup();
  }

  private void consumeField(String field, int index, Runnable writer) {
    consumer.startField(field, index);
    writer.run();
    consumer.endField(field, index);
  }

  private void writeFields(HoodieGetters getters, String[] names, FieldWriter[] writers) {
    for (int i = 0; i < writers.length; i++) {
      int index = i;
      if (!getters.isNullAt(i)) {
        consumeField(names[i], index, () -> writers[index].write(getters, index));
      }
    }
  }

  private static String[] names(GroupType group) {
    List<Type> fields = group.getFields();
    String[] names = new String[fields.size()];
    for (int i = 0; i < names.length; i++) {
      names[i] = fields.get(i).getName();
    }
    return names;
  }

  private FieldWriter[] writers(GroupType group) {
    List<Type> fields = group.getFields();
    FieldWriter[] writers = new FieldWriter[fields.size()];
    for (int i = 0; i < writers.length; i++) {
      writers[i] = makeWriter(fields.get(i));
    }
    return writers;
  }

  private FieldWriter makeWriter(Type type) {
    if (type.isPrimitive()) {
      return makePrimitiveWriter(type.asPrimitiveType());
    }
    GroupType group = type.asGroupType();
    if (group.getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation) {
      GroupType repeated = group.getType(0).asGroupType();
      String repeatedName = repeated.getName();
      String elementName = repeated.getType(0).getName();
      FieldWriter elementWriter = makeWriter(repeated.getType(0));
      return (getters, ordinal) -> {
        HoodieArray array = getters.getArray(ordinal);
        consumeGroup(() -> {
          if (array.numElements() > 0) {
            consumeField(repeatedName, 0, () -> {
              for (int j = 0; j < array.numElements(); j++) {
                int index = j;
                consumeGroup(() -> {
                  if (!array.isNullAt(index)) {
                    consumeField(elementName, 0, () -> elementWriter.write(array, index));
                  }
                });
              }
            });
          }
        });
      };
    }
    String[] names = names(group);
    FieldWriter[] writers = writers(group);
    int numFields = names.length;
    return (getters, ordinal) -> {
      HoodieRow struct = getters.getRow(ordinal, numFields);
      consumeGroup(() -> writeFields(struct, names, writers));
    };
  }

  private FieldWriter makePrimitiveWriter(PrimitiveType type) {
    LogicalTypeAnnotation annotation = type.getLogicalTypeAnnotation();
    switch (type.getPrimitiveTypeName()) {
      case BOOLEAN:
        return (getters, ordinal) -> consumer.addBoolean(getters.getBoolean(ordinal));
      case INT32:
        if (annotation instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation) {
          return (getters, ordinal) -> consumer.addInteger((Integer) dateRebase.apply(getters.getInt(ordinal)));
        }
        return (getters, ordinal) -> consumer.addInteger(getters.getInt(ordinal));
      case INT64:
        if (annotation instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) {
          return (getters, ordinal) -> consumer.addLong((long) timestampRebase.apply(getters.getLong(ordinal)));
        }
        return (getters, ordinal) -> consumer.addLong(getters.getLong(ordinal));
      case FLOAT:
        return (getters, ordinal) -> consumer.addFloat(getters.getFloat(ordinal));
      case DOUBLE:
        return (getters, ordinal) -> consumer.addDouble(getters.getDouble(ordinal));
      case BINARY:
        return (getters, ordinal) -> consumer.addBinary(Binary.fromReusedByteArray(getters.getBinary(ordinal)));
      case FIXED_LEN_BYTE_ARRAY:
        if (annotation instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
          LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimal =
              (LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) annotation;
          int precision = decimal.getPrecision();
          int scale = decimal.getScale();
          int numBytes = type.getTypeLength();
          byte[] buffer = new byte[numBytes];
          return (getters, ordinal) -> {
            byte[] bytes = getters.getDecimalUnscaledBytes(ordinal, precision, scale);
            byte[] padded = bytes;
            if (bytes.length != numBytes) {
              byte sign = bytes[0] < 0 ? (byte) -1 : (byte) 0;
              Arrays.fill(buffer, 0, numBytes - bytes.length, sign);
              System.arraycopy(bytes, 0, buffer, numBytes - bytes.length, bytes.length);
              padded = buffer;
            }
            consumer.addBinary(Binary.fromReusedByteArray(padded, 0, numBytes));
          };
        }
        throw new UnsupportedOperationException("Unsupported fixed type: " + type);
      default:
        throw new UnsupportedOperationException("Unsupported type: " + type);
    }
  }
}
