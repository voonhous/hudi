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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Flink rows for the same four workloads, materialized as BinaryRowData. MIXED has no SMALLINT column:
 * Hudi's Flink writer cannot write one today (see SmallIntCheck).
 */
public final class FlinkWorkloads {
  private static final double NULL_RATE = 0.02;
  private static final long TS_BASE_MILLIS = 1_700_000_000_000L;
  private static final int DATE_BASE_DAYS = 19_000;

  private FlinkWorkloads() {
  }

  public static RowType rowType(Workloads.Kind kind) {
    List<DataTypes.Field> fields = new ArrayList<>();
    fields.add(DataTypes.FIELD("id", DataTypes.BIGINT().notNull()));
    switch (kind) {
      case DECIMAL:
        for (int i = 0; i < 8; i++) {
          fields.add(DataTypes.FIELD("d" + i, DataTypes.DECIMAL(10, 2)));
        }
        fields.add(DataTypes.FIELD("w0", DataTypes.DECIMAL(30, 4)));
        fields.add(DataTypes.FIELD("w1", DataTypes.DECIMAL(30, 4)));
        break;
      case STRING:
        for (int i = 0; i < 8; i++) {
          fields.add(DataTypes.FIELD("s" + i, DataTypes.STRING()));
        }
        break;
      case NESTED:
        fields.add(DataTypes.FIELD("info", DataTypes.ROW(
            DataTypes.FIELD("a", DataTypes.INT()),
            DataTypes.FIELD("b", DataTypes.STRING()),
            DataTypes.FIELD("c", DataTypes.DECIMAL(10, 2)))));
        fields.add(DataTypes.FIELD("items", DataTypes.ARRAY(DataTypes.ROW(
            DataTypes.FIELD("x", DataTypes.INT()),
            DataTypes.FIELD("y", DataTypes.STRING())))));
        fields.add(DataTypes.FIELD("tags", DataTypes.ARRAY(DataTypes.STRING())));
        break;
      case MIXED:
        fields.add(DataTypes.FIELD("num", DataTypes.INT()));
        fields.add(DataTypes.FIELD("big", DataTypes.BIGINT()));
        fields.add(DataTypes.FIELD("score", DataTypes.DOUBLE()));
        fields.add(DataTypes.FIELD("name", DataTypes.STRING()));
        fields.add(DataTypes.FIELD("price", DataTypes.DECIMAL(10, 2)));
        fields.add(DataTypes.FIELD("ts", DataTypes.TIMESTAMP(6)));
        fields.add(DataTypes.FIELD("day", DataTypes.DATE()));
        fields.add(DataTypes.FIELD("labels", DataTypes.ARRAY(DataTypes.STRING())));
        break;
      default:
        throw new IllegalArgumentException(kind.name());
    }
    return (RowType) DataTypes.ROW(fields.toArray(new DataTypes.Field[0])).notNull().getLogicalType();
  }

  public static RowData[] rows(Workloads.Kind kind, int count) {
    RowType rowType = rowType(kind);
    RowDataSerializer serializer = new RowDataSerializer(rowType);
    Random random = new Random(42);
    RowData[] rows = new RowData[count];
    List<RowType.RowField> fields = rowType.getFields();
    for (int r = 0; r < count; r++) {
      Object[] values = new Object[fields.size()];
      values[0] = (long) r;
      for (int f = 1; f < values.length; f++) {
        values[f] = random.nextDouble() < NULL_RATE ? null : value(fields.get(f).getName(), fields.get(f).getType(), random);
      }
      rows[r] = serializer.toBinaryRow(GenericRowData.of(values)).copy();
    }
    return rows;
  }

  static Object value(String name, LogicalType type, Random random) {
    switch (type.getTypeRoot()) {
      case SMALLINT:
        return (short) (random.nextInt(65536) - 32768);
      case INTEGER:
        return random.nextInt();
      case BIGINT:
        return random.nextLong();
      case DOUBLE:
        return random.nextDouble() * 1e6;
      case DATE:
        return DATE_BASE_DAYS + random.nextInt(3650);
      case TIMESTAMP_WITHOUT_TIME_ZONE:
        return TimestampData.fromEpochMillis(TS_BASE_MILLIS + (long) (random.nextDouble() * 31_536_000_000L), random.nextInt(1000) * 1000);
      case VARCHAR:
      case CHAR:
        return StringData.fromString(text(random, name.equals("name") ? 32 : 16, name.equals("name") ? 64 : 256));
      case DECIMAL: {
        DecimalType dt = (DecimalType) type;
        BigInteger unscaled = new BigInteger(dt.getPrecision() <= 18 ? 30 : 95, random);
        if (random.nextBoolean()) {
          unscaled = unscaled.negate();
        }
        return DecimalData.fromBigDecimal(new BigDecimal(unscaled, dt.getScale()), dt.getPrecision(), dt.getScale());
      }
      case ROW: {
        List<RowType.RowField> fields = ((RowType) type).getFields();
        Object[] values = new Object[fields.size()];
        for (int i = 0; i < values.length; i++) {
          values[i] = value(fields.get(i).getName(), fields.get(i).getType(), random);
        }
        return GenericRowData.of(values);
      }
      case ARRAY: {
        LogicalType element = ((ArrayType) type).getElementType();
        Object[] values = new Object[random.nextInt(name.equals("labels") ? 5 : 9)];
        for (int i = 0; i < values.length; i++) {
          values[i] = value(name, element, random);
        }
        return new GenericArrayData(values);
      }
      default:
        throw new IllegalArgumentException(type.toString());
    }
  }

  private static String text(Random random, int minLength, int maxLength) {
    int length = minLength + random.nextInt(maxLength - minLength + 1);
    StringBuilder sb = new StringBuilder(length);
    for (int i = 0; i < length; i++) {
      sb.append((char) ('a' + random.nextInt(26)));
    }
    return sb.toString();
  }
}
