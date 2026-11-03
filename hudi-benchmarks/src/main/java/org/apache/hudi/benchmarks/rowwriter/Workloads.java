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

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection;
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection$;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/** Deterministic Spark rows for each workload, materialized as UnsafeRow as Spark hands them to writers. */
public final class Workloads {
  public enum Kind { DECIMAL, STRING, NESTED, MIXED }

  private static final double NULL_RATE = 0.02;
  private static final long TS_BASE_MICROS = 1_700_000_000_000_000L;
  private static final int DATE_BASE_DAYS = 19_000;

  private Workloads() {
  }

  public static StructType schema(Kind kind) {
    List<StructField> fields = new ArrayList<>();
    fields.add(DataTypes.createStructField("id", DataTypes.LongType, false));
    switch (kind) {
      case DECIMAL:
        for (int i = 0; i < 8; i++) {
          fields.add(DataTypes.createStructField("d" + i, DataTypes.createDecimalType(10, 2), true));
        }
        fields.add(DataTypes.createStructField("w0", DataTypes.createDecimalType(30, 4), true));
        fields.add(DataTypes.createStructField("w1", DataTypes.createDecimalType(30, 4), true));
        break;
      case STRING:
        for (int i = 0; i < 8; i++) {
          fields.add(DataTypes.createStructField("s" + i, DataTypes.StringType, true));
        }
        break;
      case NESTED:
        fields.add(DataTypes.createStructField("info", DataTypes.createStructType(new StructField[] {
            DataTypes.createStructField("a", DataTypes.IntegerType, true),
            DataTypes.createStructField("b", DataTypes.StringType, true),
            DataTypes.createStructField("c", DataTypes.createDecimalType(10, 2), true)}), true));
        fields.add(DataTypes.createStructField("items", DataTypes.createArrayType(DataTypes.createStructType(new StructField[] {
            DataTypes.createStructField("x", DataTypes.IntegerType, true),
            DataTypes.createStructField("y", DataTypes.StringType, true)})), true));
        fields.add(DataTypes.createStructField("tags", DataTypes.createArrayType(DataTypes.StringType), true));
        break;
      case MIXED:
        fields.add(DataTypes.createStructField("small", DataTypes.ShortType, true));
        fields.add(DataTypes.createStructField("num", DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("big", DataTypes.LongType, true));
        fields.add(DataTypes.createStructField("score", DataTypes.DoubleType, true));
        fields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("price", DataTypes.createDecimalType(10, 2), true));
        fields.add(DataTypes.createStructField("ts", DataTypes.TimestampType, true));
        fields.add(DataTypes.createStructField("day", DataTypes.DateType, true));
        fields.add(DataTypes.createStructField("labels", DataTypes.createArrayType(DataTypes.StringType), true));
        break;
      default:
        throw new IllegalArgumentException(kind.name());
    }
    return DataTypes.createStructType(fields);
  }

  public static InternalRow[] rows(Kind kind, int count) {
    StructType schema = schema(kind);
    UnsafeProjection toUnsafe = UnsafeProjection$.MODULE$.create(schema);
    Random random = new Random(42);
    InternalRow[] rows = new InternalRow[count];
    for (int r = 0; r < count; r++) {
      StructField[] fields = schema.fields();
      Object[] values = new Object[fields.length];
      values[0] = (long) r;
      for (int f = 1; f < fields.length; f++) {
        values[f] = random.nextDouble() < NULL_RATE ? null : value(fields[f].name(), fields[f].dataType(), random);
      }
      rows[r] = toUnsafe.apply(new GenericInternalRow(values)).copy();
    }
    return rows;
  }

  private static Object value(String name, DataType type, Random random) {
    if (type == DataTypes.ShortType) {
      return (short) (random.nextInt(65536) - 32768);
    } else if (type == DataTypes.IntegerType) {
      return random.nextInt();
    } else if (type == DataTypes.LongType) {
      return random.nextLong();
    } else if (type == DataTypes.DoubleType) {
      return random.nextDouble() * 1e6;
    } else if (type == DataTypes.TimestampType) {
      return TS_BASE_MICROS + (long) (random.nextDouble() * 31_536_000_000_000L);
    } else if (type == DataTypes.DateType) {
      return DATE_BASE_DAYS + random.nextInt(3650);
    } else if (type == DataTypes.StringType) {
      return UTF8String.fromString(text(random, name.equals("name") ? 32 : 16, name.equals("name") ? 64 : 256));
    } else if (type instanceof org.apache.spark.sql.types.DecimalType) {
      org.apache.spark.sql.types.DecimalType dt = (org.apache.spark.sql.types.DecimalType) type;
      BigInteger unscaled = new BigInteger(dt.precision() <= 18 ? 30 : 95, random);
      if (random.nextBoolean()) {
        unscaled = unscaled.negate();
      }
      return Decimal.apply(new BigDecimal(unscaled, dt.scale()), dt.precision(), dt.scale());
    } else if (type instanceof StructType) {
      StructField[] fields = ((StructType) type).fields();
      Object[] values = new Object[fields.length];
      for (int i = 0; i < fields.length; i++) {
        values[i] = value(fields[i].name(), fields[i].dataType(), random);
      }
      return new GenericInternalRow(values);
    } else if (type instanceof org.apache.spark.sql.types.ArrayType) {
      DataType element = ((org.apache.spark.sql.types.ArrayType) type).elementType();
      Object[] values = new Object[random.nextInt(name.equals("labels") ? 5 : 9)];
      for (int i = 0; i < values.length; i++) {
        values[i] = value(name, element, random);
      }
      return new GenericArrayData(values);
    }
    throw new IllegalArgumentException(type.toString());
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
