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

import org.apache.spark.sql.catalyst.expressions.SpecializedGetters;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * Write wrapper: a Spark row or array seen through the typed getters. The getter used for each
 * field is chosen from Spark's own type, so a ShortType field is read with getShort.
 */
abstract class SparkGetters implements HoodieGetters {
  static final int OTHER = 0;
  static final int BYTE = 1;
  static final int SHORT = 2;
  static final int STRING = 3;
  static final int BINARY = 4;

  protected SpecializedGetters data;

  abstract int kindAt(int i);

  abstract SparkRowWrapper rowAt(int i);

  abstract SparkArrayWrapper arrayAt(int i);

  static int kindOf(DataType type) {
    if (type == DataTypes.ByteType) {
      return BYTE;
    } else if (type == DataTypes.ShortType) {
      return SHORT;
    } else if (type == DataTypes.StringType) {
      return STRING;
    } else if (type == DataTypes.BinaryType) {
      return BINARY;
    }
    return OTHER;
  }

  static SparkRowWrapper rowWrapperOrNull(DataType type) {
    return type instanceof StructType ? SparkRowWrapper.of((StructType) type) : null;
  }

  static SparkArrayWrapper arrayWrapperOrNull(DataType type) {
    return type instanceof ArrayType ? SparkArrayWrapper.of(((ArrayType) type).elementType()) : null;
  }

  @Override
  public boolean isNullAt(int i) {
    return data.isNullAt(i);
  }

  @Override
  public boolean getBoolean(int i) {
    return data.getBoolean(i);
  }

  @Override
  public int getInt(int i) {
    switch (kindAt(i)) {
      case SHORT:
        return data.getShort(i);
      case BYTE:
        return data.getByte(i);
      default:
        return data.getInt(i);
    }
  }

  @Override
  public long getLong(int i) {
    return data.getLong(i);
  }

  @Override
  public float getFloat(int i) {
    return data.getFloat(i);
  }

  @Override
  public double getDouble(int i) {
    return data.getDouble(i);
  }

  @Override
  public void getBinaryRef(int i, BinaryRef ref) {
    if (kindAt(i) == STRING) {
      UTF8String s = data.getUTF8String(i);
      Object base = s.getBaseObject();
      if (base instanceof byte[]) {
        ref.set((byte[]) base, (int) (s.getBaseOffset() - Platform.BYTE_ARRAY_OFFSET), s.numBytes());
      } else {
        byte[] bytes = s.getBytes();
        ref.set(bytes, 0, bytes.length);
      }
    } else {
      byte[] bytes = data.getBinary(i);
      ref.set(bytes, 0, bytes.length);
    }
  }

  @Override
  public byte[] getBinary(int i) {
    return kindAt(i) == STRING ? data.getUTF8String(i).getBytes() : data.getBinary(i);
  }

  @Override
  public long getDecimalUnscaledLong(int i, int precision, int scale) {
    return data.getDecimal(i, precision, scale).toUnscaledLong();
  }

  @Override
  public byte[] getDecimalUnscaledBytes(int i, int precision, int scale) {
    return data.getDecimal(i, precision, scale).toJavaBigDecimal().unscaledValue().toByteArray();
  }

  @Override
  public HoodieRow getRow(int i, int numFields) {
    return rowAt(i).replace(data.getStruct(i, numFields));
  }

  @Override
  public HoodieArray getArray(int i) {
    return arrayAt(i).replace(data.getArray(i));
  }
}
