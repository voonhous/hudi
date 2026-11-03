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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.util.Utf8;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

/** Write wrapper over Avro values: an IndexedRecord or a List seen through the typed getters. */
abstract class AvroGetters implements HoodieGetters {

  abstract Object value(int i);

  abstract AvroRowWrapper rowAt(int i);

  abstract AvroArrayWrapper arrayAt(int i);

  static Schema nonNull(Schema schema) {
    if (schema.getType() == Schema.Type.UNION) {
      for (Schema branch : schema.getTypes()) {
        if (branch.getType() != Schema.Type.NULL) {
          return branch;
        }
      }
    }
    return schema;
  }

  static AvroRowWrapper rowWrapperOrNull(Schema schema) {
    Schema s = nonNull(schema);
    return s.getType() == Schema.Type.RECORD ? AvroRowWrapper.of(s) : null;
  }

  static AvroArrayWrapper arrayWrapperOrNull(Schema schema) {
    Schema s = nonNull(schema);
    return s.getType() == Schema.Type.ARRAY ? AvroArrayWrapper.of(s.getElementType()) : null;
  }

  @Override
  public boolean isNullAt(int i) {
    return value(i) == null;
  }

  @Override
  public boolean getBoolean(int i) {
    return (Boolean) value(i);
  }

  @Override
  public int getInt(int i) {
    return ((Number) value(i)).intValue();
  }

  @Override
  public long getLong(int i) {
    return ((Number) value(i)).longValue();
  }

  @Override
  public float getFloat(int i) {
    return ((Number) value(i)).floatValue();
  }

  @Override
  public double getDouble(int i) {
    return ((Number) value(i)).doubleValue();
  }

  @Override
  public void getBinaryRef(int i, BinaryRef ref) {
    Object v = value(i);
    if (v instanceof Utf8) {
      Utf8 utf8 = (Utf8) v;
      ref.set(utf8.getBytes(), 0, utf8.getByteLength());
    } else if (v instanceof ByteBuffer) {
      ByteBuffer buffer = (ByteBuffer) v;
      ref.set(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
    } else if (v instanceof GenericFixed) {
      byte[] bytes = ((GenericFixed) v).bytes();
      ref.set(bytes, 0, bytes.length);
    } else {
      byte[] bytes = v.toString().getBytes(StandardCharsets.UTF_8);
      ref.set(bytes, 0, bytes.length);
    }
  }

  @Override
  public byte[] getBinary(int i) {
    BinaryRef ref = new BinaryRef();
    getBinaryRef(i, ref);
    return ref.offset == 0 && ref.length == ref.array.length ? ref.array : java.util.Arrays.copyOfRange(ref.array, ref.offset, ref.offset + ref.length);
  }

  @Override
  public long getDecimalUnscaledLong(int i, int precision, int scale) {
    return new BigInteger(decimalBytes(value(i))).longValue();
  }

  @Override
  public byte[] getDecimalUnscaledBytes(int i, int precision, int scale) {
    return decimalBytes(value(i));
  }

  private static byte[] decimalBytes(Object v) {
    if (v instanceof GenericFixed) {
      return ((GenericFixed) v).bytes();
    }
    ByteBuffer buffer = (ByteBuffer) v;
    byte[] bytes = new byte[buffer.remaining()];
    buffer.duplicate().get(bytes);
    return bytes;
  }

  @Override
  public HoodieRow getRow(int i, int numFields) {
    return rowAt(i).replace((IndexedRecord) value(i));
  }

  @Override
  public HoodieArray getArray(int i) {
    return arrayAt(i).replace((List<?>) value(i));
  }
}
