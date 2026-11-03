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

/**
 * Prototype of the RFC-88 typed getters shared by {@code HoodieRow} and {@code HoodieArray}.
 * Only the getters the benchmark workloads need.
 */
public interface HoodieGetters {
  boolean isNullAt(int i);

  boolean getBoolean(int i);

  /** INT8, INT16, INT32, DATE. */
  int getInt(int i);

  /** INT64, TIMESTAMP (micros). */
  long getLong(int i);

  float getFloat(int i);

  double getDouble(int i);

  /** STRING as UTF-8, BINARY: a borrowed slice, valid until the next call. */
  void getBinaryRef(int i, BinaryRef ref);

  /** STRING as UTF-8, BINARY: a standalone array (copies when the value is a slice). */
  byte[] getBinary(int i);

  /** Precision up to 18. */
  long getDecimalUnscaledLong(int i, int precision, int scale);

  /** Precision above 18, big-endian two's complement. */
  byte[] getDecimalUnscaledBytes(int i, int precision, int scale);

  HoodieRow getRow(int i, int numFields);

  HoodieArray getArray(int i);
}
