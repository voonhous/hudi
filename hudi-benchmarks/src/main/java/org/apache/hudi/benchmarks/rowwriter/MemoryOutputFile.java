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

import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

import java.io.ByteArrayOutputStream;

/** An in-memory Parquet output file: either keeps the bytes (for comparison) or only counts them. */
public final class MemoryOutputFile implements OutputFile {
  private final boolean keep;
  private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
  private long position;

  public MemoryOutputFile(boolean keep) {
    this.keep = keep;
  }

  public byte[] bytes() {
    return bytes.toByteArray();
  }

  public long size() {
    return position;
  }

  @Override
  public PositionOutputStream create(long blockSizeHint) {
    return stream();
  }

  @Override
  public PositionOutputStream createOrOverwrite(long blockSizeHint) {
    return stream();
  }

  @Override
  public boolean supportsBlockSize() {
    return false;
  }

  @Override
  public long defaultBlockSize() {
    return 0;
  }

  private PositionOutputStream stream() {
    return new PositionOutputStream() {
      @Override
      public long getPos() {
        return position;
      }

      @Override
      public void write(int b) {
        if (keep) {
          bytes.write(b);
        }
        position++;
      }

      @Override
      public void write(byte[] b, int off, int len) {
        if (keep) {
          bytes.write(b, off, len);
        }
        position += len;
      }
    };
  }
}
