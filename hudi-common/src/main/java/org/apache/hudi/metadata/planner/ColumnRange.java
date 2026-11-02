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

package org.apache.hudi.metadata.planner;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.Option;

import java.io.Serializable;
import java.util.Objects;

/**
 * The statistics of one column in one file (or partition), with values in the canonical representation of Hudi
 * expressions: DATE as epoch days ({@code Integer}), TIMESTAMP as epoch micros or millis ({@code Long}, the
 * column's unit), DECIMAL as {@code BigDecimal}, STRING as {@code String}.
 *
 * <p>Any part may be unknown (empty), which a {@link StatsEvaluator} must treat as "might match".
 */
public final class ColumnRange implements Serializable {

  private static final long serialVersionUID = 1L;

  private final HoodieSchema type;
  private final Comparable<?> min;
  private final Comparable<?> max;
  private final Long nullCount;
  private final Long valueCount;

  private ColumnRange(HoodieSchema type, Comparable<?> min, Comparable<?> max, Long nullCount, Long valueCount) {
    this.type = type;
    this.min = min;
    this.max = max;
    this.nullCount = nullCount;
    this.valueCount = valueCount;
  }

  /**
   * @param type       the column's non-null type
   * @param min        the smallest non-null value, or null if unknown or there is none
   * @param max        the largest non-null value, or null if unknown or there is none
   * @param nullCount  the number of nulls, or null if unknown
   * @param valueCount the number of values including nulls, or null if unknown
   */
  public static ColumnRange of(HoodieSchema type, Comparable<?> min, Comparable<?> max, Long nullCount, Long valueCount) {
    return new ColumnRange(type, min, max, nullCount, valueCount);
  }

  public HoodieSchema type() {
    return type;
  }

  public Option<Comparable<?>> min() {
    return Option.ofNullable(min);
  }

  public Option<Comparable<?>> max() {
    return Option.ofNullable(max);
  }

  public Option<Long> nullCount() {
    return Option.ofNullable(nullCount);
  }

  public Option<Long> valueCount() {
    return Option.ofNullable(valueCount);
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof ColumnRange)) {
      return false;
    }
    ColumnRange that = (ColumnRange) o;
    return Objects.equals(type, that.type) && Objects.equals(min, that.min) && Objects.equals(max, that.max)
        && Objects.equals(nullCount, that.nullCount) && Objects.equals(valueCount, that.valueCount);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, min, max, nullCount, valueCount);
  }

  @Override
  public String toString() {
    return "ColumnRange[" + min + ", " + max + ", nulls=" + nullCount + ", values=" + valueCount + "]";
  }
}
