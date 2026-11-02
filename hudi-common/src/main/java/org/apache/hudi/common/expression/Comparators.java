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

package org.apache.hudi.common.expression;

import org.apache.hudi.common.schema.HoodieSchema;

import java.util.Comparator;

/**
 * Comparators for the values of {@link Expression}s, by expression type.
 */
public class Comparators {

  /**
   * Returns the natural-order comparator for BOOLEAN, INT, LONG, FLOAT, DOUBLE, DATE, time-micros, every
   * TIMESTAMP flavour, STRING and UUID, the types that had a comparator when expressions were typed with
   * InternalSchema (where ENUM read as STRING). Every other type, time-millis included, throws.
   *
   * @throws UnsupportedOperationException if the type has no comparator
   */
  public static <T> Comparator<T> forType(HoodieSchema type) {
    if (type == null || !isComparable(type)) {
      throw new UnsupportedOperationException("The desired type " + type + " doesn't support comparator yet");
    }
    return (Comparator<T>) Comparator.naturalOrder();
  }

  private static boolean isComparable(HoodieSchema type) {
    switch (type.getType()) {
      case BOOLEAN:
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case DATE:
      case TIMESTAMP:
      case STRING:
      case ENUM:
      case UUID:
        return true;
      case TIME:
        // RFC-88 bug list: the InternalSchema typing only had a comparator for time-micros, not time-millis.
        return ((HoodieSchema.Time) type).getPrecision() == HoodieSchema.TimePrecision.MICROS;
      default:
        return false;
    }
  }
}
