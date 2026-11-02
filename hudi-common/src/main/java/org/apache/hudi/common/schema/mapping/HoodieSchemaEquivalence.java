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
import org.apache.hudi.common.schema.HoodieSchemaType;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Structural equality of two {@link HoodieSchema}s, as the mapper contract defines it: {@code from(to(s))} must keep
 * every type, its parameters and its nullability, but not names or documentation that the target has no place for.
 *
 * <p>Always ignored: record, enum and fixed names and namespaces, docs, default values and properties other than
 * the logical type. Nullability is compared, but not the position of {@code null} in a union.
 */
public final class HoodieSchemaEquivalence {

  /**
   * How strict the comparison is.
   */
  public enum Mode {
    /**
     * For engines and catalogs: decimal backing (bytes or fixed and its size) and vector storage backing are
     * physical details and are ignored.
     */
    LOGICAL,
    /**
     * For file formats: decimal backing and vector storage backing are compared too.
     */
    PHYSICAL
  }

  private HoodieSchemaEquivalence() {
  }

  /**
   * Returns the differences, empty if the schemas are equivalent.
   */
  public static List<String> differences(HoodieSchema expected, HoodieSchema actual, Mode mode) {
    List<String> diffs = new ArrayList<>();
    compare("$", expected, actual, mode, diffs);
    return diffs;
  }

  public static boolean equivalent(HoodieSchema expected, HoodieSchema actual, Mode mode) {
    return differences(expected, actual, mode).isEmpty();
  }

  /**
   * @throws AssertionError listing the differences, if the schemas are not equivalent
   */
  public static void assertEquivalent(HoodieSchema expected, HoodieSchema actual, Mode mode) {
    List<String> diffs = differences(expected, actual, mode);
    if (!diffs.isEmpty()) {
      throw new AssertionError("Schemas are not equivalent (" + mode + "):\n  " + String.join("\n  ", diffs)
          + "\nexpected: " + expected + "\nactual:   " + actual);
    }
  }

  private static void compare(String path, HoodieSchema expected, HoodieSchema actual, Mode mode, List<String> diffs) {
    if (actual == null) {
      diffs.add(path + ": expected " + describe(expected) + " but was null");
      return;
    }
    boolean expectedNullable = expected.isNullable();
    boolean actualNullable = actual.isNullable();
    if (expectedNullable != actualNullable) {
      diffs.add(path + ": expected " + (expectedNullable ? "nullable" : "not nullable") + " but was "
          + (actualNullable ? "nullable" : "not nullable"));
    }
    HoodieSchema e = stripNull(expected);
    HoodieSchema a = stripNull(actual);
    if (e.getType() != a.getType()) {
      diffs.add(path + ": expected " + describe(e) + " but was " + describe(a));
      return;
    }
    switch (e.getType()) {
      case RECORD:
        compareFields(path, e.getFields(), a.getFields(), mode, diffs);
        break;
      case ARRAY:
        compare(path + "[]", e.getElementType(), a.getElementType(), mode, diffs);
        break;
      case MAP:
        compare(path + "{}", e.getValueType(), a.getValueType(), mode, diffs);
        break;
      case UNION:
        compareUnion(path, e, a, mode, diffs);
        break;
      case FIXED:
        check(path, "fixed size", e.getFixedSize(), a.getFixedSize(), diffs);
        break;
      case ENUM:
        check(path, "enum symbols", e.getEnumSymbols(), a.getEnumSymbols(), diffs);
        break;
      case DECIMAL:
        HoodieSchema.Decimal ed = (HoodieSchema.Decimal) e;
        HoodieSchema.Decimal ad = (HoodieSchema.Decimal) a;
        check(path, "decimal precision", ed.getPrecision(), ad.getPrecision(), diffs);
        check(path, "decimal scale", ed.getScale(), ad.getScale(), diffs);
        if (mode == Mode.PHYSICAL) {
          check(path, "decimal backing", backing(ed), backing(ad), diffs);
        }
        break;
      case TIME:
        check(path, "time precision", ((HoodieSchema.Time) e).getPrecision(), ((HoodieSchema.Time) a).getPrecision(), diffs);
        break;
      case TIMESTAMP:
        HoodieSchema.Timestamp et = (HoodieSchema.Timestamp) e;
        HoodieSchema.Timestamp at = (HoodieSchema.Timestamp) a;
        check(path, "timestamp precision", et.getPrecision(), at.getPrecision(), diffs);
        check(path, "timestamp UTC adjustment", et.isUtcAdjusted(), at.isUtcAdjusted(), diffs);
        break;
      case VECTOR:
        HoodieSchema.Vector ev = (HoodieSchema.Vector) e;
        HoodieSchema.Vector av = (HoodieSchema.Vector) a;
        check(path, "vector dimension", ev.getDimension(), av.getDimension(), diffs);
        check(path, "vector element type", ev.getVectorElementType(), av.getVectorElementType(), diffs);
        if (mode == Mode.PHYSICAL) {
          check(path, "vector storage backing", ev.getStorageBacking(), av.getStorageBacking(), diffs);
        }
        break;
      case VARIANT:
        HoodieSchema.Variant evar = (HoodieSchema.Variant) e;
        HoodieSchema.Variant avar = (HoodieSchema.Variant) a;
        check(path, "variant shredding", evar.getTypedValueField().isPresent(), avar.getTypedValueField().isPresent(), diffs);
        if (evar.getTypedValueField().isPresent() && avar.getTypedValueField().isPresent()) {
          compare(path + ".typed_value", evar.getTypedValueField().get(), avar.getTypedValueField().get(), mode, diffs);
        }
        break;
      default:
        // BOOLEAN, INT, LONG, FLOAT, DOUBLE, STRING, BYTES, DATE, UUID, BLOB, NULL: the type is all there is.
        break;
    }
  }

  private static void compareFields(String path, List<HoodieSchemaField> expected, List<HoodieSchemaField> actual,
                                    Mode mode, List<String> diffs) {
    List<String> expectedNames = names(expected);
    List<String> actualNames = names(actual);
    if (!expectedNames.equals(actualNames)) {
      diffs.add(path + ": expected fields " + expectedNames + " but was " + actualNames);
      return;
    }
    for (int i = 0; i < expected.size(); i++) {
      compare(path + "." + expected.get(i).name(), expected.get(i).schema(), actual.get(i).schema(), mode, diffs);
    }
  }

  private static void compareUnion(String path, HoodieSchema expected, HoodieSchema actual, Mode mode, List<String> diffs) {
    List<HoodieSchema> e = nonNullBranches(expected);
    List<HoodieSchema> a = nonNullBranches(actual);
    if (e.size() != a.size()) {
      diffs.add(path + ": expected " + e.size() + " union branches but was " + a.size());
      return;
    }
    for (int i = 0; i < e.size(); i++) {
      compare(path + "|" + i, e.get(i), a.get(i), mode, diffs);
    }
  }

  private static List<HoodieSchema> nonNullBranches(HoodieSchema union) {
    List<HoodieSchema> branches = new ArrayList<>();
    for (HoodieSchema branch : union.getTypes()) {
      if (branch.getType() != HoodieSchemaType.NULL) {
        branches.add(branch);
      }
    }
    return branches;
  }

  private static HoodieSchema stripNull(HoodieSchema schema) {
    if (schema.getType() != HoodieSchemaType.UNION) {
      return schema;
    }
    List<HoodieSchema> branches = nonNullBranches(schema);
    if (branches.size() == 1) {
      return branches.get(0);
    }
    // Complex union: compareUnion looks at the non-null branches.
    return schema;
  }

  private static String backing(HoodieSchema.Decimal decimal) {
    return decimal.isFixed() ? "fixed(" + decimal.getFixedSize() + ")" : "bytes";
  }

  private static List<String> names(List<HoodieSchemaField> fields) {
    List<String> names = new ArrayList<>(fields.size());
    fields.forEach(f -> names.add(f.name()));
    return names;
  }

  private static void check(String path, String what, Object expected, Object actual, List<String> diffs) {
    if (!Objects.equals(expected, actual)) {
      diffs.add(path + ": expected " + what + " " + expected + " but was " + actual);
    }
  }

  private static String describe(HoodieSchema schema) {
    return schema.getType().name();
  }
}
