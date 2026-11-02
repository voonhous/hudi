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

package org.apache.hudi.common.schema.mapping.testkit;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.schema.mapping.HoodieSchemaEquivalence;
import org.apache.hudi.common.schema.mapping.TypeSupport;
import org.apache.hudi.common.util.Option;

import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.function.Executable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * The shared contract every RFC-88 schema mapper passes (type plane rules 2 to 4). Extend
 * {@link TypeSystemMapperContractTest} or {@link FormatSchemaMapperContractTest}, not this class.
 *
 * <p>Checks, each a dynamic test named {@code <check>/<case>}:
 * <ul>
 *   <li>{@value #SUPPORT_DECLARED}: {@code support()} answers for every {@link HoodieSchemaType}, and gives a reason
 *   for LOSSY and UNSUPPORTED.</li>
 *   <li>{@value #SUPPORT_EXPECTED}: the declared level equals {@link #expectedSupport()}, the level the specification
 *   (RFC-99's interoperability table) asks for.</li>
 *   <li>{@value #ROUND_TRIP}: for every sample in every shape ({@link SchemaSamples#cases}), NATIVE cases satisfy
 *   {@code from(to(s), empty) ~ s}, LOSSY types {@code from(to(s), hint = s) ~ s}, and UNSUPPORTED types fail in
 *   {@code to(s)}. {@code ~} is {@link HoodieSchemaEquivalence}.</li>
 *   <li>{@value #MARKER_ROUND_TRIP}: LOSSY types that name a marker also satisfy {@code from(to(s), empty) ~ s}.</li>
 *   <li>{@value #EXPECTED_OUTPUT}: {@code to(s)} produces the type listed in {@link #expectedColumnTypes()}.</li>
 *   <li>{@value #MARKER_DECLARED}: when the target has field or file metadata ({@link #targetHasMetadata()}), every
 *   LOSSY type names a marker.</li>
 * </ul>
 *
 * <p>A case is checked at the level of the least supported type anywhere in it: a struct holding a LOSSY column is
 * round-tripped with the hint, and a map of an UNSUPPORTED type must fail.
 *
 * <p>A check that fails today because of a known bug is listed in {@link #knownGaps()} with the reason. Known gaps
 * are strict: a listed check that starts passing fails, so the entry is removed with the fix. Together the known
 * gaps of all mappers are RFC-88's bug list for converging the mappings.
 *
 * @param <X> the target's schema type
 */
public abstract class AbstractMapperContractTest<X> {

  public static final String SUPPORT_DECLARED = "support_declared";
  public static final String SUPPORT_EXPECTED = "support_expected";
  public static final String ROUND_TRIP = "round_trip";
  public static final String MARKER_ROUND_TRIP = "marker_round_trip";
  public static final String EXPECTED_OUTPUT = "expected_output";
  public static final String MARKER_DECLARED = "marker_declared";

  private static final List<String> CHECKS =
      Arrays.asList(SUPPORT_DECLARED, SUPPORT_EXPECTED, ROUND_TRIP, MARKER_ROUND_TRIP, EXPECTED_OUTPUT, MARKER_DECLARED);

  /**
   * Converts with the options under test.
   */
  protected abstract X to(HoodieSchema schema);

  /**
   * Converts back, with or without the hint.
   */
  protected abstract HoodieSchema from(X target, Option<HoodieSchema> hint);

  /**
   * The mapper's declaration under the options under test.
   */
  protected abstract TypeSupport support(HoodieSchemaType type);

  /**
   * The support level the specification asks for, for every {@link HoodieSchemaType}.
   */
  protected abstract Map<HoodieSchemaType, TypeSupport.Level> expectedSupport();

  /**
   * How strictly round trips are compared.
   */
  protected abstract HoodieSchemaEquivalence.Mode equivalenceMode();

  /**
   * Expected type of column {@value SchemaSamples#COLUMN} for a sample's top-level, not nullable case, keyed by
   * sample id. Samples without an entry are not checked. Requires {@link #columnType}.
   */
  protected Map<String, X> expectedColumnTypes() {
    return Collections.emptyMap();
  }

  /**
   * Extracts the type of a column from a converted record. Needed only when {@link #expectedColumnTypes()} is not
   * empty.
   */
  protected X columnType(X record, String column) {
    throw new UnsupportedOperationException("Override columnType to check expectedColumnTypes");
  }

  /**
   * Whether the target can store a per-column marker (field metadata, as Spark's {@code StructField} metadata, or
   * file metadata). When true, {@value #MARKER_DECLARED} requires a marker for every LOSSY type.
   */
  protected boolean targetHasMetadata() {
    return false;
  }

  /**
   * Checks that fail today, keyed {@code <check>/<case id>} (e.g. {@code round_trip/uuid.in_map}),
   * {@code <check>/<sample id>.*} for a sample in every shape (e.g. {@code round_trip/uuid.*}) or
   * {@code <check>/<TYPE>} for support checks (e.g. {@code support_expected/UUID}), with the reason.
   */
  protected Map<String, String> knownGaps() {
    return Collections.emptyMap();
  }

  @TestFactory
  Stream<DynamicTest> supportDeclared() {
    return Arrays.stream(HoodieSchemaType.values()).map(type -> check(SUPPORT_DECLARED, type.name(), () -> {
      TypeSupport support = support(type);
      assertNotNull(support, "support(" + type + ") returned null");
      if (!support.isNative()) {
        assertFalse(support.reason().isEmpty(), "support(" + type + ") is " + support.level() + " without a reason");
      }
    }));
  }

  @TestFactory
  Stream<DynamicTest> supportExpected() {
    return Arrays.stream(HoodieSchemaType.values()).map(type -> check(SUPPORT_EXPECTED, type.name(), () -> {
      TypeSupport.Level expected = expectedSupport().get(type);
      assertNotNull(expected, "expectedSupport() has no entry for " + type);
      assertEquals(expected, support(type).level(), "support(" + type + "): " + support(type));
    }));
  }

  @TestFactory
  Stream<DynamicTest> roundTrip() {
    return SchemaSamples.allCases().stream().map(c -> check(ROUND_TRIP, c.id(), c.sample().id(), () -> {
      HoodieSchema s = c.schema();
      TypeSupport worst = worstSupport(s);
      switch (worst.level()) {
        case NATIVE:
          HoodieSchemaEquivalence.assertEquivalent(s, from(to(s), Option.empty()), equivalenceMode());
          break;
        case LOSSY:
          HoodieSchemaEquivalence.assertEquivalent(s, from(to(s), Option.of(s)), equivalenceMode());
          break;
        case UNSUPPORTED:
          X converted;
          try {
            converted = to(s);
          } catch (Exception expected) {
            return;
          }
          fail(c.id() + " contains a type declared UNSUPPORTED (" + worst.reason() + ") but converted to " + converted);
          break;
        default:
          throw new IllegalStateException("Unknown level " + worst.level());
      }
    }));
  }

  @TestFactory
  Stream<DynamicTest> markerRoundTrip() {
    return SchemaSamples.allCases().stream()
        .filter(c -> {
          TypeSupport support = support(c.sample().type());
          return support.isLossy() && support.marker().isPresent() && worstSupport(c.schema()).isLossy()
              && typesIn(c.schema()).stream().filter(t -> support(t).isLossy()).allMatch(t -> support(t).marker().isPresent());
        })
        .map(c -> check(MARKER_ROUND_TRIP, c.id(), c.sample().id(),
            () -> HoodieSchemaEquivalence.assertEquivalent(c.schema(), from(to(c.schema()), Option.empty()), equivalenceMode())));
  }

  /**
   * Targets with field or file metadata must name a marker for every LOSSY type (RFC-88: "targets with field or file
   * metadata write one for every LOSSY column"). Checked only when {@link #targetHasMetadata()} is true.
   */
  @TestFactory
  Stream<DynamicTest> markerDeclared() {
    if (!targetHasMetadata()) {
      return Stream.empty();
    }
    return Arrays.stream(HoodieSchemaType.values())
        .filter(type -> support(type).isLossy())
        .map(type -> check(MARKER_DECLARED, type.name(), () -> assertTrue(support(type).marker().isPresent(),
            "LOSSY " + type + " names no marker, so it reads back wrong without a hint: " + support(type))));
  }

  @TestFactory
  Stream<DynamicTest> expectedOutput() {
    return expectedColumnTypes().entrySet().stream().map(e -> check(EXPECTED_OUTPUT, e.getKey(), () -> {
      SchemaSample sample = SchemaSamples.get(e.getKey());
      X actual = columnType(to(SchemaSamples.record(sample.schema())), SchemaSamples.COLUMN);
      assertEquals(e.getValue(), actual, "type of " + sample);
    }));
  }

  /**
   * Every known gap names a check and a case that exist, so a renamed sample cannot leave a stale entry.
   */
  @Test
  void knownGapsReferToExistingChecks() {
    Set<String> caseIds = SchemaSamples.allCases().stream().map(SchemaSamples.SchemaCase::id).collect(Collectors.toSet());
    Set<String> typeNames = Arrays.stream(HoodieSchemaType.values()).map(Enum::name).collect(Collectors.toSet());
    List<String> stale = new ArrayList<>();
    for (String key : knownGaps().keySet()) {
      int slash = key.indexOf('/');
      String checkName = slash < 0 ? key : key.substring(0, slash);
      String target = slash < 0 ? "" : key.substring(slash + 1);
      boolean known = CHECKS.contains(checkName)
          && (checkName.startsWith("support_") || checkName.equals(MARKER_DECLARED) ? typeNames.contains(target)
          : checkName.equals(EXPECTED_OUTPUT) ? SchemaSamples.ids().contains(target)
          : target.endsWith(".*") ? SchemaSamples.ids().contains(target.substring(0, target.length() - 2))
          : caseIds.contains(target));
      if (!known) {
        stale.add(key);
      }
    }
    assertTrue(stale.isEmpty(), "knownGaps() entries that match no check: " + stale);
  }

  /**
   * Every type has a sample and an expected support level.
   */
  @Test
  void everyTypeIsCovered() {
    Set<HoodieSchemaType> missing = EnumSet.allOf(HoodieSchemaType.class);
    missing.removeAll(new HashSet<>(SchemaSamples.coveredTypes()));
    assertTrue(missing.isEmpty(), "HoodieSchemaTypes without a sample: " + missing);
    Set<HoodieSchemaType> undeclared = EnumSet.allOf(HoodieSchemaType.class);
    undeclared.removeAll(expectedSupport().keySet());
    assertTrue(undeclared.isEmpty(), "HoodieSchemaTypes without an expected support level: " + undeclared);
  }

  private DynamicTest check(String checkName, String target, Executable body) {
    return check(checkName, target, null, body);
  }

  private DynamicTest check(String checkName, String target, String sampleId, Executable body) {
    String key = checkName + "/" + target;
    String exact = knownGaps().get(key);
    String gap = exact == null && sampleId != null ? knownGaps().get(checkName + "/" + sampleId + ".*") : exact;
    if (gap == null) {
      return DynamicTest.dynamicTest(key, body);
    }
    return DynamicTest.dynamicTest(key + " [known gap]", () -> {
      try {
        body.execute();
      } catch (Throwable expected) {
        return;
      }
      fail("Known gap '" + key + "' (" + gap + ") now passes; remove it from knownGaps()");
    });
  }

  /**
   * The least supported type anywhere in the schema: UNSUPPORTED beats LOSSY beats NATIVE.
   */
  private TypeSupport worstSupport(HoodieSchema schema) {
    TypeSupport worst = TypeSupport.nativeType();
    for (HoodieSchemaType type : typesIn(schema)) {
      TypeSupport support = support(type);
      if (support.level().ordinal() > worst.level().ordinal()) {
        worst = support;
      }
    }
    return worst;
  }

  /**
   * Every type in the schema, looking through nullable unions but not into VARIANT, BLOB or VECTOR internals.
   */
  static Set<HoodieSchemaType> typesIn(HoodieSchema schema) {
    Set<HoodieSchemaType> types = EnumSet.noneOf(HoodieSchemaType.class);
    collectTypes(schema, types);
    return types;
  }

  private static void collectTypes(HoodieSchema schema, Set<HoodieSchemaType> types) {
    if (schema.isNullable() && !schema.isComplexUnion()) {
      collectTypes(schema.getNonNullType(), types);
      return;
    }
    types.add(schema.getType());
    switch (schema.getType()) {
      case RECORD:
        schema.getFields().forEach(f -> collectTypes(f.schema(), types));
        break;
      case ARRAY:
        collectTypes(schema.getElementType(), types);
        break;
      case MAP:
        collectTypes(schema.getValueType(), types);
        break;
      case UNION:
        schema.getTypes().stream().filter(t -> t.getType() != HoodieSchemaType.NULL).forEach(t -> collectTypes(t, types));
        break;
      default:
        break;
    }
  }

  /**
   * Shorthand for building {@link #expectedSupport()}: every type NATIVE except the overrides.
   */
  protected static Map<HoodieSchemaType, TypeSupport.Level> nativeExcept(Object... typeLevelPairs) {
    Map<HoodieSchemaType, TypeSupport.Level> levels = new EnumMap<>(HoodieSchemaType.class);
    for (HoodieSchemaType type : HoodieSchemaType.values()) {
      levels.put(type, TypeSupport.Level.NATIVE);
    }
    if (typeLevelPairs.length % 2 != 0) {
      throw new IllegalArgumentException("Pass type, level pairs");
    }
    for (int i = 0; i < typeLevelPairs.length; i += 2) {
      levels.put((HoodieSchemaType) typeLevelPairs[i], (TypeSupport.Level) typeLevelPairs[i + 1]);
    }
    return levels;
  }
}
