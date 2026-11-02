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

package org.apache.hudi.common.expression.translate.testkit;

import org.apache.hudi.common.expression.Expression;
import org.apache.hudi.common.expression.Predicates;
import org.apache.hudi.common.expression.translate.ExpressionMappers;
import org.apache.hudi.common.expression.translate.ExpressionSupport;
import org.apache.hudi.common.expression.translate.ExpressionTranslator;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.Option;

import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The shared contract every RFC-88 {@link ExpressionTranslator} passes (section 4.2). Extend it in the module that
 * provides the translator.
 *
 * <p>Checks, each a dynamic test named {@code <check>/<target>}:
 * <ul>
 *   <li>{@code support_declared/<leaf>}: {@code support()} answers for every sample leaf, with a reason when
 *   unsupported.</li>
 *   <li>{@code leaf/<leaf>}: for every sample leaf the engine can state ({@link #engineLeaf}), a supported shape
 *   translates to the sample's Hudi expression, and an unsupported one translates to nothing (rule 3: dropped, not
 *   guessed).</li>
 *   <li>{@code conservative/<rule>}: rule 1 with a translatable leaf {@code p} and an untranslatable filter
 *   {@code u}: {@code p AND u -> p}, {@code p OR u -> nothing}, {@code NOT u -> nothing},
 *   {@code NOT(p AND u) -> nothing}, {@code NOT(p OR u) -> NOT p}, {@code NOT NOT p -> p},
 *   {@code p AND NOT u -> p}.</li>
 * </ul>
 *
 * <p>Known gaps work as in the schema mapper kit: keyed by the check name, strict.
 *
 * @param <E> the engine's filter type
 */
public abstract class ExpressionTranslatorContractTest<E> {

  public static final String SUPPORT_DECLARED = "support_declared";
  public static final String LEAF = "leaf";
  public static final String CONSERVATIVE = "conservative";

  protected abstract ExpressionTranslator<E> translator();

  /**
   * The engine form of a sample leaf, or empty if the engine cannot state it (the leaf is then not checked).
   */
  protected abstract Option<E> engineLeaf(ExpressionSamples.Leaf leaf);

  protected abstract E engineAnd(E left, E right);

  protected abstract E engineOr(E left, E right);

  protected abstract E engineNot(E child);

  /**
   * An engine filter the translator cannot translate, e.g. a call to a user-defined function.
   */
  protected abstract E untranslatable();

  /**
   * The table schema the filters refer to.
   */
  protected HoodieSchema schema() {
    return ExpressionSamples.SCHEMA;
  }

  /**
   * The sample leaf used as {@code p} in the conservative checks; must translate.
   */
  protected String translatableLeafId() {
    return "c_int.eq";
  }

  /**
   * Checks that fail today, keyed {@code <check>/<target>}, with the reason.
   */
  protected Map<String, String> knownGaps() {
    return Collections.emptyMap();
  }

  @TestFactory
  Stream<DynamicTest> supportDeclared() {
    return ExpressionSamples.leaves().stream().map(l -> KnownGaps.check(SUPPORT_DECLARED + "/" + l.id(), knownGaps(), () -> {
      ExpressionSupport support = translator().support(l.op(), l.column().schemaType());
      assertNotNull(support, "support(" + l.op() + ", " + l.column().schemaType() + ") returned null");
      if (!support.isSupported()) {
        assertFalse(support.reason().isEmpty(), "unsupported without a reason: " + l);
      }
    }));
  }

  @TestFactory
  Stream<DynamicTest> leaf() {
    List<DynamicTest> tests = new ArrayList<>();
    for (ExpressionSamples.Leaf l : ExpressionSamples.leaves()) {
      Option<E> engine = engineLeaf(l);
      if (!engine.isPresent()) {
        continue;
      }
      tests.add(KnownGaps.check(LEAF + "/" + l.id(), knownGaps(), () -> {
        Option<Expression> translated = translator().toHudi(engine.get(), schema());
        ExpressionSupport support = translator().support(l.op(), l.column().schemaType());
        if (support.isSupported()) {
          assertTrue(translated.isPresent(), l + " is declared supported but did not translate");
          ExpressionEquivalence.assertEquivalent(l.expression(), translated.get());
        } else {
          assertFalse(translated.isPresent(), l + " is declared unsupported (" + support.reason() + ") but translated to "
              + (translated.isPresent() ? translated.get() : ""));
        }
      }));
    }
    return tests.stream();
  }

  @TestFactory
  Stream<DynamicTest> conservative() {
    ExpressionSamples.Leaf sample = ExpressionSamples.leaf(translatableLeafId());
    Expression hudiP = sample.expression();
    List<DynamicTest> tests = new ArrayList<>();
    tests.add(rule("and_drops_unknown", () -> {
      E p = translatableLeaf(sample);
      assertTranslatesTo(engineAnd(p, untranslatable()), hudiP);
      assertTranslatesTo(engineAnd(untranslatable(), p), hudiP);
    }));
    tests.add(rule("or_with_unknown_is_unknown", () -> assertNothing(engineOr(translatableLeaf(sample), untranslatable()))));
    tests.add(rule("not_unknown_is_unknown", () -> assertNothing(engineNot(untranslatable()))));
    tests.add(rule("not_and_with_unknown_is_unknown", () -> assertNothing(engineNot(engineAnd(translatableLeaf(sample), untranslatable())))));
    tests.add(rule("not_or_with_unknown_keeps_negated_side",
        () -> assertTranslatesTo(engineNot(engineOr(translatableLeaf(sample), untranslatable())), Predicates.not(hudiP))));
    tests.add(rule("double_not", () -> assertTranslatesTo(engineNot(engineNot(translatableLeaf(sample))), hudiP)));
    tests.add(rule("and_not_unknown_drops_it", () -> assertTranslatesTo(engineAnd(translatableLeaf(sample), engineNot(untranslatable())), hudiP)));
    return tests.stream();
  }

  @Test
  void isRegistered() {
    Option<ExpressionTranslator<?>> registered = ExpressionMappers.findTranslator(translator().system());
    assertTrue(registered.isPresent(), "No ExpressionTranslator registered for '" + translator().system()
        + "'; list it in META-INF/services/" + ExpressionTranslator.class.getName());
    assertSame(translator().getClass(), registered.get().getClass());
  }

  private DynamicTest rule(String name, org.junit.jupiter.api.function.Executable body) {
    return KnownGaps.check(CONSERVATIVE + "/" + name, knownGaps(), body);
  }

  private E translatableLeaf(ExpressionSamples.Leaf sample) {
    Option<E> engine = engineLeaf(sample);
    assertTrue(engine.isPresent(), "translatableLeafId() " + sample + " has no engine form");
    return engine.get();
  }

  private void assertTranslatesTo(E engine, Expression expected) {
    Option<Expression> translated = translator().toHudi(engine, schema());
    assertTrue(translated.isPresent(), engine + " did not translate; expected " + expected);
    ExpressionEquivalence.assertEquivalent(expected, translated.get());
  }

  private void assertNothing(E engine) {
    Option<Expression> translated = translator().toHudi(engine, schema());
    assertFalse(translated.isPresent(), engine + " must translate to nothing (keep every row) but was "
        + (translated.isPresent() ? translated.get() : ""));
  }
}
