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
import org.apache.hudi.common.expression.translate.ExpressionRenderer;
import org.apache.hudi.common.expression.translate.ExpressionSupport;
import org.apache.hudi.common.schema.mapping.MappingOptions;
import org.apache.hudi.common.util.Option;

import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The shared contract every RFC-88 {@link ExpressionRenderer} passes (section 4.2). Extend it in the module that
 * provides the renderer.
 *
 * <p>Checks:
 * <ul>
 *   <li>{@code support_declared/<leaf>}: {@code support()} answers for every sample leaf, with a reason when
 *   unsupported.</li>
 *   <li>{@code leaf/<leaf>}: a supported leaf renders (to {@link #expectedRenderings()} where listed), an
 *   unsupported one renders to nothing.</li>
 *   <li>{@code conservative/<rule>}: rule 1 with a renderable leaf {@code p} and an unrenderable leaf {@code u}
 *   (the first sample leaf declared unsupported, or {@link #unrenderable()}): {@code p AND u} renders as {@code p},
 *   {@code p OR u}, {@code NOT u} and {@code NOT(p AND u)} render to nothing, {@code NOT(p OR u)} renders as
 *   {@code NOT p}.</li>
 * </ul>
 *
 * @param <T> the target's filter type; it must implement {@code equals}
 */
public abstract class ExpressionRendererContractTest<T> {

  public static final String SUPPORT_DECLARED = "support_declared";
  public static final String LEAF = "leaf";
  public static final String CONSERVATIVE = "conservative";

  protected abstract ExpressionRenderer<T> renderer();

  protected MappingOptions options() {
    return MappingOptions.DEFAULT;
  }

  /**
   * Expected rendering per sample leaf id. Leaves without an entry are only checked to render.
   */
  protected Map<String, T> expectedRenderings() {
    return Collections.emptyMap();
  }

  /**
   * The sample leaf used as {@code p} in the conservative checks; must render.
   */
  protected String renderableLeafId() {
    return "c_int.eq";
  }

  /**
   * A Hudi expression the renderer cannot render. Defaults to the first sample leaf it declares unsupported; when
   * it supports every sample, override this or the conservative checks are skipped.
   */
  protected Option<Expression> unrenderable() {
    return Option.fromJavaOptional(ExpressionSamples.leaves().stream()
        .filter(l -> !renderer().support(l.op(), l.column().schemaType()).isSupported())
        .map(ExpressionSamples.Leaf::expression)
        .findFirst());
  }

  protected Map<String, String> knownGaps() {
    return Collections.emptyMap();
  }

  @TestFactory
  Stream<DynamicTest> supportDeclared() {
    return ExpressionSamples.leaves().stream().map(l -> KnownGaps.check(SUPPORT_DECLARED + "/" + l.id(), knownGaps(), () -> {
      ExpressionSupport support = renderer().support(l.op(), l.column().schemaType());
      assertNotNull(support, "support(" + l.op() + ", " + l.column().schemaType() + ") returned null");
      if (!support.isSupported()) {
        assertFalse(support.reason().isEmpty(), "unsupported without a reason: " + l);
      }
    }));
  }

  @TestFactory
  Stream<DynamicTest> leaf() {
    return ExpressionSamples.leaves().stream().map(l -> KnownGaps.check(LEAF + "/" + l.id(), knownGaps(), () -> {
      Option<T> rendered = renderer().render(l.expression(), options());
      ExpressionSupport support = renderer().support(l.op(), l.column().schemaType());
      if (support.isSupported()) {
        assertTrue(rendered.isPresent(), l + " is declared supported but did not render");
        if (expectedRenderings().containsKey(l.id())) {
          assertEquals(expectedRenderings().get(l.id()), rendered.get(), "rendering of " + l);
        }
      } else {
        assertFalse(rendered.isPresent(), l + " is declared unsupported (" + support.reason() + ") but rendered to "
            + (rendered.isPresent() ? rendered.get() : ""));
      }
    }));
  }

  @TestFactory
  Stream<DynamicTest> conservative() {
    Option<Expression> u = unrenderable();
    if (!u.isPresent()) {
      return Stream.empty();
    }
    Expression p = ExpressionSamples.leaf(renderableLeafId()).expression();
    List<DynamicTest> tests = new ArrayList<>();
    tests.add(rule("and_drops_unknown", () -> {
      assertEquals(render(p), render(Predicates.and(p, u.get())));
      assertEquals(render(p), render(Predicates.and(u.get(), p)));
    }));
    tests.add(rule("or_with_unknown_is_unknown", () -> assertNothing(Predicates.or(p, u.get()))));
    tests.add(rule("not_unknown_is_unknown", () -> assertNothing(Predicates.not(u.get()))));
    tests.add(rule("not_and_with_unknown_is_unknown", () -> assertNothing(Predicates.not(Predicates.and(p, u.get())))));
    tests.add(rule("not_or_with_unknown_keeps_negated_side", () -> {
      Option<T> negated = renderer().render(Predicates.not(p), options());
      Option<T> rendered = renderer().render(Predicates.not(Predicates.or(p, u.get())), options());
      assertEquals(negated.isPresent() ? negated.get() : null, rendered.isPresent() ? rendered.get() : null);
    }));
    return tests.stream();
  }

  @Test
  void isRegistered() {
    Option<ExpressionRenderer<?>> registered = ExpressionMappers.findRenderer(renderer().target());
    assertTrue(registered.isPresent(), "No ExpressionRenderer registered for '" + renderer().target()
        + "'; list it in META-INF/services/" + ExpressionRenderer.class.getName());
    assertSame(renderer().getClass(), registered.get().getClass());
  }

  private DynamicTest rule(String name, org.junit.jupiter.api.function.Executable body) {
    return KnownGaps.check(CONSERVATIVE + "/" + name, knownGaps(), body);
  }

  private T render(Expression expr) {
    Option<T> rendered = renderer().render(expr, options());
    assertTrue(rendered.isPresent(), expr + " did not render");
    return rendered.get();
  }

  private void assertNothing(Expression expr) {
    Option<T> rendered = renderer().render(expr, options());
    assertFalse(rendered.isPresent(), expr + " must render to nothing (keep every row) but was "
        + (rendered.isPresent() ? rendered.get() : ""));
  }
}
