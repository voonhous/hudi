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

package org.apache.hudi.common.expression.translate;

import org.apache.hudi.common.expression.Expression;
import org.apache.hudi.common.expression.NameReference;
import org.apache.hudi.common.expression.Predicates;
import org.apache.hudi.common.expression.translate.testkit.ExpressionEquivalence;
import org.apache.hudi.common.expression.translate.testkit.ExpressionSamples;
import org.apache.hudi.common.util.Option;

import org.junit.jupiter.api.Test;

import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestConservativeTranslation {

  private static final Expression P = ExpressionSamples.leaf("c_int.eq").expression();
  private static final Expression Q = ExpressionSamples.leaf("c_long.lt").expression();
  /**
   * A leaf on a column the "target" does not know.
   */
  private static final Expression U = Predicates.isNull(new NameReference("unknown"));

  private static final Function<Expression, Option<Expression>> KNOWN_ONLY = leaf ->
      leaf.getChildren().stream().anyMatch(c -> c instanceof NameReference && ((NameReference) c).getName().equals("unknown"))
          ? Option.empty() : Option.of(leaf);

  private static Option<Expression> translate(Expression e) {
    return ConservativeTranslation.translate(e, KNOWN_ONLY);
  }

  private static void assertTranslates(Expression input, Expression expected) {
    Option<Expression> actual = translate(input);
    assertTrue(actual.isPresent(), input + " did not translate");
    ExpressionEquivalence.assertEquivalent(expected, actual.get());
  }

  @Test
  void andDropsUnknownConjuncts() {
    assertTranslates(Predicates.and(P, U), P);
    assertTranslates(Predicates.and(U, Predicates.and(P, Q)), Predicates.and(P, Q));
    assertFalse(translate(Predicates.and(U, U)).isPresent());
  }

  @Test
  void orWithUnknownSideIsUnknown() {
    assertFalse(translate(Predicates.or(P, U)).isPresent());
    assertTranslates(Predicates.or(P, Q), Predicates.or(P, Q));
  }

  @Test
  void notIsPushedToTheLeaves() {
    assertFalse(translate(Predicates.not(U)).isPresent());
    // NOT(P AND U) = NOT P OR NOT U: unknown
    assertFalse(translate(Predicates.not(Predicates.and(P, U))).isPresent());
    // NOT(P OR U) = NOT P AND NOT U: NOT P
    assertTranslates(Predicates.not(Predicates.or(P, U)), Predicates.not(P));
    // NOT(P AND Q) = NOT P OR NOT Q
    assertTranslates(Predicates.not(Predicates.and(P, Q)), Predicates.or(Predicates.not(P), Predicates.not(Q)));
    assertTranslates(Predicates.not(Predicates.not(P)), P);
  }

  @Test
  void constants() {
    assertFalse(translate(Predicates.alwaysTrue()).isPresent());
    assertTranslates(Predicates.alwaysFalse(), Predicates.alwaysFalse());
    assertTranslates(Predicates.not(Predicates.alwaysTrue()), Predicates.alwaysFalse());
    assertFalse(translate(Predicates.or(P, Predicates.alwaysTrue())).isPresent());
    assertTranslates(Predicates.and(P, Predicates.alwaysTrue()), P);
  }

  @Test
  void targetThatCannotNegate() {
    ConservativeTranslation.Target<Expression> noNot = new ConservativeTranslation.Target<Expression>() {
      @Override
      public Expression and(Expression left, Expression right) {
        return Predicates.and(left, right);
      }

      @Override
      public Expression or(Expression left, Expression right) {
        return Predicates.or(left, right);
      }

      @Override
      public Option<Expression> not(Expression leaf) {
        return Option.empty();
      }

      @Override
      public Option<Expression> alwaysFalse() {
        return Option.empty();
      }
    };
    Option<Expression> result = ConservativeTranslation.translate(Predicates.and(Q, Predicates.not(P)),
        ConservativeTranslation.HUDI_SOURCE, KNOWN_ONLY, noNot);
    ExpressionEquivalence.assertEquivalent(Q, result.get());
  }
}
