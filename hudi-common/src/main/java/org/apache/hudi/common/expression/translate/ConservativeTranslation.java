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
import org.apache.hudi.common.expression.Predicates;
import org.apache.hudi.common.util.Option;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * The conservative translation rules of RFC-88 section 4.2, rule 1, for any source and target filter model.
 *
 * <p>The result keeps every row the input keeps. {@code Option.empty()} stands for "unknown", which keeps every
 * row. NOT is pushed down to the leaves, then:
 * <ul>
 *   <li>an AND drops the conjuncts that are unknown;</li>
 *   <li>an OR with an unknown side is unknown as a whole;</li>
 *   <li>a NOT over an unknown leaf is unknown, and so is a negated leaf the target cannot negate.</li>
 * </ul>
 *
 * <p>Translators supply a {@link Source} for their engine's filter tree and translate leaves; renderers use
 * {@link #HUDI_SOURCE} and supply a {@link Target} for their filter model.
 */
public final class ConservativeTranslation {

  /**
   * The node kinds the rules distinguish. Everything that is not a combinator or a constant is a leaf.
   */
  public enum Kind {
    AND,
    OR,
    NOT,
    TRUE,
    FALSE,
    LEAF
  }

  /**
   * Reads a source filter tree.
   */
  public interface Source<I> {
    Kind kind(I node);

    /**
     * The operands of an AND, OR (two or more) or NOT (one).
     */
    List<I> children(I node);
  }

  /**
   * Builds a target filter.
   */
  public interface Target<O> {
    O and(O left, O right);

    O or(O left, O right);

    /**
     * The negation of a translated leaf, or empty if the target cannot negate it.
     */
    Option<O> not(O leaf);

    /**
     * A filter that keeps no row, or empty if the target has none.
     */
    Option<O> alwaysFalse();
  }

  /**
   * Hudi {@link Expression}s as the source, for renderers.
   */
  public static final Source<Expression> HUDI_SOURCE = new Source<Expression>() {
    @Override
    public Kind kind(Expression node) {
      if (node instanceof Predicates.And) {
        return Kind.AND;
      } else if (node instanceof Predicates.Or) {
        return Kind.OR;
      } else if (node instanceof Predicates.Not) {
        return Kind.NOT;
      } else if (node instanceof Predicates.TrueExpression) {
        return Kind.TRUE;
      } else if (node instanceof Predicates.FalseExpression) {
        return Kind.FALSE;
      }
      return Kind.LEAF;
    }

    @Override
    public List<Expression> children(Expression node) {
      return node.getChildren();
    }
  };

  /**
   * Hudi {@link Expression}s as the target, for translators.
   */
  public static final Target<Expression> HUDI_TARGET = new Target<Expression>() {
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
      return Option.of(Predicates.not(leaf));
    }

    @Override
    public Option<Expression> alwaysFalse() {
      return Option.of(Predicates.alwaysFalse());
    }
  };

  private ConservativeTranslation() {
  }

  /**
   * Translates a filter tree.
   *
   * @param root   the source filter
   * @param source reads the source tree
   * @param leaf   translates one leaf; empty when the leaf is not translatable
   * @param target builds the result
   * @return the translated filter, or empty when every row must be kept
   */
  public static <I, O> Option<O> translate(I root, Source<I> source, Function<I, Option<O>> leaf, Target<O> target) {
    return translate(root, false, source, leaf, target);
  }

  /**
   * Translates a Hudi expression with Hudi expressions as the target, e.g. to apply the rules after rewriting
   * leaves.
   */
  public static Option<Expression> translate(Expression root, Function<Expression, Option<Expression>> leaf) {
    return translate(root, HUDI_SOURCE, leaf, HUDI_TARGET);
  }

  private static <I, O> Option<O> translate(I node, boolean negated, Source<I> source, Function<I, Option<O>> leaf,
                                            Target<O> target) {
    switch (source.kind(node)) {
      case TRUE:
        return negated ? target.alwaysFalse() : Option.empty();
      case FALSE:
        return negated ? Option.empty() : target.alwaysFalse();
      case NOT:
        return translate(source.children(node).get(0), !negated, source, leaf, target);
      case AND:
        // NOT(a AND b) = NOT a OR NOT b
        return negated ? any(node, true, source, leaf, target) : all(node, false, source, leaf, target);
      case OR:
        // NOT(a OR b) = NOT a AND NOT b
        return negated ? all(node, true, source, leaf, target) : any(node, false, source, leaf, target);
      case LEAF:
        Option<O> translated = leaf.apply(node);
        if (!translated.isPresent() || !negated) {
          return translated;
        }
        return target.not(translated.get());
      default:
        throw new IllegalStateException("Unknown kind " + source.kind(node));
    }
  }

  /**
   * A conjunction keeps the operands it can translate; dropping one keeps more rows.
   */
  private static <I, O> Option<O> all(I node, boolean negated, Source<I> source, Function<I, Option<O>> leaf, Target<O> target) {
    List<O> kept = new ArrayList<>();
    for (I child : source.children(node)) {
      Option<O> translated = translate(child, negated, source, leaf, target);
      if (translated.isPresent()) {
        kept.add(translated.get());
      }
    }
    return fold(kept, target, true);
  }

  /**
   * A disjunction needs every operand; an unknown one means any row may pass.
   */
  private static <I, O> Option<O> any(I node, boolean negated, Source<I> source, Function<I, Option<O>> leaf, Target<O> target) {
    List<O> kept = new ArrayList<>();
    for (I child : source.children(node)) {
      Option<O> translated = translate(child, negated, source, leaf, target);
      if (!translated.isPresent()) {
        return Option.empty();
      }
      kept.add(translated.get());
    }
    return fold(kept, target, false);
  }

  private static <O> Option<O> fold(List<O> operands, Target<O> target, boolean conjunction) {
    if (operands.isEmpty()) {
      return Option.empty();
    }
    O result = operands.get(0);
    for (int i = 1; i < operands.size(); i++) {
      result = conjunction ? target.and(result, operands.get(i)) : target.or(result, operands.get(i));
    }
    return Option.of(result);
  }
}
