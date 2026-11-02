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
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.schema.mapping.MappingOptions;
import org.apache.hudi.common.util.Option;

/**
 * Reference implementations that self-test the expression contract kit: a translator whose "engine" is Hudi's own
 * expression tree, and a renderer to SQL-like text. Both cannot handle CONTAINS. They are registered from this
 * module's test resources.
 */
public final class ReferenceExpressionMappers {

  private ReferenceExpressionMappers() {
  }

  /**
   * Engine = Hudi expressions; leaves on CONTAINS or on a column named "udf" are untranslatable.
   */
  public static final class ReferenceTranslator implements ExpressionTranslator<Expression> {
    @Override
    public String system() {
      return "reference";
    }

    @Override
    public Option<Expression> toHudi(Expression expr, HoodieSchema schema) {
      return ConservativeTranslation.translate(expr, leaf -> isTranslatable(leaf) ? Option.of(leaf) : Option.empty());
    }

    @Override
    public ExpressionSupport support(Expression.Operator op, HoodieSchemaType type) {
      return op == Expression.Operator.CONTAINS ? ExpressionSupport.unsupported("no CONTAINS") : ExpressionSupport.supported();
    }

    private static boolean isTranslatable(Expression leaf) {
      boolean udf = leaf.getChildren().stream().anyMatch(c -> c instanceof NameReference && ((NameReference) c).getName().equals("udf"));
      boolean contains = leaf instanceof Predicates.StringContains;
      return !udf && !contains;
    }
  }

  /**
   * Renders to the expression's own text; CONTAINS is unsupported.
   */
  public static final class ReferenceRenderer implements ExpressionRenderer<String> {
    @Override
    public String target() {
      return "reference";
    }

    @Override
    public Option<String> render(Expression expr, MappingOptions opts) {
      return ConservativeTranslation.translate(expr, ConservativeTranslation.HUDI_SOURCE,
          leaf -> leaf instanceof Predicates.StringContains ? Option.empty() : Option.of(leaf.toString()),
          new ConservativeTranslation.Target<String>() {
            @Override
            public String and(String left, String right) {
              return "(" + left + " AND " + right + ")";
            }

            @Override
            public String or(String left, String right) {
              return "(" + left + " OR " + right + ")";
            }

            @Override
            public Option<String> not(String leaf) {
              return Option.of("NOT " + leaf);
            }

            @Override
            public Option<String> alwaysFalse() {
              return Option.of("FALSE");
            }
          });
    }

    @Override
    public ExpressionSupport support(Expression.Operator op, HoodieSchemaType type) {
      return op == Expression.Operator.CONTAINS ? ExpressionSupport.unsupported("no CONTAINS") : ExpressionSupport.supported();
    }
  }
}
