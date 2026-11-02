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
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.schema.mapping.MappingOptions;
import org.apache.hudi.common.util.Option;

import java.io.Serializable;

/**
 * Renders a Hudi {@link Expression} as a catalog or reader filter (RFC-88 section 4.2): HMS and Glue partition
 * filters, Parquet {@code FilterPredicate}, Spark {@code Filter}.
 *
 * <p>Rendering is conservative in the same way as {@link ExpressionTranslator}: the rendered filter keeps every row
 * the expression keeps. {@link ConservativeTranslation} implements the rules, so an implementation only renders
 * leaves and the AND / OR / NOT combinators of its target.
 *
 * <p>Implementations are discovered through {@link ExpressionMappers} from
 * {@code META-INF/services/org.apache.hudi.common.expression.translate.ExpressionRenderer}.
 *
 * @param <T> the target's filter type
 */
public interface ExpressionRenderer<T> extends Serializable {

  /**
   * The registry key, e.g. {@code "hms"}.
   */
  String target();

  /**
   * Renders the expression, or returns empty when nothing of it can be kept (every row passes).
   */
  Option<T> render(Expression expr, MappingOptions opts);

  /**
   * Whether the renderer handles the operator on a column of the given type.
   */
  ExpressionSupport support(Expression.Operator op, HoodieSchemaType type);

  default Option<T> render(Expression expr) {
    return render(expr, MappingOptions.DEFAULT);
  }
}
