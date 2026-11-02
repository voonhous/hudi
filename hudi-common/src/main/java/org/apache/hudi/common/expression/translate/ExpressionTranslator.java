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
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.util.Option;

import java.io.Serializable;

/**
 * Translates an engine filter into a Hudi {@link Expression} (RFC-88 section 4.2): Spark, Flink, Hive, Trino.
 *
 * <p>Translation is conservative: the result keeps every row the original keeps. NOT is pushed down to the leaves,
 * an untranslatable AND conjunct is dropped, an OR with an untranslatable side is dropped whole, and a NOT over an
 * untranslatable leaf is dropped. A cast is stripped only if it preserves order. {@link ConservativeTranslation}
 * implements the first three rules, so an implementation only translates leaves.
 *
 * <p>Implementations are discovered through {@link ExpressionMappers} from
 * {@code META-INF/services/org.apache.hudi.common.expression.translate.ExpressionTranslator}.
 *
 * @param <E> the engine's filter type, e.g. Spark {@code org.apache.spark.sql.sources.Filter}
 */
public interface ExpressionTranslator<E> extends Serializable {

  /**
   * The registry key, e.g. {@code "spark"}; the same keys as {@link org.apache.hudi.common.schema.mapping.TypeSystems}.
   */
  String system();

  /**
   * Translates an engine filter, or returns empty when nothing of it can be kept (every row passes).
   *
   * @param expr   the engine filter
   * @param schema the table schema the filter refers to; literals and columns are typed from it
   */
  Option<Expression> toHudi(E expr, HoodieSchema schema);

  /**
   * Whether the translator handles the operator on a column of the given type.
   */
  ExpressionSupport support(Expression.Operator op, HoodieSchemaType type);
}
