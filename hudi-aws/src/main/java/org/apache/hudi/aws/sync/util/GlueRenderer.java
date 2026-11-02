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

package org.apache.hudi.aws.sync.util;

import org.apache.hudi.common.expression.Expression;
import org.apache.hudi.common.expression.translate.ExpressionMappers;
import org.apache.hudi.common.expression.translate.ExpressionSupport;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hive.util.HmsRenderer;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * {@link org.apache.hudi.common.expression.translate.ExpressionRenderer} to an AWS Glue {@code GetPartitions}
 * expression (RFC-88 section 4.2). Replaces later {@link GlueFilterGenVisitor} as used by
 * {@link GluePartitionFilterGenerator}.
 *
 * <p>It renders like {@link HmsRenderer}, the way {@link GlueFilterGenVisitor} extends the HMS visitor, except
 * that STRING literals are single-quoted with {@code '} doubled (Glue parses expressions with JSqlParser), so every
 * string can be rendered, and column names follow JSqlParser identifiers. DATE literals stay unquoted
 * {@code yyyy-MM-dd} as {@link GlueFilterGenVisitor} writes them today. Types and operators are those
 * {@link GlueFilterGenVisitor} pushes; NOT is rendered by inverting comparisons ({@code <>}, {@code >=}, ...).
 */
public class GlueRenderer extends HmsRenderer {

  private static final long serialVersionUID = 1L;

  private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");
  private static final Set<String> KEYWORDS = new HashSet<>(Arrays.asList("and", "or", "not", "in", "between", "like", "is", "null", "true", "false"));

  /**
   * The registry instance: no partition schema, columns are placed by name and typed by their literal.
   */
  public GlueRenderer() {
    super();
  }

  /**
   * @param partitionSchema the partition columns as a record; {@code null} places any column by name
   */
  public GlueRenderer(HoodieSchema partitionSchema) {
    super(partitionSchema);
  }

  @Override
  public String target() {
    return ExpressionMappers.GLUE;
  }

  @Override
  public ExpressionSupport support(Expression.Operator op, HoodieSchemaType type) {
    switch (op) {
      case IS_NULL:
      case IS_NOT_NULL:
        return ExpressionSupport.unsupported("GetPartitions expressions have no IS NULL; GlueFilterGenVisitor does not push it");
      case STARTS_WITH:
      case CONTAINS:
        return ExpressionSupport.unsupported("Glue LIKE wildcard and escape semantics are not verified; GlueFilterGenVisitor does not push it");
      default:
        return super.support(op, type);
    }
  }

  @Override
  protected ExpressionSupport supportType(HoodieSchemaType type) {
    ExpressionSupport support = super.supportType(type);
    return support.isSupported()
        ? support
        : ExpressionSupport.unsupported("GlueFilterGenVisitor pushes INT, LONG, DATE and STRING; Glue's literal formats for other partition key types are not verified");
  }

  @Override
  protected Option<String> quoteString(String value) {
    // GlueFilterGenVisitor.quoteStringLiteral
    return Option.of("'" + value.replace("'", "''") + "'");
  }

  @Override
  protected boolean isIdentifier(String name) {
    return IDENTIFIER.matcher(name).matches() && !KEYWORDS.contains(name.toLowerCase(Locale.ROOT));
  }
}
