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
import org.apache.hudi.common.expression.Literal;
import org.apache.hudi.common.expression.NameReference;
import org.apache.hudi.common.expression.Predicates;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * The canonical table and leaf predicates the expression contract tests use.
 *
 * <p>Every column is nullable. Literal values use Hudi's canonical representation: DATE as epoch days
 * ({@code Integer}), TIMESTAMP as epoch microseconds ({@code Long}), DECIMAL as {@code BigDecimal}.
 */
public final class ExpressionSamples {

  /**
   * One column of the canonical table, with a literal of its type.
   */
  public static final class Column {
    private final String name;
    private final HoodieSchema type;
    private final Object value;
    private final Object otherValue;

    Column(String name, HoodieSchema type, Object value, Object otherValue) {
      this.name = name;
      this.type = type;
      this.value = value;
      this.otherValue = otherValue;
    }

    public String name() {
      return name;
    }

    public HoodieSchema type() {
      return type;
    }

    public HoodieSchemaType schemaType() {
      return type.getType();
    }

    public Literal<?> literal() {
      return new Literal<>(value, type);
    }

    public Literal<?> otherLiteral() {
      return new Literal<>(otherValue, type);
    }

    public NameReference ref() {
      return new NameReference(name);
    }
  }

  /**
   * One leaf predicate on one column.
   */
  public static final class Leaf {
    private final String id;
    private final Column column;
    private final Expression.Operator op;
    private final Expression expression;

    Leaf(Column column, Expression.Operator op, Expression expression) {
      this.id = column.name() + "." + op.name().toLowerCase();
      this.column = column;
      this.op = op;
      this.expression = expression;
    }

    /**
     * {@code <column>.<operator>}, e.g. {@code c_date.lt_eq}; the key of known gaps and expected renderings.
     */
    public String id() {
      return id;
    }

    public Column column() {
      return column;
    }

    public Expression.Operator op() {
      return op;
    }

    public Expression expression() {
      return expression;
    }

    @Override
    public String toString() {
      return id;
    }
  }

  public static final List<Column> COLUMNS = Collections.unmodifiableList(Arrays.asList(
      new Column("c_bool", HoodieSchema.create(HoodieSchemaType.BOOLEAN), true, false),
      new Column("c_int", HoodieSchema.create(HoodieSchemaType.INT), 5, 7),
      new Column("c_long", HoodieSchema.create(HoodieSchemaType.LONG), 5L, 7L),
      new Column("c_double", HoodieSchema.create(HoodieSchemaType.DOUBLE), 1.5d, 2.5d),
      new Column("c_string", HoodieSchema.create(HoodieSchemaType.STRING), "abc", "abd"),
      new Column("c_decimal", HoodieSchema.createDecimal(10, 2), new BigDecimal("12.34"), new BigDecimal("56.78")),
      new Column("c_date", HoodieSchema.createDate(), 19000, 19001),
      new Column("c_timestamp", HoodieSchema.createTimestampMicros(), 1700000000000000L, 1700000000000001L)));

  /**
   * The table: every {@link #COLUMNS column}, nullable.
   */
  public static final HoodieSchema SCHEMA = buildSchema();

  private static final List<Leaf> LEAVES = Collections.unmodifiableList(buildLeaves());

  private ExpressionSamples() {
  }

  public static List<Leaf> leaves() {
    return LEAVES;
  }

  public static Leaf leaf(String id) {
    return LEAVES.stream().filter(l -> l.id().equals(id)).findFirst()
        .orElseThrow(() -> new IllegalArgumentException("Unknown leaf " + id));
  }

  public static Column column(String name) {
    return COLUMNS.stream().filter(c -> c.name().equals(name)).findFirst()
        .orElseThrow(() -> new IllegalArgumentException("Unknown column " + name));
  }

  private static HoodieSchema buildSchema() {
    List<HoodieSchemaField> fields = new ArrayList<>();
    for (Column c : COLUMNS) {
      fields.add(HoodieSchemaField.of(c.name(), HoodieSchema.createNullable(c.type())));
    }
    return HoodieSchema.createRecord("expression_test", "hoodie.expression.test", null, fields);
  }

  private static List<Leaf> buildLeaves() {
    List<Leaf> leaves = new ArrayList<>();
    for (Column c : COLUMNS) {
      leaves.add(new Leaf(c, Expression.Operator.EQ, Predicates.eq(c.ref(), c.literal())));
      if (c.schemaType() != HoodieSchemaType.BOOLEAN) {
        leaves.add(new Leaf(c, Expression.Operator.LT, Predicates.lt(c.ref(), c.literal())));
        leaves.add(new Leaf(c, Expression.Operator.LT_EQ, Predicates.lteq(c.ref(), c.literal())));
        leaves.add(new Leaf(c, Expression.Operator.GT, Predicates.gt(c.ref(), c.literal())));
        leaves.add(new Leaf(c, Expression.Operator.GT_EQ, Predicates.gteq(c.ref(), c.literal())));
        leaves.add(new Leaf(c, Expression.Operator.IN, Predicates.in(c.ref(), Arrays.asList(c.literal(), c.otherLiteral()))));
      }
      leaves.add(new Leaf(c, Expression.Operator.IS_NULL, Predicates.isNull(c.ref())));
      leaves.add(new Leaf(c, Expression.Operator.IS_NOT_NULL, Predicates.isNotNull(c.ref())));
      if (c.schemaType() == HoodieSchemaType.STRING) {
        leaves.add(new Leaf(c, Expression.Operator.STARTS_WITH, Predicates.startsWith(c.ref(), c.literal())));
        leaves.add(new Leaf(c, Expression.Operator.CONTAINS, Predicates.contains(c.ref(), c.literal())));
      }
    }
    return leaves;
  }
}
