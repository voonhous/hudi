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

package org.apache.hudi.expression.translate;

import org.apache.hudi.common.expression.Expression;
import org.apache.hudi.common.expression.Literal;
import org.apache.hudi.common.expression.NameReference;
import org.apache.hudi.common.expression.Predicates;
import org.apache.hudi.common.expression.translate.ConservativeTranslation;
import org.apache.hudi.common.expression.translate.ExpressionSupport;
import org.apache.hudi.common.expression.translate.ExpressionTranslator;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.schema.mapping.TypeSystems;
import org.apache.hudi.common.util.Option;

import org.apache.spark.sql.catalyst.parser.CatalystSqlParser$;
import org.apache.spark.sql.sources.AlwaysFalse;
import org.apache.spark.sql.sources.AlwaysTrue;
import org.apache.spark.sql.sources.And;
import org.apache.spark.sql.sources.EqualNullSafe;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.GreaterThanOrEqual;
import org.apache.spark.sql.sources.In;
import org.apache.spark.sql.sources.IsNotNull;
import org.apache.spark.sql.sources.IsNull;
import org.apache.spark.sql.sources.LessThan;
import org.apache.spark.sql.sources.LessThanOrEqual;
import org.apache.spark.sql.sources.Not;
import org.apache.spark.sql.sources.Or;
import org.apache.spark.sql.sources.StringContains;
import org.apache.spark.sql.sources.StringStartsWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;

import scala.collection.Seq;

/**
 * Translates Spark data source {@link Filter}s to Hudi {@link Expression}s (RFC-88 section 4.2), registered as
 * {@link TypeSystems#SPARK}.
 *
 * <p>The leaves are those of {@code SparkFilterHelper.convertFilter}, which this replaces later: {@code EqualTo},
 * {@code EqualNullSafe}, {@code LessThan}, {@code LessThanOrEqual}, {@code GreaterThan},
 * {@code GreaterThanOrEqual}, {@code In}, {@code StringStartsWith}, {@code StringContains}, {@code IsNull} and
 * {@code IsNotNull}. AND, OR, NOT, {@code AlwaysTrue} and {@code AlwaysFalse} follow
 * {@link ConservativeTranslation}. Unlike {@code SparkFilterHelper}:
 * <ul>
 *   <li>literals are typed from the referenced column's {@link HoodieSchema} and converted to its canonical value
 *   (e.g. a {@code LocalDate} becomes a DATE literal of epoch days); a value the column type cannot hold exactly,
 *   a {@code null} comparison value, or a column missing from the schema leaves the leaf untranslated;</li>
 *   <li>an AND keeps its translatable side instead of being dropped whole;</li>
 *   <li>{@code a <=> null} becomes {@code a IS NULL} and {@code a <=> v} becomes {@code a IS NOT NULL AND a = v},
 *   both exact also under NOT;</li>
 *   <li>{@code null}s in an {@code In} list are dropped (they never match).</li>
 * </ul>
 *
 * <p>Columns are top-level fields, named as they are or as a single backtick-quoted part ({@code `a.b`}); nested
 * fields ({@code a.b}) are not supported, as Hudi expressions bind top-level fields only.
 */
public class SparkTranslator implements ExpressionTranslator<Filter> {

  private static final long serialVersionUID = 1L;

  private static final ConservativeTranslation.Source<Filter> SOURCE = new ConservativeTranslation.Source<Filter>() {
    @Override
    public ConservativeTranslation.Kind kind(Filter node) {
      if (node instanceof And) {
        return ConservativeTranslation.Kind.AND;
      } else if (node instanceof Or) {
        return ConservativeTranslation.Kind.OR;
      } else if (node instanceof Not) {
        return ConservativeTranslation.Kind.NOT;
      } else if (node instanceof AlwaysTrue) {
        return ConservativeTranslation.Kind.TRUE;
      } else if (node instanceof AlwaysFalse) {
        return ConservativeTranslation.Kind.FALSE;
      }
      return ConservativeTranslation.Kind.LEAF;
    }

    @Override
    public List<Filter> children(Filter node) {
      if (node instanceof And) {
        return Arrays.asList(((And) node).left(), ((And) node).right());
      } else if (node instanceof Or) {
        return Arrays.asList(((Or) node).left(), ((Or) node).right());
      } else if (node instanceof Not) {
        return Collections.singletonList(((Not) node).child());
      }
      return Collections.emptyList();
    }
  };

  @Override
  public String system() {
    return TypeSystems.SPARK;
  }

  @Override
  public Option<Expression> toHudi(Filter expr, HoodieSchema schema) {
    return ConservativeTranslation.translate(expr, SOURCE, leaf -> translateLeaf(leaf, schema), ConservativeTranslation.HUDI_TARGET);
  }

  @Override
  public ExpressionSupport support(Expression.Operator op, HoodieSchemaType type) {
    return SparkFilterValues.support(op, type);
  }

  private static Option<Expression> translateLeaf(Filter filter, HoodieSchema schema) {
    if (filter instanceof EqualTo) {
      return comparison(((EqualTo) filter).attribute(), ((EqualTo) filter).value(), schema, Predicates::eq);
    } else if (filter instanceof EqualNullSafe) {
      EqualNullSafe nullSafe = (EqualNullSafe) filter;
      if (nullSafe.value() == null) {
        return column(nullSafe.attribute(), schema).map(c -> Predicates.isNull(ref(c)));
      }
      // a <=> v is never null: false where a is null, which a = v alone is not (it matters under NOT)
      return comparison(nullSafe.attribute(), nullSafe.value(), schema,
          (ref, lit) -> Predicates.and(Predicates.isNotNull(ref), Predicates.eq(ref, lit)));
    } else if (filter instanceof LessThan) {
      return comparison(((LessThan) filter).attribute(), ((LessThan) filter).value(), schema, Predicates::lt);
    } else if (filter instanceof LessThanOrEqual) {
      return comparison(((LessThanOrEqual) filter).attribute(), ((LessThanOrEqual) filter).value(), schema, Predicates::lteq);
    } else if (filter instanceof GreaterThan) {
      return comparison(((GreaterThan) filter).attribute(), ((GreaterThan) filter).value(), schema, Predicates::gt);
    } else if (filter instanceof GreaterThanOrEqual) {
      return comparison(((GreaterThanOrEqual) filter).attribute(), ((GreaterThanOrEqual) filter).value(), schema, Predicates::gteq);
    } else if (filter instanceof In) {
      return in((In) filter, schema);
    } else if (filter instanceof StringStartsWith) {
      return stringPredicate(Expression.Operator.STARTS_WITH, ((StringStartsWith) filter).attribute(), ((StringStartsWith) filter).value(),
          schema, Predicates::startsWith);
    } else if (filter instanceof StringContains) {
      return stringPredicate(Expression.Operator.CONTAINS, ((StringContains) filter).attribute(), ((StringContains) filter).value(),
          schema, Predicates::contains);
    } else if (filter instanceof IsNull) {
      return column(((IsNull) filter).attribute(), schema).map(c -> Predicates.isNull(ref(c)));
    } else if (filter instanceof IsNotNull) {
      return column(((IsNotNull) filter).attribute(), schema).map(c -> Predicates.isNotNull(ref(c)));
    }
    return Option.empty();
  }

  private static Option<Expression> comparison(String attribute, Object value, HoodieSchema schema,
                                               BiFunction<Expression, Expression, Expression> build) {
    Option<HoodieSchemaField> column = column(attribute, schema);
    if (!column.isPresent() || !SparkFilterValues.support(Expression.Operator.EQ, column.get().getNonNullSchema().getType()).isSupported()) {
      return Option.empty();
    }
    return literal(value, column.get()).map(lit -> build.apply(ref(column.get()), lit));
  }

  private static Option<Expression> in(In filter, HoodieSchema schema) {
    Option<HoodieSchemaField> column = column(filter.attribute(), schema);
    if (!column.isPresent() || !SparkFilterValues.support(Expression.Operator.IN, column.get().getNonNullSchema().getType()).isSupported()) {
      return Option.empty();
    }
    List<Expression> values = new ArrayList<>();
    for (Object value : filter.values()) {
      if (value == null) {
        // a IN (.., null) is null, not true, where a matches no other value: it keeps the same rows without it
        continue;
      }
      Option<Literal<?>> literal = literal(value, column.get());
      if (!literal.isPresent()) {
        return Option.empty();
      }
      values.add(literal.get());
    }
    return values.isEmpty() ? Option.empty() : Option.of(Predicates.in(ref(column.get()), values));
  }

  private static Option<Expression> stringPredicate(Expression.Operator op, String attribute, String value, HoodieSchema schema,
                                                    BiFunction<Expression, Expression, Expression> build) {
    Option<HoodieSchemaField> column = column(attribute, schema);
    if (!column.isPresent() || !SparkFilterValues.support(op, column.get().getNonNullSchema().getType()).isSupported()) {
      return Option.empty();
    }
    return literal(value, column.get()).map(lit -> build.apply(ref(column.get()), lit));
  }

  private static Option<Literal<?>> literal(Object value, HoodieSchemaField column) {
    HoodieSchema type = column.getNonNullSchema();
    return SparkFilterValues.toCanonical(value, type).map(v -> new Literal<>(v, type));
  }

  private static NameReference ref(HoodieSchemaField column) {
    return new NameReference(column.name());
  }

  /**
   * The top-level field a filter attribute names: as it is, or as Spark quotes a name that needs it when nested
   * predicate pushdown is on ({@code `a.b`}). A multi-part (nested) name resolves to nothing.
   */
  private static Option<HoodieSchemaField> column(String attribute, HoodieSchema schema) {
    if (attribute == null || attribute.isEmpty() || !schema.hasFields()) {
      return Option.empty();
    }
    Option<HoodieSchemaField> field = schema.getField(attribute);
    if (field.isPresent()) {
      return field;
    }
    Seq<String> parts;
    try {
      parts = CatalystSqlParser$.MODULE$.parseMultipartIdentifier(attribute);
    } catch (Exception e) {
      // a ParseException, which Scala throws undeclared: not an identifier Spark could have written
      return Option.empty();
    }
    return parts.size() == 1 ? schema.getField(parts.head()) : Option.empty();
  }
}
