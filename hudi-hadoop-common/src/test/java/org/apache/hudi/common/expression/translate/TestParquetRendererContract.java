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

import org.apache.hudi.common.expression.translate.testkit.ExpressionRendererContractTest;
import org.apache.hudi.common.expression.translate.testkit.ExpressionSamples;

import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.io.api.Binary;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

/**
 * Contract test of {@link ParquetRenderer} bound to the sample table schema.
 */
class TestParquetRendererContract extends ExpressionRendererContractTest<FilterPredicate> {

  @Override
  protected ExpressionRenderer<FilterPredicate> renderer() {
    return new ParquetRenderer(ExpressionSamples.SCHEMA);
  }

  @Override
  protected Map<String, FilterPredicate> expectedRenderings() {
    Map<String, FilterPredicate> expected = new HashMap<>();
    Operators.BooleanColumn bool = FilterApi.booleanColumn("c_bool");
    expected.put("c_bool.eq", FilterApi.eq(bool, true));
    expected.put("c_bool.is_null", FilterApi.eq(bool, null));
    expected.put("c_bool.is_not_null", FilterApi.notEq(bool, null));
    comparisons(expected, FilterApi.intColumn("c_int"), 5, 7);
    comparisons(expected, FilterApi.longColumn("c_long"), 5L, 7L);
    comparisons(expected, FilterApi.doubleColumn("c_double"), 1.5d, 2.5d);
    comparisons(expected, FilterApi.binaryColumn("c_string"), Binary.fromString("abc"), Binary.fromString("abd"));
    // decimal(10,2) backed by bytes: BINARY + DECIMAL holding the minimal two's complement unscaled value
    comparisons(expected, FilterApi.binaryColumn("c_decimal"), unscaled(1234), unscaled(5678));
    comparisons(expected, FilterApi.intColumn("c_date"), 19000, 19001);
    comparisons(expected, FilterApi.longColumn("c_timestamp"), 1700000000000000L, 1700000000000001L);
    return expected;
  }

  static Binary unscaled(long value) {
    return Binary.fromConstantByteArray(BigInteger.valueOf(value).toByteArray());
  }

  private static <T extends Comparable<T>, C extends Operators.Column<T> & Operators.SupportsLtGt> void comparisons(
      Map<String, FilterPredicate> expected, C column, T value, T other) {
    String name = column.getColumnPath().toDotString();
    expected.put(name + ".eq", FilterApi.eq(column, value));
    expected.put(name + ".lt", FilterApi.lt(column, value));
    expected.put(name + ".lt_eq", FilterApi.ltEq(column, value));
    expected.put(name + ".gt", FilterApi.gt(column, value));
    expected.put(name + ".gt_eq", FilterApi.gtEq(column, value));
    expected.put(name + ".in", FilterApi.or(FilterApi.eq(column, value), FilterApi.eq(column, other)));
    expected.put(name + ".is_null", FilterApi.eq(column, null));
    expected.put(name + ".is_not_null", FilterApi.notEq(column, null));
  }
}
