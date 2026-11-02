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

package org.apache.hudi.hive.util;

import org.apache.hudi.common.expression.translate.ExpressionRenderer;
import org.apache.hudi.common.expression.translate.testkit.ExpressionRendererContractTest;
import org.apache.hudi.common.expression.translate.testkit.ExpressionSamples;

import java.util.HashMap;
import java.util.Map;

/**
 * Contract test of {@link HmsRenderer}, with every sample column as a partition column.
 */
class TestHmsRendererContract extends ExpressionRendererContractTest<String> {

  @Override
  protected ExpressionRenderer<String> renderer() {
    return new HmsRenderer(ExpressionSamples.SCHEMA);
  }

  @Override
  protected Map<String, String> expectedRenderings() {
    Map<String, String> expected = new HashMap<>();
    comparisons(expected, "c_int", "5", "7");
    comparisons(expected, "c_long", "5", "7");
    comparisons(expected, "c_string", "\"abc\"", "\"abd\"");
    comparisons(expected, "c_date", "2022-01-08", "2022-01-09");
    return expected;
  }

  static void comparisons(Map<String, String> expected, String column, String value, String other) {
    expected.put(column + ".eq", column + " = " + value);
    expected.put(column + ".lt", column + " < " + value);
    expected.put(column + ".lt_eq", column + " <= " + value);
    expected.put(column + ".gt", column + " > " + value);
    expected.put(column + ".gt_eq", column + " >= " + value);
    expected.put(column + ".in", "(" + column + " = " + value + " OR " + column + " = " + other + ")");
  }
}
