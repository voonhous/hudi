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

package org.apache.hudi.metadata.planner;

import org.apache.hudi.common.expression.Expression;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Decides from column statistics whether a file (or partition) might hold a row matching a filter (RFC-88 section
 * 4.3). Used by the stats-based lookups: column stats, partition stats, expression index.
 *
 * <p>The answer must be conservative: {@code false} only when no row can match. A column without statistics, an
 * unknown part of a range, or an expression the evaluator does not understand means "might match".
 */
public interface StatsEvaluator extends Serializable {

  /**
   * Whether a row matching the filter might exist.
   *
   * @param filter the filter, with unbound {@link org.apache.hudi.common.expression.NameReference}s to top-level
   *               columns
   * @param stats  statistics by column name; columns without an entry are unknown
   */
  boolean mightMatch(Expression filter, Map<String, ColumnRange> stats);

  /**
   * Prunes many files at once. Engine-native evaluators (e.g. over a Spark DataFrame) override this.
   *
   * @param filter the filter
   * @param stats  statistics by file name, then column name; files without an entry are kept
   * @return the files that might match, plus every file without statistics
   */
  default Set<String> prune(Expression filter, Map<String, Map<String, ColumnRange>> stats) {
    Set<String> kept = new HashSet<>();
    for (Map.Entry<String, Map<String, ColumnRange>> file : stats.entrySet()) {
      if (mightMatch(filter, file.getValue())) {
        kept.add(file.getKey());
      }
    }
    return kept;
  }
}
