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

import org.apache.hudi.common.util.Option;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;

/**
 * The result of {@link IndexPlannerMode#COMPARE}: the engine's own data skipping against the planner's, over the same
 * candidate files.
 *
 * <p>{@link #prunedOnlyByPlanner()} is the dangerous side: if the engine's pruning is correct, those files may hold
 * matching rows the planner would have skipped. {@link #prunedOnlyByEngine()} is pruning the planner misses.
 */
public final class IndexPlannerComparison {

  private static final Logger LOG = LoggerFactory.getLogger(IndexPlannerComparison.class);

  private final Set<String> prunedOnlyByPlanner;
  private final Set<String> prunedOnlyByEngine;

  private IndexPlannerComparison(Set<String> prunedOnlyByPlanner, Set<String> prunedOnlyByEngine) {
    this.prunedOnlyByPlanner = Collections.unmodifiableSet(prunedOnlyByPlanner);
    this.prunedOnlyByEngine = Collections.unmodifiableSet(prunedOnlyByEngine);
  }

  /**
   * Compares the two results over the files both started from.
   *
   * @param allFiles the candidate file names before data skipping
   * @param engine   the engine's own result; empty means it did not prune
   * @param planner  the planner's result
   */
  public static IndexPlannerComparison compare(Collection<String> allFiles, Option<Set<String>> engine, CandidateFiles planner) {
    Set<String> onlyPlanner = new TreeSet<>();
    Set<String> onlyEngine = new TreeSet<>();
    for (String file : allFiles) {
      boolean keptByEngine = !engine.isPresent() || engine.get().contains(file);
      boolean keptByPlanner = planner.isAll() || planner.fileNames().contains(file);
      if (keptByEngine && !keptByPlanner) {
        onlyPlanner.add(file);
      } else if (!keptByEngine && keptByPlanner) {
        onlyEngine.add(file);
      }
    }
    return new IndexPlannerComparison(onlyPlanner, onlyEngine);
  }

  public Set<String> prunedOnlyByPlanner() {
    return prunedOnlyByPlanner;
  }

  public Set<String> prunedOnlyByEngine() {
    return prunedOnlyByEngine;
  }

  public boolean isSame() {
    return prunedOnlyByPlanner.isEmpty() && prunedOnlyByEngine.isEmpty();
  }

  /**
   * Logs the differences: a warning when the planner prunes files the engine keeps, info otherwise.
   *
   * @param query a description of the query and table, for the log line
   */
  public void log(String query) {
    if (!prunedOnlyByPlanner.isEmpty()) {
      LOG.warn("Index planner COMPARE for {}: the planner prunes {} file(s) the engine keeps: {}", query,
          prunedOnlyByPlanner.size(), prunedOnlyByPlanner);
    }
    if (!prunedOnlyByEngine.isEmpty()) {
      LOG.info("Index planner COMPARE for {}: the engine prunes {} file(s) the planner keeps: {}", query,
          prunedOnlyByEngine.size(), prunedOnlyByEngine);
    }
    if (isSame()) {
      LOG.debug("Index planner COMPARE for {}: same result", query);
    }
  }
}
