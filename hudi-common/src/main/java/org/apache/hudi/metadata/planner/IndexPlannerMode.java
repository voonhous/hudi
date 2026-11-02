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

/**
 * How data skipping uses the {@link IndexPlanner} (RFC-88 section 4.3), set by
 * {@code hoodie.metadata.index.planner.mode}.
 */
public enum IndexPlannerMode {
  /**
   * Only the engine's own data skipping runs.
   */
  OFF,
  /**
   * Both run; the engine's own result is used, and the differences are reported (see {@link IndexPlannerComparison}).
   */
  COMPARE,
  /**
   * The planner replaces the engine's own pruning for the indexes it covers.
   */
  ON
}
