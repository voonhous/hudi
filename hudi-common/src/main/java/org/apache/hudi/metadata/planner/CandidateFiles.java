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

import org.apache.hudi.common.util.ValidationUtils;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * The files a query still has to read after an index lookup: either every file (the lookup could not prune), or a
 * set of file names. A lookup only removes files it has proof against; a file the index knows nothing about is kept.
 */
public final class CandidateFiles implements Serializable {

  private static final long serialVersionUID = 1L;

  private static final CandidateFiles ALL = new CandidateFiles(null);

  private final Set<String> fileNames;

  private CandidateFiles(Set<String> fileNames) {
    this.fileNames = fileNames;
  }

  /**
   * No pruning: every file is a candidate.
   */
  public static CandidateFiles all() {
    return ALL;
  }

  /**
   * Only these files (by file name, as in the file system view) are candidates.
   */
  public static CandidateFiles of(Set<String> fileNames) {
    ValidationUtils.checkArgument(fileNames != null, "fileNames");
    return new CandidateFiles(Collections.unmodifiableSet(new HashSet<>(fileNames)));
  }

  public boolean isAll() {
    return fileNames == null;
  }

  /**
   * The candidate file names.
   *
   * @throws IllegalStateException if every file is a candidate
   */
  public Set<String> fileNames() {
    ValidationUtils.checkState(fileNames != null, "Every file is a candidate");
    return fileNames;
  }

  /**
   * Candidates of both: each lookup only removes files it can prove irrelevant, so results combine by intersection.
   */
  public CandidateFiles intersect(CandidateFiles other) {
    if (isAll()) {
      return other;
    }
    if (other.isAll()) {
      return this;
    }
    Set<String> both = new HashSet<>(fileNames);
    both.retainAll(other.fileNames);
    return of(both);
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof CandidateFiles && Objects.equals(fileNames, ((CandidateFiles) o).fileNames);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(fileNames);
  }

  @Override
  public String toString() {
    return isAll() ? "CandidateFiles(all)" : "CandidateFiles(" + fileNames.size() + " files)";
  }
}
