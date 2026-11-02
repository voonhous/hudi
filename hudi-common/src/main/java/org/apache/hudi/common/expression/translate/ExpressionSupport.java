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

import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;

import java.io.Serializable;
import java.util.Objects;

/**
 * Whether a translator or renderer handles one operator on one column type (RFC-88 section 4.2, rule 3). Shapes
 * that are not supported are dropped by the conservative rules, never guessed.
 */
public final class ExpressionSupport implements Serializable {

  private static final long serialVersionUID = 1L;

  private static final ExpressionSupport SUPPORTED = new ExpressionSupport(true, "");

  private final boolean supported;
  private final String reason;

  private ExpressionSupport(boolean supported, String reason) {
    this.supported = supported;
    this.reason = reason;
  }

  public static ExpressionSupport supported() {
    return SUPPORTED;
  }

  public static ExpressionSupport unsupported(String reason) {
    ValidationUtils.checkArgument(!StringUtils.isNullOrEmpty(reason), "An unsupported shape requires a reason");
    return new ExpressionSupport(false, reason);
  }

  public boolean isSupported() {
    return supported;
  }

  /**
   * Why the shape is not supported; empty when it is.
   */
  public String reason() {
    return reason;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ExpressionSupport)) {
      return false;
    }
    ExpressionSupport that = (ExpressionSupport) o;
    return supported == that.supported && reason.equals(that.reason);
  }

  @Override
  public int hashCode() {
    return Objects.hash(supported, reason);
  }

  @Override
  public String toString() {
    return supported ? "SUPPORTED" : "UNSUPPORTED(" + reason + ")";
  }
}
