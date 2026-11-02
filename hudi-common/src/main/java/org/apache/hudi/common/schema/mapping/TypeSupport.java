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

package org.apache.hudi.common.schema.mapping;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;

import java.io.Serializable;
import java.util.Objects;

/**
 * How a mapper handles one {@link org.apache.hudi.common.schema.HoodieSchemaType} (RFC-88, type plane rule 2).
 *
 * <ul>
 *   <li>{@link Level#NATIVE}: the target has an exact type, so {@code from(to(s))} equals {@code s} without a hint.</li>
 *   <li>{@link Level#LOSSY}: the target keeps every value but drops type identity. The table schema (the hint) or the
 *   per-column marker named by {@link #marker()} restores the type on the way back. A reason is required.</li>
 *   <li>{@link Level#UNSUPPORTED}: the target cannot store the type's values exactly; mapping it fails fast.</li>
 * </ul>
 */
public final class TypeSupport implements Serializable {

  private static final long serialVersionUID = 1L;

  private static final TypeSupport NATIVE = new TypeSupport(Level.NATIVE, "", null);

  /**
   * Support levels.
   */
  public enum Level {
    NATIVE,
    LOSSY,
    UNSUPPORTED
  }

  private final Level level;
  private final String reason;
  private final String marker;

  private TypeSupport(Level level, String reason, String marker) {
    this.level = level;
    this.reason = reason;
    this.marker = marker;
  }

  /**
   * The target maps the type exactly.
   */
  public static TypeSupport nativeType() {
    return NATIVE;
  }

  /**
   * The target keeps the values but not the type; only the hint restores it.
   *
   * @param reason why identity is lost, e.g. "VECTOR stored as Hive binary"
   */
  public static TypeSupport lossy(String reason) {
    return lossy(reason, null);
  }

  /**
   * The target keeps the values but not the type; the hint or the named marker restores it.
   *
   * @param reason why identity is lost, e.g. "VECTOR stored as FIXED_LEN_BYTE_ARRAY"
   * @param marker the marker stored in the target that restores the type without a hint, e.g. the
   *               {@code hoodie.vector.columns} footer key; {@code null} when only the hint restores it
   */
  public static TypeSupport lossy(String reason, String marker) {
    ValidationUtils.checkArgument(!StringUtils.isNullOrEmpty(reason), "A LOSSY mapping requires a reason");
    return new TypeSupport(Level.LOSSY, reason, marker);
  }

  /**
   * The target cannot store the type's values exactly.
   *
   * @param reason why the type is not supported
   */
  public static TypeSupport unsupported(String reason) {
    ValidationUtils.checkArgument(!StringUtils.isNullOrEmpty(reason), "An UNSUPPORTED mapping requires a reason");
    return new TypeSupport(Level.UNSUPPORTED, reason, null);
  }

  public Level level() {
    return level;
  }

  /**
   * Why the type is lossy or unsupported; empty for {@link Level#NATIVE}.
   */
  public String reason() {
    return reason;
  }

  /**
   * The marker that restores a {@link Level#LOSSY} type without a hint, if the target has one.
   */
  public Option<String> marker() {
    return Option.ofNullable(marker);
  }

  public boolean isNative() {
    return level == Level.NATIVE;
  }

  public boolean isLossy() {
    return level == Level.LOSSY;
  }

  public boolean isUnsupported() {
    return level == Level.UNSUPPORTED;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TypeSupport that = (TypeSupport) o;
    return level == that.level && reason.equals(that.reason) && Objects.equals(marker, that.marker);
  }

  @Override
  public int hashCode() {
    return Objects.hash(level, reason, marker);
  }

  @Override
  public String toString() {
    if (level == Level.NATIVE) {
      return level.name();
    }
    return level + "(" + reason + (marker == null ? "" : ", marker=" + marker) + ")";
  }
}
