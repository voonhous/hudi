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

package org.apache.hudi.common.expression;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaType;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import javax.xml.bind.DatatypeConverter;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.UUID;

@AllArgsConstructor
@Getter
@EqualsAndHashCode(callSuper = false)
public class Literal<T> extends LeafExpression {

  public static <V> Literal from(V value) {
    if (value instanceof Integer || value instanceof Short) {
      return new Literal<>(value, HoodieSchema.create(HoodieSchemaType.INT));
    }

    if (value instanceof Byte) {
      return new Literal<>(((Byte)value).intValue(), HoodieSchema.create(HoodieSchemaType.INT));
    }

    if (value instanceof Long) {
      return new Literal<>(value, HoodieSchema.create(HoodieSchemaType.LONG));
    }

    if (value instanceof Boolean) {
      return new Literal<>(value, HoodieSchema.create(HoodieSchemaType.BOOLEAN));
    }

    if (value instanceof Double) {
      return new Literal<>(value, HoodieSchema.create(HoodieSchemaType.DOUBLE));
    }

    if (value instanceof Float) {
      return new Literal<>(value, HoodieSchema.create(HoodieSchemaType.FLOAT));
    }

    if (value instanceof BigDecimal) {
      BigDecimal decimal = (BigDecimal) value;
      return new Literal<>(value, decimalType(decimal));
    }

    if (value instanceof CharSequence) {
      return new Literal<>(value, HoodieSchema.create(HoodieSchemaType.STRING));
    }

    if (value instanceof byte[]) {
      byte[] bytes = (byte[]) value;
      return new Literal<>(ByteBuffer.wrap(bytes), HoodieSchema.createFixed("fixed_" + bytes.length, null, null, bytes.length));
    }

    if (value instanceof ByteBuffer) {
      return new Literal<>(value, HoodieSchema.create(HoodieSchemaType.BYTES));
    }

    if (value instanceof UUID) {
      return new Literal<>(value, HoodieSchema.createUUID());
    }

    throw new IllegalArgumentException("Cannot convert value from class "
        + value.getClass().getName() + " to Literal");
  }

  /**
   * Types a {@link BigDecimal} as {@code decimal(precision, scale)}. A decimal schema needs
   * {@code 0 <= scale <= precision}, which a {@link BigDecimal} does not guarantee (e.g. {@code 0.001} has
   * precision 1 and scale 3, {@code 1E+3} has scale -3); such values get the smallest decimal type that
   * holds them instead of failing, since the InternalSchema decimal type accepted any precision and scale.
   */
  private static HoodieSchema decimalType(BigDecimal decimal) {
    int precision = decimal.precision();
    int scale = decimal.scale();
    if (scale >= 0 && scale <= precision) {
      return HoodieSchema.createDecimal(precision, scale);
    }
    int adjustedScale = Math.max(scale, 0);
    int adjustedPrecision = Math.max(precision - Math.min(scale, 0), adjustedScale);
    return HoodieSchema.createDecimal(adjustedPrecision, adjustedScale);
  }

  private final T value;
  private final HoodieSchema dataType;

  @Override
  public Object eval(StructLike data) {
    return value;
  }

  @Override
  public <T> T accept(ExpressionVisitor<T> exprVisitor) {
    return exprVisitor.visitLiteral(this);
  }

  @Override
  public String toString() {
    if (value == null) {
      return "null";
    }

    if (value instanceof ByteBuffer) {
      return DatatypeConverter.printHexBinary(((ByteBuffer)value).array());
    }

    return value.toString();
  }
}
