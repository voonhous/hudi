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

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.util.Option;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

/**
 * Restores types a mapper dropped, from the hint (RFC-88, "How identity comes back").
 *
 * <p>Mappers call {@link #restore} at the end of {@code fromTypeSystem(x, hint)} and
 * {@code fromPhysicalSchema(x, hint)}. It walks the converted schema against the hint (the table schema) by field
 * name. Where the hint has a type the mapper declares LOSSY, and the converted node is exactly what that type turns
 * into when it goes through the mapper without a hint (its <i>image</i>), the hint's type is taken. The image check
 * keeps genuine changes: a VECTOR column that the engine now reports as {@code string} is not turned back into a
 * VECTOR. Decimals with the same precision and scale also take the hint's backing (bytes or fixed and its size).
 *
 * <p>Nullability, field names and order always come from the converted schema; the hint never adds or removes
 * fields.
 */
public final class HoodieSchemaHints {

  private HoodieSchemaHints() {
  }

  /**
   * Restores lossy types from the hint, if one is given.
   *
   * @param converted the schema the mapper produced without the hint
   * @param hint      the table schema, if any
   * @param lossy     the types the mapper declares LOSSY, e.g. {@code t -> support(t, opts).isLossy()}
   * @param image     what a hint node becomes through the mapper without a hint, e.g.
   *                  {@code n -> fromTypeSystem(toTypeSystem(n), Option.empty())}; only called for LOSSY nodes
   * @param mode      how the image is compared with the converted node: {@code LOGICAL} for engines and catalogs,
   *                  {@code PHYSICAL} for file formats
   */
  public static HoodieSchema restore(HoodieSchema converted, Option<HoodieSchema> hint, Predicate<HoodieSchemaType> lossy,
                                     UnaryOperator<HoodieSchema> image, HoodieSchemaEquivalence.Mode mode) {
    if (!hint.isPresent()) {
      return converted;
    }
    return new Walker(lossy, image, mode).walk(converted, hint.get());
  }

  private static final class Walker {
    private final Predicate<HoodieSchemaType> lossy;
    private final UnaryOperator<HoodieSchema> image;
    private final HoodieSchemaEquivalence.Mode mode;
    private final Map<HoodieSchema, HoodieSchema> images = new HashMap<>();

    Walker(Predicate<HoodieSchemaType> lossy, UnaryOperator<HoodieSchema> image, HoodieSchemaEquivalence.Mode mode) {
      this.lossy = lossy;
      this.image = image;
      this.mode = mode;
    }

    HoodieSchema walk(HoodieSchema converted, HoodieSchema hint) {
      HoodieSchema c = nonNull(converted);
      HoodieSchema h = nonNull(hint);
      if (c == null || h == null) {
        return converted;
      }
      HoodieSchema restored = walkNonNull(c, h);
      if (restored == c) {
        return converted;
      }
      return converted.isNullable() ? HoodieSchema.createNullable(restored) : restored;
    }

    private HoodieSchema walkNonNull(HoodieSchema c, HoodieSchema h) {
      if (lossy.test(h.getType()) && c.getType() != HoodieSchemaType.UNION && isImageOf(c, h)) {
        return h;
      }
      if (c.getType() != h.getType()) {
        return c;
      }
      switch (c.getType()) {
        case RECORD:
          return walkRecord(c, h);
        case ARRAY: {
          HoodieSchema element = walk(c.getElementType(), h.getElementType());
          return element == c.getElementType() ? c : HoodieSchema.createArray(element);
        }
        case MAP: {
          HoodieSchema value = walk(c.getValueType(), h.getValueType());
          return value == c.getValueType() ? c : HoodieSchema.createMap(value);
        }
        case DECIMAL: {
          HoodieSchema.Decimal cd = (HoodieSchema.Decimal) c;
          HoodieSchema.Decimal hd = (HoodieSchema.Decimal) h;
          boolean sameLogical = cd.getPrecision() == hd.getPrecision() && cd.getScale() == hd.getScale();
          return sameLogical && !HoodieSchemaEquivalence.equivalent(hd, cd, HoodieSchemaEquivalence.Mode.PHYSICAL) ? h : c;
        }
        default:
          return c;
      }
    }

    private HoodieSchema walkRecord(HoodieSchema c, HoodieSchema h) {
      List<HoodieSchemaField> fields = c.getFields();
      List<HoodieSchema> schemas = new ArrayList<>(fields.size());
      boolean changed = false;
      for (HoodieSchemaField field : fields) {
        Option<HoodieSchemaField> hintField = h.getField(field.name());
        HoodieSchema schema = hintField.isPresent() ? walk(field.schema(), hintField.get().schema()) : field.schema();
        changed |= schema != field.schema();
        schemas.add(schema);
      }
      if (!changed) {
        return c;
      }
      // Avro fields cannot be shared between records, so every field is copied.
      List<HoodieSchemaField> restored = new ArrayList<>(fields.size());
      for (int i = 0; i < fields.size(); i++) {
        restored.add(fields.get(i).withSchema(schemas.get(i)));
      }
      return HoodieSchema.createRecord(c.getName(), c.getNamespace().orElse(null), c.getDoc().orElse(null), restored);
    }

    private boolean isImageOf(HoodieSchema c, HoodieSchema h) {
      HoodieSchema hintImage = images.computeIfAbsent(h, n -> nonNull(image.apply(n)));
      return hintImage != null && HoodieSchemaEquivalence.equivalent(hintImage, c, mode);
    }

    private static HoodieSchema nonNull(HoodieSchema schema) {
      if (schema == null) {
        return null;
      }
      return schema.isNullable() && !schema.isComplexUnion() ? schema.getNonNullType() : schema;
    }
  }
}
