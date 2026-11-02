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

import org.apache.hudi.common.expression.Expression;
import org.apache.hudi.common.expression.Literal;
import org.apache.hudi.common.expression.NameReference;
import org.apache.hudi.common.expression.Predicates;
import org.apache.hudi.common.expression.translate.testkit.ExpressionSamples;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.util.Option;

import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.io.api.Binary;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link ParquetRenderer}: rendered predicates applied by parquet-mr to a file written by parquet-avro, plus the
 * literals it refuses.
 */
class TestParquetRenderer {

  /**
   * Values of each sample column, one per row: null, below, the sample literal, the other literal, above.
   */
  private static final Map<String, List<Object>> VALUES = new HashMap<>();

  static {
    VALUES.put("c_bool", Arrays.asList(null, false, true, true, false));
    VALUES.put("c_int", Arrays.asList(null, 4, 5, 7, 8));
    VALUES.put("c_long", Arrays.asList(null, 4L, 5L, 7L, 8L));
    VALUES.put("c_double", Arrays.asList(null, 0.5d, 1.5d, 2.5d, 3.5d));
    VALUES.put("c_string", Arrays.asList(null, "abb", "abc", "abd", "abe"));
    VALUES.put("c_decimal", Arrays.asList(null, new BigDecimal("-1.00"), new BigDecimal("12.34"), new BigDecimal("56.78"), new BigDecimal("99999999.99")));
    VALUES.put("c_date", Arrays.asList(null, 18999, 19000, 19001, 19002));
    VALUES.put("c_timestamp", Arrays.asList(null, 1699999999999999L, 1700000000000000L, 1700000000000001L, 1700000000000002L));
  }

  private static final int ROWS = 5;

  @TempDir
  File tempDir;

  /**
   * Every renderable sample leaf and its negation, applied by parquet-mr's record filter: a leaf keeps exactly the
   * rows where it is TRUE, a negated leaf at least those (parquet keeps nulls under NOT).
   */
  @Test
  void filtersAFileWrittenByParquetAvro() throws Exception {
    Path file = writeSampleFile();
    ParquetRenderer renderer = new ParquetRenderer(ExpressionSamples.SCHEMA);
    int checked = 0;
    for (ExpressionSamples.Leaf leaf : ExpressionSamples.leaves()) {
      Option<FilterPredicate> rendered = renderer.render(leaf.expression());
      if (!rendered.isPresent()) {
        continue;
      }
      assertEquals(expectedRows(leaf, false), read(file, rendered.get()), leaf.id());
      Option<FilterPredicate> negated = renderer.render(Predicates.not(leaf.expression()));
      assertTrue(negated.isPresent(), leaf.id());
      Set<Integer> keptByNot = read(file, negated.get());
      assertTrue(keptByNot.containsAll(expectedRows(leaf, true)), "NOT " + leaf.id() + " kept " + keptByNot);
      checked++;
    }
    assertEquals(59, checked);
  }

  @Test
  void decimalLayouts() {
    HoodieSchema fixed = HoodieSchema.createDecimal("dec", null, null, 10, 2, 5);
    ParquetRenderer renderer = new ParquetRenderer(table("d", fixed));
    // FIXED_LEN_BYTE_ARRAY(5): sign-extended to the fixed length
    assertEquals(FilterApi.eq(FilterApi.binaryColumn("d"), Binary.fromConstantByteArray(new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0x9C})),
        renderer.render(Predicates.eq(ref("d"), new Literal<>(new BigDecimal("-1.00"), fixed))).get());
    // a literal of another scale is rescaled when exact, and not rendered when it would round
    assertEquals(FilterApi.lt(FilterApi.binaryColumn("d"), Binary.fromConstantByteArray(new byte[] {0, 0, 0, 0, 50})),
        renderer.render(Predicates.lt(ref("d"), Literal.from(new BigDecimal("0.5")))).get());
    assertFalse(renderer.render(Predicates.lt(ref("d"), Literal.from(new BigDecimal("0.505")))).isPresent());
    // more digits than the column holds
    assertFalse(renderer.render(Predicates.eq(ref("d"), Literal.from(new BigDecimal("123456789.00")))).isPresent());
  }

  @Test
  void refusesInexactLiterals() {
    ParquetRenderer renderer = new ParquetRenderer(ExpressionSamples.SCHEMA);
    // UTF-8 byte order differs from Java string order at and above U+D800
    assertFalse(renderer.render(Predicates.lt(ref("c_string"), Literal.from("a"))).isPresent());
    assertEquals(FilterApi.eq(FilterApi.binaryColumn("c_string"), Binary.fromString("a")),
        renderer.render(Predicates.eq(ref("c_string"), Literal.from("a"))).get());
    assertFalse(renderer.render(Predicates.eq(ref("c_string"), Literal.from("a\uD800"))).isPresent());
    assertFalse(renderer.render(Predicates.gt(ref("c_double"), Literal.from(Double.NaN))).isPresent());
    assertFalse(renderer.render(Predicates.eq(ref("c_int"), new Literal<>(null, HoodieSchema.create(HoodieSchemaType.INT)))).isPresent());
    // a literal of another type than the column
    assertFalse(renderer.render(Predicates.eq(ref("c_long"), Literal.from(5))).isPresent());
    assertFalse(renderer.render(Predicates.eq(ref("c_timestamp"), new Literal<>(1L, HoodieSchema.createTimestampMillis()))).isPresent());
    // a column outside the schema, a nested path, two columns
    assertFalse(renderer.render(Predicates.eq(ref("other"), Literal.from(5))).isPresent());
    assertFalse(renderer.render(Predicates.eq(ref("c.x"), Literal.from(5))).isPresent());
    assertFalse(renderer.render(Predicates.eq(ref("c_int"), ref("c_long"))).isPresent());
    // IN beyond ExpressionPredicates' limit
    List<Expression> values = new ArrayList<>();
    for (int i = 0; i <= ParquetRenderer.IN_LIMIT; i++) {
      values.add(Literal.from(i));
    }
    assertFalse(renderer.render(Predicates.in(ref("c_int"), values)).isPresent());
  }

  /**
   * INT8 and INT16 columns are written as INT32 and render as INT columns, with INT8, INT16 or INT literals. CHAR
   * does not render: engines compare it ignoring trailing spaces, FilterApi compares the stored bytes.
   */
  @Test
  void narrowIntsRenderAsIntAndCharDoesNot() {
    HoodieSchema int8 = HoodieSchema.createInt8();
    HoodieSchema int16 = HoodieSchema.createInt16();
    HoodieSchema char3 = HoodieSchema.createChar(3);
    ParquetRenderer tiny = new ParquetRenderer(table("t", int8));
    assertEquals(FilterApi.eq(FilterApi.intColumn("t"), 5), tiny.render(Predicates.eq(ref("t"), new Literal<>(5, int8))).get());
    assertEquals(FilterApi.lt(FilterApi.intColumn("t"), 5), tiny.render(Predicates.lt(ref("t"), Literal.from(5))).get());
    assertFalse(tiny.render(Predicates.eq(ref("t"), Literal.from(5L))).isPresent());
    ParquetRenderer small = new ParquetRenderer(table("s", int16));
    assertEquals(FilterApi.gt(FilterApi.intColumn("s"), 300), small.render(Predicates.gt(ref("s"), new Literal<>(300, int16))).get());
    assertEquals(FilterApi.eq(FilterApi.intColumn("x"), 7), new ParquetRenderer().render(Predicates.eq(ref("x"), new Literal<>(7, int8))).get());
    ParquetRenderer chars = new ParquetRenderer(table("c", char3));
    assertFalse(chars.render(Predicates.eq(ref("c"), new Literal<>("ab ", char3))).isPresent());
    assertFalse(chars.render(Predicates.eq(ref("c"), Literal.from("ab "))).isPresent());
    assertFalse(chars.support(Expression.Operator.EQ, HoodieSchemaType.CHAR).isSupported());
    assertTrue(chars.support(Expression.Operator.LT, HoodieSchemaType.INT16).isSupported());
  }

  @Test
  void registryInstanceTypesColumnsByLiteral() {
    ParquetRenderer renderer = new ParquetRenderer();
    assertEquals(FilterApi.gt(FilterApi.intColumn("x"), 5), renderer.render(Predicates.lt(Literal.from(5), ref("x"))).get());
    assertEquals(FilterApi.eq(FilterApi.longColumn("t"), 7L), renderer.render(Predicates.eq(ref("t"), new Literal<>(7L, HoodieSchema.createTimestampMillis()))).get());
    // IS NULL has no literal to type the column
    assertFalse(renderer.render(Predicates.isNull(ref("x"))).isPresent());
    assertEquals(FilterApi.gt(FilterApi.intColumn("x"), 5),
        renderer.render(Predicates.and(Predicates.gt(ref("x"), Literal.from(5)), Predicates.isNull(ref("y")))).get());
  }

  private Path writeSampleFile() throws Exception {
    List<HoodieSchemaField> fields = new ArrayList<>();
    fields.add(HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT)));
    for (ExpressionSamples.Column c : ExpressionSamples.COLUMNS) {
      fields.add(HoodieSchemaField.of(c.name(), HoodieSchema.createNullable(c.type())));
    }
    Schema avro = HoodieSchema.createRecord("rows", "hoodie.test", null, fields).toAvroSchema();
    GenericData model = new GenericData();
    model.addLogicalTypeConversion(new Conversions.DecimalConversion());
    Path path = new Path(new File(tempDir, "rows.parquet").toURI());
    try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(path)
        .withSchema(avro).withDataModel(model).withConf(new Configuration(false)).build()) {
      for (int row = 0; row < ROWS; row++) {
        GenericRecord record = new GenericData.Record(avro);
        record.put("id", row);
        for (ExpressionSamples.Column c : ExpressionSamples.COLUMNS) {
          record.put(c.name(), VALUES.get(c.name()).get(row));
        }
        writer.write(record);
      }
    }
    return path;
  }

  private static Set<Integer> read(Path file, FilterPredicate predicate) throws Exception {
    Set<Integer> ids = new TreeSet<>();
    try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(file)
        .withFilter(FilterCompat.get(predicate)).withConf(new Configuration(false)).build()) {
      for (GenericRecord record = reader.read(); record != null; record = reader.read()) {
        ids.add((Integer) record.get("id"));
      }
    }
    return ids;
  }

  /**
   * The rows where the leaf (or its negation) is TRUE under SQL three-valued logic.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  private static Set<Integer> expectedRows(ExpressionSamples.Leaf leaf, boolean negated) {
    Set<Integer> rows = new TreeSet<>();
    List<Object> literals = new ArrayList<>();
    for (Expression child : leaf.expression().getChildren()) {
      if (child instanceof Literal) {
        literals.add(((Literal<?>) child).getValue());
      }
    }
    for (int row = 0; row < ROWS; row++) {
      Comparable value = (Comparable) VALUES.get(leaf.column().name()).get(row);
      Boolean result;
      switch (leaf.op()) {
        case IS_NULL:
          result = value == null;
          break;
        case IS_NOT_NULL:
          result = value != null;
          break;
        default:
          if (value == null) {
            result = null;
            break;
          }
          int cmp = value.compareTo(literals.get(0));
          switch (leaf.op()) {
            case EQ:
              result = cmp == 0;
              break;
            case LT:
              result = cmp < 0;
              break;
            case LT_EQ:
              result = cmp <= 0;
              break;
            case GT:
              result = cmp > 0;
              break;
            case GT_EQ:
              result = cmp >= 0;
              break;
            case IN:
              result = literals.stream().anyMatch(l -> value.compareTo(l) == 0);
              break;
            default:
              throw new IllegalArgumentException("Unexpected leaf " + leaf);
          }
      }
      if (result != null && result != negated) {
        rows.add(row);
      }
    }
    return rows;
  }

  private static HoodieSchema table(String column, HoodieSchema type) {
    return HoodieSchema.createRecord("t", null, null, Collections.singletonList(HoodieSchemaField.of(column, HoodieSchema.createNullable(type))));
  }

  private static NameReference ref(String name) {
    return new NameReference(name);
  }
}
